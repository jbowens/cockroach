// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugSyncTestCmd = &cobra.Command{
	Use:   "synctest <empty-dir> <nemesis-script> [generation] [expSeq]",
	Short: "Run a log-like workload that can help expose filesystem anomalies",
	Long: `
synctest is a tool to verify filesystem consistency in the presence of I/O errors.
It takes a directory (required to be initially empty and created on demand) into
which data will be written and a nemesis script which receives a single argument
that is either "on" or "off".

The nemesis script will be called with a parameter of "on" when the filesystem
underlying the given directory should be "disturbed". It is called with "off"
to restore the undisturbed state (note that "off" must be idempotent).

synctest will run run across multiple "epochs", each terminated by an I/O error
injected by the nemesis. After each epoch, the nemesis is turned off and the
written data is reopened, checked for data loss, new data is written, and
the nemesis turned back on. In the absence of unexpected error or user interrupt,
this process continues indefinitely.
`,
	Args: cobra.RangeArgs(2, 4),
	RunE: runDebugSyncTest,
}

type scriptNemesis string

func (sn scriptNemesis) exec(arg string) error {
	b, err := exec.Command(string(sn), arg).CombinedOutput()
	if err != nil {
		return errors.WithDetailf(err, "command output:\n%s", string(b))
	}
	fmt.Fprintf(stderr, "%s %s: %s", sn, arg, b)
	return nil
}

func (sn scriptNemesis) On() error {
	return sn.exec("on")
}

func (sn scriptNemesis) Off() error {
	return sn.exec("off")
}

type stdoutNemesis struct{}

func (sn stdoutNemesis) On() error {
	_, err := fmt.Println("on")
	return err
}

func (sn stdoutNemesis) Off() error {
	_, err := fmt.Println("off")
	return err
}

func runDebugSyncTest(cmd *cobra.Command, args []string) error {
	// TODO(tschottdorf): make this a flag.
	duration := 10 * time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Some Pebble errors during MANIFEST and WAL writes/syncs are considered
	// fatal and leave the *pebble.DB in an undefined state if the fatal is
	// recovered. To work around this, we open the DB and run tests from
	// within a subprocess. If a FATAL is encountered, the subprocess exits
	// and a new subprocess is created, opening the same database. If there
	// are more than two arguments in the argument list, this process is a
	// subprocess and we should run the tests against the provided generation
	// directory with the provided expected sequence number.
	if len(args) > 2 {
		generation := args[2]
		dir := filepath.Join(args[0], generation)
		expSeq, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return err
		}
		return runSyncer(ctx, dir, expSeq, stdoutNemesis{})
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, drainSignals...)

	nem := scriptNemesis(args[1])
	if err := nem.Off(); err != nil {
		return errors.Wrap(err, "unable to disable nemesis at beginning of run")
	}
	var generation int
	var lastSeq int64
	for {
		ch, err := execGeneration(ctx, generation, lastSeq, nem)
		if err != nil {
			return err
		}
		done := false
		for !done {
			select {
			case sig := <-sigCh:
				return errors.Errorf("interrupted (%v)", sig)
			case <-ctx.Done():
				// Clean shutdown.
				return nil
			case msg := <-ch:
				if msg.err != nil {
					return msg.err
				}
				switch msg.command {
				case "on":
					if err := nem.On(); err != nil {
						return err
					}
				case "off":
					if err := nem.Off(); err != nil {
						return err
					}
				case "done":
					done = true
					break
				default:
					seq, err := strconv.ParseInt(msg.command, 10, 64)
					if err != nil {
						return err
					}
					lastSeq = seq
				}
			}
		}
	}
}

type subprocessMsg struct {
	command string
	err     error
}

func execGeneration(
	ctx context.Context, generation int, lastSeq int64, nem nemesisI,
) (chan subprocessMsg, error) {
	var args []string
	args = append(args, os.Args...)
	args = append(args, strconv.Itoa(generation))
	args = append(args, strconv.FormatInt(lastSeq, 10))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	ch := make(chan subprocessMsg)
	go func() {
		// The subprocess provides nemesis instructions through stdout, which we
		// forward to nem. It also reports current committed sequence numbers.
		r := bufio.NewReader(stdout)
		for {
			s, err := r.ReadString('\n')
			if err == io.EOF {
				break
			} else if err != nil {
				ch <- subprocessMsg{err: err}
			}
			s = strings.TrimSpace(s)
			ch <- subprocessMsg{command: s}
		}
		if err := cmd.Wait(); err != nil {
			ch <- subprocessMsg{err: errors.WithDetailf(err, "standard error:\n%s", stderrBuf.String())}
		}
	}()
	return ch, nil
}

type nemesisI interface {
	On() error
	Off() error
}

type fatalLogger struct{}

func (l fatalLogger) Infof(string, ...interface{}) {}
func (l fatalLogger) Fatalf(format string, args ...interface{}) {
	// A FATAL error requires re-opening the database to recover.
	err := errors.Errorf(format, args...)
	fmt.Fprintf(stderr, "fatal error, exiting to reopen from a new process: %s\n", err)
	fmt.Println("done")
	os.Exit(0)
}

func runSyncer(ctx context.Context, dir string, expSeq int64, nemesis nemesisI) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	defer fmt.Println("done")

	db, err := OpenEngine(dir, stopper, OpenEngineOptions{
		// Intercept calls to Fatalf.
		Logger: fatalLogger{},
	})
	if err != nil {
		if expSeq == 0 {
			// Failed on first open, before we tried to corrupt anything. Hard stop.
			return err
		}
		fmt.Fprintln(stderr, "Data directory directory", dir, "corrupted:", err)
		// tktk: resolve this
		return err
	}

	buf := make([]byte, 128)
	var seq int64
	key := func() roachpb.Key {
		seq++
		return encoding.EncodeUvarintAscending(buf[:0:0], uint64(seq))
	}

	check := func(kv storage.MVCCKeyValue) (bool, error) {
		expKey := key()
		if !bytes.Equal(kv.Key.Key, expKey) {
			return false, errors.Errorf(
				"found unexpected key %q (expected %q)", kv.Key.Key, expKey,
			)
		}
		return false, nil // want more
	}

	fmt.Fprintf(stderr, "verifying existing sequence numbers...")
	if err := db.Iterate(roachpb.KeyMin, roachpb.KeyMax, check); err != nil {
		return err
	}
	// We must not lose writes, but sometimes we get extra ones (i.e. we caught an
	// error but the write actually went through).
	if expSeq != 0 && seq < expSeq {
		return errors.Errorf("highest persisted sequence number is %d, but expected at least %d", seq, expSeq)
	}
	fmt.Fprintf(stderr, "done (seq=%d).\nWriting new entries:\n", seq)

	waitFailure := time.After(time.Duration(rand.Int63n(5 * time.Second.Nanoseconds())))

	stopper.RunWorker(ctx, func(ctx context.Context) {
		<-waitFailure
		if err := nemesis.On(); err != nil {
			panic(err)
		}
		defer func() {
			if err := nemesis.Off(); err != nil {
				panic(err)
			}
		}()
		<-stopper.ShouldQuiesce()
	})

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, drainSignals...)

	write := func() (_ int64, err error) {
		defer func() {
			// Catch any RocksDB NPEs. They do occur when enough
			// faults are being injected.
			if r := recover(); r != nil {
				if err == nil {
					err = errors.New("recovered panic on write")
				}
				err = errors.Wrapf(err, "%v", r)
			}
		}()

		k, v := storage.MakeMVCCMetadataKey(key()), []byte("payload")
		switch seq % 2 {
		case 0:
			if err := db.Put(k, v); err != nil {
				seq--
				return seq, err
			}
			if err := db.Flush(); err != nil {
				seq--
				return seq, err
			}
		default:
			b := db.NewBatch()
			if err := b.Put(k, v); err != nil {
				seq--
				return seq, err
			}
			if err := b.Commit(true /* sync */); err != nil {
				seq--
				return seq, err
			}
		}
		return seq, nil
	}

	for {
		if lastSeq, err := write(); err != nil {
			// Exercise three cases:
			// 1. no more writes after first failure
			// 2. one more attempt, failure persists
			// 3. two more attempts, file system healed for last attempt
			for n := rand.Intn(3); n >= 0; n-- {
				if n == 1 {
					if err := nemesis.Off(); err != nil {
						return err
					}
				}
				fmt.Fprintf(stderr, "error after seq %d (trying %d additional writes): %v\n", lastSeq, n, err)
				lastSeq, err = write()
			}
			fmt.Fprintf(stderr, "error after seq %d: %v\n", lastSeq, err)
			// Intentionally swallow the error to get into the next epoch.
			return nil
		}

		select {
		case sig := <-ch:
			return errors.Errorf("interrupted (%v)", sig)
		case <-ctx.Done():
			return nil
		default:
			// otherwise, continue for another iteration
			// Write the sequence number to stdout so that parent process
			// knows to expect `seq` to be committed from now on.
			fmt.Printf("%d\n", seq)
		}
	}
}
