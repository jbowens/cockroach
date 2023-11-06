// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package disk

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestLinux_ParseMountpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/linux_mountpoints", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "parse":
			buf.Reset()
			mounts, err := parseMountpoints(td.Input)
			if err != nil {
				return err.Error()
			}
			for i := range mounts {
				if i > 0 {
					fmt.Fprintln(&buf)
				}
				fmt.Fprint(&buf, mounts[i].String())
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
	time.Sleep(time.Second)
}

func TestLinux_CollectDiskStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/linux_diskstats", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "parse":
			var disks []*monitoredDisk
			for _, cmdArg := range td.CmdArgs {
				var deviceID DeviceID
				v, err := strconv.ParseUint(cmdArg.Vals[0], 10, 32)
				require.NoError(t, err)
				deviceID.major = uint32(v)
				v, err = strconv.ParseUint(cmdArg.Vals[1], 10, 32)
				require.NoError(t, err)
				deviceID.minor = uint32(v)
				disks = append(disks, &monitoredDisk{deviceID: deviceID})
			}
			slices.SortFunc(disks, func(a, b *monitoredDisk) int { return compareDeviceIDs(a.deviceID, b.deviceID) })

			buf.Reset()
			s := &linuxStatsCollector{
				File: vfs.NewMemFile([]byte(td.Input)),
				// Use a small initial buffer size to exercise the buffer
				// resizing logic.
				buf: make([]byte, 16),
			}
			err := s.collect(disks)
			if err != nil {
				return err.Error()
			}
			for i := range disks {
				if i > 0 {
					fmt.Fprintln(&buf)
				}
				fmt.Fprintf(&buf, "%s: ", disks[i].deviceID)
				fmt.Fprint(&buf, disks[i].stats.latest.String())
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
	time.Sleep(time.Second)
}
