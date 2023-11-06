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
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// This file contains logic specific to Linux and parsing /proc/. We choose to
// NOT put this logic behind a build flag in order to more easily test this code
// in non-Linux environments.

// A linuxStatsCollector collects disk stats from /proc/diskstats. It keeps
// /proc/diskstats open, issuing `ReadAt` calls to re-read stats.
type linuxStatsCollector struct {
	vfs.File
	buf []byte
}

// collect collects disk stats for the identified devices.
func (s *linuxStatsCollector) collect(disks []*monitoredDisk) error {
	var n int
	var err error
	for {
		n, err = s.File.ReadAt(s.buf, 0)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		// err == nil
		//
		// NB: ReadAt is required to return a non-nil error when it returns n <
		// len(s.buf). A nil error indicates a full len(s.buf) bytes were read,
		// and the diskstats file does not fit in our current buffer.
		//
		// We want to grow the buffer to be large enough to fit the entirety of
		// the file. This is required for consistency. We're only guaranteed a
		// consistent read if we read the entirety of the diskstats file in a
		// single read. Reallocate (doubling) the buffer and continue.
		s.buf = make([]byte, len(s.buf)*2)
	}
	return parseDiskStats(s.buf[:n], disks, timeutil.Now())
}

// Close closes the stats collector releasing associated resources.
func (s *linuxStatsCollector) Close() error {
	return s.File.Close()
}

// readMountpoints reads the mounts from the /proc/ filesytem. The
// /proc/self/mountinfo file lists all the mounts visible to the current
// process.
//
// For the posterity, the documentation of this file is provided inline within
// this comment and was retrieved from:
// https://www.kernel.org/doc/Documentation/filesystems/proc.txt
//
//	3.5	/proc/<pid>/mountinfo - Information about mounts
//	--------------------------------------------------------
//
//	This file contains lines of the form:
//
//	36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
//	(1)(2)(3)   (4)   (5)      (6)      (7)   (8) (9)   (10)         (11)
//
//	(1) mount ID:  unique identifier of the mount (may be reused after umount)
//	(2) parent ID:  ID of parent (or of self for the top of the mount tree)
//	(3) major:minor:  value of st_dev for files on filesystem
//	(4) root:  root of the mount within the filesystem
//	(5) mount point:  mount point relative to the process's root
//	(6) mount options:  per mount options
//	(7) optional fields:  zero or more fields of the form "tag[:value]"
//	(8) separator:  marks the end of the optional fields
//	(9) filesystem type:  name of filesystem of the form "type[.subtype]"
//	(10) mount source:  filesystem specific information or "none"
//	(11) super options:  per super block options
//
//	Parsers should ignore all unrecognised optional fields.  Currently the
//	possible optional fields are:
//
//	shared:X  mount is shared in peer group X
//	master:X  mount is slave to peer group X
//	propagate_from:X  mount is slave and receives propagation from peer group X (*)
//	unbindable  mount is unbindable
func readMountpoints(fs vfs.FS) (mounts []MountInfo, err error) {
	f, err := fs.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, errors.Wrap(err, "opening /proc/self/mountinfo")
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, errors.Wrap(err, "reading /proc/self/mountinfo")
	}
	return parseMountpoints(string(b))
}

func parseMountpoints(contents string) (mounts []MountInfo, err error) {
	lines := strings.Split(strings.TrimSpace(contents), "\n")
	mounts = make([]MountInfo, len(lines))
	for i, line := range lines {
		if mounts[i], err = parseMountpoint(line); err != nil {
			return nil, errors.Wrapf(err, "/proc/self/mountinfo:%d:", i)
		}
	}
	return mounts, nil
}

func parseMountpoint(line string) (m MountInfo, err error) {
	fields := strings.Fields(line)

	// Require at least 10 fields (all non-optional fields plus the separator
	// field).
	if len(fields) < 10 {
		return m, errors.Newf("line has %d fields", len(fields))
	}
	// Find the end of the optional fields. We don't care about the optional
	// fields, so we just look for the separator that marks the end of the
	// optional fields. The earliest the separator can appear is index 6 if
	// there are zero optional fields.
	sepIdx := 6
	for len(fields) > sepIdx && fields[sepIdx] != "-" {
		sepIdx++
	}
	if sepIdx >= len(fields) {
		return MountInfo{}, errors.New("no separator found")
	}
	// There should be exactly three fields after the separator. We allow there
	// to be additional fields in case future Linux versions append additional
	// fields.
	if remaining := len(fields) - sepIdx - 1; remaining < 3 {
		return MountInfo{}, errors.Newf("expected 3 fields after a optional separator; found %d", len(fields))
	}

	// Decode the device ID from fields[2. It should be in "major:minor" form.
	devID, err := func() (dev DeviceID, err error) {
		i := strings.IndexRune(fields[2], ':')
		if i == -1 {
			return dev, errors.Newf("device field %q has no colon", fields[2])
		}
		if major, err := strconv.ParseUint(fields[2][:i], 10, 32); err != nil {
			return dev, errors.Wrapf(err, "decoding deviceID major from field %q", fields[2])
		} else {
			dev.major = uint32(major)
		}
		if minor, err := strconv.ParseUint(fields[2][i+1:], 10, 32); err != nil {
			return dev, errors.Wrapf(err, "decoding deviceID minor from field %q", fields[2])
		} else {
			dev.minor = uint32(minor)
		}
		return dev, nil
	}()
	if err != nil {
		return m, err
	}
	return MountInfo{
		DeviceID:            devID,
		RootPath:            fields[3],
		ProcessRelativePath: fields[4],
		MountOptions:        fields[5],
		Filesystem:          fields[sepIdx+1],
		MountSource:         fields[sepIdx+2],
		SuperOptions:        fields[sepIdx+3],
	}, nil
}

// parseDiskStats parses disk stats from the provided byte slice of data read
// from /proc/diskstats. It takes a slice of *monitoredDisks sorted by device
// ID. Any devices not listed are ignored during parsing.
//
// The monitored disks for which stats are found are updated using setStats.
func parseDiskStats(contents []byte, disks []*monitoredDisk, measuredAt time.Time) error {
	for lineNum := 0; len(contents) > 0; lineNum++ {
		lineBytes, rest := splitLine(contents)
		line := unsafe.String(&lineBytes[0], len(lineBytes))
		contents = rest

		line = strings.TrimSpace(line)

		var deviceID DeviceID
		if devMajor, rest, err := mustParseUint(line, 32, "deviceID.major"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		} else {
			line = rest
			deviceID.major = uint32(devMajor)
		}
		if devMinor, rest, err := mustParseUint(line, 32, "deviceID.minor"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		} else {
			line = rest
			deviceID.minor = uint32(devMinor)
		}
		diskIdx, ok := slices.BinarySearchFunc(disks, deviceID, func(a *monitoredDisk, b DeviceID) int {
			return compareDeviceIDs(a.deviceID, b)
		})
		if !ok {
			// This device doesn't exist in the list of devices being monitored,
			// so skip it.
			continue
		}

		// Skip the device name.
		_, line = splitFieldDelim(line)

		var stats Stats
		var err error
		if stats.ReadsCount, line, err = mustParseUint(line, 64, "reads count"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if stats.ReadsMerged, line, err = mustParseUint(line, 64, "reads merged"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if stats.ReadsSectors, line, err = mustParseUint(line, 64, "reads sectors"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if millis, rest, err := mustParseUint(line, 64, "reads duration"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		} else {
			line = rest
			stats.ReadsDuration = time.Duration(millis) * time.Millisecond
		}
		if stats.WritesCount, line, err = mustParseUint(line, 64, "writes count"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if stats.WritesMerged, line, err = mustParseUint(line, 64, "writes merged"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if stats.WritesSectors, line, err = mustParseUint(line, 64, "writes sectors"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if millis, rest, err := mustParseUint(line, 64, "writes duration"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		} else {
			line = rest
			stats.WritesDuration = time.Duration(millis) * time.Millisecond
		}
		if stats.InProgressCount, line, err = mustParseUint(line, 64, "inprogress iops"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if millis, rest, err := mustParseUint(line, 64, "time doing IO"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		} else {
			line = rest
			stats.CumulativeDuration = time.Duration(millis) * time.Millisecond
		}
		if millis, rest, err := mustParseUint(line, 64, "weighted IO duration"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		} else {
			line = rest
			stats.WeightedIODuration = time.Duration(millis) * time.Millisecond
		}

		// The below fields are optional.
		if stats.DiscardsCount, ok, line, err = tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if stats.DiscardsMerged, ok, line, err = tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if stats.DiscardsSectors, ok, line, err = tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if millis, ok, rest, err := tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		} else if ok {
			line = rest
			stats.DiscardsDuration = time.Duration(millis) * time.Millisecond
		}
		if stats.FlushesCount, ok, line, err = tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		}
		if millis, ok, rest, err := tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %s", lineNum, err)
		} else if ok {
			line = rest
			stats.FlushesDuration = time.Duration(millis) * time.Millisecond
		}
		disks[diskIdx].recordStats(measuredAt, stats)
	}
	return nil
}

func splitLine(b []byte) (line, rest []byte) {
	i := bytes.IndexByte(b, '\n')
	if i >= 0 {
		return b[:i], b[i+1:]
	}
	return b, nil
}

// splitFieldDelim accepts a string beginning with a non-whitespace character.
// It returns the prefix of the string of non-whitespace characters (`field`),
// and a slice that holds the beginning of the following string of
// non-whitespace characters.
func splitFieldDelim(s string) (field, next string) {
	var ok bool
	for i, r := range s {
		// Whitespace delimits fields.
		if unicode.IsSpace(r) {
			// If !ok, this is the first whitespace we've seen. Set field. We
			// don't stop iteraitng because we still need to find the start of
			// `next`.
			if !ok {
				ok = true
				field = s[:i]
			}
		} else if ok {
			// This is the first non-whitespace character after the delimiter.
			// We know this is where the following field begins and can return
			// now.
			next = s[i:]
			return field, next
		}
	}
	// We never found a delimiter, or the delimiter never ended.
	return field, next
}

func mustParseUint(s string, bitSize int, fieldName string) (v int, next string, err error) {
	var exists bool
	v, exists, next, err = tryParseUint(s, bitSize)
	if err != nil {
		return v, next, err
	} else if !exists {
		return 0, next, errors.Newf("%s field not present", errors.Safe(fieldName))
	}
	return v, next, nil
}

func tryParseUint(s string, bitSize int) (v int, ok bool, next string, err error) {
	var field string
	field, next = splitFieldDelim(s)
	if len(field) == 0 {
		return v, false, next, nil
	}
	if v, err := strconv.ParseUint(field, 10, bitSize); err != nil {
		return 0, true, next, err
	} else {
		return int(v), true, next, nil
	}
}
