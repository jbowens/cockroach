// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build linux
// +build linux

package disk

import (
	"io/fs"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

func newStatsCollector(fs vfs.FS) (*linuxStatsCollector, error) {
	file, err := fs.Open("/proc/diskstats")
	if err != nil {
		return nil, errors.Wrap(err, "opening /proc/diskstats")
	}
	return &linuxStatsCollector{
		File: file,
		buf:  make([]byte, 64),
	}, nil
}

func deviceIDFromFileInfo(fileInfo fs.FileInfo) DeviceID {
	statInfo := finfo.Sys().(*unix.Stat_t)
	return DeviceID{
		major: unix.Major(statInfo.Dev),
		minor: unix.Minor(statInfo.Dev),
	}
}
