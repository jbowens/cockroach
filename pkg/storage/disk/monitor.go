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
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// DeviceID identifies an individual block device.
type DeviceID struct {
	major uint32
	minor uint32
}

// String returns the string representation of the device ID.
func (d DeviceID) String() string {
	return fmt.Sprintf("%d:%d", d.major, d.minor)
}

func compareDeviceIDs(a, b DeviceID) int {
	if v := cmp.Compare(a.major, b.major); v != 0 {
		return v
	}
	return cmp.Compare(a.minor, b.minor)
}

// MountInfo describes an individual mounted disk or partition.
type MountInfo struct {
	// DeviceID is a unique ID identifying the mounted disk. On Linux, this is
	// a dev_t (major:minor) tuple.
	DeviceID
	// RootPath is the mount point within the filesystem.
	RootPath string
	// ProcessRelativePath is the mount point relative to the process's root.
	ProcessRelativePath string
	// MountOptions is a comma-separated list of mount options.
	MountOptions string
	// Filesystem describes the type of filesystem on the mount.
	Filesystem string
	// MountSource describes filesystem specific information.
	MountSource string
	// SuperOptions describes per-super block options.
	SuperOptions string

	// source is the description of the mount point from the original source. On
	// Linux, this is the line read from /proc/self/mountinfo.
	source string
}

// String returns a human-readable string representation of the mount info.
func (m *MountInfo) String() string {
	return fmt.Sprintf("Device %s : %s on %s at %s",
		m.DeviceID, m.Filesystem, m.MountSource, m.ProcessRelativePath)
}

// Monitoring implements disk monitoring, passing out individual Monitors that
// provide observability into individual disks.
type Monitoring struct {
	fs          vfs.FS
	knownMounts []MountInfo

	mu struct {
		syncutil.Mutex
		stop  chan struct{}
		err   atomic.Value
		disks []*monitoredDisk
	}
}

func NewMonitoring(fs vfs.FS) (*Monitoring, error) {
	m := &Monitoring{fs: fs}
	// Identify all mountpoints.
	var err error
	if m.knownMounts, err = readMountpoints(fs); err != nil {
		return nil, err
	}
	return m, nil
}

// Monitor identifies the device underlying the file or directory at the
// provided path, begins periodically retrieving its disk stats, and returns a
// Monitor handle for accessing this information.
func (m *Monitoring) Monitor(ctx context.Context, path string) (*Monitor, error) {
	// Stat the provided path to determine what device it's on.
	finfo, err := m.fs.Stat(path)
	if err != nil {
		return nil, errors.Wrapf(err, "fstat(%q)", path)
	}
	dev := deviceIDFromFileInfo(finfo)

	// Since we're opening a new monitor, increase refs.
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.stop == nil {
		// We're transitioning from not monitoring any disks to monitoring a
		// disk. We're responsible for initializing a new stats collecting
		// goroutine.
		//
		// Create the stats collector synchronously before launching a goroutine
		// because it makes error propagation easier. We can return the error up
		// to the caller now.
		collector, err := newStatsCollector(m.fs)
		if err != nil {
			return nil, err
		}
		m.mu.stop = make(chan struct{})
		go m.monitorDisks(ctx, collector, m.mu.stop)
	}

	// Find the disk in the list of monitored disks, in case it's already being
	// monitored.
	var disk *monitoredDisk
	for i := 0; i < len(m.mu.disks) && disk == nil; i++ {
		if m.mu.disks[i].deviceID == dev {
			disk = m.mu.disks[i]
		}
	}

	if disk == nil {
		// There are no open monitors for this disk.
		disk = &monitoredDisk{parent: m, deviceID: dev}
		// Find the disks's mount info.
		for _, mount := range m.knownMounts {
			if dev == mount.DeviceID {
				disk.mount = mount
			}
		}
		m.mu.disks = append(m.mu.disks, disk)
		slices.SortFunc(m.mu.disks, func(a, b *monitoredDisk) int {
			return compareDeviceIDs(a.deviceID, b.deviceID)
		})
	}

	return &Monitor{monitoredDisk: disk}, nil
}

func (m *Monitoring) unrefDisk(disk *monitoredDisk) {
	m.mu.Lock()
	disk.refs -= 1
	var stop chan struct{}
	if disk.refs == 0 {
		// No one is monitoring this disk anymore. Remove it from the slice of
		// monitored disks. We make a new slice rather than mutating in place
		// because the long-running goroutine may be reading the old slice.
		i := slices.Index(m.mu.disks, disk)
		if i == -1 {
			panic(errors.AssertionFailedf("disk %s had one ref, but is not monitored", disk.deviceID))
		}
		m.mu.disks = append(slices.Clone(m.mu.disks[:i]), m.mu.disks[i+1:]...)

		// If there are no longer any disks being monitored, we can also stop
		// the long-running goroutine. We wait until after we've dropped the
		// m.mu.mutex to perform the synchronous channel send.
		if len(m.mu.disks) == 0 {
			stop = m.mu.stop
			m.mu.stop = nil
		}
	}
	m.mu.Unlock()

	if stop != nil {
		stop <- struct{}{}
	}
}

type statsCollector interface {
	collect(disks []*monitoredDisk) error
}

func (m *Monitoring) monitorDisks(
	ctx context.Context, collector statsCollector, stop chan struct{},
) {
	const monitorPollInterval = 100 * time.Millisecond
	ticker := time.NewTicker(monitorPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			// All the individual Monitor objects have been closed and there's
			// no need for the long-lived goroutine any longer. Exit.
			close(stop)
			return
		case <-ticker.C:
			// Grab the slice of disks that we care about.
			m.mu.Lock()
			disks := m.mu.disks
			m.mu.Unlock()

			err := collector.collect(disks)
			// If stats collection encounters an error preventing collection,
			// propagate it to all the disks. The next successful collection
			// will clear stats.err.
			if err != nil {
				for i := range disks {
					disks[i].stats.Lock()
					disks[i].stats.err = err
					disks[i].stats.Unlock()
				}
			}
		}
	}
}

type monitoredDisk struct {
	parent   *Monitoring
	deviceID DeviceID
	mount    MountInfo
	refs     int // protected by parent.mu.

	stats struct {
		syncutil.Mutex
		err            error
		lastMeasuredAt time.Time
		// latest holds the most recently collected set of stats. They're
		// cumulative since host start.
		latest Stats
	}
}

func (m *monitoredDisk) recordStats(t time.Time, stats Stats) {
	m.stats.Lock()
	defer m.stats.Unlock()
	m.stats.err = nil
	m.stats.latest = stats
	m.stats.lastMeasuredAt = t
}

// A Monitor provides a statistics and other information about an individual
// disk. An individual monitor is not thread-safe, although if Cloned the clone
// may be used in parallel.
type Monitor struct {
	*monitoredDisk
	lastIncremental   Stats
	lastIncrementalAt time.Time
}

// Clone returns a new Monitor that monitors the same disk.
func (m *Monitor) Clone() *Monitor {
	m.parent.mu.Lock()
	m.refs++
	m.parent.mu.Unlock()
	return &Monitor{monitoredDisk: m.monitoredDisk}
}

// Close releases resources associated with the monitor.
func (m *Monitor) Close() {
	m.parent.unrefDisk(m.monitoredDisk)
	m.monitoredDisk = nil
}

// CumulativeStats returns the most-recent cumulative statistics retrieved. The
// stats are relative to server start.
func (m *Monitor) CumulativeStats() Stats {
	m.stats.Lock()
	s := m.stats.latest
	m.stats.Unlock()
	return s
}

// IncrementalStats returns the stats since the last time IncrementalStats was
// invoked on this monitor, as well as the duration of time between the
// associated stats collections. This may be more or less than the time between
// IncrementalStats invocations because stats collection from the underlying
// data source happens at an independent cadence. When calculating rates,
// callers should use the returned duration..
//
// If this is the first time IncrementalStats has been invoked, then
// IncrementalStats returns an empty Stats struct and duration.
func (m *Monitor) IncrementalStats() (Stats, time.Duration) {
	m.stats.Lock()
	s := m.stats.latest
	t := m.stats.lastMeasuredAt
	m.stats.Unlock()
	if t.IsZero() {
		return Stats{}, 0
	}
	dur := t.Sub(m.lastIncrementalAt)
	delta := s.Delta(&s)
	m.lastIncremental = s
	m.lastIncrementalAt = t
	return delta, dur
}

// MountInfo returns information available about the mountpoint of the disk.
func (m *Monitor) MountInfo() MountInfo {
	return m.mount
}
