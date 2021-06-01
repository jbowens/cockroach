// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package server

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

const ballastFilename = `AUTO_BALLAST`

var automaticBallastSize = settings.RegisterByteSizeSetting(
	"storage.automatic_ballast.size",
	"size to reserve for recovering from disk space exhaustion",
	0,
)

// OutOfDiskMonitor watches for out-of-disk conditions, enabling out-of-disk
// mode as necessary.
type OutOfDiskMonitor struct {
	localStores        *kvserver.Stores
	settings           *cluster.Settings
	stopper            *stop.Stopper
	ballastSizeChanged chan struct{}

	atomic struct {
		ood uint32
	}

	mu struct {
		sync.Mutex
		ood map[roachpb.StoreID]bool
	}
}

func newOutOfDiskMonitor(
	stores *kvserver.Stores, settings *cluster.Settings, stopper *stop.Stopper,
) *OutOfDiskMonitor {
	m := &OutOfDiskMonitor{
		localStores:        stores,
		settings:           settings,
		stopper:            stopper,
		ballastSizeChanged: make(chan struct{}, 1),
	}
	m.mu.ood = map[roachpb.StoreID]bool{}
	return m
}

func (m *OutOfDiskMonitor) IsOODModeEnabled() bool {
	return atomic.LoadUint32(&m.atomic.ood) == 1
}

func (m *OutOfDiskMonitor) start(ctx context.Context) error {
	automaticBallastSize.SetOnChange(&m.settings.SV, func(ctx context.Context) {
		// TODO(jackson): Fix to ensure we never block here.
		m.ballastSizeChanged <- struct{}{}
	})

	err := m.localStores.VisitStores(func(s *kvserver.Store) error {
		eng := s.Engine()
		storeID := s.StoreID()

		// Add an out-of-disk hook to the Store's engine so that we're alerted
		// when the OS surfaces an ENOSPC error.
		eng.OnOutOfDisk(func(innerFS vfs.FS) {
			m.handleENOSPC(storeID, innerFS)
		})
		return nil
	})
	if err != nil {
		return err
	}
	ballastSize := automaticBallastSize.Get(&m.settings.SV)
	if err := m.resizeBallasts(ctx, ballastSize); err != nil {
		return err
	}
	return m.stopper.RunAsyncTask(ctx, "ood-monitor", m.monitor)
}

func (m *OutOfDiskMonitor) monitor(ctx context.Context) {
	for {
		select {
		case <-m.ballastSizeChanged:
			ballastSize := automaticBallastSize.Get(&m.settings.SV)
			log.Infof(ctx, "automatic ballast cluster setting set to %d", ballastSize)
			if err := m.resizeBallasts(ctx, ballastSize); err != nil {
				log.Fatalf(ctx, "unexpected error: %s", err)
			}
		case <-m.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}
	}
}

func removeBallast(eng storage.Engine, innerFS vfs.FS) error {
	auxDir := eng.GetAuxiliaryDir()
	ballastPath := filepath.Join(auxDir, ballastFilename)
	if err := innerFS.RemoveAll(ballastPath); err != nil {
		return errors.Wrap(err, "removing ballast")
	}
	dir, err := innerFS.OpenDir(auxDir)
	if err != nil {
		return errors.Wrap(err, "opening auxiliary dir")
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return errors.Wrap(err, "syncing auxiliary dir")
	}
	return nil
}

func (m *OutOfDiskMonitor) resizeBallasts(ctx context.Context, ballastSize int64) error {
	// NB: The visitor func is called without the Stores' internal
	// lock, so it's okay to do I/O from within the func.
	err := m.localStores.VisitStores(func(s *kvserver.Store) error {
		eng := s.Engine()
		storeID := s.StoreID()
		err := m.establishBallast(eng, ballastSize)
		if err == nil {
			log.Infof(ctx, "updated ballast to %d bytes on store %d", ballastSize, storeID)
			m.mu.Lock()
			m.mu.ood[storeID] = false

			var anyStoresOutOfDisk uint32
			for _, v := range m.mu.ood {
				if v {
					anyStoresOutOfDisk = 1
				}
			}
			atomic.StoreUint32(&m.atomic.ood, anyStoresOutOfDisk)
			m.mu.Unlock()
		} else {
			// TODO(jackson): tighten up; mark as ood if ENOSPC.
			log.Errorf(ctx, "couldn't establish %d-byte ballast for store %d: %s",
				ballastSize, storeID, err)
		}
		return nil
	})
	return err
}

func (m *OutOfDiskMonitor) establishBallast(eng storage.Engine, size int64) error {
	// TODO(jackson): Remove `filepath` and `sysutil.CreateLargeFile` direct
	// OS interactions.
	ballastPath := filepath.Join(eng.GetAuxiliaryDir(), ballastFilename)
	err := eng.RemoveAll(ballastPath)
	if err != nil {
		return err
	}
	return sysutil.CreateLargeFile(ballastPath, size)
}

func (m *OutOfDiskMonitor) handleENOSPC(storeID roachpb.StoreID, innerFS vfs.FS) {
	ctx := context.TODO()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.ood[storeID] = true
	atomic.StoreUint32(&m.atomic.ood, 1)

	log.Warningf(ctx, "store %d is out of disk space, removing ballast.", storeID)

	s, err := m.localStores.GetStore(storeID)
	if err != nil {
		log.Fatalf(ctx, "unable to retrieve store %s: %s", storeID, err)
	}
	if err := removeBallast(s.Engine(), innerFS); err != nil {
		// TODO
		log.Errorf(ctx, "removing store %s ballast: %s", storeID, err)
	}
	log.Warningf(ctx, "finished removing ballast for store %s", storeID)
}
