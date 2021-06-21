// Copyright 2021 The Cockroach Authors.
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
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/elastic/gosigar"
)

// maybeWaitForDiskFullRecovery is called early on node start to look for
// stores that may have exhausted disk space, making node start unsafe.
//
// When CockroachDB encounters an ENOSPC error, it deletes a marker file.
// The absence of the file marker indicates that the store is out-of-disk, and
// starting the node is dangerous because continuing to consume disk space may
// make recovery impossible. This function looks for the marker file in all
// stores.
//
// For each store spec:
// * If the marker file is present, nothing is done.
// * If the marker file is missing, and the filesystem of the storespec's path
//   has available disk greater than both 5% total disk space and twice the
//   store's ballast size, the function creates the marker file and ballast
//   and continues to the next store.
// * If there is insufficient disk space to recover automatically, this
//   function blocks indefinitely in disk-full recovery mode. While blocking,
//   it periodically stats the marker file and queries the filesystem's disk
//   space usage. If the operator creates the marker file, the function
//   continues to the next store. If sufficient disk space becomes available,
//   it automatically creates the marker file and ballast and continues to the
//   next store.
//
// When all stores have marker files, the function returns. All stores are not
// guaranteed to have ballasts if the operator manually created a marker file.
// In that case, the onus of recreating the ballast when safe passes to the
// storage.Engine.
func maybeWaitForDiskFullRecovery(ctx context.Context, storeSpecs base.StoreSpecList) error {
	for _, spec := range storeSpecs.Specs {
		if spec.InMemory {
			continue
		}
		markerFilePath := spec.DiskFullRecoveryFile()
		_, err := os.Stat(markerFilePath)
		if err == nil {
			continue
		}
		if !oserror.IsNotExist(err) {
			return err
		}
		log.Ops.Infof(ctx, "missing disk full marker file: %s", markerFilePath)

		// This store is missing the marker file. Either this is the first
		// time this node has started on 21.2+, or the previous process exited
		// because this store ran out of disk space.

		// If there's sufficient disk space available, we can re-create the
		// marker file and emergency ballast and continue.
		ok, err := attemptDiskFullRecover(ctx, markerFilePath, spec)
		if err != nil {
			return err
		}
		if ok {
			continue
		}

		// There's insufficient disk space. This node probably rebooted after
		// the ENOSPC-handling logic removed the marker file. We want to wait
		// until the operator resizes the store's underlying disk or recreates
		// the marker file indicating they're attempting to manually recover
		// using whatever disk space was freed by the removal of the ballast.
		if err := waitForDiskFullRecovery(ctx, markerFilePath, spec); err != nil {
			return err
		}
	}
	return nil
}

func waitForDiskFullRecovery(ctx context.Context, markerPath string, spec base.StoreSpec) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Warningf(ctx, "marker file %s missing, entering disk full mode", markerPath)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Check for the existence of the marker file. The operator
			// may have manually created it indicating that they want to
			// start the node up and recover manually.
			_, err := os.Stat(markerPath)
			if err != nil && !oserror.IsNotExist(err) {
				return err
			}
			if err == nil {
				_, availBytes, err := getStorageCapacity(spec.Path)
				if err != nil {
					return err
				}
				log.Ops.Warningf(ctx, "detected marker file %s, proceeding with node start with %s available capacity",
					markerPath, humanizeutil.IBytes(int64(availBytes)))
				return nil
			}

			// Check if there's sufficient available disk space to recover
			// independently. This can happen if virtualized disks are resized
			// or non-CockroachDB data was deleted from the disk.
			ok, err := attemptDiskFullRecover(ctx, markerPath, spec)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		}
	}
	panic("unreachable")
}

func attemptDiskFullRecover(
	ctx context.Context, markerPath string, spec base.StoreSpec,
) (bool, error) {
	// Remove the ballast—if it exists—because the rest of the function
	// assumes the ballast doesn't exist and will recreate it if necessary.
	// There shouldn't be a ballast, but there could be if a previous attempt
	// to recover automatically successfully created the ballast but somehow
	// failed to create the marker file.
	ballastPath := spec.DiskFullBallastFile()
	if err := os.Remove(ballastPath); err != nil && !oserror.IsNotExist(err) {
		return false, errors.Wrapf(err, "%s", ballastPath)
	}

	totalBytes, availBytes, err := getStorageCapacity(spec.Path)
	if err != nil {
		return false, err
	}
	ballastSizeBytes := uint64(spec.BallastSize.InBytes)
	if spec.BallastSize.Percent > 0 {
		ballastSizeBytes = uint64(float64(totalBytes) * spec.BallastSize.Percent / 100)
	}

	// Require at least 5% available disk space.
	if float64(availBytes) < float64(totalBytes)*0.05 {
		return false, nil
	}
	// Also require at least double the ballast size to automatically recover.
	if availBytes < ballastSizeBytes*2 {
		return false, nil
	}

	// Disk space metrics indicate there is sufficient disk space to recover
	// automatically. Try re-creating both the ballast and the marker file.
	log.Ops.Infof(ctx, "store %s has %s of disk space available, attempting to create ballast and marker file.",
		spec.Path, humanizeutil.IBytes(int64(availBytes)))

	if err := os.MkdirAll(filepath.Dir(markerPath), os.ModePerm); err != nil {
		return false, err
	}
	if err := sysutil.CreateLargeFile(ballastPath, int64(ballastSizeBytes)); err != nil {
		return false, errors.Wrapf(err, "creating %d-byte ballast %s", ballastSizeBytes, ballastPath)
	}
	f, err := os.Create(markerPath)
	if err != nil {
		return false, errors.Wrapf(err, "%s", markerPath)
	}
	if err := f.Close(); err != nil {
		return false, errors.Wrapf(err, "%s", markerPath)
	}
	log.Ops.Infof(ctx, "created out-of-disk handling marker file %s", markerPath)
	return true, nil
}

func getStorageCapacity(path string) (total, avail uint64, err error) {
	var usage gosigar.FileSystemUsage

	// Eval path if it is a symbolic link.
	dir, err := filepath.EvalSymlinks(path)
	if err != nil {
		return total, avail, err
	}
	if err := usage.Get(dir); err != nil {
		return total, avail, err
	}
	return usage.Total, usage.Avail, nil
}
