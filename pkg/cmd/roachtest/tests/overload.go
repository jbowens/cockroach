// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerOverload(r registry.Registry) {
	registerOverloadDiskIOPS(r)
}

func registerOverloadDiskIOPS(r registry.Registry) {
	const nodes = 3
	const numCPU = 16

	r.Add(registry.TestSpec{
		Name:    `overload/disk-iops`,
		Owner:   registry.OwnerStorage,
		Timeout: 6 * time.Hour, // TODO
		Cluster: r.MakeClusterSpec(
			nodes+1,
			spec.CPU(numCPU),
			spec.PreferLocalSSD(false),
			spec.VolumeSize(1000),
		),
		Leases: registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			t.Status("restoring fixture")
			// Increase the maximum compaction concurrency to more fully
			// utilize the disk bandwidth.
			settings := install.MakeClusterSettings()
			settings.Env = append(settings.Env, "COCKROACH_ROCKSDB_CONCURRENCY=6")

			// Run backups aggressively; incrementals every 5m and full backups
			// every 15m.
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.ScheduleBackups = true
			startOpts.RoachprodOpts.ScheduleBackupArgs = `RECURRING '*/5 * * * *' FULL BACKUP '*/15 * * * *' WITH SCHEDULE OPTIONS first_run = 'now'`

			c.Start(ctx, t.L(), startOpts, settings, c.Range(1, nodes))

			t.Status("running workload")
			m := c.NewMonitor(ctx, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {
				initCmd := fmt.Sprintf(`./cockroach workload init kv --splits=1024 --scatter {pgurl:1-%d}`,
					nodes)
				c.Run(ctx, c.Node(nodes+1), initCmd)

				// Wait for upreplication.
				conn := c.Conn(ctx, t.L(), 1)
				defer conn.Close()
				require.NoError(t, WaitFor3XReplication(ctx, t, conn))

				concurrency := ifLocal(c, "", fmt.Sprintf(" --concurrency=%d", 4*numCPU*nodes))
				duration := " --duration=4h"
				histograms := " --histograms=" + t.PerfArtifactsDir() + "/stats.json"
				blockSize := fmt.Sprintf(" --min-block-bytes=%d --max-block-bytes=%d", 2048, 2048)
				url := fmt.Sprintf(" {pgurl:1-%d}", nodes)
				cmd := "./cockroach workload run kv --tolerate-errors" +
					histograms + concurrency + duration + blockSize + url
				c.Run(ctx, c.Node(nodes+1), cmd)
				return nil
			})
			m.Wait()
		},
	})
}
