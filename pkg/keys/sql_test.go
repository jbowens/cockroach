// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

import (
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestRewriteKeyToTenantPrefix(t *testing.T) {
	for _, tc := range []struct {
		oldTenant, newTenant uint64
		suffix               string
	}{
		{1, 1, "abc"},
		{1, 2, "acc"},
		{2, 1, "abc"},
		{1, 1 << 15, "abc"},
		{1 << 15, 1, "abc"},
		{2, 1 << 15, "abc"},
		{1 << 15, 2, "abc"},
		{1 << 15, 1 << 15, "abc"},
		{1 << 13, 1 << 15, "abc"},
		{1 << 15, 1 << 13, "abc"},
	} {
		t.Run(fmt.Sprintf("%d to %d %s", tc.oldTenant, tc.newTenant, tc.suffix), func(t *testing.T) {
			old := append(MakeSQLCodec(roachpb.MustMakeTenantID(tc.oldTenant)).TablePrefix(5), tc.suffix...)
			new := MakeTenantPrefix(roachpb.MustMakeTenantID(tc.newTenant))
			got, err := RewriteKeyToTenantPrefix(old, new)
			require.NoError(t, err)

			expect := append(MakeSQLCodec(roachpb.MustMakeTenantID(tc.newTenant)).TablePrefix(5), tc.suffix...)
			require.Equal(t, expect, got)
		})
	}
}

func TestGetTableDataMinMax(t *testing.T) {
	t.Logf("TableDataMin %x", TableDataMin)
	t.Logf("TableDataMax %x", TableDataMax)
	t.Logf("ScratchRangeMin %x", ScratchRangeMin)
	t.Logf("ScratchRangeMax %x", ScratchRangeMax)
	t.Logf("SystemConfigTableDataMax %x", SystemConfigTableDataMax)
	t.Logf("NamespaceTableMin %x", NamespaceTableMin)
	t.Logf("NamespaceTableMax %x", NamespaceTableMax)
	t.Logf("TenantPrefix %x", TenantPrefix)
	t.Logf("TenantTableDataMin %x", TenantTableDataMin)
	t.Logf("TenantTableDataMax %x", TenantTableDataMax)
}

func TestGetRootPrefixLen(t *testing.T) {
	for _, tc := range []struct {
		key  roachpb.Key
		want int
	}{
		{StoreIdentKey(), len(LocalStorePrefix)},
		{roachpb.Key(LocalRangeIDPrefix), len(LocalRangeIDPrefix)},
		{LocalStoreUnsafeReplicaRecoveryKeyMin, len(LocalStorePrefix)},
		{LockTableSingleKeyStart, len(LocalRangeLockTablePrefix)},
		{SystemSQLCodec.IndexPrefix(5, 2), len(SystemSQLCodec.IndexPrefix(5, 2))},
		{
			append(SystemSQLCodec.IndexPrefix(5, 2), "foobar"...),
			len(SystemSQLCodec.IndexPrefix(5, 2)),
		},
		{
			MakeSQLCodec(roachpb.MinTenantID).IndexPrefix(2, 1),
			len(MakeSQLCodec(roachpb.MinTenantID).IndexPrefix(2, 1)),
		},
		{
			MakeSQLCodec(roachpb.MaxTenantID).IndexPrefix(2, 1),
			len(MakeSQLCodec(roachpb.MaxTenantID).IndexPrefix(2, 1)),
		},
		{
			MakeSQLCodec(roachpb.TenantID{53}).IndexPrefix(2, 1),
			len(MakeSQLCodec(roachpb.TenantID{53}).IndexPrefix(2, 1)),
		},
		{
			append(MakeSQLCodec(roachpb.TenantID{53}).IndexPrefix(2, 1), "baxbazboo"...),
			len(MakeSQLCodec(roachpb.TenantID{53}).IndexPrefix(2, 1)),
		},
		{
			append(MakeSQLCodec(roachpb.TenantID{53}).IndexPrefix(2, 1), "baxbazboo"...),
			len(MakeSQLCodec(roachpb.TenantID{53}).IndexPrefix(2, 1)),
		},
		{
			append(SystemSQLCodec.IndexPrefix(math.MaxUint32, 2), "foo"...),
			len((SystemSQLCodec.IndexPrefix(math.MaxUint32, 2))),
		},
	} {
		t.Run(fmt.Sprint(tc.key), func(t *testing.T) {
			t.Logf("%x", tc.key)
			got := GetRootPrefixLength(tc.key)
			require.Equal(t, tc.want, got)
		})
	}
}
