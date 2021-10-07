// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// MVCCRangeTombstone describes a range tombstone marking a range of
// keyspace deleted. These tombstones are stored in the range local
// operations keyspace.
type MVCCRangeTombstone struct {
	Start     roachpb.Key
	End       roachpb.Key
	Timestamp hlc.Timestamp
}

// Key returns the MVCC key under which this tombstone is stored.
func (t MVCCRangeTombstone) Key() MVCCKey {
	return MVCCKey{
		Key:       t.Start,
		Timestamp: t.Timestamp,
	}
}

// value returns the value of the tombstone.
func (t MVCCRangeTombstone) value() ([]byte, error) {
	return protoutil.Marshal(&roachpb.MVCCRangeTombstoneValue{
		EndKey: t.End,
	})
}

// MVCCRangeTombstones
type MVCCRangeTombstones struct {
	iter *pebbleIterator
}
