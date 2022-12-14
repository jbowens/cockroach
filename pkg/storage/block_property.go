// Copyright 2022 The Cockroach Authors.
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
	"bytes"
	"math"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

const maxSQLPrefixPropertyLength = 60
const sqlPrefixBlockPropertyName = "crdb.sqlprefix"

type sqlPrefixCollector struct {
	current               []byte
	dataPrefixes          []byte
	tablePrefixes         []byte
	omitDataProperty      bool
	omitTableProperty     bool
	dataBlockContinuation bool
}

// Assert that sqlPrefixCollector implements Pebble's BlockPropertyCollector
// interface.
var _ pebble.BlockPropertyCollector = (*sqlPrefixCollector)(nil)

var sqlPrefixCollectorPool = sync.Pool{
	New: func() any { return new(sqlPrefixCollector) },
}

func newSQLPrefixCollector() pebble.BlockPropertyCollector {
	return sqlPrefixCollectorPool.Get().(*sqlPrefixCollector)
}

// Name returns the name of the block property collector.
func (c *sqlPrefixCollector) Name() string {
	return sqlPrefixBlockPropertyName
}

// TODO(jackson): It'd be nice if the block-property collector could receive
// hints, like the a hint of the keyspan within which keys might be added. If we
// knew that all the table would have the same SQL prefix, we could no-op the
// entirety of this collector.

// TODO(jackson): We could use a shared-prefix encoding to reduce the size of
// properties. It's unclear if it's worth it, given the higher decoding cost.

// tktk; range keys

// Add is called with each new entry added to a data block in the sstable.
// The callee can assume that these are in sorted order.
func (c *sqlPrefixCollector) Add(key pebble.InternalKey, value []byte) error {
	if len(c.current) > 0 && bytes.HasPrefix(key.UserKey, c.current) {
		// Same prefix.
		c.dataBlockContinuation = true
		return nil
	}

	// A new SQL prefix.
	l := keys.GetSQLPrefixLength(key.UserKey)
	if l > math.MaxUint8 {
		// This SQL prefix length has a length beyond what will fit within a
		// byte. This should never happen in practice; the Cockroach SQL
		// prefix is at worst 20 bytes:
		//   (1 byte ) the TenantPrefix byte `\xe`
		//   (9 bytes) uvarint encoded uint64 tenant ID
		//   (5 bytes) uvarint encoded uint32 table ID
		//   (5 bytes) uvarint encoded uint32 index ID
		// For robustness, we handle this case by refusing to encode a property
		// for the data block or the sstable. Omitting the properties is always
		// safe. The corresponding filter will interpret the absence of a
		// property as an indication that the sstable/block may contain any SQL
		// prefix.
		c.omitDataProperty = true
		c.omitTableProperty = true
		return nil
	}

	if !c.dataBlockContinuation {
		// This is the first SQL prefix that we've observed within this data
		// block. No need to encode anything yet. If it ends up being the only
		// SQL prefix in the block, we'll never encode anything.
		if !c.omitTableProperty && len(c.current) > 0 {
			c.tablePrefixes = append(append(c.tablePrefixes, byte(len(c.current))), c.current...)
		}

		// We do need to copy the SQL prefix so we can compare it to future
		// prefixes.
		c.current = append(c.current[:0], key.UserKey[:l]...)
		c.dataBlockContinuation = true
		return nil
	}

	// This is not the first SQL prefix within the block.
	if len(c.current)+1+len(c.dataPrefixes)+1+l > maxSQLPrefixPropertyLength {
		// The final data block's property will exceed the maximum length. Don't
		// bother saving the previous SQL prefix, and mark that we won't be
		// encoding a property. If the data block's encoded properties exceed
		// the max length, the sstable's will too.
		c.omitDataProperty = true
		c.omitTableProperty = true

		// Fallthrough to saving c.current. We need to save it so that we can
		// still generate properties for future data blocks.
	} else if !c.omitDataProperty && len(c.current) > 0 {
		// Accumulate the previous SQL prefix (c.current) to the data block's
		// list of prefixes.
		c.dataPrefixes = append(append(c.dataPrefixes, byte(len(c.current))), c.current...)
	}

	// Save the new SQL prefix.
	c.current = append(c.current[:0], key.UserKey[:l]...)
	return nil
}

// FinishDataBlock is called when all the entries have been added to a
// data block. Subsequent Add calls will be for the next data block. It
// returns the property value for the finished block.
func (c *sqlPrefixCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	if !c.omitTableProperty {
		if len(c.tablePrefixes)+len(c.dataPrefixes)+1+len(c.current) > maxSQLPrefixPropertyLength {
			// The encoded prefixes at the sstable-level are too long. We won't
			// encode a table-level property for this table.
			c.omitTableProperty = true
		} else {
			// NB: c.tablePrefixes never includes c.current to avoid including
			// it twice when the prefix spans two blocks.
			c.tablePrefixes = append(c.tablePrefixes, c.dataPrefixes...)
		}
	}
	if !c.omitDataProperty {
		// Only output a property for the block if we'd already accumulated at
		// least 1 prefix in data prefixes. This indicates that there was a
		// change in SQL prefix while handling this block's keys, and a property
		// has the potential to be worthwhile.
		if len(c.dataPrefixes) > 0 {
			// Write all of the accumulated prefixes, followed by the current
			// prefix prefixed with its length.
			buf = append(append(append(buf, c.dataPrefixes...), byte(len(c.current))), c.current...)
		}
	}
	c.omitDataProperty = false
	c.dataBlockContinuation = false
	c.dataPrefixes = c.dataPrefixes[:0]
	return buf, nil
}

// AddPrevDataBlockToIndexBlock adds the entry corresponding to the
// previous FinishDataBlock to the current index block.
func (c *sqlPrefixCollector) AddPrevDataBlockToIndexBlock() {
	// Not encoded for index blocks.
}

// FinishIndexBlock is called when an index block, containing all the
// key-value pairs since the last FinishIndexBlock, will no longer see new
// entries. It returns the property value for the index block.
func (c *sqlPrefixCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	// Not encoded for index blocks.
	return nil, nil
}

// FinishTable is called when the sstable is finished, and returns the
// property value for the sstable.
func (c *sqlPrefixCollector) FinishTable(buf []byte) ([]byte, error) {
	if !c.omitTableProperty {
		// Only output a property for the table if we'd already accumulated at
		// least 1 prefix in table prefixes. This indicates that there was a
		// change in SQL prefix while handling this table's keys, and a property
		// has the potential to be worthwhile.
		//
		// NB: the length of the resulting property has already been checked;
		// omitTableProperty would be set to true if this property exceeds
		// maxSQLPrefixPropertyLength.
		if len(c.tablePrefixes) > 0 {
			buf = append(buf, c.tablePrefixes...)
			if len(c.current) > 0 {
				buf = append(buf, byte(len(c.current)))
				buf = append(buf, c.current...)
			}
		}
	}

	*c = sqlPrefixCollector{
		current:       c.current[:0],
		dataPrefixes:  c.dataPrefixes[:0],
		tablePrefixes: c.tablePrefixes[:0],
	}
	sqlPrefixCollectorPool.Put(c)
	return buf, nil
}

type sqlPrefixFilter struct {
	SQLPrefix []byte
}

// Assert that sqlPrefixFilter implements Pebble's BlockPropertyFilter
// interface.
var _ pebble.BlockPropertyFilter = (*sqlPrefixFilter)(nil)

// Name returns the name of the property to read.
func (f *sqlPrefixFilter) Name() string {
	return sqlPrefixBlockPropertyName
}

// Intersects returns true if the set represented by prop intersects with
// the set in the filter.
func (f *sqlPrefixFilter) Intersects(prop []byte) (bool, error) {
	// An empty property is unconstrained. An empty property may be encoded when
	// all of a block or sstable's keys share the same SQL prefix, so the
	// ordinary user key seeking should be sufficient. The collector may also
	// write an empty property if there were too many unique SQL prefixes to
	// concisely encode. Either way, we need to read the sstable/block.
	if len(prop) == 0 {
		return true, nil
	}

	l := len(prop)
	for i := 0; i < l; {
		n := int(prop[i])
		if n > l-i-1 {
			return false, errors.Newf("unable to decode %q property; prefix length (%d) longer than remainder of the property (%d)",
				sqlPrefixBlockPropertyName, n, l-i-1)
		}
		if bytes.Equal(f.SQLPrefix, prop[i+1:i+1+n]) {
			return true, nil
		}
		i = i + 1 + n
	}
	return false, nil
}

func sharedPrefix(a, b []byte) int {
	i := 0
	for ; i < len(a) && i < len(b) && a[i] == b[i]; i++ {
	}
	return i
}
