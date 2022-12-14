// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keysutils

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
)

// MakePrettyScannerForNamedTables create a PrettyScanner that, beside what the
// PrettyScanner is generally able to decode, can also decode keys of the form
// "/<table name>/<index name>/1/2/3/..." using supplied maps from names to ids.
//
// TODO(nvanbenschoten): support tenant SQL keys.
func MakePrettyScannerForNamedTables(
	tableNameToID map[string]int, idxNameToID map[string]int,
) keysutil.PrettyScanner {
	return keysutil.MakePrettyScanner(keysutil.PrettyScanExt{
		Name: "/Table",
		ParseFunc: func(input string) (string, roachpb.Key) {
			remainder, k := parseTableKeysAsAscendingInts(input, tableNameToID, idxNameToID)
			return remainder, k
		},
	})
}

// parseTableKeysAsAscendingInts takes a pretty-printed key segment like
// "/<table name>/<index name>/1/2/3/...", a mapping of table names to ids and a
// mapping of index names to ids and turns the part before "/... " into the key
// corresponding to the respective index entry by encoding the ints as ascending
// varints (so assuming that they correspond to columns of various integer
// types).
//
// idxNameToID has to contain entries of the form "<table name>.<index name>".
// The index name "pk" is implicitly mapped to index 1; it doesn't need to
// appear in idxNameToID.
//
// Notice that the input is not expected to have the "/Table" prefix (which is
// generated by pretty-printing keys). That prefix is assumed to have been
// consumed before this function is invoked (by the PrettyScanner).
//
// The "/..." part is returned as the remainder (the first return value).
func parseTableKeysAsAscendingInts(
	input string, tableNameToID map[string]int, idxNameToID map[string]int,
) (string, roachpb.Key) {
	// Consume the table name.
	input = mustShiftSlash(input)
	slashPos := strings.Index(input, "/")
	if slashPos < 0 {
		slashPos = len(input)
	}
	remainder := input[slashPos:] // `/something/else` -> `/else`
	tableName := input[:slashPos]
	tableID, ok := tableNameToID[tableName]
	if !ok {
		panic(fmt.Sprintf("unknown table: %s", tableName))
	}
	output := keys.TODOSQLCodec.TablePrefix(uint32(tableID))
	if remainder == "" {
		return "", output
	}
	input = remainder

	// Consume the index name.
	input = mustShiftSlash(input)
	slashPos = strings.Index(input, "/")
	if slashPos < 0 {
		// Accept a string ending in "/<index name>.
		slashPos = len(input)
	}
	remainder = input[slashPos:] // `/something/else` -> `/else`
	idxName := input[:slashPos]
	var idxID int
	// The primary key index always has ID 1.
	if idxName == "pk" {
		idxID = 1
	} else {
		idxID, ok = idxNameToID[fmt.Sprintf("%s.%s", tableName, idxName)]
		if !ok {
			panic(fmt.Sprintf("unknown index: %s", idxName))
		}
	}
	output = encoding.EncodeUvarintAscending(output, uint64(idxID))
	if remainder == "" {
		return "", output
	}

	input = remainder
	remainder, moreOutput := parseAscendingIntIndexKeys(input)
	output = append(output, moreOutput...)
	return remainder, output
}

func mustShiftSlash(in string) string {
	slash, out := mustShift(in)
	if slash != "/" {
		panic("expected /: " + in)
	}
	return out
}

func mustShift(in string) (first, remainder string) {
	if len(in) == 0 {
		panic("premature end of string")
	}
	return in[:1], in[1:]
}

// parseAscendingIntIndexKeys takes a pretty-printed key segment like
// "/1/2/3/foo" and parses all the ints from the beginning turning them into a
// key segment by encoding them as ascending varints. The part after the last
// int is returned as the remainder (the first return value).
func parseAscendingIntIndexKeys(input string) (string, roachpb.Key) {
	var key roachpb.Key
	for {
		remainder, k := parseAscendingIntIndexKey(input)
		if k == nil {
			// We've failed to parse anything.
			return remainder, key
		}
		key = append(key, k...)
		input = remainder
		if remainder == "" {
			// We've consumed the whole string.
			return "", key
		}
	}
}

// ParseAscendingIntIndexKey parses one int out of a string looking like
// "/1[/...]" and encodes it as an ascending varint. The rest of the input string
// is returned as the remainder (the first return value).
//
// If the beginning of the input can't be parsed, the returned key is nil and
// the whole input is returned as the remainder.
func parseAscendingIntIndexKey(input string) (string, roachpb.Key) {
	origInput := input
	input = mustShiftSlash(input)
	slashPos := strings.Index(input, "/")
	if slashPos < 0 {
		// Deal with the case where there's no remainder: the entire string is the
		// index ID.
		slashPos = len(input)
	}
	indexValStr := input[:slashPos]
	datum, err := tree.ParseDInt(indexValStr)
	if err != nil {
		// Can't decode the key.
		return origInput, nil
	}
	remainder := input[slashPos:] // `/something/else` -> `/else`
	key, err := keyside.Encode(nil, datum, encoding.Ascending)
	if err != nil {
		panic(err)
	}
	return remainder, key
}
