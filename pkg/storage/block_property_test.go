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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
)

func TestSQLPrefixBlockPropertyCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Use a scanner that decodes the /Tenant/ and /Table/ prefixes, and appends
	// everything after the index ID to the key directly.
	scanner := keysutil.MakePrettyScanner(
		keysutil.PrettyScanExt{
			Name: "/Table",
			ParseFunc: func(input string) (string, roachpb.Key) {
				rem, rk := keys.TableKeyParse(input)
				return "", append(rk, rem...)
			},
		},
		keysutil.PrettyScanExt{
			Name: "/Tenant",
			ParseFunc: func(input string) (string, roachpb.Key) {
				rem, rk := keys.TenantKeyParse(input)
				return "", append(rk, rem...)
			},
		},
	)

	c := newSQLPrefixCollector()
	datadriven.RunTest(t, "testdata/block_property_collector_sql_prefix",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "data-block":
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.SplitN(line, ":", 2)
					prettyKey := parts[0]

					key, err := scanner.Scan(prettyKey)
					if err != nil {
						return fmt.Sprintf("decoding pretty key %q: %s", prettyKey, err)
					}
					ik := pebble.InternalKey{UserKey: key}
					var v []byte
					if len(parts) > 1 {
						v = []byte(parts[1])
					}
					if err := c.Add(ik, v); err != nil {
						return err.Error()
					}
				}

				v, err := c.FinishDataBlock(nil)
				if err != nil {
					return err.Error()
				}
				prop := decodeProperty(v)
				c.AddPrevDataBlockToIndexBlock()
				return prop
			case "finish-index":
				v, err := c.FinishIndexBlock(nil)
				if err != nil {
					return err.Error()
				}
				return decodeProperty(v)
			case "finish-table":
				v, err := c.FinishTable(nil)
				if err != nil {
					return err.Error()
				}
				c = newSQLPrefixCollector()
				return decodeProperty(v)
			default:
				return fmt.Sprintf("unrecognized command %q", d.Cmd)
			}
		})
}

func TestSQLPrefixBlockPropertyFilter(t *testing.T) {
	//testCases := []struct {
	//}{

	//}
}

func decodeProperty(prop []byte) string {
	var buf bytes.Buffer
	for len(prop) > 0 {
		n := prop[0]
		fmt.Fprintln(&buf, roachpb.Key(prop[1:1+n]))
		prop = prop[1+n:]
	}
	return buf.String()
}
