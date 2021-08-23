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
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestSafeWriteToFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use an in-memory FS that strictly enforces syncs.
	mem := vfs.NewStrictMem()
	syncDir := func(dir string) {
		fdir, err := mem.OpenDir(dir)
		require.NoError(t, err)
		require.NoError(t, fdir.Sync())
		require.NoError(t, fdir.Close())
	}
	readFile := func(filename string) []byte {
		f, err := mem.Open("foo/bar")
		require.NoError(t, err)
		b, err := ioutil.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return b
	}

	require.NoError(t, mem.MkdirAll("foo", os.ModePerm))
	syncDir("")
	f, err := mem.Create("foo/bar")
	require.NoError(t, err)
	_, err = io.WriteString(f, "Hello world")
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	syncDir("foo")

	// Discard any unsynced writes to make sure we set up the test
	// preconditions correctly.
	mem.ResetToSyncedState()
	require.Equal(t, []byte("Hello world"), readFile("foo/bar"))

	// Use SafeWriteToFile to atomically, durably change the contents of the
	// file.
	require.NoError(t, SafeWriteToFile(mem, "foo", "foo/bar", []byte("Hello everyone")))

	// Discard any unsynced writes.
	mem.ResetToSyncedState()
	require.Equal(t, []byte("Hello everyone"), readFile("foo/bar"))
}

func TestSafeFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up a 'foo/bar' safe file.
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll("foo", os.ModePerm))
	sf, err := OpenSafeFile(mem, "foo", "bar")
	require.NoError(t, err)

	// Open should return ErrNotExist.
	f, err := sf.Open()
	require.Equal(t, os.ErrNotExist, err)
	require.Nil(t, f)

	// If the Replace's closure returns an error, that error should be
	// returned.
	replaceErr := errors.New("uh oh")
	err = sf.Replace(func(w io.Writer) error {
		if _, err := io.WriteString(w, "Hello world!"); err != nil {
			return err
		}
		return replaceErr
	})
	require.Equal(t, replaceErr, err)

	// And the file should still not exist.
	f, err = sf.Open()
	require.Equal(t, os.ErrNotExist, err)
	require.Nil(t, f)

	// Replacing the file without a replaceFn error should succeed.
	require.NoError(t, sf.Replace(func(w io.Writer) error {
		_, err := io.WriteString(w, "Hello world")
		return err
	}))

	// And the first iteration should have file number 1.
	ls, err := mem.List("foo")
	require.NoError(t, err)
	require.Equal(t, []string{"bar-0000000001"}, ls)

	// Reading the file should read the correct contents.
	f, err = sf.Open()
	require.NoError(t, err)
	b, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, "Hello world", string(b))

	// Replacing the contents again should succeed.
	require.NoError(t, sf.Replace(func(w io.Writer) error {
		_, err := io.WriteString(w, "Hi")
		return err
	}))

	// The current file number should have incremented.
	ls, err = mem.List("foo")
	require.NoError(t, err)
	require.Equal(t, []string{"bar-0000000002"}, ls)
}

func TestSafeFile_Syncs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use an in-memory FS that strictly enforces syncs.
	mem := vfs.NewStrictMem()
	syncDir := func(dir string) {
		fdir, err := mem.OpenDir(dir)
		require.NoError(t, err)
		require.NoError(t, fdir.Sync())
		require.NoError(t, fdir.Close())
	}
	readFile := func(sf *SafeFile) []byte {
		f, err := sf.Open()
		require.NoError(t, err)
		b, err := ioutil.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return b
	}

	require.NoError(t, mem.MkdirAll("foo", os.ModePerm))
	syncDir("")
	sf, err := OpenSafeFile(mem, "foo", "bar")
	require.NoError(t, err)
	require.NoError(t, sf.Replace(func(w io.Writer) error {
		_, err := io.WriteString(w, "Hello world")
		return err
	}))

	// Discard any unsynced writes to make sure we set up the test
	// preconditions correctly.
	mem.ResetToSyncedState()
	require.Equal(t, []byte("Hello world"), readFile(sf))

	// Use Replace to atomically, durably change the contents of the
	// file.
	require.NoError(t, sf.Replace(func(w io.Writer) error {
		_, err := io.WriteString(w, "Hello everyone")
		return err
	}))

	// Discard any unsynced writes.
	mem.ResetToSyncedState()
	require.Equal(t, []byte("Hello everyone"), readFile(sf))
}
