// Copyright 2019 The Cockroach Authors.
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
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

const tempFileExtension = ".crdbtmp"

// SafeWriteToFile writes the byte slice to the filename, contained in dir,
// using the given fs.  It returns after both the file and the containing
// directory are synced.
func SafeWriteToFile(fs vfs.FS, dir string, filename string, b []byte) error {
	tempName := filename + tempFileExtension
	f, err := fs.Create(tempName)
	if err != nil {
		return err
	}
	bReader := bytes.NewReader(b)
	if _, err = io.Copy(f, bReader); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = fs.Rename(tempName, filename); err != nil {
		return err
	}
	fdir, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	defer fdir.Close()
	return fdir.Sync()
}

// SafeFile provides an interface for safely reading and writing a
// file that requires atomicity for file replacement. The vfs.FS
// interface does not guarantee atomicity of renames when the
// destination path already exists, making it difficult to atomically
// update files when necessary.
//
// SafeFile implements a naming scheme using monotonically increasing
// file numbers to indicate which file is the most recent.
//
// SafeFile is not safe for concurrent access. Its atomicity guarantees
// are lost if multiple processes use the same safe file namespace
// simultaneously.
//
// New clients should consider using the Engine to perform atomic writes
// to keys, rather than introducing a new file with atomicity
// requirements.
type SafeFile struct {
	fs              vfs.FS
	dir             string
	name            string
	current         uint64
	currentFilename string
}

// Open opens the file for reading, returning a vfs.File for the current
// file.
func (f *SafeFile) Open() (vfs.File, error) {
	if f.current == 0 {
		return nil, oserror.ErrNotExist
	}
	return f.fs.Open(f.fs.PathJoin(f.dir, f.currentFilename))
}

// Replace allows atomic replacement of the safe file. The provided
// replaceFn function is called for populating the new contents of the
// file. If the replaceFn function returns a non-nil error, replacement
// is aborted and the written contents are forgotten. If the replaceFn
// function returns a non-nil error, Replace will proceed with replacing
// the file.
//
// If Replace returns a nil error, the replacement is guaranteed to be
// durably persisted with the written contents and any future calls to
// Open for the same safe file will return the new contents,
func (f *SafeFile) Replace(replaceFn func(w io.Writer) error) error {
	replacementFile, err := f.ReplaceAppend(replaceFn)
	if err != nil {
		return err
	}
	return replacementFile.Close()
}

// ReplaceAppend is identical to Replace but returns a vfs.File for the
// replacement file, allowing the caller to continue to append to the
// file after the atomic rename.
func (f *SafeFile) ReplaceAppend(replaceFn func(w io.Writer) error) (vfs.File, error) {
	tempPath := f.fs.PathJoin(f.dir, f.name+tempFileExtension)
	tmp, err := f.fs.Create(tempPath)
	if err != nil {
		return nil, err
	}
	// Call the closure to write to the replacement file. If an error is
	// returned, the replacement is aborted.
	if err := replaceFn(tmp); err != nil {
		return nil, errors.CombineErrors(err, tmp.Close())
	}
	// Sync the file before we perform any replacements.
	if err := tmp.Sync(); err != nil {
		return nil, errors.CombineErrors(err, tmp.Close())
	}

	// Rename the file to the next file number path. Since the
	// destination path doesn't exist, there is no risk of a nonatomic
	// rename.
	next := f.current + 1
	nextFilename := fmt.Sprintf("%s-%010d", f.name, next)
	if err := f.fs.Rename(tempPath, f.fs.PathJoin(f.dir, nextFilename)); err != nil {
		return nil, errors.CombineErrors(err, tmp.Close())
	}
	prev, prevFilename := f.current, f.currentFilename
	f.current, f.currentFilename = next, nextFilename

	// Remove the now obsolete previous file.
	if prev > 0 {
		if err := f.fs.Remove(f.fs.PathJoin(f.dir, prevFilename)); err != nil {
			return nil, errors.CombineErrors(err, tmp.Close())
		}
	}

	// Sync the directory to ensure the rename is persisted.
	fdir, err := f.fs.OpenDir(f.dir)
	if err != nil {
		return nil, errors.CombineErrors(err, tmp.Close())
	}
	defer fdir.Close()
	if err := fdir.Sync(); err != nil {
		return nil, errors.CombineErrors(err, tmp.Close())
	}
	return tmp, nil
}

// OpenSafeFile opens a new SafeFile. If no file exists yet,
// OpenSafeFile willl succeed but a subsequent call to its Open method
// will return os.ErrNotExist.
func OpenSafeFile(fs vfs.FS, dir, name string) (*SafeFile, error) {
	ls, err := fs.List(dir)
	if err != nil {
		return nil, err
	}
	var highest uint64
	var highestFilename string
	for _, filename := range ls {
		// If the file doesn't begin with the prefix name, it's not
		// relevant.
		if !strings.HasPrefix(filename, name) {
			continue
		}

		// If the file is a temporary file leftover from an unfinished
		// replacement, remove it.
		if strings.HasSuffix(filename, tempFileExtension) {
			if err := fs.Remove(fs.PathJoin(dir, filename)); err != nil {
				return nil, err
			}
			continue
		}

		// Parse out the file number.
		i := strings.LastIndexByte(filename, '-')
		if i == -1 {
			return nil, errors.Newf("safe file %q: unexpected file %q", name, filename)
		}
		v, err := strconv.ParseUint(filename, 10, 64)
		if err != nil {
			return nil, errors.CombineErrors(errors.Newf("safe file %q: unexpected file %q", name, filename), err)
		}
		// Zero is reserved.
		if v == 0 {
			return nil, errors.Newf("safe file %q: unexpected file %q", name, filename)
		}

		switch {
		case v < highest:
			// The file at filename is old, superceded by our current
			// highest.
			if err := fs.Remove(filename); err != nil {
				return nil, err
			}
		case v == highest:
			return nil, errors.Newf("safe file %q: unexpected duplicate number %d", name, v)
		case v > highest:
			// This file replaces the existing highest.
			if highest != 0 {
				if err := fs.Remove(highestFilename); err != nil {
					return nil, err
				}
			}
			highest, highestFilename = v, filename
		}
	}
	return &SafeFile{
		fs:              fs,
		dir:             dir,
		name:            name,
		current:         highest,
		currentFilename: highestFilename,
	}, nil
}
