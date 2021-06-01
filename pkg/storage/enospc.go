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
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

func onOutOfDisk(fs vfs.FS, fn func()) vfs.FS {
	newFS := &enospcFS{inner: fs}
	newFS.mu.Cond.L = &newFS.mu.Mutex
	newFS.mu.onOutOfDisk = fn
	return newFS
}

type enospcFS struct {
	inner vfs.FS

	generation uint32 // atomic

	mu struct {
		sync.Mutex
		sync.Cond
		onOutOfDisk func()
	}
}

func (fs *enospcFS) waitUntilReady() uint32 {
	gen := atomic.LoadUint32(&fs.generation)
	if gen%2 == 0 {
		return gen
	}

	// We're currently handling an out-of-disk error.
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for gen%2 == 1 {
		fs.mu.Wait()
		gen = atomic.LoadUint32(&fs.generation)
	}
	return gen
}

func (fs *enospcFS) handleENOSPC(gen uint32) {
	if gen%2 == 1 {
		panic("non-even generation")
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()

	currentGeneration := atomic.LoadUint32(&fs.generation)

	// If the current generation is still the same, this is the first
	// goroutine to hit an ENOSPC within this generation, so it's responsible
	// for invoking the callback.
	if currentGeneration == gen {
		// Increment the generation to an odd number, indicating that the FS
		// is out-of-disk space and incoming writes should pause and wait for
		// the next generation before continuing.
		atomic.StoreUint32(&fs.generation, gen+1)

		// Drop the mutex while we invoke the callback, re-acquiring
		// afterwards.
		fs.mu.Unlock()
		fs.mu.onOutOfDisk()
		fs.mu.Lock()

		// Update the generation again to an even number, indicating that the
		// callback has been invoked.
		atomic.StoreUint32(&fs.generation, gen+2)
		fs.mu.Broadcast()
		return
	}

	// The current generation has already been increased, so either the
	// callback is currently being run by another goroutine or it's already
	// completed. Wait for it complete, if it hasn't already.
	for currentGeneration == gen+1 {
		fs.mu.Wait()
		currentGeneration = atomic.LoadUint32(&fs.generation)
	}
}

func (fs *enospcFS) Create(name string) (vfs.File, error) {
	gen := fs.waitUntilReady()

	f, err := fs.inner.Create(name)

	if err != nil && isENOSPC(err) {
		fs.handleENOSPC(gen)
		f, err = fs.inner.Create(name)
	}
	if f != nil {
		f = vfs.WithFd(f, enospcFile{
			fs:    fs,
			inner: f,
		})
	}
	return f, err
}

func (fs *enospcFS) Link(oldname, newname string) error {
	gen := fs.waitUntilReady()

	err := fs.inner.Link(oldname, newname)

	if err != nil && isENOSPC(err) {
		fs.handleENOSPC(gen)
		err = fs.inner.Link(oldname, newname)
	}
	return err
}

func (fs *enospcFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.inner.Open(name, opts...)
	if f != nil {
		f = vfs.WithFd(f, enospcFile{
			fs:    fs,
			inner: f,
		})
	}
	return f, err
}

func (fs *enospcFS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.inner.OpenDir(name)
	if f != nil {
		f = vfs.WithFd(f, enospcFile{
			fs:    fs,
			inner: f,
		})
	}
	return f, err
}

func (fs *enospcFS) Remove(name string) error {
	return fs.inner.Remove(name)
}

func (fs *enospcFS) RemoveAll(name string) error {
	return fs.inner.RemoveAll(name)
}

func (fs *enospcFS) Rename(oldname, newname string) error {
	gen := fs.waitUntilReady()

	err := fs.inner.Rename(oldname, newname)

	if err != nil && isENOSPC(err) {
		fs.handleENOSPC(gen)
		err = fs.inner.Rename(oldname, newname)
	}
	return err
}

func (fs *enospcFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	gen := fs.waitUntilReady()

	f, err := fs.inner.ReuseForWrite(oldname, newname)

	if err != nil && isENOSPC(err) {
		fs.handleENOSPC(gen)
		f, err = fs.inner.ReuseForWrite(oldname, newname)
	}

	if f != nil {
		f = vfs.WithFd(f, enospcFile{
			fs:    fs,
			inner: f,
		})
	}
	return f, err
}

func (fs *enospcFS) MkdirAll(dir string, perm os.FileMode) error {
	gen := fs.waitUntilReady()

	err := fs.inner.MkdirAll(dir, perm)

	if err != nil && isENOSPC(err) {
		fs.handleENOSPC(gen)
		err = fs.inner.MkdirAll(dir, perm)
	}
	return err
}

func (fs *enospcFS) Lock(name string) (io.Closer, error) {
	gen := fs.waitUntilReady()

	closer, err := fs.inner.Lock(name)

	if err != nil && isENOSPC(err) {
		fs.handleENOSPC(gen)
		closer, err = fs.inner.Lock(name)
	}
	return closer, err
}

func (fs *enospcFS) List(dir string) ([]string, error) {
	return fs.inner.List(dir)
}

func (fs *enospcFS) Stat(name string) (os.FileInfo, error) {
	return fs.inner.Stat(name)
}

func (fs *enospcFS) PathBase(path string) string {
	return fs.inner.PathBase(path)
}

func (fs *enospcFS) PathJoin(elem ...string) string {
	return fs.inner.PathJoin(elem...)
}

func (fs *enospcFS) PathDir(path string) string {
	return fs.inner.PathDir(path)
}

func (fs *enospcFS) GetFreeSpace(path string) (uint64, error) {
	return fs.inner.GetFreeSpace(path)
}

type enospcFile struct {
	fs    *enospcFS
	inner vfs.File
}

func (f enospcFile) Close() error {
	return f.inner.Close()
}

func (f enospcFile) Read(p []byte) (n int, err error) {
	return f.inner.Read(p)
}

func (f enospcFile) ReadAt(p []byte, off int64) (n int, err error) {
	return f.inner.ReadAt(p, off)
}

func (f enospcFile) Write(p []byte) (n int, err error) {
	gen := f.fs.waitUntilReady()

	n, err = f.inner.Write(p)

	if err != nil && isENOSPC(err) {
		f.fs.handleENOSPC(gen)
		var n2 int
		n2, err = f.inner.Write(p[n:])
		n += n2
	}
	return n, err
}

func (f enospcFile) Stat() (os.FileInfo, error) {
	return f.inner.Stat()
}

func (f enospcFile) Sync() error {
	gen := f.fs.waitUntilReady()

	err := f.inner.Sync()

	if err != nil && isENOSPC(err) {
		f.fs.handleENOSPC(gen)

		// NB: It is NOT safe to retry the Sync. See the PostgreSQL
		// 'fsyncgate' discussion. A successful Sync after a failed one does
		// not provide any guarantees. We need to bubble the error up and
		// hope we weren't syncing a WAL or MANIFEST, because we'll have no
		// choice but to crash.
		// See: https://lwn.net/Articles/752063/
	}
	return err
}

var _ vfs.FS = (*enospcFS)(nil)

func isENOSPC(err error) bool {
	err = errors.UnwrapAll(err)
	err = underlyingError(err)
	e, ok := err.(syscall.Errno)
	return ok && e == syscall.ENOSPC
}

// underlyingError returns the underlying error for known os error types.
// This function is copied from the standard library os package.
func underlyingError(err error) error {
	switch err := err.(type) {
	case *os.PathError:
		return err.Err
	case *os.LinkError:
		return err.Err
	case *os.SyscallError:
		return err.Err
	}
	return err
}
