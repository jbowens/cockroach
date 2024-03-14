// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fs

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// NewEncryptedEnvFunc creates an encrypted environment and returns the vfs.FS to use for reading
// and writing data. This should be initialized by calling engineccl.Init() before calling
// NewPebble(). The optionBytes is a binary serialized baseccl.EncryptionOptions, so that non-CCL
// code does not depend on CCL code.
var NewEncryptedEnvFunc func(
	fs vfs.FS, fr *FileRegistry, dbDir string, readOnly bool, optionBytes []byte,
) (*EncryptionEnv, error)

// resolveEncryptedEnvOptions creates the EncryptionEnv and associated file
// registry if this store has encryption-at-rest enabled; otherwise returns a
// nil EncryptionEnv.
func resolveEncryptedEnvOptions(
	ctx context.Context, unencryptedFS vfs.FS, dir string, encryptionOpts []byte, rw RWMode,
) (*EncryptionEnv, error) {
	if len(encryptionOpts) == 0 {
		// There's no encryption config. This is valid if the user doesn't
		// intend to use encryption-at-rest, and the store has never had
		// encryption-at-rest enabled. Validate that there's no file registry.
		// If there is, the caller is required to specify an
		// --enterprise-encryption flag for this store.
		if err := checkNoRegistryFile(unencryptedFS, dir); err != nil {
			return nil, fmt.Errorf("encryption was used on this store before, but no encryption flags " +
				"specified. You need a CCL build and must fully specify the --enterprise-encryption flag")
		}
		return nil, nil
	}

	// We'll need to use the encryption-at-rest filesystem. Even if the store
	// isn't configured to encrypt files, there may still be encrypted files
	// from a previous configuration.
	if NewEncryptedEnvFunc == nil {
		return nil, fmt.Errorf("encryption is enabled but no function to create the encrypted env")
	}
	// TODO(jackson): Should NewEncryptedEnvFunc load the file registry?
	fileRegistry := &FileRegistry{FS: unencryptedFS, DBDir: dir, ReadOnly: rw == ReadOnly,
		NumOldRegistryFiles: DefaultNumOldFileRegistryFiles}
	if err := fileRegistry.Load(ctx); err != nil {
		return nil, err
	}
	env, err := NewEncryptedEnvFunc(unencryptedFS, fileRegistry, dir, rw == ReadOnly, encryptionOpts)
	if err != nil {
		return nil, errors.WithSecondaryError(err, fileRegistry.Close())
	}
	return env, nil
}

// EncryptionEnv describes the encryption-at-rest environment, providing
// access to a filesystem with on-the-fly encryption.
type EncryptionEnv struct {
	// Closer closes the encryption-at-rest environment. Once the
	// environment is closed, the environment's VFS may no longer be
	// used.
	Closer io.Closer
	// FS provides the encrypted virtual filesystem. New files are
	// transparently encrypted.
	FS vfs.FS
	// Registry is non-nil if encryption-at-rest has ever been enabled on the
	// store. The registry maintains a mapping of all encrypted keys and the
	// corresponding data key with which they're encrypted.
	Registry *FileRegistry
	// StatsHandler exposes encryption-at-rest state for observability.
	StatsHandler EncryptionStatsHandler
}

// EncryptionRegistries contains the encryption-related registries:
// Both are serialized protobufs.
type EncryptionRegistries struct {
	// FileRegistry is the list of files with encryption status.
	// serialized storage/engine/enginepb/file_registry.proto::FileRegistry
	FileRegistry []byte
	// KeyRegistry is the list of keys, scrubbed of actual key data.
	// serialized ccl/storageccl/engineccl/enginepbccl/key_registry.proto::DataKeysRegistry
	KeyRegistry []byte
}

// EncryptionStatsHandler provides encryption related stats.
type EncryptionStatsHandler interface {
	// SerializedRegistries retrieves the serialized encryption-at-rest
	// registries, scrubbed of key contents.
	SerializedRegistries() (EncryptionRegistries, error)
	// Stats returns the current encryption stats.
	Stats() (EncryptionStats, error)
}

// EncryptionStats is a set of statistics around encryption-at-rest and its
// current state.
type EncryptionStats struct {
	// TotalFiles is the total number of files reported by rocksdb.
	TotalFiles uint64
	// TotalBytes is the total size of files reported by rocksdb.
	TotalBytes uint64
	// ActiveKeyFiles is the number of files using the active data key.
	ActiveKeyFiles uint64
	// ActiveKeyBytes is the size of files using the active data key.
	ActiveKeyBytes uint64
	// EncryptionType is an enum describing the active encryption algorithm.
	// See: ccl/storageccl/engineccl/enginepbccl/key_registry.proto
	EncryptionType int32
	// EncryptionStatus is a serialized enginepbccl/stats.proto::EncryptionStatus protobuf.
	EncryptionStatus []byte
}
