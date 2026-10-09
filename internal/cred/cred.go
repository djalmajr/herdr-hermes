// Package cred stores the per-user dispatcher API key on the node. Phase 1
// ships one file store; OS keychain backends come later behind the same
// interface. The key bytes plus a trailing newline are the only content of
// the credentials file, written atomically with mode 0600.
package cred

import (
	"errors"
	"os"
	"strings"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// ErrNotConfigured reports that no key is stored yet.
var ErrNotConfigured = errors.New("cred: no key configured")

// Store is where the per-user dispatcher API key lives.
type Store interface {
	Get() (string, error)
	Set(string) error
	Delete() error
	Kind() string
}

// FileStore keeps the key at Path (<ConfigDir>/credentials).
type FileStore struct {
	Path string
}

// Kind is the backend name reported by auth commands.
func (s *FileStore) Kind() string { return "file" }

// Get returns the stored key. A missing file is ErrNotConfigured; a file
// whose content is only the trailing newline is an error, never an empty
// key.
func (s *FileStore) Get() (string, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return "", ErrNotConfigured
	}
	if err != nil {
		return "", err
	}
	key := strings.TrimSuffix(string(data), "\n")
	if key == "" {
		return "", errors.New("cred: stored key is empty")
	}
	return key, nil
}

// Set stores the key, replacing any previous one. The file holds the key
// bytes plus one trailing newline and is written with temp + rename, mode
// 0600.
func (s *FileStore) Set(key string) error {
	return outbox.WriteFileAtomic(s.Path, append([]byte(key), '\n'))
}

// Delete removes the stored key. A missing file is not an error.
func (s *FileStore) Delete() error {
	if err := os.Remove(s.Path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}
