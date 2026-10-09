package cli

import (
	"path/filepath"

	"github.com/djalmajr/herdr-hermes/internal/cred"
)

// keyConfigured reports whether a dispatcher API key is stored on this
// machine, backed by the file store. Slice 2's doctor and sync read this
// variable; slice 3 ships the real implementation.
var keyConfigured = func(env Env) bool {
	s := newCredStore(env)
	_, err := s.Get()
	return err == nil
}

// newCredStore builds the file credential store under the config dir.
func newCredStore(env Env) *cred.FileStore {
	return &cred.FileStore{Path: filepath.Join(env.ConfigDir, "credentials")}
}
