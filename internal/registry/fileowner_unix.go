//go:build !windows

package registry

import (
	"os"
	"syscall"
)

// ownedBySelfOrRoot reports whether fi belongs to the current user or to
// root: nobody else can replace what such an entry holds.
func ownedBySelfOrRoot(fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	return st.Uid == 0 || int(st.Uid) == os.Geteuid()
}
