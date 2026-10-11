package registry

import "os"

// ownedBySelfOrRoot is not checked on Windows, where access is governed by
// ACLs rather than owners and mode bits.
func ownedBySelfOrRoot(os.FileInfo) bool { return true }
