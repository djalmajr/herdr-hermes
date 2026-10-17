//go:build !windows

package outbox

import "os"

// replaceFile is the non-Windows counterpart of the _windows replace: the
// rename(2) behind os.Rename already replaces a file other processes hold
// open, so it is used as is.
func replaceFile(from, to string) error {
	return os.Rename(from, to)
}
