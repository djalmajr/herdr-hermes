package outbox

import (
	"errors"
	"os"
	"path/filepath"
)

// replaceFile moves from over to, the last step of an atomic replace. In the
// same directory it renames through an os.Root, which on Windows asks for a
// POSIX-semantics replace (and falls back to the classic one when that is
// not supported): that replace succeeds while a lock-free reader holds the
// destination open with FILE_SHARE_DELETE, where os.Rename (MoveFileEx)
// fails with ERROR_ACCESS_DENIED. A holder without FILE_SHARE_DELETE still
// makes it fail. A move across directories, or a directory that cannot be
// opened as a root, keeps os.Rename exactly as before.
func replaceFile(from, to string) error {
	dir := filepath.Dir(to)
	if filepath.Dir(from) != dir {
		return os.Rename(from, to)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return os.Rename(from, to)
	}
	defer root.Close()
	err = root.Rename(filepath.Base(from), filepath.Base(to))
	if err == nil {
		return nil
	}
	// Keep the "rename <from> <to>: <message>" shape of os.Rename's error
	// with the full paths and the underlying errno.
	under := err
	var linkErr *os.LinkError
	var pathErr *os.PathError
	switch {
	case errors.As(err, &linkErr):
		under = linkErr.Err
	case errors.As(err, &pathErr):
		under = pathErr.Err
	}
	return &os.LinkError{Op: "rename", Old: from, New: to, Err: under}
}
