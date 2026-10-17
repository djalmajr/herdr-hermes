package outbox

import (
	"io"
	"os"
	"syscall"
)

// ReadReplacedFile reads path the way a lock-free reader must on Windows:
// the atomic replace (WriteFileAtomic's rename) holds a DELETE-access
// handle on the file for a moment, and an open without FILE_SHARE_DELETE
// fails then with a sharing violation. The open here admits that handle —
// and nothing else changes: other holders (an exclusive one, share mode 0)
// still fail, and the error is returned as is, with the same
// "open <path>: <message>" text os.ReadFile produces, so os.IsNotExist
// and the existing error wrapping keep working. No reparse-point flag: the
// read keeps following links exactly as os.ReadFile does today.
func ReadReplacedFile(path string) ([]byte, error) {
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	h, err := syscall.CreateFile(name, syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	f := os.NewFile(uintptr(h), path)
	defer f.Close()
	return io.ReadAll(f)
}
