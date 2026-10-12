//go:build linux

package worker

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"golang.org/x/sys/unix"
)

// PresentationFile is the name of the immutable presentation document in an epoch directory.
const PresentationFile = "presentation.json"

// ErrPresentationConflict reports an existing presentation.json whose bytes differ from the
// canonical encoding being written: the document is immutable and nothing is replaced.
var ErrPresentationConflict = errors.New("worker: presentation already written with other content")

// WritePresentation publishes p as presentation.json in dir, an existing absolute private 0700
// directory of the effective uid. The canonical encoding is written to a private temporary file,
// synced, renamed into place without replacing anything and the directory is synced. An existing
// file with identical bytes is left as it is; one with different bytes is ErrPresentationConflict.
// The producer must call this before it publishes its first part.
func WritePresentation(dir string, p ipc.Presentation) error {
	data, err := ipc.EncodePresentation(p)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("%w: directory %q is not absolute", ErrInvalidOptions, dir)
	}
	dirfd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("%w: open directory: %v", ErrInvalidOptions, err)
	}
	defer unix.Close(dirfd)
	var st unix.Stat_t
	if err := unix.Fstat(dirfd, &st); err != nil {
		return fmt.Errorf("%w: fstat directory: %v", ErrInvalidOptions, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != uint32(unix.Geteuid()) || st.Mode&0o777 != 0o700 {
		return fmt.Errorf("%w: directory is not a private 0700 directory of uid %d", ErrInvalidOptions, unix.Geteuid())
	}
	same, err := presentationMatches(dirfd, data)
	if err != nil || same {
		return err
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Errorf("worker: random temp name: %w", err)
	}
	name := ".presentation." + hex.EncodeToString(b[:]) + ".tmp"
	fd, err := unix.Openat(dirfd, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return fmt.Errorf("worker: create %s: %w", name, err)
	}
	err = pwriteFull(fd, data, 0)
	if err == nil {
		err = unix.Fsync(fd)
	}
	if cerr := unix.Close(fd); err == nil && cerr != nil {
		err = cerr
	}
	if err == nil {
		err = unix.Renameat2(dirfd, name, dirfd, PresentationFile, unix.RENAME_NOREPLACE)
	}
	if err != nil {
		_ = unix.Unlinkat(dirfd, name, 0)
		if errors.Is(err, unix.EEXIST) {
			// Lost a race with another writer of this epoch: identical bytes are still success.
			same, merr := presentationMatches(dirfd, data)
			if merr == nil && same {
				return nil
			}
		}
		return fmt.Errorf("worker: publish %s: %w", PresentationFile, err)
	}
	if err := unix.Fsync(dirfd); err != nil {
		return fmt.Errorf("worker: sync directory: %w", err)
	}
	return nil
}

// presentationMatches reports whether presentation.json exists in dirfd with exactly data; an
// existing file with other content, or one that is not a private regular file, is an error.
func presentationMatches(dirfd int, data []byte) (bool, error) {
	fd, err := unix.Openat(dirfd, PresentationFile, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%w: open existing %s: %v", ErrPresentationConflict, PresentationFile, err)
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return false, fmt.Errorf("%w: fstat existing %s: %v", ErrPresentationConflict, PresentationFile, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != uint32(unix.Geteuid()) || st.Mode&0o777 != 0o600 ||
		st.Size != int64(len(data)) {
		return false, fmt.Errorf("%w: existing %s is not an identical private file", ErrPresentationConflict, PresentationFile)
	}
	have := make([]byte, len(data))
	n, err := unix.Pread(fd, have, 0)
	if err != nil || n != len(data) || !bytes.Equal(have, data) {
		return false, fmt.Errorf("%w: existing %s differs", ErrPresentationConflict, PresentationFile)
	}
	return true, nil
}
