//go:build linux

package supervisor

import (
	"errors"
	"fmt"
	"os"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"golang.org/x/sys/unix"
)

// presentationFile is the immutable presentation document the producer writes into its epoch
// directory before its first part.
const presentationFile = "presentation.json"

// LoadPresentation reads and validates presentation.json of src's epoch. A missing file is
// ErrTimelineNotReady: the producer has not published yet. Any other failure, or a document whose
// identity or config is not exactly src's, wraps ErrTimelineInvalid.
func LoadPresentation(src *Source) (ipc.Presentation, error) {
	if src == nil {
		return ipc.Presentation{}, fmt.Errorf("%w: nil source", ErrTimelineInvalid)
	}
	info := src.Info()
	dirfd, err := openPrivateDir(info.Directory)
	if err != nil {
		return ipc.Presentation{}, fmt.Errorf("%w: %v", ErrTimelineInvalid, err)
	}
	defer closeFD(dirfd)
	fd, err := unix.Openat(dirfd, presentationFile, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return ipc.Presentation{}, fmt.Errorf("%w: %s: %v", ErrTimelineNotReady, presentationFile, os.ErrNotExist)
		}
		return ipc.Presentation{}, fmt.Errorf("%w: open %s: %v", ErrTimelineInvalid, presentationFile, err)
	}
	defer closeFD(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return ipc.Presentation{}, fmt.Errorf("%w: fstat %s: %v", ErrTimelineInvalid, presentationFile, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != uint32(unix.Geteuid()) || st.Nlink != 1 || st.Mode&0o777 != 0o600 {
		return ipc.Presentation{}, fmt.Errorf("%w: %s is not a private 0600 regular file of uid %d with one link", ErrTimelineInvalid, presentationFile, unix.Geteuid())
	}
	if st.Size > ipc.MaxPresentationSize {
		return ipc.Presentation{}, fmt.Errorf("%w: %s is %d bytes, limit %d", ErrTimelineInvalid, presentationFile, st.Size, ipc.MaxPresentationSize)
	}
	data := make([]byte, st.Size)
	if err := preadFull(fd, data, 0); err != nil {
		return ipc.Presentation{}, fmt.Errorf("%w: read %s: %v", ErrTimelineInvalid, presentationFile, err)
	}
	pres, err := ipc.DecodePresentation(data)
	if err != nil {
		return ipc.Presentation{}, fmt.Errorf("%w: %s: %v", ErrTimelineInvalid, presentationFile, err)
	}
	if pres.Run != info.Run || pres.Source != info.Source || pres.Epoch != info.Epoch || pres.Config != info.Config {
		return ipc.Presentation{}, fmt.Errorf("%w: %s does not describe epoch %s/%s/%d", ErrTimelineInvalid, presentationFile, info.Run, info.Source, info.Epoch)
	}
	return pres, nil
}
