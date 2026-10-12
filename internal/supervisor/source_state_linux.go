//go:build linux

package supervisor

// owner.json: the durable description of one source epoch beside its journal, written once at
// creation and never replaced. It binds the epoch to the boot its memfds live in, its scope,
// config and effective policy, the one worker process allowed to publish into it and the exact
// kernel identities of its descriptors in track order, under the canonical names they are stored
// under in the systemd file-descriptor store. A restarted supervisor reads it to know which
// inherited descriptors to adopt and whom to serve; without it, or with a mismatch, the epoch is
// lost and nothing is allocated afresh here.

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"golang.org/x/sys/unix"
)

const (
	ownerFile   = "owner.json"
	journalFile = "journal.bin" // the journal's own file name; checked absent before creation
	ownerFormat = 1
	bootIDPath  = "/proc/sys/kernel/random/boot_id"

	maxOwnerFile = 16 * 1024
)

// ownerState is the JSON content of owner.json.
type ownerState struct {
	Format    int              `json:"format"`
	WireMajor int              `json:"wire_major"`
	BootID    string           `json:"boot_id"`
	Run       string           `json:"run"`
	Source    string           `json:"source"`
	Epoch     uint64           `json:"epoch"`
	Config    ipc.Config       `json:"config"`
	Policy    policyState      `json:"policy"`
	Worker    workerState      `json:"worker"`
	Tracks    []ipc.FDIdentity `json:"tracks"`
}

// policyState is the effective retention policy in nanoseconds.
type policyState struct {
	WindowNS     int64 `json:"window_ns"`
	MaxSegmentNS int64 `json:"max_segment_ns"`
}

// workerState is the kernel identity of the one worker process allowed to publish.
type workerState struct {
	UID int    `json:"uid"`
	PID int    `json:"pid"`
	Ino uint64 `json:"ino"`
}

// openPrivateDir opens dir, which must be an existing absolute private 0700 directory of the
// effective uid, and returns its descriptor. Every later operation is relative to it.
func openPrivateDir(dir string) (int, error) {
	if !filepath.IsAbs(dir) {
		return -1, fmt.Errorf("%w: directory %q is not absolute", ErrInvalidOptions, dir)
	}
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("%w: open directory: %v", ErrInvalidOptions, err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		unix.Close(fd)
		return -1, fmt.Errorf("%w: fstat directory: %v", ErrInvalidOptions, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != uint32(unix.Geteuid()) || st.Mode&0o777 != 0o700 {
		unix.Close(fd)
		return -1, fmt.Errorf("%w: directory is not a private 0700 directory of uid %d", ErrInvalidOptions, unix.Geteuid())
	}
	return fd, nil
}

// absent requires name not to exist in the directory dirfd, without following symlinks.
func absent(dirfd int, name string) error {
	var st unix.Stat_t
	err := unix.Fstatat(dirfd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
	switch {
	case err == nil:
		return fmt.Errorf("%w: %s exists", os.ErrExist, name)
	case errors.Is(err, unix.ENOENT):
		return nil
	}
	return fmt.Errorf("%w: stat %s: %v", ErrInvalidOptions, name, err)
}

// writeOwnerState publishes state as owner.json in dirfd: a private temporary file is written and
// synced, renamed into place without replacing anything, and the directory synced. On error the
// temporary file is removed and owner.json is untouched.
func writeOwnerState(dirfd int, state ownerState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("supervisor: encode %s: %w", ownerFile, err)
	}
	if len(data) > maxOwnerFile {
		return fmt.Errorf("supervisor: %s of %d bytes exceeds %d", ownerFile, len(data), maxOwnerFile)
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Errorf("supervisor: random temp name: %w", err)
	}
	name := ".owner." + hex.EncodeToString(b[:]) + ".tmp"
	fd, err := unix.Openat(dirfd, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return fmt.Errorf("supervisor: create %s: %w", name, err)
	}
	err = pwriteFull(fd, data, 0)
	if err == nil {
		err = unix.Fsync(fd)
	}
	if cerr := unix.Close(fd); err == nil && cerr != nil {
		err = cerr
	}
	if err == nil {
		err = unix.Renameat2(dirfd, name, dirfd, ownerFile, unix.RENAME_NOREPLACE)
	}
	if err != nil {
		_ = unix.Unlinkat(dirfd, name, 0)
		return fmt.Errorf("supervisor: publish %s: %w", ownerFile, err)
	}
	if err := unix.Fsync(dirfd); err != nil {
		return fmt.Errorf("supervisor: sync directory: %w", err)
	}
	return nil
}

// readOwnerState reads and strictly decodes owner.json from dirfd: a private 0600 regular file of
// the effective uid with one link, at most maxOwnerFile bytes, valid UTF-8, exactly one JSON
// object without unknown fields, describing this boot and format.
func readOwnerState(dirfd int) (ownerState, error) {
	var state ownerState
	fd, err := unix.Openat(dirfd, ownerFile, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return state, fmt.Errorf("%w: %s: %v", ErrLostEpoch, ownerFile, os.ErrNotExist)
		}
		return state, fmt.Errorf("%w: open %s: %v", ErrLostEpoch, ownerFile, err)
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return state, fmt.Errorf("%w: fstat %s: %v", ErrLostEpoch, ownerFile, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != uint32(unix.Geteuid()) || st.Nlink != 1 || st.Mode&0o777 != 0o600 {
		return state, fmt.Errorf("%w: %s is not a private 0600 regular file of uid %d with one link", ErrLostEpoch, ownerFile, unix.Geteuid())
	}
	if st.Size > maxOwnerFile {
		return state, fmt.Errorf("%w: %s is %d bytes, limit %d", ErrLostEpoch, ownerFile, st.Size, maxOwnerFile)
	}
	data := make([]byte, st.Size)
	if err := preadFull(fd, data, 0); err != nil {
		return state, fmt.Errorf("%w: read %s: %v", ErrLostEpoch, ownerFile, err)
	}
	if !utf8.Valid(data) {
		return state, fmt.Errorf("%w: %s is not valid UTF-8", ErrLostEpoch, ownerFile)
	}
	if err := checkOwnerShape(data); err != nil {
		return state, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&state); err != nil {
		return state, fmt.Errorf("%w: decode %s: %v", ErrLostEpoch, ownerFile, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return state, fmt.Errorf("%w: data after the JSON value of %s", ErrLostEpoch, ownerFile)
	}
	boot, err := currentBootID()
	if err != nil {
		return state, err
	}
	switch {
	case state.Format != ownerFormat:
		return state, fmt.Errorf("%w: %s format %d, want %d", ErrLostEpoch, ownerFile, state.Format, ownerFormat)
	case state.WireMajor != ipc.PublicationVersion:
		return state, fmt.Errorf("%w: %s wire major %d, want %d", ErrLostEpoch, ownerFile, state.WireMajor, ipc.PublicationVersion)
	case state.BootID != boot:
		return state, fmt.Errorf("%w: %s was written in boot %q, now %q", ErrLostEpoch, ownerFile, state.BootID, boot)
	case state.Epoch == 0:
		return state, fmt.Errorf("%w: %s epoch is zero", ErrLostEpoch, ownerFile)
	case state.Policy.WindowNS <= 0 || state.Policy.MaxSegmentNS <= 0:
		return state, fmt.Errorf("%w: %s policy %+v is not positive", ErrLostEpoch, ownerFile, state.Policy)
	case len(state.Tracks) != state.Config.Tracks:
		return state, fmt.Errorf("%w: %s lists %d descriptors, config has %d tracks", ErrLostEpoch, ownerFile, len(state.Tracks), state.Config.Tracks)
	}
	if err := checkOwner(WorkerOwner{UID: state.Worker.UID, PID: state.Worker.PID, Ino: state.Worker.Ino}); err != nil {
		return state, fmt.Errorf("%w: %s: %v", ErrLostEpoch, ownerFile, err)
	}
	names := make(map[string]bool, len(state.Tracks))
	for i, id := range state.Tracks {
		switch {
		case id.Track != i:
			return state, fmt.Errorf("%w: %s descriptor %d names track %d", ErrLostEpoch, ownerFile, i, id.Track)
		case id.Name == "" || names[id.Name]:
			return state, fmt.Errorf("%w: %s descriptor %d name %q is empty or repeated", ErrLostEpoch, ownerFile, i, id.Name)
		}
		names[id.Name] = true
	}
	return state, nil
}

// The properties every object of owner.json must carry, exactly and non-null.
var (
	ownerProps  = []string{"format", "wire_major", "boot_id", "run", "source", "epoch", "config", "policy", "worker", "tracks"}
	configProps = []string{"mode", "codec", "tracks", "capacity"}
	policyProps = []string{"window_ns", "max_segment_ns"}
	workerProps = []string{"uid", "pid", "ino"}
	trackProps  = []string{"track", "name", "dev", "ino", "size"}
)

// checkOwnerShape verifies the shape of owner.json before typed decoding, which cannot see it: a
// missing property, a null and a case-variant name all decode silently to a zero value or to the
// right field. Every object must carry exactly its required properties, case-sensitively and
// non-null; tracks must be an array of such objects. Scalar types and values are left to the typed
// decode and the semantic checks.
func checkOwnerShape(data []byte) error {
	root, err := ownerObject(data, ownerFile, ownerProps)
	if err != nil {
		return err
	}
	if _, err := ownerObject(root["config"], "config", configProps); err != nil {
		return err
	}
	if _, err := ownerObject(root["policy"], "policy", policyProps); err != nil {
		return err
	}
	if _, err := ownerObject(root["worker"], "worker", workerProps); err != nil {
		return err
	}
	raw := root["tracks"]
	if firstByte(raw) != '[' {
		return fmt.Errorf("%w: %s tracks is not an array", ErrLostEpoch, ownerFile)
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return fmt.Errorf("%w: %s tracks: %v", ErrLostEpoch, ownerFile, err)
	}
	for i, item := range items {
		if _, err := ownerObject(item, fmt.Sprintf("tracks[%d]", i), trackProps); err != nil {
			return err
		}
	}
	return nil
}

// ownerObject parses raw as a JSON object carrying exactly the required properties, none null.
func ownerObject(raw json.RawMessage, what string, required []string) (map[string]json.RawMessage, error) {
	if firstByte(raw) != '{' {
		return nil, fmt.Errorf("%w: %s is not a JSON object", ErrLostEpoch, what)
	}
	var props map[string]json.RawMessage
	if err := json.Unmarshal(raw, &props); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrLostEpoch, what, err)
	}
	for name, value := range props {
		switch {
		case !slices.Contains(required, name):
			return nil, fmt.Errorf("%w: %s has unexpected property %q", ErrLostEpoch, what, name)
		case firstByte(value) == 'n':
			return nil, fmt.Errorf("%w: %s property %q is null", ErrLostEpoch, what, name)
		}
	}
	for _, name := range required {
		if _, ok := props[name]; !ok {
			return nil, fmt.Errorf("%w: %s lacks property %q", ErrLostEpoch, what, name)
		}
	}
	return props, nil
}

// firstByte returns the first byte of the JSON value raw past any leading space, or 0 if none.
func firstByte(raw []byte) byte {
	if raw = bytes.TrimSpace(raw); len(raw) == 0 {
		return 0
	}
	return raw[0]
}

// checkOwner requires a positive PID, a nonzero pidfs inode and a non-negative UID.
func checkOwner(o WorkerOwner) error {
	switch {
	case o.PID <= 0:
		return fmt.Errorf("%w: worker pid %d is not positive", ErrInvalidOptions, o.PID)
	case o.Ino == 0:
		return fmt.Errorf("%w: worker pidfs inode is zero", ErrInvalidOptions)
	case o.UID < 0:
		return fmt.Errorf("%w: worker uid %d is negative", ErrInvalidOptions, o.UID)
	}
	return nil
}

// currentBootID returns the kernel's boot id, read with a bounded buffer.
func currentBootID() (string, error) {
	f, err := os.Open(bootIDPath)
	if err != nil {
		return "", fmt.Errorf("supervisor: boot id: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 128))
	if err != nil {
		return "", fmt.Errorf("supervisor: read boot id: %w", err)
	}
	id := strings.TrimSuffix(string(b), "\n")
	if id == "" || len(b) == 128 || strings.ContainsAny(id, "\n\r\t ") || !utf8.ValidString(id) {
		return "", errors.New("supervisor: unusable boot id")
	}
	return id, nil
}

// pwriteFull writes all of b at off, retrying short writes and EINTR.
func pwriteFull(fd int, b []byte, off int64) error {
	for len(b) > 0 {
		n, err := unix.Pwrite(fd, b, off)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		b, off = b[n:], off+int64(n)
	}
	return nil
}

// preadFull fills b from off, retrying short reads and EINTR.
func preadFull(fd int, b []byte, off int64) error {
	for len(b) > 0 {
		n, err := unix.Pread(fd, b, off)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrUnexpectedEOF
		}
		b, off = b[n:], off+int64(n)
	}
	return nil
}

// closeFD closes one descriptor; a failed close is not retried.
func closeFD(fd int) { _ = unix.Close(fd) }

// closeUnique closes each non-negative descriptor in fds exactly once, however often it is listed.
func closeUnique(fds []int) {
	for i, fd := range fds {
		if fd < 0 {
			continue
		}
		seen := false
		for _, prev := range fds[:i] {
			if prev == fd {
				seen = true
				break
			}
		}
		if !seen {
			_ = unix.Close(fd)
		}
	}
}
