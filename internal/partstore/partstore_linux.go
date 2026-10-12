//go:build linux

package partstore

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"golang.org/x/sys/unix"
)

// maxNameLen is the kernel's memfd name limit (NAME_MAX minus the "memfd:" prefix).
const maxNameLen = 249

// requiredSeals fix the size of every track; forbiddenSeals would stop the worker writing.
// F_SEAL_SEAL keeps anyone from adding a write seal later.
const (
	requiredSeals  = unix.F_SEAL_SHRINK | unix.F_SEAL_GROW | unix.F_SEAL_SEAL
	forbiddenSeals = unix.F_SEAL_WRITE | unix.F_SEAL_FUTURE_WRITE
)

// trackLabels name the tracks in ipc track-id order.
var trackLabels = [ipc.Tracks1v1]string{"vhi", "vmid", "vlo", "a", "b"}

// EpochStore holds the memfds of one source epoch, in track order, each mapped read-only. The
// zero value and a nil pointer behave as closed. A store contains a lock and must not be copied
// after first use.
type EpochStore struct {
	mu   sync.RWMutex
	open bool
	ids  []ipc.FDIdentity
	fds  []int
	maps [][]byte
}

// AllocateEpoch creates one memfd per configured track for the epoch, each ftruncated to
// cfg.Capacity and sealed against shrinking, growing and further sealing, but never against
// writing. The descriptors are O_RDWR and close-on-exec; the store maps them read-only. Invalid
// arguments are ErrInvalidConfig; on any error nothing is left open.
func AllocateEpoch(run, source string, epoch uint64, cfg ipc.Config) (*EpochStore, error) {
	names, err := setup(run, source, epoch, cfg)
	if err != nil {
		return nil, err
	}
	fds := make([]int, 0, len(names))
	ids := make([]ipc.FDIdentity, 0, len(names))
	for track, name := range names {
		fd, err := unix.MemfdCreate(name, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
		if err != nil {
			closeUnique(fds)
			return nil, fmt.Errorf("partstore: memfd_create %s: %w", name, err)
		}
		fds = append(fds, fd)
		if err := unix.Ftruncate(fd, cfg.Capacity); err != nil {
			closeUnique(fds)
			return nil, fmt.Errorf("partstore: ftruncate %s: %w", name, err)
		}
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, requiredSeals); err != nil {
			closeUnique(fds)
			return nil, fmt.Errorf("partstore: seal %s: %w", name, err)
		}
		id, err := identify(fd, track, name)
		if err != nil {
			closeUnique(fds)
			return nil, err
		}
		ids = append(ids, id)
	}
	return mapAll(fds, ids, cfg.Capacity)
}

// AdoptEpoch takes ownership of the descriptors in fds, immediately and on every path including
// errors: the caller must transfer descriptors it owns, never another store's. It accepts them
// only if fds[i] and expected[i] both describe track i of this epoch exactly: the canonical name
// (checked through /proc/self/fd), the expected kernel dev/ino and size, a regular file of
// cfg.Capacity bytes, O_RDWR, the required seals and no write seal. Negative or repeated numbers,
// aliased inodes and count mismatches are refused. Each adopted descriptor is set close-on-exec.
// Invalid run, source, epoch or cfg is ErrInvalidConfig; every other refusal is
// ErrIdentityMismatch. On error every supplied non-negative descriptor has been closed once.
func AdoptEpoch(run, source string, epoch uint64, cfg ipc.Config, fds []int, expected []ipc.FDIdentity) (*EpochStore, error) {
	owned := append([]int(nil), fds...)
	names, err := setup(run, source, epoch, cfg)
	if err != nil {
		closeUnique(owned)
		return nil, err
	}
	if err := checkHandles(owned, expected, len(names)); err != nil {
		closeUnique(owned)
		return nil, err
	}
	ids := make([]ipc.FDIdentity, 0, len(names))
	for track, name := range names {
		id, err := verify(owned[track], track, name, cfg.Capacity, expected[track])
		if err != nil {
			closeUnique(owned)
			return nil, err
		}
		for _, prev := range ids {
			if prev.Dev == id.Dev && prev.Ino == id.Ino {
				closeUnique(owned)
				return nil, fmt.Errorf("%w: track %d is the same memfd as track %d", ErrIdentityMismatch, track, prev.Track)
			}
		}
		ids = append(ids, id)
	}
	return mapAll(owned, ids, cfg.Capacity)
}

// Identities returns a copy of the kernel identities of the tracks, in track order.
func (s *EpochStore) Identities() ([]ipc.FDIdentity, error) {
	if s == nil {
		return nil, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.open {
		return nil, ErrClosed
	}
	return append([]ipc.FDIdentity(nil), s.ids...), nil
}

// DupFDs returns a new close-on-exec duplicate of each track's descriptor, in track order. The
// duplicates are the caller's: they stay valid after Close and the caller closes them, typically
// after ipc.WriteMsg has sent them. On error no duplicate is left open.
func (s *EpochStore) DupFDs() ([]int, error) {
	if s == nil {
		return nil, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.open {
		return nil, ErrClosed
	}
	dups := make([]int, 0, len(s.fds))
	for track, fd := range s.fds {
		d, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			closeUnique(dups)
			return nil, fmt.Errorf("partstore: dup track %d: %w", track, err)
		}
		dups = append(dups, d)
	}
	return dups, nil
}

// ReadSpan returns a new copy of length bytes at offset in the track's memory. The copy is the
// caller's and stays valid after Close. A span outside 0..capacity is ErrSpanOutOfBounds. Whether
// the span is published, and not being rewritten, is the caller's to know.
func (s *EpochStore) ReadSpan(track int, offset, length int64) ([]byte, error) {
	if s == nil {
		return nil, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.open {
		return nil, ErrClosed
	}
	if track < 0 || track >= len(s.maps) {
		return nil, fmt.Errorf("%w: track %d", ErrSpanOutOfBounds, track)
	}
	m := s.maps[track]
	capacity := int64(len(m))
	if offset < 0 || length <= 0 || offset > capacity || length > capacity-offset {
		return nil, fmt.Errorf("%w: track %d span %d+%d, capacity %d", ErrSpanOutOfBounds, track, offset, length, capacity)
	}
	out := make([]byte, length)
	copy(out, m[offset:offset+length])
	return out, nil
}

// Close unmaps and closes every track once and reports every failure joined. It is idempotent
// and harmless on a nil or zero store. Duplicates from DupFDs and copies from ReadSpan survive it.
func (s *EpochStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return nil
	}
	s.open = false
	var errs []error
	for track, m := range s.maps {
		if err := unix.Munmap(m); err != nil {
			errs = append(errs, fmt.Errorf("partstore: munmap track %d: %w", track, err))
		}
	}
	for track, fd := range s.fds {
		if err := unix.Close(fd); err != nil {
			errs = append(errs, fmt.Errorf("partstore: close track %d: %w", track, err))
		}
	}
	s.maps, s.fds, s.ids = nil, nil, nil
	return errors.Join(errs...)
}

// setup validates the epoch's identity and config as the ipc setup hello would and returns the
// canonical memfd name of each configured track.
func setup(run, source string, epoch uint64, cfg ipc.Config) ([]string, error) {
	if epoch == 0 {
		return nil, fmt.Errorf("%w: epoch must be positive", ErrInvalidConfig)
	}
	hello := ipc.Hello{Version: ipc.Version, Run: run, Source: source, PID: os.Getpid(), Config: cfg}
	if err := ipc.ValidatePacket(ipc.Packet{Type: ipc.TypeHello, Hello: &hello}); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	if cfg.Capacity > int64(math.MaxInt) {
		return nil, fmt.Errorf("%w: capacity %d does not fit a mapping", ErrInvalidConfig, cfg.Capacity)
	}
	names := make([]string, cfg.Tracks)
	for track := range names {
		names[track] = canonicalName(run, source, epoch, track)
		if n := len(names[track]); n > maxNameLen {
			return nil, fmt.Errorf("%w: memfd name of %d bytes exceeds %d", ErrInvalidConfig, n, maxNameLen)
		}
	}
	return names, nil
}

// canonicalName is "egress.<run>.<source>.e<epoch>.<label>" with run and source in unpadded
// URL-safe base64, so that any UTF-8 identifier yields unambiguous, delimiter-free components.
// The name is a label only; identity is the kernel's dev/ino.
func canonicalName(run, source string, epoch uint64, track int) string {
	return "egress." + base64.RawURLEncoding.EncodeToString([]byte(run)) +
		"." + base64.RawURLEncoding.EncodeToString([]byte(source)) +
		".e" + strconv.FormatUint(epoch, 10) + "." + trackLabels[track]
}

// checkHandles refuses a descriptor or expected list of the wrong length, a negative descriptor
// number and a number listed twice, before any descriptor is inspected.
func checkHandles(fds []int, expected []ipc.FDIdentity, count int) error {
	switch {
	case len(fds) != count:
		return fmt.Errorf("%w: %d descriptors, want %d", ErrIdentityMismatch, len(fds), count)
	case len(expected) != count:
		return fmt.Errorf("%w: %d expected identities, want %d", ErrIdentityMismatch, len(expected), count)
	}
	for i, fd := range fds {
		if fd < 0 {
			return fmt.Errorf("%w: track %d descriptor %d is negative", ErrIdentityMismatch, i, fd)
		}
		for _, prev := range fds[:i] {
			if prev == fd {
				return fmt.Errorf("%w: descriptor %d listed twice", ErrIdentityMismatch, fd)
			}
		}
	}
	return nil
}

// identify returns the kernel identity of fd as track's memfd named name.
func identify(fd, track int, name string) (ipc.FDIdentity, error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return ipc.FDIdentity{}, fmt.Errorf("%w: fstat track %d: %w", ErrIdentityMismatch, track, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return ipc.FDIdentity{}, fmt.Errorf("%w: track %d mode %#o is not a regular file", ErrIdentityMismatch, track, st.Mode)
	}
	return ipc.FDIdentity{Track: track, Name: name, Dev: uint64(st.Dev), Ino: st.Ino, Size: st.Size}, nil
}

// verify checks that fd is exactly the memfd that want describes for track, and that want itself
// names track with the canonical name and capacity, then sets the descriptor close-on-exec.
func verify(fd, track int, name string, capacity int64, want ipc.FDIdentity) (ipc.FDIdentity, error) {
	switch {
	case want.Track != track:
		return ipc.FDIdentity{}, fmt.Errorf("%w: expected[%d] describes track %d", ErrIdentityMismatch, track, want.Track)
	case want.Name != name:
		return ipc.FDIdentity{}, fmt.Errorf("%w: track %d expected name %q, want %q", ErrIdentityMismatch, track, want.Name, name)
	case want.Size != capacity:
		return ipc.FDIdentity{}, fmt.Errorf("%w: track %d expected size %d, want capacity %d", ErrIdentityMismatch, track, want.Size, capacity)
	}
	id, err := identify(fd, track, name)
	if err != nil {
		return ipc.FDIdentity{}, err
	}
	if id.Dev != want.Dev || id.Ino != want.Ino || id.Size != capacity {
		return ipc.FDIdentity{}, fmt.Errorf("%w: track %d is dev %d ino %d size %d, expected dev %d ino %d size %d",
			ErrIdentityMismatch, track, id.Dev, id.Ino, id.Size, want.Dev, want.Ino, capacity)
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		return ipc.FDIdentity{}, fmt.Errorf("%w: F_GETFL track %d: %w", ErrIdentityMismatch, track, err)
	}
	if flags&unix.O_ACCMODE != unix.O_RDWR {
		return ipc.FDIdentity{}, fmt.Errorf("%w: track %d access mode %#o is not O_RDWR", ErrIdentityMismatch, track, flags&unix.O_ACCMODE)
	}
	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	if err != nil {
		return ipc.FDIdentity{}, fmt.Errorf("%w: F_GET_SEALS track %d: %w", ErrIdentityMismatch, track, err)
	}
	if seals&requiredSeals != requiredSeals || seals&forbiddenSeals != 0 {
		return ipc.FDIdentity{}, fmt.Errorf("%w: track %d seals %#x, want %#x and no write seal", ErrIdentityMismatch, track, seals, requiredSeals)
	}
	link, err := os.Readlink("/proc/self/fd/" + strconv.Itoa(fd))
	if err != nil {
		return ipc.FDIdentity{}, fmt.Errorf("%w: track %d: %w", ErrIdentityMismatch, track, err)
	}
	if target := "/memfd:" + name + " (deleted)"; link != target {
		return ipc.FDIdentity{}, fmt.Errorf("%w: track %d is %q, want %q", ErrIdentityMismatch, track, link, target)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		return ipc.FDIdentity{}, fmt.Errorf("%w: F_SETFD track %d: %w", ErrIdentityMismatch, track, err)
	}
	return id, nil
}

// mapAll maps every descriptor read-only and shared and returns the open store. On error every
// mapping made so far is unmapped and every descriptor closed.
func mapAll(fds []int, ids []ipc.FDIdentity, capacity int64) (*EpochStore, error) {
	maps := make([][]byte, 0, len(fds))
	for track, fd := range fds {
		m, err := unix.Mmap(fd, 0, int(capacity), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			for _, done := range maps {
				_ = unix.Munmap(done)
			}
			closeUnique(fds)
			return nil, fmt.Errorf("partstore: mmap track %d: %w", track, err)
		}
		maps = append(maps, m)
	}
	return &EpochStore{open: true, ids: ids, fds: fds, maps: maps}, nil
}

// closeUnique closes each non-negative descriptor in fds exactly once, however often it is
// listed. A failed close is not retried.
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
