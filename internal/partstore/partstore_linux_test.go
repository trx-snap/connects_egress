//go:build linux

package partstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"golang.org/x/sys/unix"
)

// expectedCanonicalName computes the canonical memfd name independently from the production code.
func expectedCanonicalName(run, source string, epoch uint64, track int) string {
	labels := [5]string{"vhi", "vmid", "vlo", "a", "b"}
	return "egress." + base64.RawURLEncoding.EncodeToString([]byte(run)) +
		"." + base64.RawURLEncoding.EncodeToString([]byte(source)) +
		".e" + strconv.FormatUint(epoch, 10) + "." + labels[track]
}

func isFDClosed(fd int) bool {
	_, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	return errors.Is(err, unix.EBADF)
}

func isFDOpen(fd int) bool {
	return !isFDClosed(fd)
}

func isCloexec(t *testing.T, fd int) bool {
	t.Helper()
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		t.Fatalf("F_GETFD %d: %v", fd, err)
	}
	return flags&unix.FD_CLOEXEC != 0
}

func openFDs(t *testing.T) []int {
	t.Helper()
	d, err := os.Open("/proc/self/fd")
	if err != nil {
		t.Fatalf("open /proc/self/fd: %v", err)
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		t.Fatalf("read /proc/self/fd: %v", err)
	}
	self := int(d.Fd())
	var fds []int
	for _, name := range names {
		fd, err := strconv.Atoi(name)
		if err != nil {
			t.Fatalf("/proc/self/fd entry %q: %v", name, err)
		}
		if fd != self {
			fds = append(fds, fd)
		}
	}
	slices.Sort(fds)
	return fds
}

func countMemfdMaps(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Fatalf("read /proc/self/maps: %v", err)
	}
	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, "/memfd:") {
			count++
		}
	}
	return count
}

func verifyMappingReadOnly(t *testing.T, m []byte, expectedIno uint64, expectedName string) {
	t.Helper()
	if len(m) == 0 {
		t.Fatalf("empty mapping")
	}
	ptr := uintptr(unsafe.Pointer(&m[0]))
	data, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Fatalf("read /proc/self/maps: %v", err)
	}
	found := false
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		parts := strings.Split(fields[0], "-")
		if len(parts) != 2 {
			continue
		}
		start, err1 := strconv.ParseUint(parts[0], 16, 64)
		end, err2 := strconv.ParseUint(parts[1], 16, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		if uint64(ptr) >= start && uint64(ptr) < end {
			found = true
			perms := fields[1]
			if len(perms) < 4 || perms[0] != 'r' || perms[1] != '-' || perms[3] != 's' {
				t.Fatalf("mapping perms %q, want r--s (read-only shared)", perms)
			}
			ino, err := strconv.ParseUint(fields[4], 10, 64)
			if err != nil || ino != expectedIno {
				t.Fatalf("mapping ino %d, want %d", ino, expectedIno)
			}
			if len(fields) >= 6 && !strings.Contains(fields[5], "/memfd:"+expectedName) {
				t.Fatalf("mapping pathname %q does not contain /memfd:%s", fields[5], expectedName)
			}
			break
		}
	}
	if !found {
		t.Fatalf("mapping pointer %x not found in /proc/self/maps", ptr)
	}
}

func seqpair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	clean0 := true
	clean1 := true
	defer func() {
		if clean0 {
			unix.Close(fds[0])
		}
		if clean1 {
			unix.Close(fds[1])
		}
	}()

	f0 := os.NewFile(uintptr(fds[0]), "seqpair0")
	c0, err0 := net.FileConn(f0)
	f0.Close()
	clean0 = false
	if err0 != nil {
		t.Fatalf("FileConn 0: %v", err0)
	}

	f1 := os.NewFile(uintptr(fds[1]), "seqpair1")
	c1, err1 := net.FileConn(f1)
	f1.Close()
	clean1 = false
	if err1 != nil {
		c0.Close()
		t.Fatalf("FileConn 1: %v", err1)
	}

	uc0, ok0 := c0.(*net.UnixConn)
	uc1, ok1 := c1.(*net.UnixConn)
	if !ok0 || !ok1 {
		c0.Close()
		c1.Close()
		t.Fatalf("FileConn returned %T, %T, want *net.UnixConn", c0, c1)
	}
	t.Cleanup(func() { uc0.Close(); uc1.Close() })
	return uc0, uc1
}

func createRealMemfd(t *testing.T, name string, capacity int64, seals int) int {
	t.Helper()
	fd, err := unix.MemfdCreate(name, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatalf("MemfdCreate %s: %v", name, err)
	}
	if err := unix.Ftruncate(fd, capacity); err != nil {
		unix.Close(fd)
		t.Fatalf("Ftruncate: %v", err)
	}
	if seals != 0 {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals); err != nil {
			unix.Close(fd)
			t.Fatalf("F_ADD_SEALS: %v", err)
		}
	}
	return fd
}

func createReadOnlyMemfd(t *testing.T, name string, capacity int64) int {
	t.Helper()
	fd := createRealMemfd(t, name, capacity, requiredSeals)
	roFD, err := unix.Open(fmt.Sprintf("/proc/self/fd/%d", fd), unix.O_RDONLY|unix.O_CLOEXEC, 0)
	unix.Close(fd)
	if err != nil {
		t.Fatalf("open ro: %v", err)
	}
	return roFD
}

func createRealPipe(t *testing.T) (r, w int) {
	t.Helper()
	var p [2]int
	if err := unix.Pipe2(p[:], unix.O_CLOEXEC); err != nil {
		t.Fatalf("Pipe2: %v", err)
	}
	return p[0], p[1]
}

func createRealRegularFile(t *testing.T, capacity int64) int {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "real-reg-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := f.Name()
	if err := f.Truncate(capacity); err != nil {
		f.Close()
		t.Fatalf("Truncate: %v", err)
	}
	f.Close()
	opened, err := unix.Open(name, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("Open reg file: %v", err)
	}
	return opened
}

// -----------------------------------------------------------------------------
// LANE 1: Allocation Matrix
// -----------------------------------------------------------------------------

func TestAllocationMatrix(t *testing.T) {
	modes := []struct {
		mode   string
		tracks int
	}{
		{ipc.ModeSolo, ipc.TracksSolo},
		{ipc.Mode1v1, ipc.Tracks1v1},
	}
	codecs := []string{ipc.CodecH264, ipc.CodecHEVC}
	capacities := []int64{4096, 16 * 1024 * 1024, 32 * 1024 * 1024}

	for _, m := range modes {
		for _, c := range codecs {
			for _, cap := range capacities {
				name := fmt.Sprintf("%s_%s_%d", m.mode, c, cap)
				t.Run(name, func(t *testing.T) {
					run := fmt.Sprintf("run-%s-%s-%d", m.mode, c, cap)
					source := fmt.Sprintf("src-%s-%s", m.mode, c)
					epoch := uint64(42)
					cfg := ipc.Config{
						Mode:     m.mode,
						Codec:    c,
						Tracks:   m.tracks,
						Capacity: cap,
					}
					store, err := AllocateEpoch(run, source, epoch, cfg)
					if err != nil {
						t.Fatalf("AllocateEpoch: %v", err)
					}
					defer func() {
						if err := store.Close(); err != nil {
							t.Fatalf("Close: %v", err)
						}
					}()

					ids, err := store.Identities()
					if err != nil {
						t.Fatalf("Identities: %v", err)
					}
					if len(ids) != cfg.Tracks {
						t.Fatalf("got %d identities, want %d", len(ids), cfg.Tracks)
					}

					seenInos := make(map[uint64]bool)
					for _, id := range ids {
						if seenInos[id.Ino] {
							t.Fatalf("duplicate inode %d across tracks", id.Ino)
						}
						seenInos[id.Ino] = true
					}

					dups, err := store.DupFDs()
					if err != nil {
						t.Fatalf("DupFDs: %v", err)
					}
					defer func() {
						for _, d := range dups {
							unix.Close(d)
						}
					}()

					for track := 0; track < cfg.Tracks; track++ {
						fd := dups[track]
						id := ids[track]
						wantName := expectedCanonicalName(run, source, epoch, track)
						if id.Name != wantName {
							t.Errorf("track %d name %q, want %q", track, id.Name, wantName)
						}
						if id.Track != track {
							t.Errorf("id.Track = %d, want %d", id.Track, track)
						}
						if id.Size != cfg.Capacity {
							t.Errorf("id.Size = %d, want %d", id.Size, cfg.Capacity)
						}

						link, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
						if err != nil {
							t.Fatalf("Readlink fd %d: %v", fd, err)
						}
						wantLink := "/memfd:" + wantName + " (deleted)"
						if link != wantLink {
							t.Errorf("fd %d link %q, want %q", fd, link, wantLink)
						}

						var st unix.Stat_t
						if err := unix.Fstat(fd, &st); err != nil {
							t.Fatalf("fstat fd %d: %v", fd, err)
						}
						if st.Mode&unix.S_IFMT != unix.S_IFREG {
							t.Errorf("fd %d mode %#o is not S_IFREG", fd, st.Mode)
						}
						if uint64(st.Dev) != id.Dev || st.Ino != id.Ino || st.Size != cfg.Capacity {
							t.Errorf("fd %d stat dev/ino/size %d/%d/%d, want %d/%d/%d",
								fd, st.Dev, st.Ino, st.Size, id.Dev, id.Ino, cfg.Capacity)
						}

						fl, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
						if err != nil {
							t.Fatalf("F_GETFL fd %d: %v", fd, err)
						}
						if fl&unix.O_ACCMODE != unix.O_RDWR {
							t.Errorf("fd %d access mode %#o, want O_RDWR", fd, fl&unix.O_ACCMODE)
						}

						if !isCloexec(t, fd) {
							t.Errorf("fd %d is not close-on-exec", fd)
						}

						seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
						if err != nil {
							t.Fatalf("F_GET_SEALS fd %d: %v", fd, err)
						}
						if seals&requiredSeals != requiredSeals {
							t.Errorf("fd %d seals %#x missing required %#x", fd, seals, requiredSeals)
						}
						if seals&forbiddenSeals != 0 {
							t.Errorf("fd %d seals %#x has forbidden seals", fd, seals)
						}

						if err := unix.Ftruncate(fd, cfg.Capacity+1); !errors.Is(err, unix.EPERM) {
							t.Errorf("grow ftruncate err = %v, want EPERM", err)
						}
						if err := unix.Ftruncate(fd, cfg.Capacity-1); !errors.Is(err, unix.EPERM) {
							t.Errorf("shrink ftruncate err = %v, want EPERM", err)
						}

						if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_WRITE); !errors.Is(err, unix.EPERM) {
							t.Errorf("add write seal err = %v, want EPERM", err)
						}

						testByte := []byte{byte(track + 1)}
						if n, err := unix.Pwrite(fd, testByte, 0); err != nil || n != 1 {
							t.Errorf("Pwrite track %d: n=%d, err=%v", track, n, err)
						}

						verifyMappingReadOnly(t, store.maps[track], id.Ino, wantName)
					}
				})
			}
		}
	}
}

// -----------------------------------------------------------------------------
// LANE 2: Invalid Constructor Inputs
// -----------------------------------------------------------------------------

func TestInvalidConstructorInputs(t *testing.T) {
	validSolo := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 4096}

	cases := []struct {
		name   string
		run    string
		source string
		epoch  uint64
		cfg    ipc.Config
	}{
		{"empty_run", "", "src-1", 1, validSolo},
		{"empty_source", "run-1", "", 1, validSolo},
		{"invalid_utf8_run", string([]byte{0xff, 0xfe}), "src-1", 1, validSolo},
		{"invalid_utf8_source", "run-1", string([]byte{0x80, 0x81}), 1, validSolo},
		{"epoch_zero", "run-1", "src-1", 0, validSolo},
		{"invalid_mode", "run-1", "src-1", 1, ipc.Config{Mode: "quad", Codec: ipc.CodecH264, Tracks: 4, Capacity: 4096}},
		{"invalid_codec", "run-1", "src-1", 1, ipc.Config{Mode: ipc.ModeSolo, Codec: "av1", Tracks: 4, Capacity: 4096}},
		{"solo_with_5_tracks", "run-1", "src-1", 1, ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: 5, Capacity: 4096}},
		{"solo_with_3_tracks", "run-1", "src-1", 1, ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: 3, Capacity: 4096}},
		{"1v1_with_4_tracks", "run-1", "src-1", 1, ipc.Config{Mode: ipc.Mode1v1, Codec: ipc.CodecH264, Tracks: 4, Capacity: 4096}},
		{"1v1_with_6_tracks", "run-1", "src-1", 1, ipc.Config{Mode: ipc.Mode1v1, Codec: ipc.CodecH264, Tracks: 6, Capacity: 4096}},
		{"capacity_zero", "run-1", "src-1", 1, ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: 4, Capacity: 0}},
		{"capacity_negative", "run-1", "src-1", 1, ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: 4, Capacity: -4096}},
		{"oversized_canonical_name", strings.Repeat("A", 200), "src-1", 1, validSolo},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s1, err := AllocateEpoch(tc.run, tc.source, tc.epoch, tc.cfg)
			if s1 != nil || !errors.Is(err, ErrInvalidConfig) {
				if s1 != nil {
					s1.Close()
				}
				t.Errorf("AllocateEpoch: s1=%v, err=%v, want ErrInvalidConfig", s1, err)
			}

			s2, err := AdoptEpoch(tc.run, tc.source, tc.epoch, tc.cfg, nil, nil)
			if s2 != nil || !errors.Is(err, ErrInvalidConfig) {
				if s2 != nil {
					s2.Close()
				}
				t.Errorf("AdoptEpoch: s2=%v, err=%v, want ErrInvalidConfig", s2, err)
			}
		})
	}

	t.Run("valid_unicode_delimiters_max_epoch", func(t *testing.T) {
		run := "run/with?delim#and \x00 NUL & 🌟 unicode"
		source := "src:sub/dir\x00✓"
		epoch := uint64(math.MaxUint64)
		cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 8192}

		store, err := AllocateEpoch(run, source, epoch, cfg)
		if err != nil {
			t.Fatalf("AllocateEpoch: %v", err)
		}
		defer store.Close()

		ids, err := store.Identities()
		if err != nil {
			t.Fatalf("Identities: %v", err)
		}
		for tr, id := range ids {
			wantName := expectedCanonicalName(run, source, epoch, tr)
			if id.Name != wantName {
				t.Errorf("track %d name %q, want %q", tr, id.Name, wantName)
			}
		}
	})
}

// -----------------------------------------------------------------------------
// LANE 3: Adopt Success and All Failure Paths (Shared Cases)
// -----------------------------------------------------------------------------

func TestAdoptSuccess(t *testing.T) {
	run := "run-adopt-success"
	source := "src-adopt-success"
	epoch := uint64(77)
	cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecHEVC, Tracks: ipc.TracksSolo, Capacity: 8192}

	orig, err := AllocateEpoch(run, source, epoch, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}
	defer orig.Close()

	dups, err := orig.DupFDs()
	if err != nil {
		t.Fatalf("DupFDs: %v", err)
	}
	ids, err := orig.Identities()
	if err != nil {
		t.Fatalf("Identities: %v", err)
	}

	if _, err := unix.FcntlInt(uintptr(dups[0]), unix.F_SETFD, 0); err != nil {
		t.Fatalf("F_SETFD: %v", err)
	}
	if isCloexec(t, dups[0]) {
		t.Fatalf("CLOEXEC was not cleared")
	}

	passFDs := slices.Clone(dups)
	passIDs := slices.Clone(ids)

	adopted, err := AdoptEpoch(run, source, epoch, cfg, passFDs, passIDs)
	if err != nil {
		t.Fatalf("AdoptEpoch: %v", err)
	}
	defer adopted.Close()

	if !isCloexec(t, adopted.fds[0]) {
		t.Errorf("adopted fd[0] is not CLOEXEC")
	}

	passFDs[0] = 99999
	passIDs[0].Name = "corrupted"

	adoptedIDs, err := adopted.Identities()
	if err != nil {
		t.Fatalf("Identities: %v", err)
	}
	if !reflect.DeepEqual(adoptedIDs, ids) {
		t.Errorf("adopted identities changed when caller slice was mutated")
	}

	t.Run("adopt_after_original_close", func(t *testing.T) {
		orig2, err := AllocateEpoch("run-after-close", "src-after-close", 10, cfg)
		if err != nil {
			t.Fatalf("AllocateEpoch: %v", err)
		}
		d2, err := orig2.DupFDs()
		if err != nil {
			t.Fatalf("DupFDs: %v", err)
		}
		id2, err := orig2.Identities()
		if err != nil {
			t.Fatalf("Identities: %v", err)
		}
		if _, err := unix.Pwrite(d2[0], []byte("Z"), 0); err != nil {
			t.Fatalf("Pwrite: %v", err)
		}
		if err := orig2.Close(); err != nil {
			t.Fatalf("orig2.Close: %v", err)
		}

		adopted2, err := AdoptEpoch("run-after-close", "src-after-close", 10, cfg, d2, id2)
		if err != nil {
			t.Fatalf("AdoptEpoch after orig2.Close: %v", err)
		}
		defer adopted2.Close()

		data, err := adopted2.ReadSpan(0, 0, 1)
		if err != nil {
			t.Fatalf("ReadSpan: %v", err)
		}
		if string(data) != "Z" {
			t.Errorf("ReadSpan got %q, want 'Z'", string(data))
		}
	})
}

type adoptTestCase struct {
	name         string
	wantErr      error
	expectedDiag string
	setup        func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) (fds []int, expected []ipc.FDIdentity)
}

func createStandardFixtures(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
	t.Helper()
	fds := make([]int, cfg.Tracks)
	ids := make([]ipc.FDIdentity, cfg.Tracks)
	for tr := 0; tr < cfg.Tracks; tr++ {
		name := expectedCanonicalName(run, source, epoch, tr)
		fd := createRealMemfd(t, name, cfg.Capacity, requiredSeals)
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			unix.Close(fd)
			t.Fatalf("fstat fixture track %d: %v", tr, err)
		}
		fds[tr] = fd
		ids[tr] = ipc.FDIdentity{
			Track: tr,
			Name:  name,
			Dev:   uint64(st.Dev),
			Ino:   st.Ino,
			Size:  st.Size,
		}
	}
	return fds, ids
}

// adoptionTestCases defines the single shared table of adoption failure cases for public and child tests.
func adoptionTestCases() []adoptTestCase {
	return []adoptTestCase{
		{
			name:         "invalid_cfg_epoch0",
			wantErr:      ErrInvalidConfig,
			expectedDiag: "epoch must be positive",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				return createStandardFixtures(t, run, source, 0, cfg)
			},
		},
		{
			name:         "count_too_few_fds",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "descriptors, want",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				unix.Close(fds[len(fds)-1])
				return fds[:len(fds)-1], ids[:len(ids)-1]
			},
		},
		{
			name:         "count_extra_fds",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "descriptors, want",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				extra := createRealMemfd(t, "extra", cfg.Capacity, requiredSeals)
				return append(fds, extra), ids
			},
		},
		{
			name:         "count_expected_mismatch",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "expected identities, want",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				return fds, ids[:len(ids)-1]
			},
		},
		{
			name:         "negative_fd",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "is negative",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				unix.Close(fds[1])
				fds[1] = -1
				return fds, ids
			},
		},
		{
			name:         "repeated_fd",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "listed twice",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				unix.Close(fds[1])
				fds[1] = fds[0]
				return fds, ids
			},
		},
		{
			name:         "wrong_expected_track",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "describes track",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				ids[0].Track = 1
				return fds, ids
			},
		},
		{
			name:         "wrong_expected_order",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "describes track",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				ids[0], ids[1] = ids[1], ids[0]
				return fds, ids
			},
		},
		{
			name:         "wrong_expected_name",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "expected name",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				ids[0].Name = "wrong_name"
				return fds, ids
			},
		},
		{
			name:         "wrong_expected_dev",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "expected dev",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				ids[0].Dev += 1
				return fds, ids
			},
		},
		{
			name:         "wrong_expected_ino",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "expected dev",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				ids[0].Ino += 1
				return fds, ids
			},
		},
		{
			name:         "wrong_expected_size",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "expected size",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				ids[0].Size += 1
				return fds, ids
			},
		},
		{
			// A regular file is refused before the /proc link check, but the exact diagnostic depends
			// on the filesystem: F_GET_SEALS fails with EINVAL where sealing is unsupported (ext4,
			// btrfs), while on tmpfs it succeeds with F_SEAL_SEAL and the seal set mismatch is
			// reported instead. Only the ErrIdentityMismatch identity is stable across filesystems.
			name:    "regular_file",
			wantErr: ErrIdentityMismatch,
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				reg := createRealRegularFile(t, cfg.Capacity)
				var st unix.Stat_t
				if err := unix.Fstat(reg, &st); err != nil {
					t.Fatalf("fstat reg: %v", err)
				}
				unix.Close(fds[0])
				fds[0] = reg
				ids[0].Dev = uint64(st.Dev)
				ids[0].Ino = st.Ino
				ids[0].Size = cfg.Capacity
				return fds, ids
			},
		},
		{
			name:         "pipe",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "is not a regular file",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				r, w := createRealPipe(t)
				unix.Close(w)
				unix.Close(fds[0])
				fds[0] = r
				return fds, ids
			},
		},
		{
			name:         "wrong_kernel_name_memfd",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "want",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				badNameFD := createRealMemfd(t, "wrong-kernel-name", cfg.Capacity, requiredSeals)
				var st unix.Stat_t
				if err := unix.Fstat(badNameFD, &st); err != nil {
					t.Fatalf("fstat badNameFD: %v", err)
				}
				unix.Close(fds[0])
				fds[0] = badNameFD
				ids[0].Dev = uint64(st.Dev)
				ids[0].Ino = st.Ino
				ids[0].Size = cfg.Capacity
				return fds, ids
			},
		},
		{
			name:         "read_only_memfd",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "is not O_RDWR",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				ro := createReadOnlyMemfd(t, ids[0].Name, cfg.Capacity)
				var st unix.Stat_t
				if err := unix.Fstat(ro, &st); err != nil {
					t.Fatalf("fstat ro: %v", err)
				}
				unix.Close(fds[0])
				fds[0] = ro
				ids[0].Dev = uint64(st.Dev)
				ids[0].Ino = st.Ino
				ids[0].Size = cfg.Capacity
				return fds, ids
			},
		},
		{
			name:         "wrong_actual_size",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "size",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				bad := createRealMemfd(t, ids[0].Name, cfg.Capacity+1024, requiredSeals)
				var st unix.Stat_t
				if err := unix.Fstat(bad, &st); err != nil {
					t.Fatalf("fstat bad: %v", err)
				}
				unix.Close(fds[0])
				fds[0] = bad
				ids[0].Dev = uint64(st.Dev)
				ids[0].Ino = st.Ino
				ids[0].Size = cfg.Capacity
				return fds, ids
			},
		},
		{
			name:         "wrong_seals_no_seals",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "seals",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				bad := createRealMemfd(t, ids[0].Name, cfg.Capacity, 0)
				var st unix.Stat_t
				if err := unix.Fstat(bad, &st); err != nil {
					t.Fatalf("fstat bad: %v", err)
				}
				unix.Close(fds[0])
				fds[0] = bad
				ids[0].Dev = uint64(st.Dev)
				ids[0].Ino = st.Ino
				ids[0].Size = cfg.Capacity
				return fds, ids
			},
		},
		{
			name:         "write_sealed_memfd",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "and no write seal",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				bad := createRealMemfd(t, ids[0].Name, cfg.Capacity, requiredSeals|unix.F_SEAL_WRITE)
				var st unix.Stat_t
				if err := unix.Fstat(bad, &st); err != nil {
					t.Fatalf("fstat bad: %v", err)
				}
				unix.Close(fds[0])
				fds[0] = bad
				ids[0].Dev = uint64(st.Dev)
				ids[0].Ino = st.Ino
				ids[0].Size = cfg.Capacity
				return fds, ids
			},
		},
		{
			name:         "inode_alias_canonical_name",
			wantErr:      ErrIdentityMismatch,
			expectedDiag: "want",
			setup: func(t *testing.T, run, source string, epoch uint64, cfg ipc.Config) ([]int, []ipc.FDIdentity) {
				fds, ids := createStandardFixtures(t, run, source, epoch, cfg)
				dup0, err := unix.FcntlInt(uintptr(fds[0]), unix.F_DUPFD_CLOEXEC, 0)
				if err != nil {
					t.Fatalf("DUP: %v", err)
				}
				var st unix.Stat_t
				if err := unix.Fstat(dup0, &st); err != nil {
					t.Fatalf("fstat dup0: %v", err)
				}
				unix.Close(fds[1])
				fds[1] = dup0
				ids[1].Dev = uint64(st.Dev)
				ids[1].Ino = st.Ino
				ids[1].Size = cfg.Capacity
				return fds, ids
			},
		},
	}
}

func runAdoptFailureTest(t *testing.T, tc adoptTestCase, run, source string, epoch uint64, cfg ipc.Config) {
	t.Helper()
	heldFD, err := unix.MemfdCreate("held-canary", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatalf("%s: create held fd: %v", tc.name, err)
	}
	defer unix.Close(heldFD)

	ep := epoch
	if tc.name == "invalid_cfg_epoch0" {
		ep = 0
	}
	fds, expected := tc.setup(t, run, source, ep, cfg)

	uniqueSupplied := make(map[int]bool)
	for _, fd := range fds {
		if fd >= 0 {
			uniqueSupplied[fd] = true
		}
	}

	store, err := AdoptEpoch(run, source, ep, cfg, fds, expected)
	if store != nil {
		store.Close()
		t.Fatalf("%s: AdoptEpoch returned non-nil store", tc.name)
	}
	if !errors.Is(err, tc.wantErr) {
		t.Fatalf("%s: err = %v, want %v", tc.name, err, tc.wantErr)
	}
	if tc.expectedDiag != "" && !strings.Contains(err.Error(), tc.expectedDiag) {
		t.Fatalf("%s: diagnostic %q not found in err %v", tc.name, tc.expectedDiag, err)
	}

	for fd := range uniqueSupplied {
		if isFDOpen(fd) {
			t.Fatalf("%s: supplied descriptor %d was NOT closed", tc.name, fd)
		}
	}

	if !isFDOpen(heldFD) {
		t.Fatalf("%s: caller-held canary descriptor was closed", tc.name)
	}
}

func TestAdoptFailures(t *testing.T) {
	run := "run-adopt-fail"
	source := "src-adopt-fail"
	epoch := uint64(55)
	cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 4096}

	for _, tc := range adoptionTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			runAdoptFailureTest(t, tc, run, source, epoch, cfg)
		})
	}
}

// -----------------------------------------------------------------------------
// LANE 4: Binary Pwrite -> Publication -> ReadSpan
// -----------------------------------------------------------------------------

func TestReadSpanAndPublication(t *testing.T) {
	run := "run-readspan"
	source := "src-readspan"
	epoch := uint64(88)
	cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 65536}

	store, err := AllocateEpoch(run, source, epoch, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}

	dups, err := store.DupFDs()
	if err != nil {
		store.Close()
		t.Fatalf("DupFDs: %v", err)
	}

	type pubEvent struct {
		track  int
		offset int64
		length int64
		data   []byte
	}

	ch := make(chan pubEvent, 10)

	payload0 := bytes.Repeat([]byte{0x11}, 32)
	payload1 := bytes.Repeat([]byte{0x22}, 64)
	payload2 := bytes.Repeat([]byte{0x33}, 128)
	endOffset := cfg.Capacity - int64(len(payload2))

	if _, err := unix.Pwrite(dups[0], payload0, 0); err != nil {
		t.Fatalf("Pwrite 0: %v", err)
	}
	if _, err := unix.Pwrite(dups[1], payload1, 1024); err != nil {
		t.Fatalf("Pwrite 1: %v", err)
	}
	if _, err := unix.Pwrite(dups[2], payload2, endOffset); err != nil {
		t.Fatalf("Pwrite 2: %v", err)
	}

	ch <- pubEvent{track: 0, offset: 0, length: int64(len(payload0)), data: payload0}
	ch <- pubEvent{track: 1, offset: 1024, length: int64(len(payload1)), data: payload1}
	ch <- pubEvent{track: 2, offset: endOffset, length: int64(len(payload2)), data: payload2}
	close(ch)

	for ev := range ch {
		got, err := store.ReadSpan(ev.track, ev.offset, ev.length)
		if err != nil {
			t.Fatalf("ReadSpan track %d offset %d: %v", ev.track, ev.offset, err)
		}
		if !bytes.Equal(got, ev.data) {
			t.Fatalf("ReadSpan track %d data mismatch", ev.track)
		}
	}

	t0Data := []byte("track_zero_unique_payload")
	t1Data := []byte("track_ones_unique_payload")
	if _, err := unix.Pwrite(dups[0], t0Data, 2048); err != nil {
		t.Fatalf("Pwrite: %v", err)
	}
	if _, err := unix.Pwrite(dups[1], t1Data, 2048); err != nil {
		t.Fatalf("Pwrite: %v", err)
	}
	read0, err := store.ReadSpan(0, 2048, int64(len(t0Data)))
	if err != nil || !bytes.Equal(read0, t0Data) {
		t.Fatalf("track 0 read mismatch: got %q, want %q", read0, t0Data)
	}
	read1, err := store.ReadSpan(1, 2048, int64(len(t1Data)))
	if err != nil || !bytes.Equal(read1, t1Data) {
		t.Fatalf("track 1 read mismatch: got %q, want %q", read1, t1Data)
	}

	badInputs := []struct {
		name   string
		track  int
		offset int64
		length int64
	}{
		{"track_negative", -1, 0, 10},
		{"track_too_high", cfg.Tracks, 0, 10},
		{"track_way_high", 99, 0, 10},
		{"offset_negative", 0, -1, 10},
		{"length_zero", 0, 0, 0},
		{"length_negative", 0, 0, -1},
		{"offset_at_capacity", 0, cfg.Capacity, 1},
		{"offset_past_capacity", 0, cfg.Capacity + 1, 1},
		{"span_exceeds_capacity", 0, cfg.Capacity - 5, 6},
		{"math_maxint64_offset_overflow", 0, math.MaxInt64 - 5, 10},
		{"math_maxint64_length_overflow", 0, 10, math.MaxInt64},
		{"math_maxint64_both", 0, math.MaxInt64, 1},
	}

	for _, bi := range badInputs {
		t.Run(bi.name, func(t *testing.T) {
			got, err := store.ReadSpan(bi.track, bi.offset, bi.length)
			if got != nil || !errors.Is(err, ErrSpanOutOfBounds) {
				t.Fatalf("ReadSpan(%d, %d, %d) = %v, %v, want nil, ErrSpanOutOfBounds",
					bi.track, bi.offset, bi.length, got, err)
			}
		})
	}

	span, err := store.ReadSpan(0, 0, 10)
	if err != nil {
		t.Fatalf("ReadSpan: %v", err)
	}
	origByte := span[0]
	span[0] ^= 0xff

	span2, err := store.ReadSpan(0, 0, 10)
	if err != nil {
		t.Fatalf("ReadSpan2: %v", err)
	}
	if span2[0] != origByte {
		t.Fatalf("mutating returned bytes changed backing memory! got %x, want %x", span2[0], origByte)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}
	if span2[0] != origByte {
		t.Fatalf("copied data corrupted after store.Close")
	}

	buf := make([]byte, 1)
	if n, err := unix.Pread(dups[0], buf, 0); err != nil || n != 1 || buf[0] != origByte {
		t.Fatalf("dup FD pread after close failed: n=%d err=%v buf=%x", n, err, buf[0])
	}
	for _, d := range dups {
		unix.Close(d)
	}
}

// -----------------------------------------------------------------------------
// LANE 5: Real SOCK_SEQPACKET Integration
// -----------------------------------------------------------------------------

func TestTransportSeqpacketIntegration(t *testing.T) {
	trackCounts := []struct {
		name   string
		mode   string
		tracks int
	}{
		{"solo", ipc.ModeSolo, ipc.TracksSolo},
		{"1v1", ipc.Mode1v1, ipc.Tracks1v1},
	}

	for _, tc := range trackCounts {
		t.Run(tc.name, func(t *testing.T) {
			a, b := seqpair(t)
			run := "run-seqpacket-" + tc.name
			source := "src-seqpacket-" + tc.name
			epoch := uint64(99)
			cfg := ipc.Config{
				Mode:     tc.mode,
				Codec:    ipc.CodecH264,
				Tracks:   tc.tracks,
				Capacity: 8192,
			}

			supStore, err := AllocateEpoch(run, source, epoch, cfg)
			if err != nil {
				t.Fatalf("AllocateEpoch: %v", err)
			}

			dups, err := supStore.DupFDs()
			if err != nil {
				supStore.Close()
				t.Fatalf("DupFDs: %v", err)
			}

			expectedPayloads := make([][]byte, cfg.Tracks)
			for tr := 0; tr < cfg.Tracks; tr++ {
				expectedPayloads[tr] = []byte(fmt.Sprintf("seqpacket-data-track-%d-%s", tr, tc.name))
				if _, err := unix.Pwrite(dups[tr], expectedPayloads[tr], 0); err != nil {
					supStore.Close()
					for _, d := range dups {
						unix.Close(d)
					}
					t.Fatalf("Pwrite track %d: %v", tr, err)
				}
			}

			ids, err := supStore.Identities()
			if err != nil {
				supStore.Close()
				for _, d := range dups {
					unix.Close(d)
				}
				t.Fatalf("Identities: %v", err)
			}

			welcomePacket := ipc.Packet{
				Type: ipc.TypeWelcome,
				Welcome: &ipc.Welcome{
					Version: ipc.Version,
					Run:     run,
					Source:  source,
					Epoch:   epoch,
					Setup:   true,
					Applied: 0,
					Tracks:  ids,
				},
			}

			sendDeadline := time.Now().Add(2 * time.Second)
			if err := ipc.WriteMsg(a, welcomePacket, dups, sendDeadline); err != nil {
				supStore.Close()
				for _, d := range dups {
					unix.Close(d)
				}
				t.Fatalf("WriteMsg: %v", err)
			}

			for _, d := range dups {
				unix.Close(d)
			}
			if err := supStore.Close(); err != nil {
				t.Fatalf("supStore.Close: %v", err)
			}

			recvDeadline := time.Now().Add(2 * time.Second)
			gotP, recvFDs, err := ipc.ReadMsg(b, cfg.Tracks, recvDeadline)
			if err != nil {
				t.Fatalf("ReadMsg: %v", err)
			}
			if gotP.Type != ipc.TypeWelcome || gotP.Welcome == nil {
				for _, fd := range recvFDs {
					unix.Close(fd)
				}
				t.Fatalf("unexpected packet received: %+v", gotP)
			}

			workerStore, err := AdoptEpoch(run, source, epoch, cfg, recvFDs, gotP.Welcome.Tracks)
			if err != nil {
				t.Fatalf("AdoptEpoch: %v", err)
			}
			defer workerStore.Close()

			for tr := 0; tr < cfg.Tracks; tr++ {
				want := expectedPayloads[tr]
				got, err := workerStore.ReadSpan(tr, 0, int64(len(want)))
				if err != nil {
					t.Fatalf("workerStore.ReadSpan track %d: %v", tr, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("track %d payload mismatch: got %q, want %q", tr, got, want)
				}
			}
		})
	}
}

// -----------------------------------------------------------------------------
// LANE 6: Concurrent Access, Barrier Coordination and Idempotent Close
// -----------------------------------------------------------------------------

func TestConcurrentAccessAndClose(t *testing.T) {
	run := "run-concurrent"
	source := "src-concurrent"
	epoch := uint64(123)
	cfg := ipc.Config{Mode: ipc.Mode1v1, Codec: ipc.CodecH264, Tracks: ipc.Tracks1v1, Capacity: 65536}

	store, err := AllocateEpoch(run, source, epoch, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}
	defer store.Close()

	dups, err := store.DupFDs()
	if err != nil {
		t.Fatalf("DupFDs: %v", err)
	}
	defer func() {
		for _, d := range dups {
			unix.Close(d)
		}
	}()

	stablePatterns := make([][]byte, cfg.Tracks)
	for tr := 0; tr < cfg.Tracks; tr++ {
		p := make([]byte, 1024)
		for i := range p {
			p[i] = byte((tr*31 + i) & 0xff)
		}
		stablePatterns[tr] = p
		if _, err := unix.Pwrite(dups[tr], p, 0); err != nil {
			t.Fatalf("Pwrite: %v", err)
		}
	}

	ids, err := store.Identities()
	if err != nil {
		t.Fatalf("Identities: %v", err)
	}

	const (
		numReaders    = 8
		numIdentities = 4
		numDupFDs     = 4
		numClosers    = 4
		totalWorkers  = numReaders + numIdentities + numDupFDs
	)

	startBarrier := make(chan struct{})
	var firstOpBarrier sync.WaitGroup
	firstOpBarrier.Add(totalWorkers)

	var wg sync.WaitGroup

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-startBarrier

			firstDone := false
			for iter := 0; iter < 100; iter++ {
				track := iter % cfg.Tracks
				span, err := store.ReadSpan(track, 0, 1024)
				if !firstDone {
					firstDone = true
					firstOpBarrier.Done()
				}
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("reader %d iter %d: unexpected err %v", id, iter, err)
					}
					if span != nil {
						t.Errorf("reader %d iter %d: non-nil span on error", id, iter)
					}
					return
				}
				if !bytes.Equal(span, stablePatterns[track]) {
					t.Errorf("reader %d iter %d: corrupted span read", id, iter)
					return
				}
			}
		}(i)
	}

	for i := 0; i < numIdentities; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-startBarrier

			firstDone := false
			for iter := 0; iter < 100; iter++ {
				gotIDs, err := store.Identities()
				if !firstDone {
					firstDone = true
					firstOpBarrier.Done()
				}
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("identities %d iter %d: unexpected err %v", id, iter, err)
					}
					if gotIDs != nil {
						t.Errorf("identities %d iter %d: non-nil ids on error", id, iter)
					}
					return
				}
				if !reflect.DeepEqual(gotIDs, ids) {
					t.Errorf("identities %d iter %d: identities mismatch", id, iter)
					return
				}
			}
		}(i)
	}

	for i := 0; i < numDupFDs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-startBarrier

			firstDone := false
			for iter := 0; iter < 100; iter++ {
				newDups, err := store.DupFDs()
				if !firstDone {
					firstDone = true
					firstOpBarrier.Done()
				}
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("dupfds %d iter %d: unexpected err %v", id, iter, err)
					}
					if newDups != nil {
						t.Errorf("dupfds %d iter %d: non-nil dups on error", id, iter)
					}
					return
				}
				if len(newDups) != cfg.Tracks {
					t.Errorf("dupfds %d iter %d: got %d dups, want %d", id, iter, len(newDups), cfg.Tracks)
				}
				for tr, fd := range newDups {
					if !isCloexec(t, fd) {
						t.Errorf("dup fd %d track %d is not close-on-exec", fd, tr)
					}
					var st unix.Stat_t
					if err := unix.Fstat(fd, &st); err != nil {
						t.Errorf("dup fd %d fstat: %v", fd, err)
					} else if st.Size != cfg.Capacity {
						t.Errorf("dup fd %d size %d, want %d", fd, st.Size, cfg.Capacity)
					}
					buf := make([]byte, 16)
					if n, err := unix.Pread(fd, buf, 0); err != nil || n != 16 {
						t.Errorf("dup fd %d pread: n=%d err=%v", fd, n, err)
					} else if !bytes.Equal(buf, stablePatterns[tr][:16]) {
						t.Errorf("dup fd %d payload mismatch", fd)
					}
					unix.Close(fd)
				}
			}
		}(i)
	}

	close(startBarrier)
	firstOpBarrier.Wait()

	closeErrors := make([]error, numClosers)
	for i := 0; i < numClosers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			closeErrors[idx] = store.Close()
		}(i)
	}

	wg.Wait()

	for idx, cerr := range closeErrors {
		if cerr != nil {
			t.Errorf("closer %d returned error: %v", idx, cerr)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatalf("repeated Close failed: %v", err)
	}

	var nilStore *EpochStore
	if err := nilStore.Close(); err != nil {
		t.Fatalf("nilStore.Close: %v", err)
	}
	if ids, err := nilStore.Identities(); !errors.Is(err, ErrClosed) || ids != nil {
		t.Fatalf("nilStore.Identities = (%v, %v), want (nil, ErrClosed)", ids, err)
	}
	if dups, err := nilStore.DupFDs(); !errors.Is(err, ErrClosed) || dups != nil {
		t.Fatalf("nilStore.DupFDs = (%v, %v), want (nil, ErrClosed)", dups, err)
	}
	if span, err := nilStore.ReadSpan(0, 0, 10); !errors.Is(err, ErrClosed) || span != nil {
		t.Fatalf("nilStore.ReadSpan = (%v, %v), want (nil, ErrClosed)", span, err)
	}

	var zeroStore EpochStore
	if err := zeroStore.Close(); err != nil {
		t.Fatalf("zeroStore.Close: %v", err)
	}
	if ids, err := zeroStore.Identities(); !errors.Is(err, ErrClosed) || ids != nil {
		t.Fatalf("zeroStore.Identities = (%v, %v), want (nil, ErrClosed)", ids, err)
	}
	if dups, err := zeroStore.DupFDs(); !errors.Is(err, ErrClosed) || dups != nil {
		t.Fatalf("zeroStore.DupFDs = (%v, %v), want (nil, ErrClosed)", dups, err)
	}
	if span, err := zeroStore.ReadSpan(0, 0, 10); !errors.Is(err, ErrClosed) || span != nil {
		t.Fatalf("zeroStore.ReadSpan = (%v, %v), want (nil, ErrClosed)", span, err)
	}
}

// -----------------------------------------------------------------------------
// LANE 7: Isolated Bounded Subprocesses for Baseline Parity
// -----------------------------------------------------------------------------

func warmRuntime(t *testing.T) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err == nil {
		unix.Close(fds[0])
		unix.Close(fds[1])
	}
	dummy, err := unix.MemfdCreate("warmup", unix.MFD_CLOEXEC)
	if err == nil {
		unix.Close(dummy)
	}
	_, _ = os.ReadFile("/proc/self/maps")
}

func runChildParitySuccess(t *testing.T) {
	warmRuntime(t)
	baseFDs := openFDs(t)
	baseMaps := countMemfdMaps(t)

	cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 4096}
	store, err := AllocateEpoch("run-sub-succ", "src-sub-succ", 1, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}
	dups, err := store.DupFDs()
	if err != nil {
		t.Fatalf("DupFDs: %v", err)
	}
	for _, d := range dups {
		unix.Close(d)
	}
	if _, err := store.ReadSpan(0, 0, 10); err != nil {
		t.Fatalf("ReadSpan: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	curFDs := openFDs(t)
	curMaps := countMemfdMaps(t)
	if !slices.Equal(baseFDs, curFDs) {
		t.Fatalf("FD leak in alloc success: before=%v after=%v", baseFDs, curFDs)
	}
	if curMaps != baseMaps {
		t.Fatalf("maps leak in alloc success: before=%d after=%d", baseMaps, curMaps)
	}

	s1, err := AllocateEpoch("run-adopt-succ", "src-adopt-succ", 2, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}
	d1, err := s1.DupFDs()
	if err != nil {
		t.Fatalf("DupFDs: %v", err)
	}
	id1, err := s1.Identities()
	if err != nil {
		t.Fatalf("Identities: %v", err)
	}
	s1.Close()

	s2, err := AdoptEpoch("run-adopt-succ", "src-adopt-succ", 2, cfg, d1, id1)
	if err != nil {
		t.Fatalf("AdoptEpoch: %v", err)
	}
	if _, err := s2.ReadSpan(0, 0, 10); err != nil {
		t.Fatalf("ReadSpan s2: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("Close s2: %v", err)
	}

	finalFDs := openFDs(t)
	finalMaps := countMemfdMaps(t)
	if !slices.Equal(baseFDs, finalFDs) {
		t.Fatalf("FD leak in adopt success: before=%v after=%v", baseFDs, finalFDs)
	}
	if finalMaps != baseMaps {
		t.Fatalf("maps leak in adopt success: before=%d after=%d", baseMaps, finalMaps)
	}
}

func runChildParityAdoptFailures(t *testing.T) {
	warmRuntime(t)
	baseFDs := openFDs(t)
	baseMaps := countMemfdMaps(t)

	run := "run-child-fail"
	source := "src-child-fail"
	epoch := uint64(33)
	cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 4096}

	for _, sc := range adoptionTestCases() {
		ep := epoch
		if sc.name == "invalid_cfg_epoch0" {
			ep = 0
		}
		fds, ids := sc.setup(t, run, source, ep, cfg)

		s, err := AdoptEpoch(run, source, ep, cfg, fds, ids)
		if s != nil {
			s.Close()
			t.Fatalf("%s: expected nil store", sc.name)
		}
		if !errors.Is(err, sc.wantErr) {
			t.Fatalf("%s: got %v, want %v", sc.name, err, sc.wantErr)
		}
		if sc.expectedDiag != "" && !strings.Contains(err.Error(), sc.expectedDiag) {
			t.Fatalf("%s: diagnostic %q not found in err %v", sc.name, sc.expectedDiag, err)
		}
		curFDs := openFDs(t)
		curMaps := countMemfdMaps(t)
		if !slices.Equal(baseFDs, curFDs) {
			t.Fatalf("%s leaked FDs: before=%v after=%v", sc.name, baseFDs, curFDs)
		}
		if curMaps != baseMaps {
			t.Fatalf("%s leaked maps: before=%d after=%d", sc.name, baseMaps, curMaps)
		}
	}
}

func runChildRlimitAllocate(t *testing.T) {
	warmRuntime(t)
	var rlim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rlim); err != nil {
		t.Fatalf("Getrlimit: %v", err)
	}
	baseFDs := openFDs(t)
	baseMaps := countMemfdMaps(t)

	newLimit := uint64(len(baseFDs) + 2)
	if newLimit >= rlim.Max {
		t.Fatalf("cannot lower rlimit: base=%d max=%d", len(baseFDs), rlim.Max)
	}
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: newLimit, Max: rlim.Max}); err != nil {
		t.Fatalf("Setrlimit: %v", err)
	}

	cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 4096}
	store, err := AllocateEpoch("run-rlim", "src-rlim", 1, cfg)

	if rerr := unix.Setrlimit(unix.RLIMIT_NOFILE, &rlim); rerr != nil {
		t.Fatalf("restore Setrlimit: %v", rerr)
	}

	if store != nil {
		store.Close()
		t.Fatalf("AllocateEpoch succeeded despite RLIMIT_NOFILE")
	}
	if err == nil {
		t.Fatalf("AllocateEpoch returned nil error despite failure")
	}

	curFDs := openFDs(t)
	curMaps := countMemfdMaps(t)
	if !slices.Equal(baseFDs, curFDs) {
		t.Fatalf("FDs leaked on partial allocation: before=%v after=%v", baseFDs, curFDs)
	}
	if curMaps != baseMaps {
		t.Fatalf("maps leaked on partial allocation: before=%d after=%d", baseMaps, curMaps)
	}
}

func runChildRlimitDupFDs(t *testing.T) {
	warmRuntime(t)
	var rlim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rlim); err != nil {
		t.Fatalf("Getrlimit: %v", err)
	}
	baseFDs := openFDs(t)
	baseMaps := countMemfdMaps(t)

	cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 4096}
	store, err := AllocateEpoch("run-rlim-dup", "src-rlim-dup", 1, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}
	defer store.Close()

	allocatedFDs := openFDs(t)
	newLimit := uint64(len(allocatedFDs) + 1)
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: newLimit, Max: rlim.Max}); err != nil {
		t.Fatalf("Setrlimit: %v", err)
	}

	dups, err := store.DupFDs()

	if rerr := unix.Setrlimit(unix.RLIMIT_NOFILE, &rlim); rerr != nil {
		t.Fatalf("restore Setrlimit: %v", rerr)
	}

	if dups != nil {
		for _, d := range dups {
			unix.Close(d)
		}
		t.Fatalf("DupFDs succeeded despite RLIMIT_NOFILE")
	}
	if err == nil {
		t.Fatalf("DupFDs returned nil error despite failure")
	}

	curFDs := openFDs(t)
	if !slices.Equal(allocatedFDs, curFDs) {
		t.Fatalf("FDs leaked on partial DupFDs: before=%v after=%v", allocatedFDs, curFDs)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	finalFDs := openFDs(t)
	finalMaps := countMemfdMaps(t)
	if !slices.Equal(baseFDs, finalFDs) {
		t.Fatalf("FDs leaked after Close: base=%v final=%v", baseFDs, finalFDs)
	}
	if finalMaps != baseMaps {
		t.Fatalf("maps leaked after Close: base=%d final=%d", baseMaps, finalMaps)
	}
}

func runChildMapAllRollback(t *testing.T) {
	warmRuntime(t)
	baseFDs := openFDs(t)
	baseMaps := countMemfdMaps(t)

	fd0, err := unix.MemfdCreate("mapall-valid", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatalf("MemfdCreate: %v", err)
	}
	if err := unix.Ftruncate(fd0, 4096); err != nil {
		unix.Close(fd0)
		t.Fatalf("Ftruncate: %v", err)
	}

	var pipeFDs [2]int
	if err := unix.Pipe2(pipeFDs[:], unix.O_CLOEXEC); err != nil {
		unix.Close(fd0)
		t.Fatalf("Pipe2: %v", err)
	}
	fd1 := pipeFDs[0]
	unix.Close(pipeFDs[1])

	fds := []int{fd0, fd1}
	ids := []ipc.FDIdentity{
		{Track: 0, Name: "track-0", Size: 4096},
		{Track: 1, Name: "track-1", Size: 4096},
	}

	t.Log("internal rollback evidence: exercising mapAll rollback on mmap failure")
	store, err := mapAll(fds, ids, 4096)
	if store != nil {
		store.Close()
		t.Fatalf("mapAll succeeded unexpectedly")
	}
	if err == nil {
		t.Fatalf("mapAll returned nil error")
	}

	if isFDOpen(fd0) {
		t.Fatalf("fd0 was NOT closed by mapAll on error")
	}
	if isFDOpen(fd1) {
		t.Fatalf("fd1 was NOT closed by mapAll on error")
	}

	curFDs := openFDs(t)
	curMaps := countMemfdMaps(t)
	if !slices.Equal(baseFDs, curFDs) {
		t.Fatalf("FDs leaked after mapAll rollback: base=%v cur=%v", baseFDs, curFDs)
	}
	if curMaps != baseMaps {
		t.Fatalf("maps leaked after mapAll rollback: base=%d cur=%d", baseMaps, curMaps)
	}
}

func TestSubprocessIsolation(t *testing.T) {
	if subtest := os.Getenv("PARTSTORE_CHILD_SUBTEST"); subtest != "" {
		switch subtest {
		case "parity_success":
			runChildParitySuccess(t)
		case "parity_adopt_failures":
			runChildParityAdoptFailures(t)
		case "rlimit_allocate":
			runChildRlimitAllocate(t)
		case "rlimit_dupfds":
			runChildRlimitDupFDs(t)
		case "mapall_rollback":
			runChildMapAllRollback(t)
		default:
			t.Fatalf("unknown child subtest: %s", subtest)
		}
		if !t.Failed() {
			fmt.Printf("CHILD_OK %s\n", subtest)
		}
		return
	}

	subtests := []string{
		"parity_success",
		"parity_adopt_failures",
		"rlimit_allocate",
		"rlimit_dupfds",
		"mapall_rollback",
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	for _, st := range subtests {
		t.Run(st, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSubprocessIsolation$", "-test.v", "-test.count=1", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), "PARTSTORE_CHILD_SUBTEST="+st)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("subprocess %s failed: %v\noutput:\n%s", st, err, string(out))
			}
			if !strings.Contains(string(out), "CHILD_OK "+st) {
				t.Fatalf("subprocess %s missing confirmation token:\n%s", st, string(out))
			}
		})
	}
}
