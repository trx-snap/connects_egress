//go:build linux

package ipc

// Tests of the Linux transport on real SOCK_SEQPACKET socketpairs: packets with and without
// descriptors, the rejections that must close delivered descriptors, deadlines, closure and the
// peer's kernel identity. The descriptors are plain temporary files, so nothing here is evidence
// about memfd identity or seals. Receiver descriptor leaks are measured in a child process from
// /proc/self/fd, never inferred from the sender's descriptor numbers.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const (
	testDeadline = 5 * time.Second
	leakChildEnv = "IPC_TRANSPORT_LEAK_CHILD" // marks the child that measures receiver leaks
)

func deadline() time.Time { return time.Now().Add(testDeadline) }

func past() time.Time { return time.Now().Add(-time.Second) }

// seqpair returns both ends of a close-on-exec SOCK_SEQPACKET socketpair as connections.
func seqpair(t *testing.T) (a, b *net.UnixConn) {
	t.Helper()
	return socketpair(t, unix.SOCK_SEQPACKET)
}

// socketpair wraps both ends of a socketpair of the given type with net.FileConn. The originals,
// which FileConn duplicates, are closed at once; the connections are closed at cleanup.
func socketpair(t *testing.T, typ int) (a, b *net.UnixConn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, typ|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	var conns [2]*net.UnixConn
	for i, fd := range fds {
		f := os.NewFile(uintptr(fd), "socketpair")
		c, err := net.FileConn(f)
		f.Close() // the connection holds its own duplicate
		if err != nil {
			t.Fatalf("FileConn: %v", err)
		}
		uc, ok := c.(*net.UnixConn)
		if !ok {
			c.Close()
			t.Fatalf("FileConn returned %T, want *net.UnixConn", c)
		}
		t.Cleanup(func() { uc.Close() })
		conns[i] = uc
	}
	return conns[0], conns[1]
}

// tempFiles creates n temporary files, each holding fileContent of its index, and returns their
// descriptors. The files are closed at cleanup; the descriptors are not for the caller to close.
func tempFiles(t *testing.T, n int) []int {
	t.Helper()
	dir := t.TempDir()
	fds := make([]int, n)
	for i := range fds {
		f, err := os.Create(filepath.Join(dir, fmt.Sprintf("track-%d", i)))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := f.WriteString(fileContent(i)); err != nil {
			t.Fatalf("write: %v", err)
		}
		t.Cleanup(func() { f.Close() })
		fds[i] = int(f.Fd())
	}
	return fds
}

func fileContent(i int) string { return fmt.Sprintf("track-%d payload", i) }

func identity(t *testing.T, fd int) (dev, ino uint64) {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		t.Fatalf("fstat %d: %v", fd, err)
	}
	return st.Dev, st.Ino
}

func isCloexec(t *testing.T, fd int) bool {
	t.Helper()
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		t.Fatalf("F_GETFD %d: %v", fd, err)
	}
	return flags&unix.FD_CLOEXEC != 0
}

func readContent(t *testing.T, fd int) string {
	t.Helper()
	buf := make([]byte, 64)
	n, err := unix.Pread(fd, buf, 0)
	if err != nil {
		t.Fatalf("pread %d: %v", fd, err)
	}
	return string(buf[:n])
}

// rawSend writes data and fds through the net package directly, past WriteMsg's checks.
func rawSend(t *testing.T, conn *net.UnixConn, data []byte, fds []int) {
	t.Helper()
	var oob []byte
	if len(fds) > 0 {
		oob = unix.UnixRights(fds...)
	}
	if err := conn.SetWriteDeadline(deadline()); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if _, _, err := conn.WriteMsgUnix(data, oob, nil); err != nil {
		t.Fatalf("WriteMsgUnix: %v", err)
	}
}

// rawRead receives one datagram through the net package directly.
func rawRead(t *testing.T, conn *net.UnixConn) []byte {
	t.Helper()
	if err := conn.SetReadDeadline(deadline()); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, MaxPacketSize)
	n, _, _, _, err := conn.ReadMsgUnix(buf, nil)
	if err != nil {
		t.Fatalf("ReadMsgUnix: %v", err)
	}
	return buf[:n]
}

func wantInvalidTransport(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, ErrInvalidTransport) {
		t.Errorf("%s: err = %v, want ErrInvalidTransport", what, err)
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout() && errors.Is(err, os.ErrDeadlineExceeded)
}

func TestTransportPacketsWithoutDescriptors(t *testing.T) {
	a, b := seqpair(t)
	for _, tc := range validPackets() {
		if rightsFor(tc.p) != 0 {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			// The datagram is exactly the codec's encoding.
			if err := WriteMsg(a, tc.p, nil, deadline()); err != nil {
				t.Fatalf("WriteMsg: %v", err)
			}
			if got, want := rawRead(t, b), mustEncode(t, tc.p); !bytes.Equal(got, want) {
				t.Fatalf("datagram %s, want %s", got, want)
			}
			for _, maxFDs := range []int{0, maxRights} {
				if err := WriteMsg(a, tc.p, nil, deadline()); err != nil {
					t.Fatalf("WriteMsg: %v", err)
				}
				got, fds, err := ReadMsg(b, maxFDs, deadline())
				if err != nil {
					t.Fatalf("ReadMsg(maxFDs=%d): %v", maxFDs, err)
				}
				if fds != nil {
					t.Fatalf("ReadMsg returned descriptors %v for a packet without any", fds)
				}
				if !reflect.DeepEqual(got, tc.p) {
					t.Fatalf("ReadMsg changed the packet:\n got %+v\nwant %+v", got, tc.p)
				}
			}
		})
	}
}

func TestTransportSetupWelcomeDescriptors(t *testing.T) {
	for _, n := range []int{TracksSolo, Tracks1v1} {
		for _, maxFDs := range []int{n, maxRights} {
			for _, reversed := range []bool{false, true} {
				name := fmt.Sprintf("%d descriptors, maxFDs %d", n, maxFDs)
				if reversed {
					name += ", reversed"
				}
				t.Run(name, func(t *testing.T) {
					a, b := seqpair(t)
					fds := tempFiles(t, n)
					send := slices.Clone(fds)
					if reversed {
						slices.Reverse(send)
					}
					p := welcome(true, n)
					if err := WriteMsg(a, p, send, deadline()); err != nil {
						t.Fatalf("WriteMsg: %v", err)
					}
					got, rfds, err := ReadMsg(b, maxFDs, deadline())
					if err != nil {
						t.Fatalf("ReadMsg: %v", err)
					}
					defer closeFDs(rfds...) // the receiver owns them now
					if !reflect.DeepEqual(got, p) {
						t.Fatalf("ReadMsg changed the packet:\n got %+v\nwant %+v", got, p)
					}
					if len(rfds) != n {
						t.Fatalf("received %d descriptors %v, want %d", len(rfds), rfds, n)
					}
					for i, rfd := range rfds {
						// Order follows the array sent, not descriptor numbers; the same open file
						// arrives under a new close-on-exec number.
						sdev, sino := identity(t, send[i])
						rdev, rino := identity(t, rfd)
						if rdev != sdev || rino != sino {
							t.Errorf("descriptor %d: identity %d/%d, want %d/%d", i, rdev, rino, sdev, sino)
						}
						if slices.Contains(fds, rfd) {
							t.Errorf("descriptor %d arrived as the sender's own number %d", i, rfd)
						}
						if !isCloexec(t, rfd) {
							t.Errorf("descriptor %d (%d) is not close-on-exec", i, rfd)
						}
						want := fileContent(i)
						if reversed {
							want = fileContent(n - 1 - i)
						}
						if got := readContent(t, rfd); got != want {
							t.Errorf("descriptor %d content %q, want %q", i, got, want)
						}
					}
					// The sender's descriptors were borrowed, not closed.
					for i, fd := range fds {
						if got := readContent(t, fd); got != fileContent(i) {
							t.Errorf("sender descriptor %d content %q after send", fd, got)
						}
					}
				})
			}
		}
	}
}

func TestTransportWriteRejectsBeforeSending(t *testing.T) {
	a, b := seqpair(t)
	fds := tempFiles(t, 5)
	bad := welcome(true, TracksSolo)
	bad.Welcome.Version = 0
	huge := math.MaxInt32 // not a constant, so huge+1 compiles on any int width
	cases := []struct {
		name string
		p    Packet
		fds  []int
		want error
	}{
		{"setup welcome short of descriptors", welcome(true, TracksSolo), fds[:3], ErrInvalidTransport},
		{"setup welcome with one too many", welcome(true, TracksSolo), fds[:5], ErrInvalidTransport},
		{"setup welcome without descriptors", welcome(true, Tracks1v1), nil, ErrInvalidTransport},
		{"reconnect welcome with descriptors", welcome(false, 0), fds[:1], ErrInvalidTransport},
		{"entry with a descriptor", entry(KindPart), fds[:1], ErrInvalidTransport},
		{"hello with descriptors", hello(ModeSolo, CodecH264, 0), fds[:1], ErrInvalidTransport},
		{"negative descriptor", welcome(true, TracksSolo), []int{fds[0], fds[1], fds[2], -1}, ErrInvalidTransport},
		{"descriptor beyond int32", welcome(true, TracksSolo), []int{fds[0], fds[1], fds[2], huge + 1}, ErrInvalidTransport},
		{"invalid packet with descriptors", bad, fds[:4], ErrUnsupportedVersion},
		{"invalid packet", Packet{Type: TypeHello}, nil, ErrInvalidPacket},
	}
	for _, tc := range cases {
		if err := WriteMsg(a, tc.p, tc.fds, deadline()); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
	// Nothing reached the peer, and the sender still holds every descriptor.
	if _, _, err := ReadMsg(b, maxRights, time.Now().Add(100*time.Millisecond)); !isTimeout(err) {
		t.Errorf("after rejected writes ReadMsg: %v, want a timeout", err)
	}
	for i, fd := range fds {
		if got := readContent(t, fd); got != fileContent(i) {
			t.Errorf("sender descriptor %d content %q", fd, got)
		}
	}
}

// badReceive is one datagram ReadMsg must reject: how to send it (with fds[:rights] of the seven
// descriptors a test opens), how to read it, and the error to expect. A tainted scenario leaves
// the receiving socket unusable, so no later valid receive is expected on it.
type badReceive struct {
	name    string
	maxFDs  int
	tainted bool
	setup   func(t *testing.T, b *net.UnixConn)
	send    func(t *testing.T, a *net.UnixConn, fds []int)
	want    error
}

func sendRaw(data []byte, rights int) func(t *testing.T, a *net.UnixConn, fds []int) {
	return func(t *testing.T, a *net.UnixConn, fds []int) { rawSend(t, a, data, fds[:rights]) }
}

func sendPacket(p Packet, rights int) func(t *testing.T, a *net.UnixConn, fds []int) {
	return func(t *testing.T, a *net.UnixConn, fds []int) { rawSend(t, a, mustEncode(t, p), fds[:rights]) }
}

// passCred makes the kernel attach SCM_CREDENTIALS to every datagram b receives.
func passCred(t *testing.T, b *net.UnixConn) {
	t.Helper()
	raw, err := b.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}
	var serr error
	if err := raw.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1)
	}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if serr != nil {
		t.Fatalf("SO_PASSCRED: %v", serr)
	}
}

// passPidfd makes the kernel attach SCM_PIDFD, and a new pidfd for the sender, to every datagram b
// receives.
func passPidfd(t *testing.T, b *net.UnixConn) {
	t.Helper()
	raw, err := b.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}
	var serr error
	if err := raw.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSPIDFD, 1)
	}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if serr != nil {
		t.Fatalf("SO_PASSPIDFD: %v", serr)
	}
}

func badReceives() []badReceive {
	unsupported := welcome(true, TracksSolo)
	unsupported.Welcome.Version = 2
	unsupportedJSON, err := json.Marshal(unsupported)
	if err != nil {
		panic(err)
	}
	oversized := bytes.Repeat([]byte("x"), MaxPacketSize+1)
	return []badReceive{
		{name: "malformed JSON with rights", maxFDs: maxRights, send: sendRaw([]byte(`{"type":`), 4), want: ErrInvalidPacket},
		{name: "unsupported version with rights", maxFDs: maxRights, send: sendRaw(unsupportedJSON, 4), want: ErrUnsupportedVersion},
		{name: "entry with a right, none allowed", maxFDs: 0, send: sendPacket(entry(KindPart), 1), want: ErrInvalidTransport},
		{name: "entry with a right, five allowed", maxFDs: maxRights, send: sendPacket(entry(KindPart), 1), want: ErrInvalidTransport},
		{name: "setup welcome missing a right", maxFDs: maxRights, send: sendPacket(welcome(true, TracksSolo), 3), want: ErrInvalidTransport},
		{name: "setup welcome without rights", maxFDs: maxRights, send: sendPacket(welcome(true, TracksSolo), 0), want: ErrInvalidTransport},
		{name: "setup welcome with one right too many", maxFDs: maxRights, send: sendPacket(welcome(true, TracksSolo), 5), want: ErrInvalidTransport},
		{name: "setup welcome beyond maxFDs", maxFDs: 2, send: sendPacket(welcome(true, TracksSolo), 4), want: ErrInvalidTransport},
		{name: "1v1 setup welcome beyond maxFDs", maxFDs: TracksSolo, send: sendPacket(welcome(true, Tracks1v1), 5), want: ErrInvalidTransport},
		{name: "control truncation", maxFDs: maxRights, send: sendPacket(entry(KindPart), 7), want: ErrInvalidTransport},
		{name: "oversized datagram with rights", maxFDs: maxRights, send: sendRaw(oversized, 4), want: ErrPacketTooLarge},
		{name: "oversized datagram", maxFDs: maxRights, send: sendRaw(oversized, 0), want: ErrPacketTooLarge},
		{name: "credentials control message", maxFDs: 0, tainted: true, setup: passCred, send: sendPacket(entry(KindPart), 0), want: ErrInvalidTransport},
		{name: "credentials control message with rights", maxFDs: maxRights, tainted: true, setup: passCred, send: sendPacket(welcome(true, TracksSolo), 4), want: ErrInvalidTransport},
		{name: "pidfd control message", maxFDs: 0, tainted: true, setup: passPidfd, send: sendPacket(entry(KindPhases), 0), want: ErrInvalidTransport},
	}
}

func TestTransportReadRejects(t *testing.T) {
	fds := tempFiles(t, 7)
	for _, tc := range badReceives() {
		t.Run(tc.name, func(t *testing.T) {
			a, b := seqpair(t)
			if tc.setup != nil {
				tc.setup(t, b)
			}
			tc.send(t, a, fds)
			p, got, err := ReadMsg(b, tc.maxFDs, deadline())
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if got != nil || !reflect.DeepEqual(p, Packet{}) {
				t.Fatalf("rejected receive returned %+v, %v", p, got)
			}
			if tc.tainted {
				return
			}
			// The rejection consumed only its own datagram.
			if err := WriteMsg(a, entry(KindPhases), nil, deadline()); err != nil {
				t.Fatalf("WriteMsg after rejection: %v", err)
			}
			if p, _, err := ReadMsg(b, tc.maxFDs, deadline()); err != nil || !reflect.DeepEqual(p, entry(KindPhases)) {
				t.Fatalf("ReadMsg after rejection: %+v, %v", p, err)
			}
		})
	}
}

// TestReceiverClosesRightsOnFailure runs every rejected receive in a child process and compares
// /proc/self/fd before and after each one: the descriptors the datagram delivered must be gone.
func TestReceiverClosesRightsOnFailure(t *testing.T) {
	if os.Getenv(leakChildEnv) == "1" {
		leakChild(t)
		return
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("Executable: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^TestReceiverClosesRightsOnFailure$", "-test.count=1", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), leakChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	if want := fmt.Sprintf("leak-scenarios-ok %d\n", len(badReceives())); !bytes.Contains(out, []byte(want)) {
		t.Fatalf("child did not report every scenario:\n%s", out)
	}
}

func leakChild(t *testing.T) {
	fds := tempFiles(t, 7)
	// A successful round trip first, so that everything the runtime opens lazily already exists.
	a, b := seqpair(t)
	if err := WriteMsg(a, entry(KindPhases), nil, deadline()); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	if _, _, err := ReadMsg(b, 0, deadline()); err != nil {
		t.Fatalf("ReadMsg: %v", err)
	}
	scenarios := badReceives()
	for _, tc := range scenarios {
		a, b := seqpair(t)
		if tc.setup != nil {
			tc.setup(t, b)
		}
		tc.send(t, a, fds)
		before := openFDs(t)
		_, got, err := ReadMsg(b, tc.maxFDs, deadline())
		after := openFDs(t)
		if !errors.Is(err, tc.want) || got != nil {
			t.Fatalf("%s: ReadMsg = %v, %v", tc.name, got, err)
		}
		if !slices.Equal(before, after) {
			t.Errorf("%s: open descriptors %v before, %v after", tc.name, before, after)
		}
	}
	if !t.Failed() {
		fmt.Printf("leak-scenarios-ok %d\n", len(scenarios))
	}
}

// openFDs lists this process's open descriptors from /proc/self/fd, without the one listing them.
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
			t.Fatalf("/proc/self/fd entry %q", name)
		}
		if fd != self {
			fds = append(fds, fd)
		}
	}
	slices.Sort(fds)
	return fds
}

// cmsg builds one control message of the given level, type and data, padded to its space.
func cmsg(t *testing.T, level, typ int32, data []byte) []byte {
	t.Helper()
	hdr := unix.Cmsghdr{Level: level, Type: typ}
	hdr.SetLen(unix.CmsgLen(len(data)))
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.NativeEndian, hdr); err != nil {
		t.Fatalf("header: %v", err)
	}
	buf.Write(data)
	buf.Write(make([]byte, unix.CmsgSpace(len(data))-buf.Len()))
	return buf.Bytes()
}

func TestParseRightsKeepsEarlierDescriptors(t *testing.T) {
	rights := unix.UnixRights(3, 4)
	creds := unix.UnixCredentials(&unix.Ucred{Pid: 1, Uid: 2, Gid: 3})
	if got, err := parseRights(nil); got != nil || err != nil {
		t.Errorf("parseRights(nil) = %v, %v", got, err)
	}
	if got, err := parseRights(rights); !slices.Equal(got, []int{3, 4}) || err != nil {
		t.Errorf("parseRights(rights) = %v, %v", got, err)
	}
	if got, err := parseRights(append(slices.Clone(rights), unix.UnixRights(5)...)); !slices.Equal(got, []int{3, 4, 5}) || err != nil {
		t.Errorf("parseRights(rights, rights) = %v, %v", got, err)
	}
	cases := []struct {
		name string
		oob  []byte
		want []int
	}{
		{"credentials after rights", append(slices.Clone(rights), creds...), []int{3, 4}},
		{"credentials before rights", append(slices.Clone(creds), rights...), []int{3, 4}},
		{"credentials between rights", slices.Concat(rights, creds, unix.UnixRights(5)), []int{3, 4, 5}},
		{"trailing bytes short of a header", append(slices.Clone(rights), 1, 2, 3), []int{3, 4}},
		{"header longer than the data", append(slices.Clone(rights), rights[:unix.SizeofCmsghdr]...), []int{3, 4}},
		{"rights data not whole descriptors", slices.Concat(rights, cmsg(t, unix.SOL_SOCKET, unix.SCM_RIGHTS, []byte{9, 0, 0, 0, 9, 0}), unix.UnixRights(5)), []int{3, 4, 5}},
		{"only a credentials message", creds, nil},
		{"unknown level", slices.Concat(cmsg(t, unix.SOL_IP, unix.SCM_RIGHTS, []byte{9, 0, 0, 0}), rights), []int{3, 4}},
	}
	for _, tc := range cases {
		got, err := parseRights(tc.oob)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: descriptors %v, want %v", tc.name, got, tc.want)
		}
		wantInvalidTransport(t, tc.name, err)
	}
}

// An unexpected SCM_PIDFD message is always an error, and the descriptor it names is returned with
// the rights only so that ReadMsg closes it. Only the kernel's int-sized payload names one.
func TestParseRightsCollectsPidfdForCleanup(t *testing.T) {
	pidfd := func(fd int32) []byte {
		return cmsg(t, unix.SOL_SOCKET, unix.SCM_PIDFD, binary.NativeEndian.AppendUint32(nil, uint32(fd)))
	}
	rights := unix.UnixRights(3, 4)
	creds := unix.UnixCredentials(&unix.Ucred{Pid: 1, Uid: 2, Gid: 3})
	cases := []struct {
		name string
		oob  []byte
		want []int
	}{
		{"pidfd alone", pidfd(7), []int{7}},
		{"pidfd before rights", slices.Concat(pidfd(7), rights), []int{7, 3, 4}},
		{"pidfd after rights", slices.Concat(rights, pidfd(7)), []int{3, 4, 7}},
		{"pidfd between rights", slices.Concat(rights, pidfd(7), unix.UnixRights(5)), []int{3, 4, 7, 5}},
		{"pidfd with credentials", slices.Concat(creds, pidfd(7), rights), []int{7, 3, 4}},
		{"two pidfd messages", slices.Concat(pidfd(7), pidfd(8), rights), []int{7, 8, 3, 4}},
		{"descriptor zero", pidfd(0), []int{0}},
		{"negative descriptor", slices.Concat(pidfd(-1), rights), []int{3, 4}},
		{"empty payload", slices.Concat(cmsg(t, unix.SOL_SOCKET, unix.SCM_PIDFD, nil), rights), []int{3, 4}},
		{"short payload", slices.Concat(cmsg(t, unix.SOL_SOCKET, unix.SCM_PIDFD, []byte{7, 0}), rights), []int{3, 4}},
		{"long payload", slices.Concat(cmsg(t, unix.SOL_SOCKET, unix.SCM_PIDFD, []byte{7, 0, 0, 0, 8, 0, 0, 0}), rights), []int{3, 4}},
		{"same type at another level", slices.Concat(cmsg(t, unix.SOL_IP, unix.SCM_PIDFD, binary.NativeEndian.AppendUint32(nil, 7)), rights), []int{3, 4}},
	}
	for _, tc := range cases {
		got, err := parseRights(tc.oob)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: descriptors %v, want %v", tc.name, got, tc.want)
		}
		wantInvalidTransport(t, tc.name, err)
	}
}

func TestTransportInvalidArguments(t *testing.T) {
	a, b := seqpair(t)
	stream1, stream2 := socketpair(t, unix.SOCK_STREAM)
	dgram1, dgram2 := socketpair(t, unix.SOCK_DGRAM)
	closed, _ := seqpair(t)
	closed.Close()

	_, _, err := ReadMsg(nil, 0, deadline())
	wantInvalidTransport(t, "ReadMsg(nil)", err)
	wantInvalidTransport(t, "WriteMsg(nil)", WriteMsg(nil, entry(KindPart), nil, deadline()))
	uid, pid, err := PeerCredentials(nil)
	wantInvalidTransport(t, "PeerCredentials(nil)", err)
	if uid != -1 || pid != -1 {
		t.Errorf("PeerCredentials(nil) = %d, %d", uid, pid)
	}
	id, err := PeerProcessIdentity(nil)
	wantInvalidTransport(t, "PeerProcessIdentity(nil)", err)
	if id.Pidfd != -1 {
		t.Errorf("PeerProcessIdentity(nil).Pidfd = %d", id.Pidfd)
	}

	_, _, err = ReadMsg(b, 0, time.Time{})
	wantInvalidTransport(t, "ReadMsg zero deadline", err)
	wantInvalidTransport(t, "WriteMsg zero deadline", WriteMsg(a, entry(KindPart), nil, time.Time{}))
	for _, maxFDs := range []int{-1, maxRights + 1} {
		_, _, err = ReadMsg(b, maxFDs, deadline())
		wantInvalidTransport(t, fmt.Sprintf("ReadMsg maxFDs %d", maxFDs), err)
	}

	for _, c := range []struct {
		name string
		a, b *net.UnixConn
	}{{"stream", stream1, stream2}, {"dgram", dgram1, dgram2}} {
		wantInvalidTransport(t, c.name+" WriteMsg", WriteMsg(c.a, entry(KindPart), nil, deadline()))
		_, _, err = ReadMsg(c.b, 0, deadline())
		wantInvalidTransport(t, c.name+" ReadMsg", err)
		_, _, err = PeerCredentials(c.b)
		wantInvalidTransport(t, c.name+" PeerCredentials", err)
		id, err := PeerProcessIdentity(c.b)
		wantInvalidTransport(t, c.name+" PeerProcessIdentity", err)
		if id.Pidfd != -1 {
			t.Errorf("%s PeerProcessIdentity.Pidfd = %d", c.name, id.Pidfd)
		}
	}

	if err := WriteMsg(closed, entry(KindPart), nil, deadline()); !errors.Is(err, net.ErrClosed) {
		t.Errorf("WriteMsg(closed) = %v, want net.ErrClosed", err)
	}
	if _, _, err := ReadMsg(closed, 0, deadline()); !errors.Is(err, net.ErrClosed) {
		t.Errorf("ReadMsg(closed) = %v, want net.ErrClosed", err)
	}
	if _, _, err := PeerCredentials(closed); !errors.Is(err, net.ErrClosed) {
		t.Errorf("PeerCredentials(closed) = %v, want net.ErrClosed", err)
	}
	if id, err := PeerProcessIdentity(closed); !errors.Is(err, net.ErrClosed) || id.Pidfd != -1 {
		t.Errorf("PeerProcessIdentity(closed) = %+v, %v, want net.ErrClosed", id, err)
	}
}

func TestTransportDeadlines(t *testing.T) {
	a, b := seqpair(t)

	p, fds, err := ReadMsg(b, 0, past())
	if !isTimeout(err) || fds != nil || !reflect.DeepEqual(p, Packet{}) {
		t.Fatalf("ReadMsg past deadline = %+v, %v, %v", p, fds, err)
	}
	// The deadline is left in place, not restored.
	if _, _, _, _, err := b.ReadMsgUnix(make([]byte, 1), nil); !isTimeout(err) {
		t.Errorf("read after ReadMsg timed out: %v, want the deadline still set", err)
	}
	if err := WriteMsg(a, entry(KindPart), nil, past()); !isTimeout(err) {
		t.Errorf("WriteMsg past deadline: %v", err)
	}
	if _, _, err := a.WriteMsgUnix([]byte("x"), nil, nil); !isTimeout(err) {
		t.Errorf("write after WriteMsg timed out: %v, want the deadline still set", err)
	}

	// Each helper sets only its own direction: a past read deadline on b does not stop b writing,
	// and a past write deadline on a does not stop a reading; neither helper touches the other
	// direction's deadline.
	if err := b.SetReadDeadline(past()); err != nil {
		t.Fatal(err)
	}
	if err := WriteMsg(b, entry(KindPart), nil, deadline()); err != nil {
		t.Fatalf("WriteMsg with a past read deadline: %v", err)
	}
	if _, _, _, _, err := b.ReadMsgUnix(make([]byte, 1), nil); !isTimeout(err) {
		t.Errorf("WriteMsg changed the read deadline: %v", err)
	}
	if err := a.SetWriteDeadline(past()); err != nil {
		t.Fatal(err)
	}
	if p, _, err := ReadMsg(a, 0, deadline()); err != nil || !reflect.DeepEqual(p, entry(KindPart)) {
		t.Fatalf("ReadMsg with a past write deadline: %+v, %v", p, err)
	}
	if _, _, err := a.WriteMsgUnix([]byte("x"), nil, nil); !isTimeout(err) {
		t.Errorf("ReadMsg changed the write deadline: %v", err)
	}

	// A fresh deadline on the next call is enough to continue.
	if err := WriteMsg(a, entry(KindTerminal), nil, deadline()); err != nil {
		t.Fatalf("WriteMsg with a fresh deadline: %v", err)
	}
	if p, _, err := ReadMsg(b, 0, deadline()); err != nil || !reflect.DeepEqual(p, entry(KindTerminal)) {
		t.Fatalf("ReadMsg with a fresh deadline: %+v, %v", p, err)
	}
}

func TestTransportClosure(t *testing.T) {
	t.Run("peer close is EOF after pending data", func(t *testing.T) {
		a, b := seqpair(t)
		if err := WriteMsg(a, entry(KindInit), nil, deadline()); err != nil {
			t.Fatalf("WriteMsg: %v", err)
		}
		a.Close()
		if p, _, err := ReadMsg(b, 0, deadline()); err != nil || !reflect.DeepEqual(p, entry(KindInit)) {
			t.Fatalf("ReadMsg of pending data: %+v, %v", p, err)
		}
		p, fds, err := ReadMsg(b, 0, deadline())
		if err != io.EOF || fds != nil || !reflect.DeepEqual(p, Packet{}) {
			t.Fatalf("ReadMsg after peer close = %+v, %v, %v, want io.EOF", p, fds, err)
		}
		if err := WriteMsg(b, entry(KindInit), nil, deadline()); err == nil {
			t.Error("WriteMsg to a closed peer succeeded")
		}
	})
	t.Run("peer close unblocks a read", func(t *testing.T) {
		a, b := seqpair(t)
		errc := make(chan error, 1)
		go func() {
			_, _, err := ReadMsg(b, 0, time.Now().Add(30*time.Second))
			errc <- err
		}()
		time.Sleep(100 * time.Millisecond)
		a.Close()
		select {
		case err := <-errc:
			if err != io.EOF {
				t.Fatalf("ReadMsg = %v, want io.EOF", err)
			}
		case <-time.After(testDeadline):
			t.Fatal("ReadMsg still blocked after the peer closed")
		}
	})
	t.Run("local close unblocks a read", func(t *testing.T) {
		_, b := seqpair(t)
		errc := make(chan error, 1)
		go func() {
			_, _, err := ReadMsg(b, 0, time.Now().Add(30*time.Second))
			errc <- err
		}()
		time.Sleep(100 * time.Millisecond)
		b.Close()
		select {
		case err := <-errc:
			if !errors.Is(err, net.ErrClosed) {
				t.Fatalf("ReadMsg = %v, want net.ErrClosed", err)
			}
		case <-time.After(testDeadline):
			t.Fatal("ReadMsg still blocked after the local close")
		}
	})
}

func TestPeerCredentialsAndIdentity(t *testing.T) {
	_, b := seqpair(t)
	uid, pid, err := PeerCredentials(b)
	if err != nil {
		t.Fatalf("PeerCredentials: %v", err)
	}
	if uid != os.Getuid() || pid != os.Getpid() {
		t.Errorf("PeerCredentials = %d, %d, want %d, %d", uid, pid, os.Getuid(), os.Getpid())
	}

	id, err := PeerProcessIdentity(b)
	if err != nil {
		t.Fatalf("PeerProcessIdentity: %v", err)
	}
	defer closeFDs(id.Pidfd) // owned by the caller
	if id.UID != os.Getuid() || id.PID != os.Getpid() {
		t.Errorf("identity %d/%d, want %d/%d", id.UID, id.PID, os.Getuid(), os.Getpid())
	}
	if id.Pidfd < 0 {
		t.Fatalf("Pidfd = %d", id.Pidfd)
	}
	if !isCloexec(t, id.Pidfd) {
		t.Errorf("pidfd %d is not close-on-exec", id.Pidfd)
	}
	if _, ino := identity(t, id.Pidfd); ino != id.Ino {
		t.Errorf("Ino = %d, fstat says %d", id.Ino, ino)
	}
	// The pidfd refers to this process: a signal 0 reaches it, and a pidfd opened by PID has the
	// same pidfs inode.
	if err := unix.PidfdSendSignal(id.Pidfd, 0, nil, 0); err != nil {
		t.Errorf("pidfd_send_signal(0): %v", err)
	}
	own, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		t.Fatalf("pidfd_open: %v", err)
	}
	defer closeFDs(own)
	if _, ino := identity(t, own); ino != id.Ino {
		t.Errorf("Ino = %d, pidfd_open gives %d", id.Ino, ino)
	}

	// Every call hands out its own descriptor.
	again, err := PeerProcessIdentity(b)
	if err != nil {
		t.Fatalf("second PeerProcessIdentity: %v", err)
	}
	if again.Pidfd == id.Pidfd || again.Ino != id.Ino {
		t.Errorf("second identity %+v, first %+v", again, id)
	}
	if err := unix.Close(again.Pidfd); err != nil {
		t.Errorf("closing the second pidfd: %v", err)
	}
}
