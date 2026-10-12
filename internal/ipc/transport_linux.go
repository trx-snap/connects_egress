//go:build linux

package ipc

// The Linux transport of the wire contract: one packet per SOCK_SEQPACKET datagram, the setup
// welcome's descriptors as SCM_RIGHTS, and the peer's kernel identity. The caller owns the
// connection and serializes each direction (one read and one write may run concurrently). Each
// helper sets only its own direction's deadline and never restores it, never closes, reconnects or
// watches the connection: cancellation is the caller closing it. Authentication policy belongs to
// the session owner; these helpers only report what the kernel says.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// ErrInvalidTransport reports a transport argument the helpers refuse: a nil, unusable or
// non-SOCK_SEQPACKET connection, a zero deadline, a descriptor count outside the contract, a
// descriptor number that is not a valid int32, or received control data that is not exactly the
// SCM_RIGHTS the packet calls for.
var ErrInvalidTransport = errors.New("ipc: invalid transport")

// maxRights bounds the descriptors one packet may carry: a 1v1 setup welcome's five.
const maxRights = Tracks1v1

// PeerIdentity is what the kernel reports about the connection's peer: its credentials
// (SO_PEERCRED), a pidfd referring to it (SO_PEERPIDFD) and that pidfd's pidfs inode, which no
// other process shares even after the PID is reused. The caller owns Pidfd and closes it.
type PeerIdentity struct {
	UID   int
	PID   int
	Ino   uint64
	Pidfd int
}

// PeerCredentials returns the UID and PID of conn's peer as the kernel reports them. It enforces
// no policy: whether the peer is acceptable is the session owner's decision. Both are -1 on error.
func PeerCredentials(conn *net.UnixConn) (uid, pid int, err error) {
	raw, err := seqpacket(conn)
	if err != nil {
		return -1, -1, err
	}
	cred, err := peerCred(raw)
	if err != nil {
		return -1, -1, err
	}
	return int(cred.Uid), int(cred.Pid), nil
}

// PeerProcessIdentity returns the peer's credentials with a pidfd referring to it and the pidfd's
// pidfs inode. A kernel without SO_PEERPIDFD (before 6.5) or without pidfs (before 6.9, when
// every pidfd shared one anonymous inode) is an error, never a PID-only identity. On success the
// caller owns Pidfd; on error nothing is left open and Pidfd is -1.
func PeerProcessIdentity(conn *net.UnixConn) (PeerIdentity, error) {
	none := PeerIdentity{Pidfd: -1}
	raw, err := seqpacket(conn)
	if err != nil {
		return none, err
	}
	cred, err := peerCred(raw)
	if err != nil {
		return none, err
	}
	pidfd := -1
	var perr error
	if err := raw.Control(func(fd uintptr) {
		pidfd, perr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
	}); err != nil {
		return none, err
	}
	if perr != nil {
		return none, fmt.Errorf("ipc: SO_PEERPIDFD: %w", perr)
	}
	var fs unix.Statfs_t
	if err := unix.Fstatfs(pidfd, &fs); err != nil {
		closeFDs(pidfd)
		return none, fmt.Errorf("ipc: fstatfs pidfd: %w", err)
	}
	if fs.Type != unix.PID_FS_MAGIC {
		closeFDs(pidfd)
		return none, fmt.Errorf("ipc: pidfd on filesystem %#x is not pidfs, no inode identity: %w", fs.Type, unix.ENOTSUP)
	}
	var st unix.Stat_t
	if err := unix.Fstat(pidfd, &st); err != nil {
		closeFDs(pidfd)
		return none, fmt.Errorf("ipc: fstat pidfd: %w", err)
	}
	return PeerIdentity{UID: int(cred.Uid), PID: int(cred.Pid), Ino: st.Ino, Pidfd: pidfd}, nil
}

// WriteMsg sends p as one datagram with fds attached as SCM_RIGHTS, by deadline. A setup welcome
// carries exactly len(Welcome.Tracks) descriptors, in that array's order; every other packet
// carries none. The descriptors stay the caller's: borrowed for the send, never closed here. The
// packet, the descriptor count and every descriptor number are checked before anything is
// written, and a partial send is an error that is not retried.
func WriteMsg(conn *net.UnixConn, p Packet, fds []int, deadline time.Time) error {
	if deadline.IsZero() {
		return fmt.Errorf("%w: zero deadline", ErrInvalidTransport)
	}
	data, err := EncodePacket(p)
	if err != nil {
		return err
	}
	return writeDatagram(conn, p.Type, data, fds, rightsFor(p), deadline)
}

// writeDatagram sends the encoded packet data of a typ packet as one datagram with fds attached
// as SCM_RIGHTS, by deadline, after checking that exactly want descriptors are given and that
// every number is a valid int32. The descriptors are borrowed, never closed; a partial send is an
// error that is not retried.
func writeDatagram(conn *net.UnixConn, typ string, data []byte, fds []int, want int, deadline time.Time) error {
	if len(fds) != want {
		return fmt.Errorf("%w: %s packet with %d descriptors, want %d", ErrInvalidTransport, typ, len(fds), want)
	}
	for _, fd := range fds {
		if fd < 0 || fd > math.MaxInt32 {
			return fmt.Errorf("%w: descriptor number %d", ErrInvalidTransport, fd)
		}
	}
	if _, err := seqpacket(conn); err != nil {
		return err
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	var oob []byte
	if len(fds) > 0 {
		oob = unix.UnixRights(fds...)
	}
	n, oobn, err := conn.WriteMsgUnix(data, oob, nil)
	switch {
	case err != nil:
		return err
	case n != len(data) || oobn != len(oob):
		return fmt.Errorf("ipc: partial send: %d of %d bytes, %d of %d control bytes", n, len(data), oobn, len(oob))
	}
	return nil
}

// ReadMsg receives one datagram by deadline and returns its packet with the descriptors it
// carried, at most maxFDs (0 to 5) of them, close-on-exec. The descriptors become the caller's
// only with a nil error: on any error the packet is zero, the list nil and every descriptor the
// datagram delivered has been closed. A datagram beyond MaxPacketSize is ErrPacketTooLarge and
// one that breaks the wire contract keeps the codec's error; control data other than SCM_RIGHTS,
// truncated control data and a descriptor count other than the packet calls for are
// ErrInvalidTransport. The peer closing the connection is io.EOF; a deadline or a local close is
// the net package's own error.
func ReadMsg(conn *net.UnixConn, maxFDs int, deadline time.Time) (Packet, []int, error) {
	data, fds, err := readDatagram(conn, maxFDs, deadline)
	if err != nil {
		return Packet{}, nil, err
	}
	p, err := DecodePacket(data)
	if err != nil {
		closeFDs(fds...)
		return Packet{}, nil, err
	}
	if want := rightsFor(p); len(fds) != want {
		closeFDs(fds...)
		return Packet{}, nil, fmt.Errorf("%w: %s packet with %d descriptors, want %d", ErrInvalidTransport, p.Type, len(fds), want)
	}
	return p, fds, nil
}

// readDatagram receives one datagram by deadline and returns its undecoded bytes with the
// descriptors it carried, at most maxFDs of them, close-on-exec. The descriptors are the caller's
// only with a nil error: on any error the data is nil, the list nil and every descriptor the
// datagram delivered has been closed. The caller decodes the bytes and, when that or its own
// descriptor-count check fails, closes the descriptors itself.
func readDatagram(conn *net.UnixConn, maxFDs int, deadline time.Time) ([]byte, []int, error) {
	switch {
	case maxFDs < 0 || maxFDs > maxRights:
		return nil, nil, fmt.Errorf("%w: maxFDs %d is outside 0..%d", ErrInvalidTransport, maxFDs, maxRights)
	case deadline.IsZero():
		return nil, nil, fmt.Errorf("%w: zero deadline", ErrInvalidTransport)
	}
	if _, err := seqpacket(conn); err != nil {
		return nil, nil, err
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, nil, err
	}
	buf := make([]byte, MaxPacketSize)
	// Room for one descriptor too many, so that an excess is received and rejected rather than
	// truncated away. The net package receives with MSG_CMSG_CLOEXEC on Linux.
	oob := make([]byte, unix.CmsgSpace((maxFDs+1)*4))
	n, oobn, flags, _, err := conn.ReadMsgUnix(buf, oob)
	if errors.Is(err, io.EOF) {
		err = io.EOF // ReadMsgUnix wraps the peer's close in an OpError; callers compare to io.EOF
	}
	fds, cerr := parseRights(oob[:oobn])
	switch {
	case err != nil:
	case cerr != nil:
		err = cerr
	case flags&unix.MSG_CTRUNC != 0:
		err = fmt.Errorf("%w: control data truncated", ErrInvalidTransport)
	case flags&unix.MSG_TRUNC != 0:
		err = fmt.Errorf("%w: datagram exceeds %d bytes", ErrPacketTooLarge, MaxPacketSize)
	case len(fds) > maxFDs:
		err = fmt.Errorf("%w: %d descriptors received, at most %d expected", ErrInvalidTransport, len(fds), maxFDs)
	}
	if err != nil {
		closeFDs(fds...)
		return nil, nil, err
	}
	return buf[:n], fds, nil
}

// rightsFor returns the number of descriptors a valid packet carries: a setup welcome's tracks,
// otherwise none.
func rightsFor(p Packet) int {
	if p.Type == TypeWelcome && p.Welcome != nil && p.Welcome.Setup {
		return len(p.Welcome.Tracks)
	}
	return 0
}

// parseRights returns every descriptor the control data oob delivers, and the first thing wrong
// with it: a message other than SCM_RIGHTS, a rights payload that is not whole descriptors, or a
// header that does not fit. A bad message of known length is skipped rather than stopping the
// parse, so the descriptors of later messages are still returned, with the error, for closing.
//
// SCM_PIDFD is the one unexpected message that also delivers a descriptor, when the socket has
// SO_PASSPIDFD set. It is always an error, and its descriptor is returned with the rights only so
// that the caller closes it: a non-nil error is what keeps it from ever being accepted.
func parseRights(oob []byte) (fds []int, err error) {
	for len(oob) > 0 {
		if len(oob) < unix.SizeofCmsghdr {
			return fds, firstErr(err, fmt.Errorf("%w: %d trailing control bytes", ErrInvalidTransport, len(oob)))
		}
		hdr, data, rest, perr := unix.ParseOneSocketControlMessage(oob)
		if perr != nil {
			return fds, firstErr(err, fmt.Errorf("%w: control message header: %v", ErrInvalidTransport, perr))
		}
		switch {
		case hdr.Level == unix.SOL_SOCKET && hdr.Type == unix.SCM_PIDFD:
			err = firstErr(err, fmt.Errorf("%w: unexpected SCM_PIDFD control message of %d data bytes", ErrInvalidTransport, len(data)))
			// Only the kernel's own int-sized payload names a descriptor; any other length is not
			// guessed at.
			if len(data) == 4 {
				if fd := int(int32(binary.NativeEndian.Uint32(data))); fd >= 0 {
					fds = append(fds, fd)
				}
			}
		case hdr.Level != unix.SOL_SOCKET || hdr.Type != unix.SCM_RIGHTS:
			err = firstErr(err, fmt.Errorf("%w: unexpected control message level %d type %d", ErrInvalidTransport, hdr.Level, hdr.Type))
		case len(data)%4 != 0:
			err = firstErr(err, fmt.Errorf("%w: SCM_RIGHTS data of %d bytes", ErrInvalidTransport, len(data)))
		default:
			got, perr := unix.ParseUnixRights(&unix.SocketControlMessage{Header: hdr, Data: data})
			if perr != nil {
				err = firstErr(err, fmt.Errorf("%w: SCM_RIGHTS: %v", ErrInvalidTransport, perr))
			}
			fds = append(fds, got...)
		}
		oob = rest
	}
	return fds, err
}

func firstErr(have, err error) error {
	if have != nil {
		return have
	}
	return err
}

// seqpacket checks that conn is a usable SOCK_SEQPACKET connection and returns its raw
// connection. A closed connection is the net package's own error.
func seqpacket(conn *net.UnixConn) (syscall.RawConn, error) {
	if conn == nil {
		return nil, fmt.Errorf("%w: nil connection", ErrInvalidTransport)
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTransport, err)
	}
	var typ int
	var terr error
	if err := raw.Control(func(fd uintptr) {
		typ, terr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
	}); err != nil {
		return nil, err
	}
	switch {
	case terr != nil:
		return nil, fmt.Errorf("ipc: SO_TYPE: %w", terr)
	case typ != unix.SOCK_SEQPACKET:
		return nil, fmt.Errorf("%w: socket type %d is not SOCK_SEQPACKET", ErrInvalidTransport, typ)
	}
	return raw, nil
}

func peerCred(raw syscall.RawConn) (*unix.Ucred, error) {
	var cred *unix.Ucred
	var cerr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return nil, err
	}
	if cerr != nil {
		return nil, fmt.Errorf("ipc: SO_PEERCRED: %w", cerr)
	}
	return cred, nil
}

// closeFDs closes each descriptor once. A failed close is not retried: the descriptor is gone
// either way, and there is nothing further to do with it.
func closeFDs(fds ...int) {
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}
