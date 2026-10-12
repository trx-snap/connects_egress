//go:build linux

// Package systemd is the supervisor's adapter to the systemd file-descriptor store: sd_notify
// datagrams out, LISTEN_FDS descriptors in. Marker: FDSTORE_ADAPTER_IMPLEMENTED.
package systemd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

var (
	// ErrNoNotifySocket is returned when the notify address is empty or unusable.
	ErrNoNotifySocket = errors.New("systemd: no usable NOTIFY_SOCKET address")
	// ErrInvalidFDName is returned for an FDNAME systemd would refuse.
	ErrInvalidFDName = errors.New("systemd: invalid FDNAME")
	// ErrInvalidActivation is returned for a LISTEN_* environment this process must not trust.
	ErrInvalidActivation = errors.New("systemd: invalid LISTEN_* environment")
)

const (
	listenFDsStart = 3                     // SD_LISTEN_FDS_START
	unixPathMax    = 108                   // sizeof(sockaddr_un.sun_path)
	barrierTimeout = 2 * time.Second       // this adapter's ceiling for Barrier
	pollSlice      = 20 * time.Millisecond // longest single blocking poll in Barrier
	maxFDName      = 255                   // sd_notify FDNAME length bound
)

// Notifier sends sd_notify datagrams to one address. It holds no socket and no state: every
// call opens, uses and closes its own unconnected datagram socket, so concurrent calls are
// independent and the caller orders startup transitions itself. A message being sent proves
// only that it was enqueued to the socket, never that systemd received, accepted or stored
// anything.
type Notifier struct {
	address string
}

// NewNotifier validates address, normally the value of NOTIFY_SOCKET: an absolute pathname or an
// @abstract name, without NUL, short enough for sockaddr_un. Empty is ErrNoNotifySocket.
func NewNotifier(address string) (*Notifier, error) {
	switch {
	case address == "":
		return nil, ErrNoNotifySocket
	case strings.IndexByte(address, 0) >= 0:
		return nil, fmt.Errorf("%w: address contains NUL", ErrNoNotifySocket)
	case len(address) >= unixPathMax:
		return nil, fmt.Errorf("%w: address of %d bytes does not fit sockaddr_un", ErrNoNotifySocket, len(address))
	case address[0] != '/' && address[0] != '@':
		return nil, fmt.Errorf("%w: address %q is neither absolute nor abstract", ErrNoNotifySocket, address)
	}
	return &Notifier{address: address}, nil
}

// StoreFD asks systemd to keep fd in the unit's store under name. The descriptor is borrowed:
// the caller keeps it open for the duration of the call and still owns it afterwards. A nil
// error means the datagram was enqueued, not that systemd received it or the store accepted it.
func (n *Notifier) StoreFD(ctx context.Context, name string, fd int) error {
	if err := checkFDName(name); err != nil {
		return err
	}
	if fd < 0 {
		return fmt.Errorf("systemd: fd %d is negative", fd)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
		return fmt.Errorf("systemd: fd %d: %w", fd, err)
	}
	return n.send(ctx, "FDSTORE=1\nFDNAME="+name+"\n", fd)
}

// RemoveFD asks systemd to drop every descriptor stored under name.
func (n *Notifier) RemoveFD(ctx context.Context, name string) error {
	if err := checkFDName(name); err != nil {
		return err
	}
	return n.send(ctx, "FDSTOREREMOVE=1\nFDNAME="+name+"\n", -1)
}

// Ready sends READY=1.
func (n *Notifier) Ready(ctx context.Context) error {
	return n.send(ctx, "READY=1\n", -1)
}

// Barrier sends a standalone BARRIER=1 carrying the write end of a local pipe and waits, at most
// two seconds, for systemd to close it, which it does once every earlier datagram from this
// process has been processed. Processed is not accepted: a stored descriptor may still have
// been refused for name, kind or capacity. Timeout is wrapped context.DeadlineExceeded.
func (n *Notifier) Barrier(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, barrierTimeout)
	defer cancel()
	var p [2]int
	if err := unix.Pipe2(p[:], unix.O_CLOEXEC|unix.O_NONBLOCK); err != nil {
		return fmt.Errorf("systemd: barrier pipe: %w", err)
	}
	defer unix.Close(p[0])
	err := n.send(ctx, "BARRIER=1\n", p[1])
	// The local writer must be gone before waiting, or the pipe can never report HUP.
	unix.Close(p[1])
	if err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	fds := []unix.PollFd{{Fd: int32(p[0]), Events: unix.POLLIN}}
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("systemd: barrier: %w", err)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("systemd: barrier: %w", context.DeadlineExceeded)
		}
		if remaining > pollSlice {
			remaining = pollSlice
		}
		ms := int((remaining + time.Millisecond - 1) / time.Millisecond)
		if ms < 1 {
			ms = 1
		}
		fds[0].Revents = 0
		cnt, err := unix.Poll(fds, ms)
		switch {
		case errors.Is(err, unix.EINTR):
			continue
		case err != nil:
			return fmt.Errorf("systemd: barrier poll: %w", err)
		case cnt == 0:
			continue
		}
		re := fds[0].Revents
		switch {
		case re&(unix.POLLERR|unix.POLLNVAL) != 0:
			return fmt.Errorf("systemd: barrier pipe reported events %#x", re)
		case re&unix.POLLIN != 0:
			return errors.New("systemd: barrier pipe received unexpected data")
		case re&unix.POLLHUP != 0:
			return nil
		}
	}
}

// send transmits one datagram, with fd attached as SCM_RIGHTS when non-negative, from a socket
// that lives for this call only. The datagram is sent whole or not at all; EAGAIN is returned
// as is, without retry.
func (n *Notifier) send(ctx context.Context, msg string, fd int) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("systemd: notify: %w", err)
	}
	sock, err := unix.Socket(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("systemd: notify socket: %w", err)
	}
	defer unix.Close(sock)
	var oob []byte
	if fd >= 0 {
		oob = unix.UnixRights(fd)
	}
	line, _, _ := strings.Cut(msg, "\n")
	sent, err := unix.SendmsgN(sock, []byte(msg), oob, &unix.SockaddrUnix{Name: n.address}, unix.MSG_NOSIGNAL)
	switch {
	case err != nil:
		return fmt.Errorf("systemd: notify %s: %w", line, err)
	case sent != len(msg):
		return fmt.Errorf("systemd: notify %s: sent %d of %d bytes", line, sent, len(msg))
	}
	return nil
}

// checkFDName enforces sd_notify's FDNAME rule: 1..255 printable ASCII bytes, no colon.
func checkFDName(name string) error {
	if name == "" || len(name) > maxFDName {
		return fmt.Errorf("%w: length %d not in 1..%d", ErrInvalidFDName, len(name), maxFDName)
	}
	for i := 0; i < len(name); i++ {
		if c := name[i]; c < 0x20 || c > 0x7e || c == ':' {
			return fmt.Errorf("%w: byte %#x at %d", ErrInvalidFDName, c, i)
		}
	}
	return nil
}

// ConsumeInheritedFDs takes ownership of the descriptors systemd passed at startup, by name.
// It is called once, before this process opens descriptors of its own or starts goroutines,
// because it trusts that fds 3..3+LISTEN_FDS are exactly the inherited ones. LISTEN_PID,
// LISTEN_FDS, LISTEN_FDNAMES and LISTEN_PIDFDID are captured and cleared on every path so a
// child cannot inherit them. limit is the caller's bounded capacity for this unit; a count
// above it, an unsafe count or a PID that is not this process is ErrInvalidActivation and no
// descriptor is touched. Once PID and count are trusted the range is owned: every descriptor is
// made close-on-exec first, and a malformed or missing LISTEN_FDNAMES then closes the whole
// range once. Store order is arbitrary, so descriptors are returned by name only. Content is not checked here; AdoptEpoch does that.
func ConsumeInheritedFDs(limit int) (map[string]int, error) {
	pid, havePID := os.LookupEnv("LISTEN_PID")
	count, haveCount := os.LookupEnv("LISTEN_FDS")
	names, haveNames := os.LookupEnv("LISTEN_FDNAMES")
	_, havePIDFDID := os.LookupEnv("LISTEN_PIDFDID")
	for _, v := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES", "LISTEN_PIDFDID"} {
		os.Unsetenv(v)
	}
	if !havePID && !haveCount && !haveNames && !havePIDFDID {
		return map[string]int{}, nil
	}
	if limit <= 0 {
		return nil, fmt.Errorf("systemd: fd limit %d is not positive", limit)
	}
	if !haveCount {
		return nil, fmt.Errorf("%w: LISTEN_FDS missing", ErrInvalidActivation)
	}
	n, err := strconv.Atoi(count)
	if err != nil || n < 0 {
		return nil, fmt.Errorf("%w: LISTEN_FDS=%q is not a count", ErrInvalidActivation, count)
	}
	if n == 0 && names == "" {
		return map[string]int{}, nil
	}
	if n > math.MaxInt-listenFDsStart {
		return nil, fmt.Errorf("%w: LISTEN_FDS=%d overflows the descriptor range", ErrInvalidActivation, n)
	}
	if n > limit {
		return nil, fmt.Errorf("%w: LISTEN_FDS=%d exceeds limit %d", ErrInvalidActivation, n, limit)
	}
	if p, err := strconv.Atoi(pid); !havePID || err != nil || p <= 0 || p != os.Getpid() {
		return nil, fmt.Errorf("%w: LISTEN_PID=%q is not this process %d", ErrInvalidActivation, pid, os.Getpid())
	}

	// Ownership starts here: the range is ours to keep or to close.
	owned := make([]int, 0, n)
	for i := 0; i < n; i++ {
		owned = append(owned, listenFDsStart+i)
	}
	fail := func(err error) (map[string]int, error) {
		for _, fd := range owned {
			unix.Close(fd)
		}
		return nil, err
	}
	for _, fd := range owned {
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil {
			return fail(fmt.Errorf("%w: fd %d: %w", ErrInvalidActivation, fd, err))
		}
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
			return fail(fmt.Errorf("systemd: fd %d set close-on-exec: %w", fd, err))
		}
	}
	if !haveNames {
		return fail(fmt.Errorf("%w: LISTEN_FDNAMES missing", ErrInvalidActivation))
	}
	list := strings.Split(names, ":")
	if len(list) != n {
		return fail(fmt.Errorf("%w: LISTEN_FDNAMES names %d descriptors, LISTEN_FDS says %d", ErrInvalidActivation, len(list), n))
	}
	out := make(map[string]int, n)
	for i, name := range list {
		if err := checkFDName(name); err != nil {
			return fail(fmt.Errorf("%w: LISTEN_FDNAMES entry %d: %w", ErrInvalidActivation, i, err))
		}
		if _, dup := out[name]; dup {
			return fail(fmt.Errorf("%w: LISTEN_FDNAMES repeats %q", ErrInvalidActivation, name))
		}
		out[name] = owned[i]
	}
	return out, nil
}
