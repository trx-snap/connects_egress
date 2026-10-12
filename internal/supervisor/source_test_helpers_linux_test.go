//go:build linux

package supervisor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

var pubTestSeq atomic.Uint64

func pubTestTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatalf("pubTestTempDir: chmod 0700: %v", err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("pubTestTempDir: stat: %v", err)
	}
	if fi.Mode().Perm() != 0700 {
		t.Fatalf("pubTestTempDir: mode = %#o, want 0700", fi.Mode().Perm())
	}
	return dir
}

func pubTestCheckSockaddrLen(t *testing.T, addr string) {
	t.Helper()
	if strings.HasPrefix(addr, "@") {
		if len(addr) >= 80 {
			t.Fatalf("abstract socket address %q length %d >= 80 limit", addr, len(addr))
		}
	} else {
		const unixPathMax = 108
		if len(addr) >= unixPathMax {
			t.Fatalf("filesystem socket address %q length %d >= unixPathMax %d", addr, len(addr), unixPathMax)
		}
	}
}

func pubTestSelfOwner(t *testing.T) WorkerOwner {
	t.Helper()
	pid := os.Getpid()
	uid := os.Geteuid()
	pfd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		t.Fatalf("pubTestSelfOwner: pidfd_open: %v", err)
	}
	defer unix.Close(pfd)
	var st unix.Stat_t
	if err := unix.Fstat(pfd, &st); err != nil {
		t.Fatalf("pubTestSelfOwner: fstat pidfd: %v", err)
	}
	return WorkerOwner{UID: uid, PID: pid, Ino: st.Ino}
}

type pubTestMockFDStore struct {
	t             *testing.T
	addr          string
	conn          *net.UnixConn
	mu            sync.Mutex
	stored        map[string]int
	barrierClosed bool
	expectedCount int
	err           error
	done          chan struct{}
	closed        bool
	stopOnce      sync.Once
}

func pubTestStartMockNotifier(t *testing.T, expectedCount int) *pubTestMockFDStore {
	t.Helper()
	seq := pubTestSeq.Add(1)
	addr := fmt.Sprintf("@pbn-%d-%d", os.Getpid(), seq)
	pubTestCheckSockaddrLen(t, addr)

	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: addr, Net: "unixgram"})
	if err != nil {
		t.Fatalf("pubTestStartMockNotifier: ListenUnixgram %q: %v", addr, err)
	}

	mock := &pubTestMockFDStore{
		t:             t,
		addr:          addr,
		conn:          conn,
		stored:        make(map[string]int),
		expectedCount: expectedCount,
		done:          make(chan struct{}),
	}

	t.Cleanup(func() {
		mock.Close()
	})

	go mock.loop()
	return mock
}

func (m *pubTestMockFDStore) Address() string {
	return m.addr
}

func (m *pubTestMockFDStore) loop() {
	defer close(m.done)
	buf := make([]byte, 512)
	oob := make([]byte, 256)

	for {
		_ = m.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, oobn, flags, _, err := m.conn.ReadMsgUnix(buf, oob)
		if err != nil {
			m.mu.Lock()
			closed := m.closed
			m.mu.Unlock()
			if closed {
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				m.mu.Lock()
				allDone := len(m.stored) == m.expectedCount && m.barrierClosed
				m.mu.Unlock()
				if allDone {
					return
				}
				continue
			}
			m.recordError(fmt.Errorf("ReadMsgUnix: %w", err))
			return
		}

		// Parse delivered rights before any truncation or payload error exits
		var fds []int
		if oobn > 0 {
			scms, parseErr := unix.ParseSocketControlMessage(oob[:oobn])
			if parseErr != nil {
				m.recordError(fmt.Errorf("ParseSocketControlMessage: %w", parseErr))
				return
			}
			for _, scm := range scms {
				gotFDs, parseRightsErr := unix.ParseUnixRights(&scm)
				if parseRightsErr != nil {
					for _, fd := range fds {
						_ = unix.Close(fd)
					}
					m.recordError(fmt.Errorf("ParseUnixRights: %w", parseRightsErr))
					return
				}
				fds = append(fds, gotFDs...)
			}
		}

		if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
			for _, fd := range fds {
				_ = unix.Close(fd)
			}
			m.recordError(fmt.Errorf("datagram truncated: flags=%#x", flags))
			return
		}

		msg := string(buf[:n])
		lines := strings.Split(strings.TrimSpace(msg), "\n")
		var isFDStore, isBarrier bool
		var fdName string

		for _, line := range lines {
			if line == "FDSTORE=1" {
				isFDStore = true
			} else if strings.HasPrefix(line, "FDNAME=") {
				fdName = strings.TrimPrefix(line, "FDNAME=")
			} else if line == "BARRIER=1" {
				isBarrier = true
			}
		}

		if isFDStore {
			if len(fds) != 1 || fdName == "" {
				for _, fd := range fds {
					_ = unix.Close(fd)
				}
				m.recordError(fmt.Errorf("malformed FDSTORE: fds=%d name=%q", len(fds), fdName))
				return
			}
			m.mu.Lock()
			if _, exists := m.stored[fdName]; exists {
				m.mu.Unlock()
				_ = unix.Close(fds[0])
				m.recordError(fmt.Errorf("duplicate FDNAME %q stored", fdName))
				return
			}
			m.stored[fdName] = fds[0]
			m.mu.Unlock()
		} else if isBarrier {
			if len(fds) != 1 {
				for _, fd := range fds {
					_ = unix.Close(fd)
				}
				m.recordError(fmt.Errorf("malformed BARRIER: fds=%d", len(fds)))
				return
			}
			// Close the barrier pipe right immediately
			_ = unix.Close(fds[0])
			m.mu.Lock()
			m.barrierClosed = true
			m.mu.Unlock()
		} else {
			for _, fd := range fds {
				_ = unix.Close(fd)
			}
			m.recordError(fmt.Errorf("unknown datagram: %q", msg))
			return
		}
	}
}

func (m *pubTestMockFDStore) recordError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err == nil {
		m.err = err
	}
	for _, fd := range m.stored {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
	}
	m.stored = make(map[string]int)
}

func (m *pubTestMockFDStore) WaitStored(timeout time.Duration) {
	m.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		err := m.err
		count := len(m.stored)
		barrier := m.barrierClosed
		m.mu.Unlock()

		if err != nil {
			m.t.Fatalf("mock fdstore error: %v", err)
		}
		if count == m.expectedCount && barrier {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	m.mu.Lock()
	count := len(m.stored)
	barrier := m.barrierClosed
	err := m.err
	m.mu.Unlock()
	m.t.Fatalf("mock fdstore timed out: count=%d/%d, barrier=%v, err=%v", count, m.expectedCount, barrier, err)
}

func (m *pubTestMockFDStore) StoredNames() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.stored))
	for name := range m.stored {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (m *pubTestMockFDStore) DupStored() map[string]int {
	m.t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make(map[string]int, len(m.stored))
	for name, fd := range m.stored {
		dup, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			for _, dfd := range res {
				_ = unix.Close(dfd)
			}
			m.t.Fatalf("DupStored: F_DUPFD_CLOEXEC %q: %v", name, err)
		}
		res[name] = dup
	}
	return res
}

func (m *pubTestMockFDStore) Close() {
	m.stopOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()

		_ = m.conn.Close()
		select {
		case <-m.done:
		case <-time.After(5 * time.Second):
			m.t.Errorf("mock fdstore loop failed to join within 5s")
		}

		m.mu.Lock()
		for name, fd := range m.stored {
			if fd >= 0 {
				_ = unix.Close(fd)
				delete(m.stored, name)
			}
		}
		m.mu.Unlock()
	})
}

type pubTestServer struct {
	t        *testing.T
	sockPath string
	listener *net.UnixListener
	cancel   context.CancelFunc
	done     chan struct{}
}

func pubTestStartServer(t *testing.T, s *Source, dir string) *pubTestServer {
	t.Helper()
	seq := pubTestSeq.Add(1)
	sockPath := filepath.Join(dir, fmt.Sprintf("p-%d.sock", seq))
	pubTestCheckSockaddrLen(t, sockPath)

	l, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: sockPath, Net: "unixpacket"})
	if err != nil {
		t.Fatalf("pubTestStartServer: ListenUnix: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	srv := &pubTestServer{
		t:        t,
		sockPath: sockPath,
		listener: l,
		cancel:   cancel,
		done:     done,
	}

	t.Cleanup(func() {
		srv.Close()
	})

	go func() {
		defer close(done)
		var wg sync.WaitGroup
		defer wg.Wait()

		for {
			conn, err := l.AcceptUnix()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(c *net.UnixConn) {
				defer wg.Done()
				_ = s.ServeConn(ctx, c)
			}(conn)
		}
	}()

	return srv
}

func (srv *pubTestServer) Socket() string {
	return srv.sockPath
}

func (srv *pubTestServer) Close() {
	srv.cancel()
	_ = srv.listener.Close()
	select {
	case <-srv.done:
	case <-time.After(5 * time.Second):
		srv.t.Errorf("pubTestServer: accept loop failed to terminate within 5s")
	}
	_ = os.Remove(srv.sockPath)
}
