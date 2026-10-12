//go:build linux

package systemd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const (
	childScenarioEnv = "CONNECTS_EGRESS_FDSTORE_CHILD_SCENARIO"
)

func closeRights(fds []int) {
	for i, fd := range fds {
		if fd >= 0 {
			unix.Close(fd)
			fds[i] = -1
		}
	}
}

// Helper to create a temporary memfd holding content, returning an owned FD.
// Fails clearly without pipe fallbacks because memfd is a verified prerequisite.
func createTestFD(t *testing.T, content string) int {
	t.Helper()
	fd, err := unix.MemfdCreate("test-fd", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatalf("MemfdCreate: %v", err)
	}
	if len(content) > 0 {
		if err := unix.Ftruncate(fd, int64(len(content))); err != nil {
			unix.Close(fd)
			t.Fatalf("Ftruncate: %v", err)
		}
		if _, err := unix.Pwrite(fd, []byte(content), 0); err != nil {
			unix.Close(fd)
			t.Fatalf("Pwrite: %v", err)
		}
	}
	t.Cleanup(func() { unix.Close(fd) })
	return fd
}

func listenUnixgram(t *testing.T, address string) *net.UnixConn {
	t.Helper()
	laddr := &net.UnixAddr{Name: address, Net: "unixgram"}
	conn, err := net.ListenUnixgram("unixgram", laddr)
	if err != nil {
		t.Fatalf("ListenUnixgram %q: %v", address, err)
	}
	t.Cleanup(func() {
		conn.Close()
		if len(address) > 0 && address[0] != '@' {
			os.Remove(address)
		}
	})
	return conn
}

func recvDatagram(t *testing.T, conn *net.UnixConn, timeout time.Duration) (string, []int) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, 512)
	oob := make([]byte, 256)
	n, oobn, flags, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		t.Fatalf("ReadMsgUnix: %v", err)
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		t.Fatalf("ReadMsgUnix truncated: flags=%#x", flags)
	}
	payload := string(buf[:n])
	var fds []int
	if oobn > 0 {
		scms, err := unix.ParseSocketControlMessage(oob[:oobn])
		if err != nil {
			t.Fatalf("ParseSocketControlMessage: %v", err)
		}
		for _, scm := range scms {
			gotFDs, err := unix.ParseUnixRights(&scm)
			if err != nil {
				closeRights(fds)
				t.Fatalf("ParseUnixRights: %v", err)
			}
			fds = append(fds, gotFDs...)
		}
	}
	return payload, fds
}

func isFDOpen(fd int) bool {
	_, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	return err == nil
}

func isFDClosed(fd int) bool {
	_, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	return errors.Is(err, unix.EBADF)
}

func isFDCloexec(fd int) bool {
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		return false
	}
	return flags&unix.FD_CLOEXEC != 0
}

func readFD(fd int, length int) ([]byte, error) {
	buf := make([]byte, length)
	n, err := unix.Pread(fd, buf, 0)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

type barrierJob struct {
	result chan error
	cancel context.CancelFunc
}

func startBarrier(t *testing.T, n *Notifier, parentCtx context.Context) *barrierJob {
	t.Helper()
	ctx, cancel := context.WithCancel(parentCtx)
	resCh := make(chan error, 1)
	doneCh := make(chan struct{})

	// Registered before launching; cancels and waits bounded <= 3s.
	// LIFO cleanup guarantees any received rights cleanup runs before this join.
	t.Cleanup(func() {
		cancel()
		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Errorf("startBarrier: goroutine did not terminate within 3s")
		}
	})

	go func() {
		defer close(doneCh)
		resCh <- n.Barrier(ctx)
	}()

	return &barrierJob{result: resCh, cancel: cancel}
}

// Lane 1: Datagram receiver tests for StoreFD, RemoveFD, Ready on pathname and @abstract sockets.
func TestNotifier_DatagramProtocol(t *testing.T) {
	socketAddresses := []struct {
		name string
		addr func(t *testing.T) string
	}{
		{
			name: "pathname",
			addr: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "notify.sock")
			},
		},
		{
			name: "abstract",
			addr: func(t *testing.T) string {
				return fmt.Sprintf("@connects-notify-%d-%d", os.Getpid(), time.Now().UnixNano())
			},
		},
	}

	for _, sa := range socketAddresses {
		t.Run(sa.name, func(t *testing.T) {
			addr := sa.addr(t)
			conn := listenUnixgram(t, addr)
			notifier, err := NewNotifier(addr)
			if err != nil {
				t.Fatalf("NewNotifier(%q): %v", addr, err)
			}

			// StoreFD: exact payload and one SCM_RIGHTS FD matching original identity
			borrowedFD := createTestFD(t, "borrowed-test-content")
			var origStat unix.Stat_t
			if err := unix.Fstat(borrowedFD, &origStat); err != nil {
				t.Fatalf("fstat borrowedFD: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			if err := notifier.StoreFD(ctx, "lane1-name", borrowedFD); err != nil {
				t.Fatalf("StoreFD: %v", err)
			}

			payload, rights := recvDatagram(t, conn, 2*time.Second)
			t.Cleanup(func() { closeRights(rights) })

			wantPayload := "FDSTORE=1\nFDNAME=lane1-name\n"
			if payload != wantPayload {
				t.Fatalf("StoreFD payload = %q, want %q", payload, wantPayload)
			}
			if len(rights) != 1 {
				t.Fatalf("StoreFD delivered %d rights, want 1", len(rights))
			}
			rcvFD := rights[0]

			var rcvStat unix.Stat_t
			if err := unix.Fstat(rcvFD, &rcvStat); err != nil {
				t.Fatalf("fstat rcvFD: %v", err)
			}
			if rcvStat.Dev != origStat.Dev || rcvStat.Ino != origStat.Ino || rcvStat.Size != origStat.Size {
				t.Fatalf("identity mismatch: got dev=%d ino=%d size=%d, want dev=%d ino=%d size=%d",
					rcvStat.Dev, rcvStat.Ino, rcvStat.Size, origStat.Dev, origStat.Ino, origStat.Size)
			}

			rcvBytes, err := readFD(rcvFD, 64)
			closeRights(rights) // close received duplicate
			if err != nil || string(rcvBytes) != "borrowed-test-content" {
				t.Fatalf("rcvFD content = %q, err = %v", string(rcvBytes), err)
			}

			// Original remains usable
			origBytes, err := readFD(borrowedFD, 64)
			if err != nil || string(origBytes) != "borrowed-test-content" {
				t.Fatalf("original borrowedFD content = %q, err = %v after duplicate closed", string(origBytes), err)
			}

			// RemoveFD: exact payload, zero rights
			if err := notifier.RemoveFD(ctx, "lane1-name"); err != nil {
				t.Fatalf("RemoveFD: %v", err)
			}
			remPayload, remRights := recvDatagram(t, conn, 2*time.Second)
			t.Cleanup(func() { closeRights(remRights) })

			wantRemPayload := "FDSTOREREMOVE=1\nFDNAME=lane1-name\n"
			if remPayload != wantRemPayload {
				t.Fatalf("RemoveFD payload = %q, want %q", remPayload, wantRemPayload)
			}
			if len(remRights) != 0 {
				t.Fatalf("RemoveFD delivered %d rights, want 0", len(remRights))
			}

			// Ready: exact payload, zero rights
			if err := notifier.Ready(ctx); err != nil {
				t.Fatalf("Ready: %v", err)
			}
			rdyPayload, rdyRights := recvDatagram(t, conn, 2*time.Second)
			t.Cleanup(func() { closeRights(rdyRights) })

			wantRdyPayload := "READY=1\n"
			if rdyPayload != wantRdyPayload {
				t.Fatalf("Ready payload = %q, want %q", rdyPayload, wantRdyPayload)
			}
			if len(rdyRights) != 0 {
				t.Fatalf("Ready delivered %d rights, want 0", len(rdyRights))
			}
		})
	}
}

// Lane 2: Constructor, FDNAME, negative/closed descriptor and pre-canceled context rejections.
func TestNotifier_ConstructorAndValidation(t *testing.T) {
	// Constructor rejection cases
	invalidAddresses := []struct {
		name string
		addr string
	}{
		{"empty", ""},
		{"relative", "relative/path.sock"},
		{"with_nul", "/var/run/\x00notify.sock"},
		{"len108", "/" + strings.Repeat("a", 107)},
		{"len110", "/" + strings.Repeat("a", 109)},
	}
	for _, tc := range invalidAddresses {
		_, err := NewNotifier(tc.addr)
		if !errors.Is(err, ErrNoNotifySocket) {
			t.Errorf("NewNotifier(%s %q): got err = %v, want ErrNoNotifySocket", tc.name, tc.addr, err)
		}
	}

	// Constructor accepted cases
	validAddresses := []string{
		"/var/run/systemd/notify",
		"@/org/freedesktop/systemd1/notify",
		"@" + strings.Repeat("x", 106),
	}
	for _, addr := range validAddresses {
		n, err := NewNotifier(addr)
		if err != nil || n == nil {
			t.Errorf("NewNotifier(%q): got %v, want valid Notifier", addr, err)
		}
	}

	dummyAddr := filepath.Join(t.TempDir(), "dummy.sock")
	conn := listenUnixgram(t, dummyAddr)
	notifier, err := NewNotifier(dummyAddr)
	if err != nil {
		t.Fatalf("NewNotifier: %v", err)
	}

	// FDNAME rejection cases
	invalidNames := []struct {
		name   string
		fdName string
	}{
		{"empty", ""},
		{"colon", "my:store"},
		{"newline", "my\nstore"},
		{"DEL", "my\x7fstore"},
		{"nonASCII", "my\x80store"},
		{"len256", strings.Repeat("a", 256)},
	}
	ctx := context.Background()
	for _, tc := range invalidNames {
		if err := notifier.StoreFD(ctx, tc.fdName, 0); !errors.Is(err, ErrInvalidFDName) {
			t.Errorf("StoreFD(%s %q): got err = %v, want ErrInvalidFDName", tc.name, tc.fdName, err)
		}
		if err := notifier.RemoveFD(ctx, tc.fdName); !errors.Is(err, ErrInvalidFDName) {
			t.Errorf("RemoveFD(%s %q): got err = %v, want ErrInvalidFDName", tc.name, tc.fdName, err)
		}
	}

	// FDNAME accepted cases: printable space and len 255
	validNames := []string{
		"printable space",
		strings.Repeat("c", 255),
	}
	testFD := createTestFD(t, "data")
	for _, name := range validNames {
		if err := notifier.StoreFD(ctx, name, testFD); err != nil {
			t.Errorf("StoreFD(%q): got unexpected err = %v", name, err)
		}
		_, rights := recvDatagram(t, conn, 2*time.Second)
		closeRights(rights)
	}

	// Negative and closed FD rejection; borrowed FD untouched
	if err := notifier.StoreFD(ctx, "validname", -1); err == nil {
		t.Error("StoreFD(-1) succeeded, want error")
	}

	// Obtain a disposable fresh FD directly, close once, then assert rejection before opening any other FD.
	disposableFD, err := unix.MemfdCreate("disposable-closed", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatalf("MemfdCreate disposable: %v", err)
	}
	if err := unix.Close(disposableFD); err != nil {
		t.Fatalf("Close disposable: %v", err)
	}
	if err := notifier.StoreFD(ctx, "validname", disposableFD); err == nil {
		t.Error("StoreFD(closedFD) succeeded, want error")
	}

	borrowed := createTestFD(t, "still-valid")
	if err := notifier.StoreFD(ctx, "bad:name", borrowed); !errors.Is(err, ErrInvalidFDName) {
		t.Errorf("StoreFD with bad name: got err = %v, want ErrInvalidFDName", err)
	}
	if !isFDOpen(borrowed) {
		t.Fatalf("borrowed descriptor was closed after invalid name rejection")
	}
	b, err := readFD(borrowed, 32)
	if err != nil || string(b) != "still-valid" {
		t.Fatalf("borrowed descriptor content corrupted or unreadable: %q, %v", string(b), err)
	}

	// Pre-canceled context rejects without send
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := notifier.StoreFD(canceledCtx, "validname", borrowed); err == nil {
		t.Error("StoreFD with pre-canceled context succeeded, want error")
	}
	if err := notifier.RemoveFD(canceledCtx, "validname"); err == nil {
		t.Error("RemoveFD with pre-canceled context succeeded, want error")
	}
	if err := notifier.Ready(canceledCtx); err == nil {
		t.Error("Ready with pre-canceled context succeeded, want error")
	}

	// Ensure no datagram arrived on conn
	if err := conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, 64)
	if _, _, _, _, err := conn.ReadMsgUnix(buf, nil); err == nil {
		t.Error("received unexpected datagram from pre-canceled call")
	}
}

// Lane 3: Barrier protocol, timings, cancellation, unexpected data and error transport.
func TestNotifier_Barrier(t *testing.T) {
	// Case 1: Happy path: read standalone BARRIER=1 + writer FD; close received FD, require nil
	t.Run("success", func(t *testing.T) {
		addr := filepath.Join(t.TempDir(), "barrier-happy.sock")
		conn := listenUnixgram(t, addr)
		notifier, err := NewNotifier(addr)
		if err != nil {
			t.Fatalf("NewNotifier: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		job := startBarrier(t, notifier, ctx)
		payload, rights := recvDatagram(t, conn, 2*time.Second)
		t.Cleanup(func() { closeRights(rights) })

		if payload != "BARRIER=1\n" || len(rights) != 1 {
			t.Fatalf("unexpected datagram: payload=%q, rights=%v", payload, rights)
		}
		closeRights(rights) // Closing writer triggers POLLHUP in Barrier

		select {
		case err := <-job.result:
			if err != nil {
				t.Fatalf("Barrier returned err = %v, want nil", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for Barrier to complete")
		}
	})

	// Case 2: Hold received FD through short context deadline -> DeadlineExceeded within generous 500ms
	t.Run("short_deadline", func(t *testing.T) {
		addr := filepath.Join(t.TempDir(), "barrier-deadline.sock")
		conn := listenUnixgram(t, addr)
		notifier, err := NewNotifier(addr)
		if err != nil {
			t.Fatalf("NewNotifier: %v", err)
		}

		shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		job := startBarrier(t, notifier, shortCtx)
		payload, rights := recvDatagram(t, conn, 2*time.Second)
		t.Cleanup(func() { closeRights(rights) })

		if payload != "BARRIER=1\n" || len(rights) != 1 {
			t.Fatalf("unexpected datagram: payload=%q, rights=%v", payload, rights)
		}

		select {
		case err := <-job.result:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Barrier returned err = %v, want context.DeadlineExceeded", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Barrier failed to return DeadlineExceeded within 500ms")
		}
	})

	// Case 3: Cancel after receive -> Canceled within 500ms
	t.Run("cancel_after_receive", func(t *testing.T) {
		addr := filepath.Join(t.TempDir(), "barrier-cancel.sock")
		conn := listenUnixgram(t, addr)
		notifier, err := NewNotifier(addr)
		if err != nil {
			t.Fatalf("NewNotifier: %v", err)
		}

		job := startBarrier(t, notifier, context.Background())
		payload, rights := recvDatagram(t, conn, 2*time.Second)
		t.Cleanup(func() { closeRights(rights) })

		if payload != "BARRIER=1\n" || len(rights) != 1 {
			t.Fatalf("unexpected datagram: payload=%q, rights=%v", payload, rights)
		}

		job.cancel()

		select {
		case err := <-job.result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Barrier returned err = %v, want context.Canceled", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Barrier failed to return Canceled within 500ms")
		}
	})

	// Case 4: Write one byte then close -> non-nil unexpected-data error
	t.Run("unexpected_data", func(t *testing.T) {
		addr := filepath.Join(t.TempDir(), "barrier-data.sock")
		conn := listenUnixgram(t, addr)
		notifier, err := NewNotifier(addr)
		if err != nil {
			t.Fatalf("NewNotifier: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		job := startBarrier(t, notifier, ctx)
		payload, rights := recvDatagram(t, conn, 2*time.Second)
		t.Cleanup(func() { closeRights(rights) })

		if payload != "BARRIER=1\n" || len(rights) != 1 {
			t.Fatalf("unexpected datagram: payload=%q, rights=%v", payload, rights)
		}

		if _, err := unix.Write(rights[0], []byte{0x42}); err != nil {
			t.Fatalf("write to barrier pipe: %v", err)
		}
		closeRights(rights)

		select {
		case err := <-job.result:
			if err == nil || !strings.Contains(err.Error(), "unexpected data") {
				t.Fatalf("Barrier returned err = %v, want unexpected data error", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Barrier failed to return unexpected-data error within 500ms")
		}
	})

	// Case 5: Default 2s bound held receiver <= 3s
	t.Run("default_timeout_held", func(t *testing.T) {
		addr := filepath.Join(t.TempDir(), "barrier-hold.sock")
		conn := listenUnixgram(t, addr)
		notifier, err := NewNotifier(addr)
		if err != nil {
			t.Fatalf("NewNotifier: %v", err)
		}

		job := startBarrier(t, notifier, context.Background())
		payload, rights := recvDatagram(t, conn, 2*time.Second)
		t.Cleanup(func() { closeRights(rights) })

		if payload != "BARRIER=1\n" || len(rights) != 1 {
			t.Fatalf("unexpected datagram: payload=%q, rights=%v", payload, rights)
		}

		select {
		case err := <-job.result:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Barrier returned err = %v, want context.DeadlineExceeded", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Barrier exceeded 3s bounded wait for 2s timeout")
		}
	})

	// Case 6: Empty/error transport closes resources
	t.Run("unreachable_transport", func(t *testing.T) {
		notifier, err := NewNotifier("/nonexistent/systemd/notify.sock")
		if err != nil {
			t.Fatalf("NewNotifier: %v", err)
		}
		job := startBarrier(t, notifier, context.Background())
		select {
		case err := <-job.result:
			if err == nil {
				t.Fatal("Barrier succeeded on unreachable address, want send error")
			}
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for Barrier send failure")
		}
	})
}

// Lane 4: Subprocess activation environment and descriptor inheritance tests.
// Helper to create two parent extra files (memfds) passed as ExtraFiles to child.
// Fails clearly without pipe fallbacks because memfd is a verified prerequisite.
func createParentExtraFiles(t *testing.T, payload1, payload2 string) (*os.File, *os.File) {
	t.Helper()
	fd1, err := unix.MemfdCreate("child-fd-3", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatalf("MemfdCreate child-fd-3: %v", err)
	}
	if err := unix.Ftruncate(fd1, int64(len(payload1))); err != nil {
		unix.Close(fd1)
		t.Fatalf("Ftruncate child-fd-3: %v", err)
	}
	if _, err := unix.Pwrite(fd1, []byte(payload1), 0); err != nil {
		unix.Close(fd1)
		t.Fatalf("Pwrite child-fd-3: %v", err)
	}
	f1 := os.NewFile(uintptr(fd1), "extra-3")
	t.Cleanup(func() { f1.Close() })

	fd2, err := unix.MemfdCreate("child-fd-4", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatalf("MemfdCreate child-fd-4: %v", err)
	}
	if err := unix.Ftruncate(fd2, int64(len(payload2))); err != nil {
		unix.Close(fd2)
		t.Fatalf("Ftruncate child-fd-4: %v", err)
	}
	if _, err := unix.Pwrite(fd2, []byte(payload2), 0); err != nil {
		unix.Close(fd2)
		t.Fatalf("Pwrite child-fd-4: %v", err)
	}
	f2 := os.NewFile(uintptr(fd2), "extra-4")
	t.Cleanup(func() { f2.Close() })

	return f1, f2
}

func filteredEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "LISTEN_") ||
			strings.HasPrefix(kv, "NOTIFY_SOCKET=") ||
			strings.HasPrefix(kv, childScenarioEnv+"=") {
			continue
		}
		env = append(env, kv)
	}
	return env
}

func runSubprocessScenario(t *testing.T, scenario string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	f1, f2 := createParentExtraFiles(t, "child-fd-3-data", "child-fd-4-data")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestInheritedChildHelper$", "-test.v")
	cmd.ExtraFiles = []*os.File{f1, f2}
	cmd.Env = append(filteredEnv(), childScenarioEnv+"="+scenario)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	var waitDone bool
	t.Cleanup(func() {
		if cmd.Process != nil && !waitDone {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			waitDone = true
		}
	})

	if err := cmd.Start(); err != nil {
		t.Fatalf("cmd.Start (%s): %v", scenario, err)
	}

	err = cmd.Wait()
	waitDone = true
	if err != nil {
		t.Fatalf("child scenario %s failed: %v\nChild Output:\n%s", scenario, err, out.String())
	}
	if !strings.Contains(out.String(), "CHILD_SCENARIO_SUCCESS: "+scenario) {
		t.Fatalf("child scenario %s missing success marker; output:\n%s", scenario, out.String())
	}
}

// Subprocess test suite. Not run in parallel because of environment mutations.
func TestConsumeInheritedFDs_Subprocess(t *testing.T) {
	scenarios := []string{
		"absent",
		"count0",
		"validreverse",
		"missing_names",
		"count_name_mismatch",
		"duplicate_names",
		"invalid_name",
		"missing_fd",
		"wrong_pid",
		"bad_count",
		"negative_count",
		"above_limit_count",
		"partial_count_missing",
	}

	for _, sc := range scenarios {
		t.Run(sc, func(t *testing.T) {
			runSubprocessScenario(t, sc)
		})
	}
}

// Large count MaxInt with limit MaxInt must error without touching 3/4.
func TestConsumeInheritedFDs_LargeCountMaxIntControl(t *testing.T) {
	runSubprocessScenario(t, "large_count_maxint")
}

// TestInheritedChildHelper is the child subprocess entrypoint selected by childScenarioEnv.
func TestInheritedChildHelper(t *testing.T) {
	scenario := os.Getenv(childScenarioEnv)
	if scenario == "" {
		return
	}
	runChildScenario(t, scenario)
}

func mustSetenv(t *testing.T, key, value string) {
	t.Helper()
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("Setenv(%q, %q): %v", key, value, err)
	}
}

func mustUnsetenv(t *testing.T, key string) {
	t.Helper()
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("Unsetenv(%q): %v", key, err)
	}
}

func assertCommonChildPostconditions(t *testing.T, wantNotify string) {
	t.Helper()
	for _, v := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES", "LISTEN_PIDFDID"} {
		if val, ok := os.LookupEnv(v); ok {
			t.Fatalf("activation env %s still present: %q", v, val)
		}
	}
	if got := os.Getenv("NOTIFY_SOCKET"); got != wantNotify {
		t.Fatalf("NOTIFY_SOCKET = %q, want %q", got, wantNotify)
	}
}

func runChildScenario(t *testing.T, scenario string) {
	t.Helper()
	switch scenario {
	case "absent":
		mustUnsetenv(t, "LISTEN_PID")
		mustUnsetenv(t, "LISTEN_FDS")
		mustUnsetenv(t, "LISTEN_FDNAMES")
		mustUnsetenv(t, "LISTEN_PIDFDID")
		mustSetenv(t, "NOTIFY_SOCKET", "/run/systemd/notify")

		fds, err := ConsumeInheritedFDs(2)
		if err != nil {
			t.Fatalf("ConsumeInheritedFDs: %v", err)
		}
		if len(fds) != 0 {
			t.Fatalf("len(fds) = %d, want 0", len(fds))
		}
		if !isFDOpen(3) || !isFDOpen(4) {
			t.Fatal("fds 3 and 4 were closed on absent environment")
		}
		unix.Close(3)
		unix.Close(4)
		assertCommonChildPostconditions(t, "/run/systemd/notify")

	case "count0":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", "0")
		mustSetenv(t, "LISTEN_FDNAMES", "")
		mustSetenv(t, "NOTIFY_SOCKET", "/run/systemd/notify")

		fds, err := ConsumeInheritedFDs(2)
		if err != nil {
			t.Fatalf("ConsumeInheritedFDs: %v", err)
		}
		if len(fds) != 0 {
			t.Fatalf("len(fds) = %d, want 0", len(fds))
		}
		if !isFDOpen(3) || !isFDOpen(4) {
			t.Fatal("fds 3 and 4 were closed on count 0")
		}
		unix.Close(3)
		unix.Close(4)
		assertCommonChildPostconditions(t, "/run/systemd/notify")

	case "validreverse":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", "2")
		mustSetenv(t, "LISTEN_FDNAMES", "second:first")
		mustSetenv(t, "LISTEN_PIDFDID", "pidfd-sample")
		mustSetenv(t, "NOTIFY_SOCKET", "/run/systemd/notify")

		fds, err := ConsumeInheritedFDs(2)
		if err != nil {
			t.Fatalf("ConsumeInheritedFDs: %v", err)
		}
		if len(fds) != 2 {
			t.Fatalf("len(fds) = %d, want 2", len(fds))
		}
		// Assert return-by-name (not track-order)
		if fds["second"] != 3 {
			t.Fatalf("fds[second] = %d, want 3", fds["second"])
		}
		if fds["first"] != 4 {
			t.Fatalf("fds[first] = %d, want 4", fds["first"])
		}
		// Assert CLOEXEC
		if !isFDCloexec(3) {
			t.Fatal("fd 3 is not CLOEXEC")
		}
		if !isFDCloexec(4) {
			t.Fatal("fd 4 is not CLOEXEC")
		}
		// Assert bytes preserved
		b3, err := readFD(3, 32)
		if err != nil || string(b3) != "child-fd-3-data" {
			t.Fatalf("fd 3 read: %q, err = %v", string(b3), err)
		}
		b4, err := readFD(4, 32)
		if err != nil || string(b4) != "child-fd-4-data" {
			t.Fatalf("fd 4 read: %q, err = %v", string(b4), err)
		}
		// Caller closes returned FDs
		unix.Close(fds["second"])
		unix.Close(fds["first"])

		assertCommonChildPostconditions(t, "/run/systemd/notify")

		// Second consume empty
		fds2, err2 := ConsumeInheritedFDs(2)
		if err2 != nil || len(fds2) != 0 {
			t.Fatalf("second consume: fds=%v, err=%v", fds2, err2)
		}

	case "missing_names":
		// Missing names AFTER valid PID and count closes 3/4
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", "2")
		mustUnsetenv(t, "LISTEN_FDNAMES")

		fds, err := ConsumeInheritedFDs(2)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDClosed(3) {
			t.Fatal("fd 3 was not closed on missing names after valid PID/count")
		}
		if !isFDClosed(4) {
			t.Fatal("fd 4 was not closed on missing names after valid PID/count")
		}

	case "count_name_mismatch":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", "2")
		mustSetenv(t, "LISTEN_FDNAMES", "onlyone")

		fds, err := ConsumeInheritedFDs(2)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDClosed(3) || !isFDClosed(4) {
			t.Fatal("fds 3 and 4 were not closed on count/name mismatch")
		}

	case "duplicate_names":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", "2")
		mustSetenv(t, "LISTEN_FDNAMES", "dup:dup")

		fds, err := ConsumeInheritedFDs(2)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDClosed(3) || !isFDClosed(4) {
			t.Fatal("fds 3 and 4 were not closed on duplicate names")
		}

	case "invalid_name":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", "2")
		mustSetenv(t, "LISTEN_FDNAMES", "valid:invalid\nname")

		fds, err := ConsumeInheritedFDs(2)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDClosed(3) || !isFDClosed(4) {
			t.Fatal("fds 3 and 4 were not closed on invalid name")
		}

	case "missing_fd":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", "2")
		mustSetenv(t, "LISTEN_FDNAMES", "first:second")
		unix.Close(4) // Close FD 4 immediately before Consume

		fds, err := ConsumeInheritedFDs(2)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDClosed(3) {
			t.Fatal("remaining fd 3 was not closed when fd 4 was missing")
		}

	case "wrong_pid":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()+99999))
		mustSetenv(t, "LISTEN_FDS", "2")
		mustSetenv(t, "LISTEN_FDNAMES", "first:second")

		fds, err := ConsumeInheritedFDs(2)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDOpen(3) || !isFDOpen(4) {
			t.Fatal("fds 3 and 4 were closed on wrong PID error")
		}
		unix.Close(3)
		unix.Close(4)

	case "bad_count":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", "not-a-number")
		mustSetenv(t, "LISTEN_FDNAMES", "first:second")

		fds, err := ConsumeInheritedFDs(2)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDOpen(3) || !isFDOpen(4) {
			t.Fatal("fds 3 and 4 were closed on bad count error")
		}
		unix.Close(3)
		unix.Close(4)

	case "negative_count":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", "-2")
		mustSetenv(t, "LISTEN_FDNAMES", "first:second")

		fds, err := ConsumeInheritedFDs(2)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDOpen(3) || !isFDOpen(4) {
			t.Fatal("fds 3 and 4 were closed on negative count error")
		}
		unix.Close(3)
		unix.Close(4)

	case "above_limit_count":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", "3")
		mustSetenv(t, "LISTEN_FDNAMES", "first:second:third")

		fds, err := ConsumeInheritedFDs(2)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDOpen(3) || !isFDOpen(4) {
			t.Fatal("fds 3 and 4 were closed on above-limit count error")
		}
		unix.Close(3)
		unix.Close(4)

	case "partial_count_missing":
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustUnsetenv(t, "LISTEN_FDS")
		mustSetenv(t, "LISTEN_FDNAMES", "first:second")

		fds, err := ConsumeInheritedFDs(2)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDOpen(3) || !isFDOpen(4) {
			t.Fatal("fds 3 and 4 were closed on partial count-missing error")
		}
		unix.Close(3)
		unix.Close(4)

	case "large_count_maxint":
		// Control case for safe 3+count guard (not executed in parent suite).
		mustSetenv(t, "LISTEN_PID", strconv.Itoa(os.Getpid()))
		mustSetenv(t, "LISTEN_FDS", strconv.Itoa(math.MaxInt))
		mustSetenv(t, "LISTEN_FDNAMES", "first:second")

		fds, err := ConsumeInheritedFDs(math.MaxInt)
		if !errors.Is(err, ErrInvalidActivation) {
			t.Fatalf("err = %v, want ErrInvalidActivation", err)
		}
		if fds != nil {
			t.Fatalf("fds = %v, want nil map", fds)
		}
		if !isFDOpen(3) || !isFDOpen(4) {
			t.Fatal("fds 3 and 4 were touched on large count MaxInt error")
		}
		unix.Close(3)
		unix.Close(4)

	default:
		t.Fatalf("unknown child scenario: %q", scenario)
	}

	fmt.Printf("CHILD_SCENARIO_SUCCESS: %s\n", scenario)
}
