//go:build linux

package supervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"github.com/connectsapp/connects-egress/internal/systemd"
	"github.com/connectsapp/connects-egress/internal/worker"
	"golang.org/x/sys/unix"
)

const crashChildEnv = "CONNECTS_EGRESS_SOURCE_CRASH_CHILD"

type pubTestReadyDoc struct {
	Ready      bool             `json:"ready"`
	PID        int              `json:"pid"`
	Epoch      uint64           `json:"epoch"`
	Identities []ipc.FDIdentity `json:"identities"`
	EntryHigh  uint64           `json:"entry_high"`
	RecordHigh uint64           `json:"record_high"`
	PendingID  uint64           `json:"pending_id"`
}

func TestPublicationCrashChildHelper(t *testing.T) {
	if os.Getenv(crashChildEnv) != "1" {
		t.Skip("skipping crash child helper when not in child environment")
	}
	runCrashChild()
}

func runCrashChild() {
	childCtx, childCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer childCancel()

	mode := os.Getenv("CRASH_MODE")
	dir := os.Getenv("CRASH_DIR")
	sockPath := os.Getenv("CRASH_SOCK")

	switch mode {
	case "create":
		runCrashChildCreate(childCtx, childCancel, dir, sockPath)
	case "replace":
		runCrashChildReplace(childCtx, childCancel, dir, sockPath)
	default:
		fmt.Fprintf(os.Stderr, "unknown crash mode: %q\n", mode)
		os.Exit(1)
	}
}

func runCrashChildCreate(childCtx context.Context, childCancel context.CancelFunc, dir, sockPath string) {
	notifySock := os.Getenv("CRASH_NOTIFY_SOCK")
	ownerUID, err := strconv.Atoi(os.Getenv("CRASH_OWNER_UID"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid CRASH_OWNER_UID: %v\n", err)
		os.Exit(1)
	}
	ownerPID, err := strconv.Atoi(os.Getenv("CRASH_OWNER_PID"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid CRASH_OWNER_PID: %v\n", err)
		os.Exit(1)
	}
	ownerIno, err := strconv.ParseUint(os.Getenv("CRASH_OWNER_INO"), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid CRASH_OWNER_INO: %v\n", err)
		os.Exit(1)
	}
	hookPoint := os.Getenv("CRASH_HOOK_POINT")

	n, err := systemd.NewNotifier(notifySock)
	if err != nil {
		fmt.Fprintf(os.Stderr, "systemd.NewNotifier: %v\n", err)
		os.Exit(1)
	}

	cfg := ipc.Config{
		Mode:     ipc.ModeSolo,
		Codec:    ipc.CodecH264,
		Tracks:   ipc.TracksSolo,
		Capacity: 4096,
	}
	pol := partstore.Policy{
		Window:     6 * time.Second,
		MaxSegment: 2 * time.Second,
	}
	owner := WorkerOwner{UID: ownerUID, PID: ownerPID, Ino: ownerIno}
	opts := SourceOptions{
		Directory: dir,
		Run:       "run-crash",
		Source:    "src-crash",
		Epoch:     1,
		Config:    cfg,
		Policy:    pol,
		Owner:     owner,
	}

	s, err := CreateSource(childCtx, opts, n)
	if err != nil {
		fmt.Fprintf(os.Stderr, "CreateSource: %v\n", err)
		os.Exit(1)
	}

	if hookPoint != "" {
		s.hook = func(pt string) {
			if pt == hookPoint {
				fmt.Fprintf(os.Stdout, "HOOK:%s\n", pt)
				_ = os.Stdout.Sync()
				<-childCtx.Done()
			}
		}
	}

	runCrashChildServe(childCtx, childCancel, s, sockPath)
}

func runCrashChildReplace(childCtx context.Context, childCancel context.CancelFunc, dir, sockPath string) {
	countStr := os.Getenv("CRASH_FD_COUNT")
	namesStr := os.Getenv("CRASH_FD_NAMES")

	os.Setenv("LISTEN_PID", strconv.Itoa(os.Getpid()))
	os.Setenv("LISTEN_FDS", countStr)
	os.Setenv("LISTEN_FDNAMES", namesStr)

	fds, err := systemd.ConsumeInheritedFDs(1024)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ConsumeInheritedFDs: %v\n", err)
		os.Exit(1)
	}

	s, err := OpenSource(childCtx, dir, fds)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenSource: %v\n", err)
		os.Exit(1)
	}

	runCrashChildServe(childCtx, childCancel, s, sockPath)
}

func runCrashChildServe(childCtx context.Context, childCancel context.CancelFunc, s *Source, sockPath string) {
	l, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: sockPath, Net: "unixpacket"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ListenUnix: %v\n", err)
		_ = s.Close()
		os.Exit(1)
	}

	snap, err := s.Snapshot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Snapshot: %v\n", err)
		_ = l.Close()
		_ = s.Close()
		os.Exit(1)
	}
	var pendingID uint64
	if len(snap.Pending) > 0 {
		pendingID = snap.Pending[0].ID
	}
	readyDoc := pubTestReadyDoc{
		Ready:      true,
		PID:        os.Getpid(),
		Epoch:      s.epoch,
		Identities: append([]ipc.FDIdentity(nil), s.ids...),
		EntryHigh:  snap.EntryHigh,
		RecordHigh: snap.RecordHigh,
		PendingID:  pendingID,
	}
	readyBytes, _ := json.Marshal(readyDoc)
	fmt.Fprintf(os.Stdout, "READY:%s\n", string(readyBytes))
	_ = os.Stdout.Sync()

	stdinDone := make(chan struct{})
	go func() {
		defer close(stdinDone)
		var buf [1]byte
		_, _ = os.Stdin.Read(buf[:])
	}()

	go func() {
		select {
		case <-stdinDone:
			childCancel()
		case <-childCtx.Done():
			_ = os.Stdin.Close()
		}
		_ = l.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := l.AcceptUnix()
		if err != nil {
			break
		}
		wg.Add(1)
		go func(c *net.UnixConn) {
			defer wg.Done()
			_ = s.ServeConn(childCtx, c)
		}(conn)
	}

	_ = l.Close()
	childCancel()
	_ = os.Stdin.Close()
	<-stdinDone
	wg.Wait()
	_ = s.Close()
	os.Exit(0)
}

type pubTestStdoutWriter struct {
	mu      sync.Mutex
	readyCh chan pubTestReadyDoc
	hookCh  chan string
	lineBuf []byte
	total   int
	max     int
}

func newPubTestStdoutWriter(maxBytes int) *pubTestStdoutWriter {
	return &pubTestStdoutWriter{
		readyCh: make(chan pubTestReadyDoc, 1),
		hookCh:  make(chan string, 1),
		max:     maxBytes,
	}
}

func (w *pubTestStdoutWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := len(p)
	if w.total >= w.max {
		return n, nil
	}
	if w.total+len(p) > w.max {
		p = p[:w.max-w.total]
	}
	w.total += len(p)

	w.lineBuf = append(w.lineBuf, p...)

	for {
		idx := bytes.IndexByte(w.lineBuf, '\n')
		if idx < 0 {
			break
		}
		line := string(w.lineBuf[:idx])
		w.lineBuf = w.lineBuf[idx+1:]

		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "READY:") {
			jsonStr := strings.TrimPrefix(line, "READY:")
			var doc pubTestReadyDoc
			if err := json.Unmarshal([]byte(jsonStr), &doc); err == nil {
				select {
				case w.readyCh <- doc:
				default:
				}
			}
		} else if strings.HasPrefix(line, "HOOK:") {
			point := strings.TrimPrefix(line, "HOOK:")
			select {
			case w.hookCh <- point:
			default:
			}
		}
	}
	return n, nil
}

type pubTestStderrWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	limit int
}

func newPubTestStderrWriter(limit int) *pubTestStderrWriter {
	return &pubTestStderrWriter{limit: limit}
}

func (w *pubTestStderrWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := len(p)
	if w.limit > 0 && w.buf.Len() >= w.limit {
		return n, nil
	}
	if w.limit > 0 && w.buf.Len()+len(p) > w.limit {
		p = p[:w.limit-w.buf.Len()]
	}
	w.buf.Write(p)
	return n, nil
}

func (w *pubTestStderrWriter) Snapshot() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

type pubTestOwnedChild struct {
	t         *testing.T
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	stdinOnce sync.Once
	stdinErr  error
	stdout    *pubTestStdoutWriter
	stderr    *pubTestStderrWriter
	done      chan struct{}
	waitErr   error
}

type pubTestChildOptions struct {
	Mode       string
	Dir        string
	SockPath   string
	NotifySock string
	Owner      WorkerOwner
	HookPoint  string
	ExtraFiles []*os.File
	ExtraNames []string
}

func startPubTestChild(t *testing.T, opts pubTestChildOptions) (*pubTestOwnedChild, error) {
	t.Helper()

	defer func() {
		for _, f := range opts.ExtraFiles {
			if f != nil {
				_ = f.Close()
			}
		}
	}()

	cmd := exec.Command(os.Args[0], "-test.run=^TestPublicationCrashChildHelper$")
	cmd.SysProcAttr = &unix.SysProcAttr{
		Pdeathsig: unix.SIGKILL,
	}

	env := append(os.Environ(),
		crashChildEnv+"=1",
		"CRASH_MODE="+opts.Mode,
		"CRASH_DIR="+opts.Dir,
		"CRASH_SOCK="+opts.SockPath,
	)
	if opts.NotifySock != "" {
		env = append(env, "CRASH_NOTIFY_SOCK="+opts.NotifySock)
	}
	if opts.Owner.UID != 0 || opts.Owner.PID != 0 {
		env = append(env,
			"CRASH_OWNER_UID="+strconv.Itoa(opts.Owner.UID),
			"CRASH_OWNER_PID="+strconv.Itoa(opts.Owner.PID),
			"CRASH_OWNER_INO="+strconv.FormatUint(opts.Owner.Ino, 10),
		)
	}
	if opts.HookPoint != "" {
		env = append(env, "CRASH_HOOK_POINT="+opts.HookPoint)
	}
	if len(opts.ExtraFiles) > 0 {
		cmd.ExtraFiles = opts.ExtraFiles
		env = append(env,
			"CRASH_FD_COUNT="+strconv.Itoa(len(opts.ExtraFiles)),
			"CRASH_FD_NAMES="+strings.Join(opts.ExtraNames, ":"),
		)
	}
	cmd.Env = env

	stdoutWriter := newPubTestStdoutWriter(64 * 1024)
	stderrWriter := newPubTestStderrWriter(64 * 1024)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("cmd.StdinPipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("cmd.Start: %w", err)
	}

	child := &pubTestOwnedChild{
		t:      t,
		cmd:    cmd,
		stdin:  stdinPipe,
		stdout: stdoutWriter,
		stderr: stderrWriter,
		done:   make(chan struct{}),
	}

	go func() {
		child.waitErr = child.cmd.Wait()
		close(child.done)
	}()

	t.Cleanup(func() {
		child.cleanup()
	})

	return child, nil
}

func (c *pubTestOwnedChild) PID() int {
	if c.cmd.Process != nil {
		return c.cmd.Process.Pid
	}
	return 0
}

func (c *pubTestOwnedChild) Stderr() string {
	if c.stderr != nil {
		return c.stderr.Snapshot()
	}
	return ""
}

func (c *pubTestOwnedChild) CloseStdin() error {
	c.stdinOnce.Do(func() {
		if c.stdin != nil {
			c.stdinErr = c.stdin.Close()
		}
	})
	return c.stdinErr
}

func (c *pubTestOwnedChild) Kill(sig syscall.Signal) error {
	if c.cmd.Process == nil {
		return errors.New("child process not started")
	}
	return c.cmd.Process.Signal(sig)
}

func (c *pubTestOwnedChild) Wait(timeout time.Duration) error {
	select {
	case <-c.done:
		return c.waitErr
	case <-time.After(timeout):
		return fmt.Errorf("child PID %d failed to exit within %v; stderr:\n%s", c.PID(), timeout, c.Stderr())
	}
}

func (c *pubTestOwnedChild) WaitReady(timeout time.Duration) (pubTestReadyDoc, error) {
	select {
	case doc := <-c.stdout.readyCh:
		return doc, nil
	case <-c.done:
		return pubTestReadyDoc{}, fmt.Errorf("child PID %d exited prematurely before READY: %v; stderr:\n%s", c.PID(), c.waitErr, c.Stderr())
	case <-time.After(timeout):
		return pubTestReadyDoc{}, fmt.Errorf("child PID %d READY timed out within %v; stderr:\n%s", c.PID(), timeout, c.Stderr())
	}
}

func (c *pubTestOwnedChild) WaitHook(timeout time.Duration) (string, error) {
	select {
	case pt := <-c.stdout.hookCh:
		return pt, nil
	case <-c.done:
		return "", fmt.Errorf("child PID %d exited prematurely before hook: %v; stderr:\n%s", c.PID(), c.waitErr, c.Stderr())
	case <-time.After(timeout):
		return "", fmt.Errorf("child PID %d hook timed out within %v; stderr:\n%s", c.PID(), timeout, c.Stderr())
	}
}

func (c *pubTestOwnedChild) cleanup() {
	select {
	case <-c.done:
		return
	default:
	}

	_ = c.CloseStdin()

	select {
	case <-c.done:
		return
	case <-time.After(3 * time.Second):
		if c.cmd.Process != nil {
			_ = c.cmd.Process.Signal(unix.SIGKILL)
		}
		select {
		case <-c.done:
			c.t.Errorf("cleanup: unexpected cleanup kill of child PID %d after 3s EOF timeout; stderr:\n%s", c.PID(), c.Stderr())
		case <-time.After(2 * time.Second):
			c.t.Errorf("cleanup: child PID %d reap timed out after SIGKILL; stderr:\n%s", c.PID(), c.Stderr())
		}
	}
}

func TestPublicationCrashDurablePoints(t *testing.T) {
	points := []struct {
		point string
		loss  string
	}{
		{"reserve_durable", "grant-loss"},
		{"commit_durable", "ack-loss"},
	}

	for _, pt := range points {
		t.Run(pt.point+"_"+pt.loss, func(t *testing.T) {
			dir := pubTestTempDir(t)
			owner := pubTestSelfOwner(t)
			mock := pubTestStartMockNotifier(t, ipc.TracksSolo)

			sockPath := filepath.Join(dir, "crash.sock")
			pubTestCheckSockaddrLen(t, sockPath)

			// 1. Start initial child with env-gated test helper and Pdeathsig
			child, err := startPubTestChild(t, pubTestChildOptions{
				Mode:       "create",
				Dir:        dir,
				SockPath:   sockPath,
				NotifySock: mock.Address(),
				Owner:      owner,
				HookPoint:  pt.point,
			})
			if err != nil {
				t.Fatalf("start initial child: %v", err)
			}

			// Wait for initial typed READY
			readyDoc, err := child.WaitReady(2 * time.Second)
			if err != nil {
				t.Fatalf("initial child READY: %v", err)
			}
			if !readyDoc.Ready {
				t.Fatalf("initial child not ready: %+v", readyDoc)
			}
			if readyDoc.PID != child.PID() {
				t.Fatalf("initial child PID = %d, want %d", readyDoc.PID, child.PID())
			}
			if readyDoc.Epoch != 1 {
				t.Fatalf("initial child Epoch = %d, want 1", readyDoc.Epoch)
			}
			if len(readyDoc.Identities) != ipc.TracksSolo {
				t.Fatalf("initial child identities count = %d, want %d", len(readyDoc.Identities), ipc.TracksSolo)
			}
			if readyDoc.EntryHigh != 0 || readyDoc.RecordHigh != 0 || readyDoc.PendingID != 0 {
				t.Fatalf("initial child watermarks not zero: %+v", readyDoc)
			}

			mock.WaitStored(2 * time.Second)

			// 2. Start Publisher in parent
			cfg := ipc.Config{
				Mode:     ipc.ModeSolo,
				Codec:    ipc.CodecH264,
				Tracks:   ipc.TracksSolo,
				Capacity: 4096,
			}
			pub, err := worker.NewPublisher(worker.PublisherOptions{
				Socket:        sockPath,
				Run:           "run-crash",
				Source:        "src-crash",
				SupervisorUID: os.Geteuid(),
				Config:        cfg,
			})
			if err != nil {
				t.Fatalf("NewPublisher: %v", err)
			}
			defer pub.Close()

			initData := bytes.Repeat([]byte{0xbb}, 16)
			initEntry := ipc.Entry{
				Kind:   ipc.KindInit,
				Track:  0,
				Codecs: "avc1.64001f",
			}

			type pubResult struct {
				entry ipc.Entry
				err   error
			}
			pubResCh := make(chan pubResult, 1)
			pubCtx, pubCancel := context.WithCancel(context.Background())
			defer pubCancel()

			go func() {
				e, err := pub.Publish(pubCtx, initEntry, initData)
				pubResCh <- pubResult{e, err}
			}()

			// Wait for child to hit hook
			hitPoint, err := child.WaitHook(2 * time.Second)
			if err != nil {
				t.Fatalf("child hook %q: %v", pt.point, err)
			}
			if hitPoint != pt.point {
				t.Fatalf("unexpected hook point: got %q, want %q", hitPoint, pt.point)
			}

			// Capture pre-fault socket stat
			var preStat unix.Stat_t
			if err := unix.Lstat(sockPath, &preStat); err != nil {
				t.Fatalf("lstat sock before kill: %v", err)
			}

			// 3. SIGKILL exact started child once and reap before replacement
			// Deliberate fault kill: terminating initial supervisor child at durable point
			if err := child.Kill(syscall.SIGKILL); err != nil {
				t.Fatalf("deliberate fault kill: %v", err)
			}
			waitErr := child.Wait(2 * time.Second)
			var exitErr *exec.ExitError
			if !errors.As(waitErr, &exitErr) {
				t.Fatalf("initial child wait error = %v, want exit error", waitErr)
			}
			ws, ok := exitErr.Sys().(syscall.WaitStatus)
			if !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
				t.Fatalf("initial child exit status = %v, want SIGKILL", exitErr)
			}

			// 4. Verify stale socket type/dev/ino matches preStat and remove
			var postStat unix.Stat_t
			if err := unix.Lstat(sockPath, &postStat); err != nil {
				t.Fatalf("lstat sock after reap: %v", err)
			}
			if postStat.Dev != preStat.Dev || postStat.Ino != preStat.Ino {
				t.Fatalf("socket inode changed while old child was alive: got %d/%d, want %d/%d",
					postStat.Dev, postStat.Ino, preStat.Dev, preStat.Ino)
			}
			if postStat.Mode&unix.S_IFMT != unix.S_IFSOCK || int(postStat.Uid) != os.Geteuid() {
				t.Fatalf("stale socket %q invalid mode/uid: mode=%#o, uid=%d", sockPath, postStat.Mode, postStat.Uid)
			}
			if err := os.Remove(sockPath); err != nil {
				t.Fatalf("remove stale socket: %v", err)
			}

			// 5. Prepare ExtraFiles from mock-held descriptors via DupStored
			storedFDs := mock.DupStored()
			names := mock.StoredNames()

			extraFiles := make([]*os.File, len(names))
			for i, name := range names {
				extraFiles[i] = os.NewFile(uintptr(storedFDs[name]), name)
			}

			// 6. Start replacement child
			child2, err := startPubTestChild(t, pubTestChildOptions{
				Mode:       "replace",
				Dir:        dir,
				SockPath:   sockPath,
				ExtraFiles: extraFiles,
				ExtraNames: names,
			})
			if err != nil {
				t.Fatalf("start replacement child: %v", err)
			}

			// 7. Replacement READY checks:
			// Grant-loss replacement READY must show pendingID1/high0; ACK-loss READY high1/pending0.
			ready2, err := child2.WaitReady(2 * time.Second)
			if err != nil {
				t.Fatalf("replacement child READY: %v", err)
			}
			if !ready2.Ready {
				t.Fatalf("replacement child not ready: %+v", ready2)
			}
			if ready2.PID != child2.PID() {
				t.Fatalf("replacement child PID = %d, want %d", ready2.PID, child2.PID())
			}
			if ready2.Epoch != 1 {
				t.Fatalf("replacement child Epoch = %d, want 1", ready2.Epoch)
			}
			if !reflect.DeepEqual(ready2.Identities, readyDoc.Identities) {
				t.Fatalf("replacement identities differ from initial:\ngot:  %+v\nwant: %+v", ready2.Identities, readyDoc.Identities)
			}

			if pt.point == "reserve_durable" {
				if ready2.PendingID != 1 || ready2.EntryHigh != 0 {
					t.Fatalf("grant-loss replacement READY: pendingID=%d (want 1), entryHigh=%d (want 0)", ready2.PendingID, ready2.EntryHigh)
				}
			} else if pt.point == "commit_durable" {
				if ready2.EntryHigh != 1 || ready2.PendingID != 0 {
					t.Fatalf("ack-loss replacement READY: entryHigh=%d (want 1), pendingID=%d (want 0)", ready2.EntryHigh, ready2.PendingID)
				}
			}

			// 8. Same Publisher reconnects; assert ACK seq1, same epoch/identities
			select {
			case res := <-pubResCh:
				if res.err != nil {
					t.Fatalf("Publish after reconnect failed: %v", res.err)
				}
				if res.entry.Seq != 1 {
					t.Fatalf("Publish returned seq = %d, want 1", res.entry.Seq)
				}
				if res.entry.Epoch != 1 {
					t.Fatalf("Publish returned epoch = %d, want 1", res.entry.Epoch)
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("Publisher failed to complete Publish after reconnect within 3s")
			}

			// 9. Cleanly close replacement via stdin EOF; replacement must exit 0
			if err := child2.CloseStdin(); err != nil {
				t.Fatalf("child2.CloseStdin: %v", err)
			}
			if err := child2.Wait(3 * time.Second); err != nil {
				t.Fatalf("replacement child exit failed: %v; stderr:\n%s", err, child2.Stderr())
			}

			// 10. Reopen in parent to verify durable init bytes and final high waters
			heldFDs := mock.DupStored()
			sParent, err := OpenSource(context.Background(), dir, heldFDs)
			if err != nil {
				t.Fatalf("OpenSource in parent: %v", err)
			}
			defer sParent.Close()

			gotInit, err := sParent.ReadInit(0)
			if err != nil {
				t.Fatalf("sParent.ReadInit: %v", err)
			}
			if !bytes.Equal(gotInit, initData) {
				t.Fatalf("ReadInit mismatch: got %x, want %x", gotInit, initData)
			}

			snapFinal, err := sParent.Snapshot()
			if err != nil {
				t.Fatalf("sParent.Snapshot: %v", err)
			}
			if snapFinal.EntryHigh != 1 || snapFinal.RecordHigh != 2 || len(snapFinal.Pending) != 0 {
				t.Fatalf("final snapshot mismatch: EntryHigh=%d (want 1), RecordHigh=%d (want 2), Pending=%d (want 0)",
					snapFinal.EntryHigh, snapFinal.RecordHigh, len(snapFinal.Pending))
			}
		})
	}
}
