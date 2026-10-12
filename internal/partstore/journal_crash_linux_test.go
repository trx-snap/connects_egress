//go:build linux

package partstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"golang.org/x/sys/unix"
)

type crashChildEnv struct {
	Dir        string           `json:"dir"`
	Run        string           `json:"run"`
	Source     string           `json:"source"`
	Epoch      uint64           `json:"epoch"`
	Config     ipc.Config       `json:"config"`
	Policy     Policy           `json:"policy"`
	Identities []ipc.FDIdentity `json:"identities"`
	PendingID  uint64           `json:"pending_id"`
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type readyDetectingWriter struct {
	safeBuffer
	once    sync.Once
	readyCh chan struct{}
	lineBuf []byte
}

func newReadyDetectingWriter() *readyDetectingWriter {
	return &readyDetectingWriter{readyCh: make(chan struct{})}
}

func (w *readyDetectingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	w.lineBuf = append(w.lineBuf, p...)
	for {
		idx := bytes.IndexByte(w.lineBuf, '\n')
		if idx < 0 {
			break
		}
		line := strings.TrimSpace(string(w.lineBuf[:idx]))
		w.lineBuf = w.lineBuf[idx+1:]
		if line == "READY" {
			w.once.Do(func() { close(w.readyCh) })
		}
	}
	return n, err
}

func failChild(code int, format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(code)
}

func TestJournalCrashHelper(t *testing.T) {
	mode := os.Getenv("CONNECTS_JOURNAL_CRASH_MODE")
	if mode == "" {
		return
	}
	envJSON := os.Getenv("CONNECTS_JOURNAL_CRASH_ENV")
	if envJSON == "" {
		failChild(2, "missing crash env json")
	}
	var env crashChildEnv
	if err := json.Unmarshal([]byte(envJSON), &env); err != nil {
		failChild(3, "unmarshal crash env: %v", err)
	}

	fds := make([]int, env.Config.Tracks)
	for i := range fds {
		fds[i] = 3 + i
	}
	store, err := AdoptEpoch(env.Run, env.Source, env.Epoch, env.Config, fds, env.Identities)
	if err != nil {
		failChild(4, "child AdoptEpoch: %v", err)
	}
	defer store.Close()

	switch mode {
	case "A", "B":
		j, _, err := OpenJournal(env.Dir, store, env.Run, env.Source, env.Epoch, env.Config, env.Policy)
		if err != nil {
			failChild(5, "child OpenJournal: %v", err)
		}
		target := "compact_before_rename"
		if mode == "B" {
			target = "compact_renamed"
		}
		j.hook = func(pt string) error {
			if pt == target {
				fmt.Println("READY")
				_ = os.Stdout.Sync()
				time.Sleep(10 * time.Second)
			}
			return nil
		}
		if err := j.Compact(); err != nil {
			failChild(6, "child Compact: %v", err)
		}
		if err := j.Close(); err != nil {
			failChild(22, "child Close: %v", err)
		}
		return

	case "C":
		signal.Ignore(syscall.SIGXFSZ)
		var origRlim unix.Rlimit
		if err := unix.Getrlimit(unix.RLIMIT_FSIZE, &origRlim); err != nil {
			failChild(7, "child Getrlimit: %v", err)
		}

		j, r, err := OpenJournal(env.Dir, store, env.Run, env.Source, env.Epoch, env.Config, env.Policy)
		if err != nil {
			failChild(8, "child OpenJournal: %v", err)
		}
		snap, err := r.Snapshot()
		if err != nil || len(snap.Pending) != 1 || snap.Pending[0].ID != env.PendingID {
			failChild(9, "child pending grant mismatch: %v, %+v", err, snap)
		}
		res := snap.Pending[0]

		jPath := filepath.Join(env.Dir, journalFile)
		bytesBefore, err := os.ReadFile(jPath)
		if err != nil {
			failChild(10, "child ReadFile before: %v", err)
		}
		origOffset, origHigh := j.offset, j.high

		newRlim := origRlim
		newRlim.Cur = 64
		if err := unix.Setrlimit(unix.RLIMIT_FSIZE, &newRlim); err != nil {
			failChild(11, "child Setrlimit 64: %v", err)
		}

		compactErr := j.Compact()
		if compactErr == nil {
			failChild(12, "child Compact should fail with rlimit")
		}
		errStr := strings.ToLower(compactErr.Error())
		if !errors.Is(compactErr, unix.EFBIG) && !strings.Contains(errStr, "file too large") && !strings.Contains(errStr, "efbig") {
			failChild(13, "child Compact error missing EFBIG diagnostic: %v", compactErr)
		}

		bytesAfter, err := os.ReadFile(jPath)
		if err != nil {
			failChild(14, "child ReadFile after: %v", err)
		}
		if !bytes.Equal(bytesBefore, bytesAfter) {
			failChild(15, "child active journal bytes changed on pre-rename failure")
		}
		if j.poisoned {
			failChild(16, "child journal unexpectedly poisoned on pre-rename write error")
		}
		if j.offset != origOffset || j.high != origHigh {
			failChild(17, "child journal modified: offset %d vs %d, high %d vs %d", j.offset, origOffset, j.high, origHigh)
		}

		if err := unix.Setrlimit(unix.RLIMIT_FSIZE, &origRlim); err != nil {
			failChild(18, "child restore rlimit: %v", err)
		}

		if err := r.CancelAfterQuiescence(res); err != nil {
			failChild(19, "child CancelAfterQuiescence: %v", err)
		}
		if err := j.Close(); err != nil {
			failChild(20, "child Close: %v", err)
		}
		return

	default:
		failChild(21, "unknown crash test mode %q", mode)
	}
}

func runCrashParent(t *testing.T, mode string) {
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 4096, Policy{})
	if err := f.ring.Close(); err != nil {
		t.Fatalf("close initial ring: %v", err)
	}

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	j, r, err := CreateJournal(dir, f.store, f.run, f.source, f.epoch, f.cfg, Policy{})
	if err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	res, err := r.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve init: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close parent journal: %v", err)
	}

	ids, err := f.store.Identities()
	if err != nil {
		t.Fatalf("Identities: %v", err)
	}
	pol, err := Policy{}.effective()
	if err != nil {
		t.Fatalf("Policy effective: %v", err)
	}

	envJSON := mustMarshal(crashChildEnv{
		Dir: dir, Run: f.run, Source: f.source, Epoch: f.epoch,
		Config: f.cfg, Policy: pol, Identities: ids, PendingID: res.ID,
	})

	execPath, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}

	cmd := exec.Command(execPath, "-test.run=^TestJournalCrashHelper$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(),
		"CONNECTS_JOURNAL_CRASH_MODE="+mode,
		"CONNECTS_JOURNAL_CRASH_ENV="+string(envJSON),
	)

	transferDups, err := f.store.DupFDs()
	if err != nil {
		t.Fatalf("DupFDs: %v", err)
	}
	transferFiles := make([]*os.File, len(transferDups))
	for i, fd := range transferDups {
		transferFiles[i] = os.NewFile(uintptr(fd), fmt.Sprintf("transfer-fd-%d", i))
	}
	closeTransferFiles := func() {
		for _, tf := range transferFiles {
			if tf != nil {
				_ = tf.Close()
			}
		}
	}
	t.Cleanup(closeTransferFiles)

	cmd.ExtraFiles = transferFiles

	outWriter := newReadyDetectingWriter()
	errWriter := &safeBuffer{}
	cmd.Stdout, cmd.Stderr = outWriter, errWriter

	var parentRlimBefore, parentRlimAfter unix.Rlimit
	if mode == "C" {
		if err := unix.Getrlimit(unix.RLIMIT_FSIZE, &parentRlimBefore); err != nil {
			closeTransferFiles()
			t.Fatalf("parent Getrlimit: %v", err)
		}
	}

	if err := cmd.Start(); err != nil {
		closeTransferFiles()
		t.Fatalf("cmd.Start: %v", err)
	}
	closeTransferFiles()

	var waitErr error
	done := make(chan struct{})
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()

	t.Cleanup(func() {
		select {
		case <-done:
		default:
			if cmd.Process != nil {
				if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					t.Errorf("cleanup kill child %d: %v", cmd.Process.Pid, err)
				}
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Errorf("cleanup timed out waiting for child process reap")
			}
		}
	})

	switch mode {
	case "A", "B":
		select {
		case <-outWriter.readyCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for READY in mode %s\nstdout: %s\nstderr: %s", mode, outWriter.String(), errWriter.String())
		}
		if err := cmd.Process.Signal(unix.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Fatalf("signal SIGKILL child: %v", err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for child reap")
		}

		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) {
			t.Fatalf("expected ExitError from SIGKILL, got: %v", waitErr)
		}
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatalf("child was not killed with SIGKILL: status=%v signal=%v", status, status.Signal())
		}

		j2, r2, err := OpenJournal(dir, f.store, f.run, f.source, f.epoch, f.cfg, Policy{})
		if err != nil {
			t.Fatalf("reopen after kill: %v", err)
		}
		t.Cleanup(func() { _ = j2.Close() })

		snap2, err := r2.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot reopen: %v", err)
		}
		if snap2.RecordHigh != 1 || len(snap2.Pending) != 1 || snap2.Pending[0].ID != res.ID {
			t.Fatalf("recovered pending mismatch: want ID %d high 1, got pending %+v high %d", res.ID, snap2.Pending, snap2.RecordHigh)
		}

		if err := r2.CancelAfterQuiescence(res); err != nil {
			t.Fatalf("CancelAfterQuiescence: %v", err)
		}
		res2, err := r2.Reserve(ipc.KindInit, 0, 64)
		if err != nil {
			t.Fatalf("Reserve on active inode: %v", err)
		}
		if err := j2.Close(); err != nil {
			t.Fatalf("Close j2: %v", err)
		}

		j3, r3, err := OpenJournal(dir, f.store, f.run, f.source, f.epoch, f.cfg, Policy{})
		if err != nil {
			t.Fatalf("reopen after append: %v", err)
		}
		t.Cleanup(func() { _ = j3.Close() })

		snap3, err := r3.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot reopen3: %v", err)
		}
		if snap3.RecordHigh != 3 || len(snap3.Pending) != 1 || snap3.Pending[0].ID != res2.ID {
			t.Fatalf("active inode check failed: want high 3 ID %d, got high %d pending %+v", res2.ID, snap3.RecordHigh, snap3.Pending)
		}

	case "C":
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for child in mode C\nstdout: %s\nstderr: %s", outWriter.String(), errWriter.String())
		}

		if waitErr != nil {
			t.Fatalf("child mode C expected exit 0, got %v\nstdout: %s\nstderr: %s", waitErr, outWriter.String(), errWriter.String())
		}
		if err := unix.Getrlimit(unix.RLIMIT_FSIZE, &parentRlimAfter); err != nil {
			t.Fatalf("parent Getrlimit: %v", err)
		}
		if parentRlimBefore != parentRlimAfter {
			t.Fatalf("parent RLIMIT_FSIZE changed: before=%+v after=%+v", parentRlimBefore, parentRlimAfter)
		}

		j2, r2, err := OpenJournal(dir, f.store, f.run, f.source, f.epoch, f.cfg, Policy{})
		if err != nil {
			t.Fatalf("reopen mode C: %v", err)
		}
		t.Cleanup(func() { _ = j2.Close() })

		snap2, err := r2.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot mode C: %v", err)
		}
		if snap2.RecordHigh != 2 || len(snap2.Pending) != 0 {
			t.Fatalf("mode C reopen want high 2 pending 0, got high %d pending %+v", snap2.RecordHigh, snap2.Pending)
		}
	}
}

func TestJournalCrashLaneA(t *testing.T) { runCrashParent(t, "A") }
func TestJournalCrashLaneB(t *testing.T) { runCrashParent(t, "B") }
func TestJournalCrashLaneC(t *testing.T) { runCrashParent(t, "C") }
