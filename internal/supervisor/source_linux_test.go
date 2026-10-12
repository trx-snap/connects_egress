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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"golang.org/x/sys/unix"
)

func auditSocketpair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("auditSocketpair: %v", err)
	}
	f0 := os.NewFile(uintptr(fds[0]), "sock0")
	f1 := os.NewFile(uintptr(fds[1]), "sock1")
	c0, err0 := net.FileConn(f0)
	f0.Close()
	if err0 != nil {
		f1.Close()
		t.Fatalf("FileConn 0: %v", err0)
	}
	c1, err1 := net.FileConn(f1)
	f1.Close()
	if err1 != nil {
		c0.Close()
		t.Fatalf("FileConn 1: %v", err1)
	}
	uc0, ok0 := c0.(*net.UnixConn)
	uc1, ok1 := c1.(*net.UnixConn)
	if !ok0 || !ok1 {
		c0.Close()
		c1.Close()
		t.Fatalf("FileConn returned %T and %T, want *net.UnixConn", c0, c1)
	}
	return uc0, uc1
}

func TestAuditOwnerShape(t *testing.T) {
	boot, err := currentBootID()
	if err != nil {
		t.Fatalf("currentBootID: %v", err)
	}
	cfg := ipc.Config{
		Mode:     ipc.ModeSolo,
		Codec:    ipc.CodecH264,
		Tracks:   4,
		Capacity: 1024 * 1024,
	}
	pol := policyState{
		WindowNS:     6_000_000_000,
		MaxSegmentNS: 2_000_000_000,
	}
	wrk := workerState{
		UID: 0,
		PID: os.Getpid(),
		Ino: 9999,
	}

	store, err := partstore.AllocateEpoch("run-audit", "src-audit", 1, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}
	defer store.Close()

	ids, err := store.Identities()
	if err != nil {
		t.Fatalf("Identities: %v", err)
	}

	base := ownerState{
		Format:    ownerFormat,
		WireMajor: ipc.PublicationVersion,
		BootID:    boot,
		Run:       "run-audit",
		Source:    "src-audit",
		Epoch:     1,
		Config:    cfg,
		Policy:    pol,
		Worker:    wrk,
		Tracks:    ids,
	}

	baseDir := t.TempDir()
	if err := os.Chmod(baseDir, 0700); err != nil {
		t.Fatalf("chmod baseDir: %v", err)
	}
	if fi, err := os.Stat(baseDir); err != nil || fi.Mode().Perm() != 0700 {
		t.Fatalf("baseDir mode = %v, want 0700: %v", fi.Mode().Perm(), err)
	}

	testCandidate := func(name string, raw []byte, wantErr error) {
		candDir := filepath.Join(baseDir, name)
		if err := os.Mkdir(candDir, 0700); err != nil {
			t.Fatalf("%s: mkdir: %v", name, err)
		}
		if fi, err := os.Stat(candDir); err != nil || fi.Mode().Perm() != 0700 {
			t.Fatalf("%s mode = %v, want 0700: %v", candDir, fi.Mode().Perm(), err)
		}
		ownerPath := filepath.Join(candDir, ownerFile)
		if err := os.WriteFile(ownerPath, raw, 0600); err != nil {
			t.Fatalf("%s: writefile: %v", name, err)
		}
		dirfd, err := openPrivateDir(candDir)
		if err != nil {
			t.Fatalf("%s: openPrivateDir: %v", name, err)
		}
		defer unix.Close(dirfd)

		got, err := readOwnerState(dirfd)
		if wantErr == nil {
			if err != nil {
				t.Fatalf("%s (positive control): expected success, got: %v", name, err)
			}
			if got.Epoch != base.Epoch || got.Worker.UID != 0 {
				t.Fatalf("%s (positive control): unexpected state: %+v", name, got)
			}
		} else {
			if err == nil {
				t.Errorf("%s: defect control failed: expected error %v, got nil", name, wantErr)
			} else if !errors.Is(err, wantErr) {
				t.Errorf("%s: expected error %v, got: %v", name, wantErr, err)
			}
		}
	}

	baseJSON, err := json.Marshal(base)
	if err != nil {
		t.Fatalf("marshal base: %v", err)
	}

	// Baseline positive control
	testCandidate("baseline", baseJSON, nil)

	// Helper to clone root map
	cloneRoot := func() map[string]json.RawMessage {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(baseJSON, &m); err != nil {
			t.Fatalf("unmarshal baseJSON: %v", err)
		}
		return m
	}

	// 1. Worker group
	{
		testCandidate("worker-positive-control", baseJSON, nil)

		// delete worker.uid
		{
			root := cloneRoot()
			var wMap map[string]json.RawMessage
			if err := json.Unmarshal(root["worker"], &wMap); err != nil {
				t.Fatalf("unmarshal worker: %v", err)
			}
			delete(wMap, "uid")
			wRaw, _ := json.Marshal(wMap)
			root["worker"] = wRaw
			raw, _ := json.Marshal(root)
			testCandidate("worker-delete-uid", raw, ErrLostEpoch)
		}

		// null worker.uid
		{
			root := cloneRoot()
			var wMap map[string]json.RawMessage
			if err := json.Unmarshal(root["worker"], &wMap); err != nil {
				t.Fatalf("unmarshal worker: %v", err)
			}
			wMap["uid"] = json.RawMessage("null")
			wRaw, _ := json.Marshal(wMap)
			root["worker"] = wRaw
			raw, _ := json.Marshal(root)
			testCandidate("worker-null-uid", raw, ErrLostEpoch)
		}

		// replace with UID (uppercase)
		{
			root := cloneRoot()
			var wMap map[string]json.RawMessage
			if err := json.Unmarshal(root["worker"], &wMap); err != nil {
				t.Fatalf("unmarshal worker: %v", err)
			}
			delete(wMap, "uid")
			wMap["UID"] = json.RawMessage("0")
			wRaw, _ := json.Marshal(wMap)
			root["worker"] = wRaw
			raw, _ := json.Marshal(root)
			testCandidate("worker-uppercase-UID", raw, ErrLostEpoch)
		}

		// replace with Uid (mixed case)
		{
			root := cloneRoot()
			var wMap map[string]json.RawMessage
			if err := json.Unmarshal(root["worker"], &wMap); err != nil {
				t.Fatalf("unmarshal worker: %v", err)
			}
			delete(wMap, "uid")
			wMap["Uid"] = json.RawMessage("0")
			wRaw, _ := json.Marshal(wMap)
			root["worker"] = wRaw
			raw, _ := json.Marshal(root)
			testCandidate("worker-mixedcase-Uid", raw, ErrLostEpoch)
		}
	}

	// 2. Config group
	{
		testCandidate("config-positive-control", baseJSON, nil)

		cfgKeys := []string{"mode", "codec", "tracks", "capacity"}
		for _, key := range cfgKeys {
			// missing key
			{
				root := cloneRoot()
				var cMap map[string]json.RawMessage
				if err := json.Unmarshal(root["config"], &cMap); err != nil {
					t.Fatalf("unmarshal config: %v", err)
				}
				delete(cMap, key)
				cRaw, _ := json.Marshal(cMap)
				root["config"] = cRaw
				raw, _ := json.Marshal(root)
				testCandidate("config-missing-"+key, raw, ErrLostEpoch)
			}
			// null key
			{
				root := cloneRoot()
				var cMap map[string]json.RawMessage
				if err := json.Unmarshal(root["config"], &cMap); err != nil {
					t.Fatalf("unmarshal config: %v", err)
				}
				cMap[key] = json.RawMessage("null")
				cRaw, _ := json.Marshal(cMap)
				root["config"] = cRaw
				raw, _ := json.Marshal(root)
				testCandidate("config-null-"+key, raw, ErrLostEpoch)
			}
			// wrong case
			{
				root := cloneRoot()
				var cMap map[string]json.RawMessage
				if err := json.Unmarshal(root["config"], &cMap); err != nil {
					t.Fatalf("unmarshal config: %v", err)
				}
				val := cMap[key]
				delete(cMap, key)
				wrongKey := string(bytes.ToUpper([]byte{key[0]})) + key[1:]
				cMap[wrongKey] = val
				cRaw, _ := json.Marshal(cMap)
				root["config"] = cRaw
				raw, _ := json.Marshal(root)
				testCandidate("config-wrongcase-"+key, raw, ErrLostEpoch)
			}
		}
	}

	// 3. Policy group
	{
		testCandidate("policy-positive-control", baseJSON, nil)

		polKeys := []string{"window_ns", "max_segment_ns"}
		for _, key := range polKeys {
			// missing key
			{
				root := cloneRoot()
				var pMap map[string]json.RawMessage
				if err := json.Unmarshal(root["policy"], &pMap); err != nil {
					t.Fatalf("unmarshal policy: %v", err)
				}
				delete(pMap, key)
				pRaw, _ := json.Marshal(pMap)
				root["policy"] = pRaw
				raw, _ := json.Marshal(root)
				testCandidate("policy-missing-"+key, raw, ErrLostEpoch)
			}
			// null key
			{
				root := cloneRoot()
				var pMap map[string]json.RawMessage
				if err := json.Unmarshal(root["policy"], &pMap); err != nil {
					t.Fatalf("unmarshal policy: %v", err)
				}
				pMap[key] = json.RawMessage("null")
				pRaw, _ := json.Marshal(pMap)
				root["policy"] = pRaw
				raw, _ := json.Marshal(root)
				testCandidate("policy-null-"+key, raw, ErrLostEpoch)
			}
			// wrong case
			{
				root := cloneRoot()
				var pMap map[string]json.RawMessage
				if err := json.Unmarshal(root["policy"], &pMap); err != nil {
					t.Fatalf("unmarshal policy: %v", err)
				}
				val := pMap[key]
				delete(pMap, key)
				wrongKey := string(bytes.ToUpper([]byte{key[0]})) + key[1:]
				pMap[wrongKey] = val
				pRaw, _ := json.Marshal(pMap)
				root["policy"] = pRaw
				raw, _ := json.Marshal(root)
				testCandidate("policy-wrongcase-"+key, raw, ErrLostEpoch)
			}
		}
	}

	// 4. Descriptor / Tracks group
	{
		testCandidate("tracks-positive-control", baseJSON, nil)

		trkKeys := []string{"track", "name", "dev", "ino", "size"}
		for _, key := range trkKeys {
			// missing key in tracks[0]
			{
				root := cloneRoot()
				var trkList []map[string]json.RawMessage
				if err := json.Unmarshal(root["tracks"], &trkList); err != nil {
					t.Fatalf("unmarshal tracks: %v", err)
				}
				delete(trkList[0], key)
				tRaw, _ := json.Marshal(trkList)
				root["tracks"] = tRaw
				raw, _ := json.Marshal(root)
				testCandidate("tracks-missing-"+key, raw, ErrLostEpoch)
			}
			// null key in tracks[0]
			{
				root := cloneRoot()
				var trkList []map[string]json.RawMessage
				if err := json.Unmarshal(root["tracks"], &trkList); err != nil {
					t.Fatalf("unmarshal tracks: %v", err)
				}
				trkList[0][key] = json.RawMessage("null")
				tRaw, _ := json.Marshal(trkList)
				root["tracks"] = tRaw
				raw, _ := json.Marshal(root)
				testCandidate("tracks-null-"+key, raw, ErrLostEpoch)
			}
			// wrong case in tracks[0]
			{
				root := cloneRoot()
				var trkList []map[string]json.RawMessage
				if err := json.Unmarshal(root["tracks"], &trkList); err != nil {
					t.Fatalf("unmarshal tracks: %v", err)
				}
				val := trkList[0][key]
				delete(trkList[0], key)
				wrongKey := string(bytes.ToUpper([]byte{key[0]})) + key[1:]
				trkList[0][wrongKey] = val
				tRaw, _ := json.Marshal(trkList)
				root["tracks"] = tRaw
				raw, _ := json.Marshal(root)
				testCandidate("tracks-wrongcase-"+key, raw, ErrLostEpoch)
			}
		}
	}
}

func TestAuditAdmissionNoPreemption(t *testing.T) {
	oldSrv, oldCli := auditSocketpair(t)
	newSrv, newCli := auditSocketpair(t)

	// Immediate defer cleanup of all sockets BEFORE any assertion
	defer oldCli.Close()
	defer oldSrv.Close()
	defer newCli.Close()
	defer newSrv.Close()

	oldDone := make(chan struct{})
	var oldDoneOnce sync.Once
	releaseOldDone := func() {
		oldDoneOnce.Do(func() {
			close(oldDone)
		})
	}
	defer releaseOldDone()

	oldSess := &session{conn: oldSrv, done: oldDone}
	s := &Source{active: oldSess}
	newSess := &session{conn: newSrv, done: make(chan struct{})}

	type admitRes struct {
		err error
	}
	resCh := make(chan admitRes, 1)
	admitJoined := make(chan struct{})
	defer func() {
		releaseOldDone()
		select {
		case <-admitJoined:
		case <-time.After(2 * time.Second):
		}
	}()

	go func() {
		resCh <- admitRes{err: s.admit(newSess)}
		close(admitJoined)
	}()

	var res admitRes
	var timedOut bool
	select {
	case res = <-resCh:
	case <-time.After(250 * time.Millisecond):
		timedOut = true
		// Safely release current blocking behavior before joining
		releaseOldDone()
		select {
		case res = <-resCh:
		case <-time.After(2 * time.Second):
			t.Errorf("UNIT: admit(new) goroutine failed to join after releasing old.done")
			return
		}
	}

	releaseOldDone()

	// Assertions AFTER release and join:
	if timedOut {
		t.Errorf("UNIT: defect control failed: admit(new) blocked past 250ms waiting on old.done (preempted old session)")
	} else if res.err == nil {
		t.Errorf("UNIT: defect control failed: admit(new) returned nil, expected error when old session is active")
	}

	s.mu.Lock()
	curActive := s.active
	s.mu.Unlock()
	if curActive != oldSess {
		t.Errorf("UNIT: defect control failed: s.active replaced before validation: got %v, want old session %v", curActive, oldSess)
	}

	// Check old peer remains usable via one bounded byte roundtrip
	_ = oldCli.SetDeadline(time.Now().Add(500 * time.Millisecond))
	_ = oldSrv.SetDeadline(time.Now().Add(500 * time.Millisecond))

	payload := []byte{0x42}
	var roundtripErr error
	if _, err := oldCli.Write(payload); err != nil {
		roundtripErr = fmt.Errorf("write oldCli: %w", err)
	} else {
		buf := make([]byte, 1)
		if _, err := io.ReadFull(oldSrv, buf); err != nil {
			roundtripErr = fmt.Errorf("read oldSrv: %w", err)
		} else if buf[0] != 0x42 {
			roundtripErr = fmt.Errorf("corrupt byte: got %x, want 42", buf[0])
		}
	}
	if roundtripErr != nil {
		t.Errorf("UNIT: defect control failed: old session connection not usable for byte roundtrip: %v", roundtripErr)
	}
}

func TestAuditSourceConcurrentCloseJoins(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0700 {
		t.Fatalf("dir mode = %v, want 0700: %v", fi.Mode().Perm(), err)
	}

	cfg := ipc.Config{
		Mode:     ipc.ModeSolo,
		Codec:    ipc.CodecH264,
		Tracks:   4,
		Capacity: 1024 * 1024,
	}
	pol := partstore.Policy{
		Window:     6 * time.Second,
		MaxSegment: 2 * time.Second,
	}

	store, err := partstore.AllocateEpoch("run-close", "src-close", 1, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}
	defer store.Close()

	ids, err := store.Identities()
	if err != nil {
		t.Fatalf("Identities: %v", err)
	}

	j, r, err := partstore.CreateJournal(dir, store, "run-close", "src-close", 1, cfg, pol)
	if err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	defer j.Close()

	owner := WorkerOwner{UID: os.Getuid(), PID: os.Getpid(), Ino: 12345}
	s := newSource(dir, "run-close", "src-close", 1, cfg, pol, ids, owner, store, j, r)
	if err := s.load(); err != nil {
		t.Fatalf("s.load: %v", err)
	}
	defer func() { _ = s.Close() }()

	srvConn, cliConn := auditSocketpair(t)
	defer cliConn.Close()
	defer srvConn.Close()

	barrier := make(chan struct{})
	var barrierOnce sync.Once
	releaseBarrier := func() {
		barrierOnce.Do(func() {
			close(barrier)
		})
	}
	defer releaseBarrier()

	sess := &session{conn: srvConn, done: barrier}
	s.active = sess

	type closeResult struct {
		err error
	}
	close1Ch := make(chan closeResult, 1)
	close1Joined := make(chan struct{})
	defer func() {
		releaseBarrier()
		select {
		case <-close1Joined:
		case <-time.After(2 * time.Second):
		}
	}()

	go func() {
		close1Ch <- closeResult{err: s.Close()}
		close(close1Joined)
	}()

	// Observe s.closed under mu
	observedClosed := false
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		c := s.closed
		s.mu.Unlock()
		if c {
			observedClosed = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	close2Ch := make(chan closeResult, 1)
	close2Joined := make(chan struct{})
	close2Started := false

	defer func() {
		if close2Started {
			releaseBarrier()
			select {
			case <-close2Joined:
			case <-time.After(2 * time.Second):
			}
		}
	}()

	var secondReturnedEarly bool
	var earlyErr2 error

	if !observedClosed {
		t.Errorf("UNIT: first Close did not set s.closed within deadline")
	} else {
		close2Started = true
		go func() {
			close2Ch <- closeResult{err: s.Close()}
			close(close2Joined)
		}()

		// Second Close must NOT return while barrier open: bounded 100-250ms negative observation
		select {
		case res2 := <-close2Ch:
			secondReturnedEarly = true
			earlyErr2 = res2.err
		case <-time.After(150 * time.Millisecond):
			// Desired behavior: second Close is still waiting
		}
	}

	// Release barrier
	releaseBarrier()

	// Join both goroutines (bounded <= 2s)
	var res1 closeResult
	select {
	case res1 = <-close1Ch:
	case <-time.After(2 * time.Second):
		t.Errorf("UNIT: first Close failed to join within 2s")
		return
	}

	var res2 closeResult
	if close2Started {
		if secondReturnedEarly {
			res2 = closeResult{err: earlyErr2}
		} else {
			select {
			case res2 = <-close2Ch:
			case <-time.After(2 * time.Second):
				t.Errorf("UNIT: second Close failed to join within 2s")
				return
			}
		}
	}

	if !observedClosed {
		return
	}

	// Assertions AFTER join:
	if secondReturnedEarly {
		t.Errorf("UNIT: defect control failed: second Close returned early while first Close held barrier open")
	}
	if res1.err != nil {
		t.Errorf("close1 returned unexpected error: %v", res1.err)
	}
	if res2.err != nil {
		t.Errorf("close2 returned unexpected error: %v", res2.err)
	}

	// Assert ring and store are closed
	if _, err := store.ReadSpan(0, 0, 1); !errors.Is(err, partstore.ErrClosed) {
		t.Errorf("expected store to be closed, got: %v", err)
	}
	if _, err := r.Snapshot(); !errors.Is(err, partstore.ErrClosed) {
		t.Errorf("expected ring to be closed, got: %v", err)
	}
}

func TestAuditOpenSourceConsumesAllFDs(t *testing.T) {
	cfg := ipc.Config{
		Mode:     ipc.ModeSolo,
		Codec:    ipc.CodecH264,
		Tracks:   4,
		Capacity: 1024 * 1024,
	}
	pol := partstore.Policy{
		Window:     6 * time.Second,
		MaxSegment: 2 * time.Second,
	}

	isFDOpenAndMatches := func(fd int, dev, ino uint64) bool {
		if fd < 0 {
			return false
		}
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
			return false
		}
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			return false
		}
		return uint64(st.Dev) == dev && st.Ino == ino
	}

	// 1. Valid untouched control first
	{
		dir1 := t.TempDir()
		if err := os.Chmod(dir1, 0700); err != nil {
			t.Fatalf("chmod dir1: %v", err)
		}
		if fi, err := os.Stat(dir1); err != nil || fi.Mode().Perm() != 0700 {
			t.Fatalf("dir1 mode = %v, want 0700: %v", fi.Mode().Perm(), err)
		}

		store1, err := partstore.AllocateEpoch("run-valid", "src-valid", 1, cfg)
		if err != nil {
			t.Fatalf("AllocateEpoch: %v", err)
		}
		defer store1.Close()

		ids1, err := store1.Identities()
		if err != nil {
			t.Fatalf("Identities: %v", err)
		}
		j1, _, err := partstore.CreateJournal(dir1, store1, "run-valid", "src-valid", 1, cfg, pol)
		if err != nil {
			t.Fatalf("CreateJournal: %v", err)
		}
		if err := j1.Close(); err != nil {
			t.Fatalf("j1.Close: %v", err)
		}

		boot, err := currentBootID()
		if err != nil {
			t.Fatalf("currentBootID: %v", err)
		}
		ownerState1 := ownerState{
			Format:    ownerFormat,
			WireMajor: ipc.PublicationVersion,
			BootID:    boot,
			Run:       "run-valid",
			Source:    "src-valid",
			Epoch:     1,
			Config:    cfg,
			Policy:    policyState{WindowNS: int64(pol.Window), MaxSegmentNS: int64(pol.MaxSegment)},
			Worker:    workerState{UID: os.Getuid(), PID: os.Getpid(), Ino: 12345},
			Tracks:    ids1,
		}
		dirfd1, err := openPrivateDir(dir1)
		if err != nil {
			t.Fatalf("openPrivateDir: %v", err)
		}
		if err := writeOwnerState(dirfd1, ownerState1); err != nil {
			closeFD(dirfd1)
			t.Fatalf("writeOwnerState: %v", err)
		}
		closeFD(dirfd1)

		dups1, err := store1.DupFDs()
		if err != nil {
			t.Fatalf("DupFDs: %v", err)
		}
		fdsMap1 := make(map[string]int, len(ids1))
		for i, id := range ids1 {
			fdsMap1[id.Name] = dups1[i]
		}

		s1, err := OpenSource(context.Background(), dir1, fdsMap1)
		if err != nil {
			t.Fatalf("positive control: OpenSource failed: %v", err)
		}
		if err := s1.Close(); err != nil {
			t.Fatalf("positive control: s1.Close: %v", err)
		}
	}

	// 2. Defect fixture: cloned owner identities names [A, A, C, D], supplied map retains A, B, C, D
	{
		dir2 := t.TempDir()
		if err := os.Chmod(dir2, 0700); err != nil {
			t.Fatalf("chmod dir2: %v", err)
		}
		if fi, err := os.Stat(dir2); err != nil || fi.Mode().Perm() != 0700 {
			t.Fatalf("dir2 mode = %v, want 0700: %v", fi.Mode().Perm(), err)
		}

		store2, err := partstore.AllocateEpoch("run-leak", "src-leak", 1, cfg)
		if err != nil {
			t.Fatalf("AllocateEpoch: %v", err)
		}
		defer store2.Close()

		ids2, err := store2.Identities()
		if err != nil {
			t.Fatalf("Identities: %v", err)
		}

		j2, _, err := partstore.CreateJournal(dir2, store2, "run-leak", "src-leak", 1, cfg, pol)
		if err != nil {
			t.Fatalf("CreateJournal: %v", err)
		}
		if err := j2.Close(); err != nil {
			t.Fatalf("j2.Close: %v", err)
		}

		// Cloned owner identities names [A, A, C, D]
		clonedTracks := append([]ipc.FDIdentity(nil), ids2...)
		clonedTracks[1].Name = clonedTracks[0].Name

		boot, err := currentBootID()
		if err != nil {
			t.Fatalf("currentBootID: %v", err)
		}
		ownerState2 := ownerState{
			Format:    ownerFormat,
			WireMajor: ipc.PublicationVersion,
			BootID:    boot,
			Run:       "run-leak",
			Source:    "src-leak",
			Epoch:     1,
			Config:    cfg,
			Policy:    policyState{WindowNS: int64(pol.Window), MaxSegmentNS: int64(pol.MaxSegment)},
			Worker:    workerState{UID: os.Getuid(), PID: os.Getpid(), Ino: 12345},
			Tracks:    clonedTracks,
		}
		dirfd2, err := openPrivateDir(dir2)
		if err != nil {
			t.Fatalf("openPrivateDir: %v", err)
		}
		if err := writeOwnerState(dirfd2, ownerState2); err != nil {
			closeFD(dirfd2)
			t.Fatalf("writeOwnerState: %v", err)
		}
		closeFD(dirfd2)

		dups2, err := store2.DupFDs()
		if err != nil {
			t.Fatalf("DupFDs: %v", err)
		}

		type trackedFD struct {
			fd   int
			name string
			dev  uint64
			ino  uint64
		}
		tracked := make([]trackedFD, len(dups2))
		for i, fd := range dups2 {
			var st unix.Stat_t
			if err := unix.Fstat(fd, &st); err != nil {
				t.Fatalf("fstat dup %d: %v", i, err)
			}
			tracked[i] = trackedFD{
				fd:   fd,
				name: ids2[i].Name,
				dev:  uint64(st.Dev),
				ino:  st.Ino,
			}
		}

		// Clean only still-live leaked handles matching original dev/ino; never blindly double-close
		defer func() {
			for _, tfd := range tracked {
				if isFDOpenAndMatches(tfd.fd, tfd.dev, tfd.ino) {
					_ = unix.Close(tfd.fd)
				}
			}
		}()

		// Supplied map retains four distinct actual names A, B, C, D
		fdsMap2 := make(map[string]int, len(ids2))
		for i, id := range ids2 {
			fdsMap2[id.Name] = dups2[i]
		}

		s2, err2 := OpenSource(context.Background(), dir2, fdsMap2)
		if s2 != nil {
			defer s2.Close()
		}

		// Assertions AFTER OpenSource finishes
		if err2 == nil {
			t.Errorf("UNIT: defect control failed: OpenSource succeeded, expected ErrLostEpoch")
		} else if !errors.Is(err2, ErrLostEpoch) {
			t.Errorf("UNIT: expected ErrLostEpoch, got: %v", err2)
		}

		// Expect EVERY offered FD closed, including omitted B
		for i, tfd := range tracked {
			if isFDOpenAndMatches(tfd.fd, tfd.dev, tfd.ino) {
				t.Errorf("UNIT: defect control failed: offered descriptor %d (name %q, fd %d, dev %d, ino %d) was LEAKED (remains open after OpenSource)",
					i, tfd.name, tfd.fd, tfd.dev, tfd.ino)
			}
		}
	}
}
