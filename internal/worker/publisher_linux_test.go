//go:build linux

package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
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

func TestAuditDescriptorAdoption(t *testing.T) {
	runTest := func(tcName string, setupWelcome func(hello ipc.Hello) (ipc.PublicationWelcome, []int, error), expectFatal bool) {
		workerConn, peerConn := auditSocketpair(t)
		defer workerConn.Close()
		defer peerConn.Close()

		opts := PublisherOptions{
			Socket:        "/tmp/fake.sock",
			Run:           "run-worker-audit",
			Source:        "src-worker-audit",
			SupervisorUID: os.Getuid(),
			Config:        ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: 4, Capacity: 1024 * 1024},
		}
		p, err := NewPublisher(opts)
		if err != nil {
			t.Fatalf("%s: NewPublisher: %v", tcName, err)
		}
		defer p.Close()

		type peerRes struct {
			err error
		}
		peerCh := make(chan peerRes, 1)
		peerJoined := make(chan struct{})

		defer func() {
			_ = p.Close()
			_ = workerConn.Close()
			_ = peerConn.Close()
			select {
			case <-peerJoined:
			case <-time.After(2 * time.Second):
			}
		}()

		go func() {
			defer close(peerJoined)
			pkt, _, err := ipc.ReadPublicationMsg(peerConn, 0, time.Now().Add(2*time.Second))
			if err != nil {
				peerCh <- peerRes{err: fmt.Errorf("read hello: %w", err)}
				return
			}
			if pkt.Type != ipc.TypeHello || pkt.Hello == nil {
				peerCh <- peerRes{err: fmt.Errorf("expected hello packet, got %v", pkt.Type)}
				return
			}
			w, fdsToSend, err := setupWelcome(*pkt.Hello)
			if err != nil {
				peerCh <- peerRes{err: fmt.Errorf("setupWelcome generator: %w", err)}
				return
			}
			welcomePkt := ipc.PublicationPacket{
				Version: ipc.PublicationVersion,
				Type:    ipc.TypeWelcome,
				Welcome: &w,
			}
			err = ipc.WritePublicationMsg(peerConn, welcomePkt, fdsToSend, time.Now().Add(2*time.Second))
			// SENDER handles closed once
			for _, fd := range fdsToSend {
				if fd >= 0 {
					_ = unix.Close(fd)
				}
			}
			if err != nil {
				peerCh <- peerRes{err: fmt.Errorf("write welcome: %w", err)}
				return
			}
			peerCh <- peerRes{nil}
		}()

		setupErr := p.setup(workerConn)

		// Snapshot setupErr and retained descriptor count BEFORE p.Close
		p.mu.Lock()
		retainedFDs := len(p.fds)
		p.mu.Unlock()

		// p.Close and socket closes guaranteed before joining peer goroutine
		_ = p.Close()
		_ = workerConn.Close()
		_ = peerConn.Close()

		var pRes peerRes
		select {
		case pRes = <-peerCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: peer goroutine failed to join within 2s", tcName)
		}
		if pRes.err != nil {
			t.Fatalf("%s: fake peer error: %v", tcName, pRes.err)
		}

		// Assertions AFTER cleanup/join use snapshot
		if !expectFatal {
			if setupErr != nil {
				t.Fatalf("%s (positive control): setup failed: %v", tcName, setupErr)
			}
			if retainedFDs != opts.Config.Tracks {
				t.Errorf("%s (positive control): expected %d fds, got %d", tcName, opts.Config.Tracks, retainedFDs)
			}
		} else {
			if setupErr == nil {
				t.Errorf("%s: defect control failed: setup succeeded, want ErrFatal", tcName)
			} else if !errors.Is(setupErr, ErrFatal) {
				t.Errorf("%s: setup error is %v, want ErrFatal", tcName, setupErr)
			}
			if retainedFDs != 0 {
				t.Errorf("%s: publisher retained %d fds after failure, want 0", tcName, retainedFDs)
			}
		}
	}

	// 1. Positive control: canonical sealed allocated epoch
	runTest("positive-canonical-sealed", func(h ipc.Hello) (ipc.PublicationWelcome, []int, error) {
		store, err := partstore.AllocateEpoch(h.Run, h.Source, 1, h.Config)
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		defer store.Close()
		ids, err := store.Identities()
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		dups, err := store.DupFDs()
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		w := ipc.PublicationWelcome{
			State: ipc.Welcome{
				Version: ipc.Version,
				Run:     h.Run,
				Source:  h.Source,
				Epoch:   1,
				Setup:   true,
				Applied: 0,
				Tracks:  ids,
			},
		}
		return w, dups, nil
	}, false)

	// 2. Negative independently: unsealed memfds (differ ONLY in seals, use canonical names from correct scope)
	runTest("negative-unsealed-memfds", func(h ipc.Hello) (ipc.PublicationWelcome, []int, error) {
		canonStore, err := partstore.AllocateEpoch(h.Run, h.Source, 1, h.Config)
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		canonIds, err := canonStore.Identities()
		_ = canonStore.Close()
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}

		fds := make([]int, 0, h.Config.Tracks)
		ids := make([]ipc.FDIdentity, 0, h.Config.Tracks)
		for i := 0; i < h.Config.Tracks; i++ {
			name := canonIds[i].Name
			fd, err := unix.MemfdCreate(name, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
			if err != nil {
				for _, f := range fds {
					_ = unix.Close(f)
				}
				return ipc.PublicationWelcome{}, nil, err
			}
			if err := unix.Ftruncate(fd, h.Config.Capacity); err != nil {
				_ = unix.Close(fd)
				for _, f := range fds {
					_ = unix.Close(f)
				}
				return ipc.PublicationWelcome{}, nil, err
			}
			// Unsealed: no F_ADD_SEALS
			var st unix.Stat_t
			if err := unix.Fstat(fd, &st); err != nil {
				_ = unix.Close(fd)
				for _, f := range fds {
					_ = unix.Close(f)
				}
				return ipc.PublicationWelcome{}, nil, err
			}
			fds = append(fds, fd)
			ids = append(ids, ipc.FDIdentity{
				Track: i,
				Name:  name,
				Dev:   uint64(st.Dev),
				Ino:   st.Ino,
				Size:  st.Size,
			})
		}
		w := ipc.PublicationWelcome{
			State: ipc.Welcome{
				Version: ipc.Version,
				Run:     h.Run,
				Source:  h.Source,
				Epoch:   1,
				Setup:   true,
				Applied: 0,
				Tracks:  ids,
			},
		}
		return w, fds, nil
	}, true)

	// 3. Negative independently: missing grow/shrink seal (differ ONLY in seals, use canonical names from correct scope)
	runTest("negative-missing-grow-shrink-seal", func(h ipc.Hello) (ipc.PublicationWelcome, []int, error) {
		canonStore, err := partstore.AllocateEpoch(h.Run, h.Source, 1, h.Config)
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		canonIds, err := canonStore.Identities()
		_ = canonStore.Close()
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}

		fds := make([]int, 0, h.Config.Tracks)
		ids := make([]ipc.FDIdentity, 0, h.Config.Tracks)
		for i := 0; i < h.Config.Tracks; i++ {
			name := canonIds[i].Name
			fd, err := unix.MemfdCreate(name, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
			if err != nil {
				for _, f := range fds {
					_ = unix.Close(f)
				}
				return ipc.PublicationWelcome{}, nil, err
			}
			if err := unix.Ftruncate(fd, h.Config.Capacity); err != nil {
				_ = unix.Close(fd)
				for _, f := range fds {
					_ = unix.Close(f)
				}
				return ipc.PublicationWelcome{}, nil, err
			}
			// Only add F_SEAL_SEAL, omitting F_SEAL_GROW and F_SEAL_SHRINK
			if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_SEAL); err != nil {
				_ = unix.Close(fd)
				for _, f := range fds {
					_ = unix.Close(f)
				}
				return ipc.PublicationWelcome{}, nil, err
			}
			var st unix.Stat_t
			if err := unix.Fstat(fd, &st); err != nil {
				_ = unix.Close(fd)
				for _, f := range fds {
					_ = unix.Close(f)
				}
				return ipc.PublicationWelcome{}, nil, err
			}
			fds = append(fds, fd)
			ids = append(ids, ipc.FDIdentity{
				Track: i,
				Name:  name,
				Dev:   uint64(st.Dev),
				Ino:   st.Ino,
				Size:  st.Size,
			})
		}
		w := ipc.PublicationWelcome{
			State: ipc.Welcome{
				Version: ipc.Version,
				Run:     h.Run,
				Source:  h.Source,
				Epoch:   1,
				Setup:   true,
				Applied: 0,
				Tracks:  ids,
			},
		}
		return w, fds, nil
	}, true)

	// 4. Negative independently: descriptors allocated for DIFFERENT source but welcome names expected source
	runTest("negative-different-source-descriptors", func(h ipc.Hello) (ipc.PublicationWelcome, []int, error) {
		diffStore, err := partstore.AllocateEpoch(h.Run, "other-source", 1, h.Config)
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		defer diffStore.Close()
		diffIds, err := diffStore.Identities()
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		diffDups, err := diffStore.DupFDs()
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		w := ipc.PublicationWelcome{
			State: ipc.Welcome{
				Version: ipc.Version,
				Run:     h.Run,
				Source:  h.Source, // names expected source
				Epoch:   1,
				Setup:   true,
				Applied: 0,
				Tracks:  diffIds, // describes descriptors allocated for other-source
			},
		}
		return w, diffDups, nil
	}, true)

	// 5. Negative independently: two track IDs alias same inode
	runTest("negative-aliased-inode-descriptors", func(h ipc.Hello) (ipc.PublicationWelcome, []int, error) {
		store, err := partstore.AllocateEpoch(h.Run, h.Source, 1, h.Config)
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		defer store.Close()
		ids, err := store.Identities()
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		dups, err := store.DupFDs()
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		// Close track 1 descriptor, set dups[1] = -1 immediately before Fcntl so cleanup never closes stale fd
		_ = unix.Close(dups[1])
		dups[1] = -1
		aliasFD, err := unix.FcntlInt(uintptr(dups[0]), unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			for _, f := range dups {
				if f >= 0 {
					_ = unix.Close(f)
				}
			}
			return ipc.PublicationWelcome{}, nil, err
		}
		dups[1] = aliasFD
		ids[1].Dev = ids[0].Dev
		ids[1].Ino = ids[0].Ino
		ids[1].Name = ids[0].Name
		ids[1].Size = ids[0].Size

		w := ipc.PublicationWelcome{
			State: ipc.Welcome{
				Version: ipc.Version,
				Run:     h.Run,
				Source:  h.Source,
				Epoch:   1,
				Setup:   true,
				Applied: 0,
				Tracks:  ids,
			},
		}
		return w, dups, nil
	}, true)
}

func TestAuditPublisherConcurrentCloseJoins(t *testing.T) {
	opts := PublisherOptions{
		Socket:        "/tmp/fake.sock",
		SupervisorUID: os.Getuid(),
		Run:           "run-close",
		Source:        "src-close",
		Config:        ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: 4, Capacity: 1024 * 1024},
	}
	p, err := NewPublisher(opts)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	activeCh := make(chan struct{})
	var barrierOnce sync.Once
	releaseBarrier := func() {
		barrierOnce.Do(func() {
			close(activeCh)
		})
	}
	defer releaseBarrier()

	cancelCalled := make(chan struct{})
	cancelFunc := func() {
		select {
		case <-cancelCalled:
		default:
			close(cancelCalled)
		}
	}

	// Injected operation barrier under lc
	p.lc.Lock()
	p.cancel = cancelFunc
	p.active = activeCh
	p.lc.Unlock()

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
		close1Ch <- closeResult{err: p.Close()}
		close(close1Joined)
	}()

	// Observe p.closed under lc
	deadline := time.Now().Add(500 * time.Millisecond)
	observedClosed := false
	for time.Now().Before(deadline) {
		p.lc.Lock()
		c := p.closed
		p.lc.Unlock()
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
		t.Errorf("UNIT: first Close did not set p.closed within deadline")
	} else {
		close2Started = true
		go func() {
			close2Ch <- closeResult{err: p.Close()}
			close(close2Joined)
		}()

		// Second Close must wait: bounded 100-250ms negative observation
		select {
		case res2 := <-close2Ch:
			secondReturnedEarly = true
			earlyErr2 = res2.err
		case <-time.After(150 * time.Millisecond):
			// Desired behavior: second Close is waiting
		}
	}

	// Release barrier
	releaseBarrier()

	// Join close1
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
		t.Errorf("UNIT: defect control failed: second Close returned early while active operation barrier was open")
	}
	if res1.err != nil {
		t.Errorf("close1 returned unexpected error: %v", res1.err)
	}
	if res2.err != nil {
		t.Errorf("close2 returned unexpected error: %v", res2.err)
	}
}

func TestAuditKnownCanceledContext(t *testing.T) {
	opts := PublisherOptions{
		Socket:        "/tmp/fake.sock",
		SupervisorUID: os.Getuid(),
		Run:           "run-cancel",
		Source:        "src-cancel",
		Config:        ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: 4, Capacity: 1024 * 1024},
	}
	p, err := NewPublisher(opts)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	c0, c1 := auditSocketpair(t)
	defer c0.Close()
	defer c1.Close()

	p.conn = c0

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled

	var invoked bool
	callback := func(c *net.UnixConn) error {
		invoked = true
		return nil
	}

	err = p.withConn(ctx, callback)

	// Close both endpoints, allow watcher to exit
	_ = c0.Close()
	_ = c1.Close()
	p.conn = nil

	// Assertions
	if invoked {
		t.Errorf("UNIT: defect control failed: callback was invoked for already-canceled context")
	}
	if err == nil {
		t.Errorf("UNIT: defect control failed: withConn returned nil error on already-canceled context")
	} else if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrFatal) {
		t.Errorf("UNIT: expected cancellation or ErrFatal, got: %v", err)
	}
}
