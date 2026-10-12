//go:build linux

// Package worker holds the worker role's side of the supervisor contract. Publisher is its IPC2
// publication client: it receives the epoch's writable descriptors once, writes every init and
// part into exactly the extent the supervisor grants and publishes entries one at a time, so that
// no byte is ever visible before the supervisor has accepted it.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"golang.org/x/sys/unix"
)

var (
	// ErrInvalidOptions reports PublisherOptions the publisher refuses.
	ErrInvalidOptions = errors.New("worker: invalid publisher options")

	// ErrInvalidEntry reports a Publish argument that breaks the contract before anything is sent:
	// the publisher's own fields set, bytes for a metadata entry or none for an init or part, bytes
	// beyond the capacity, an unconfigured track or an entry the canonical validation refuses.
	ErrInvalidEntry = errors.New("worker: invalid entry")

	// ErrClosed reports a Publish on a closed publisher, or one cut short by Close.
	ErrClosed = errors.New("worker: publisher closed")

	// ErrFatal reports that the publisher is poisoned: a reject other than "full", a protocol
	// violation by the supervisor, an ambiguous write into a track, or a cancelled operation. The
	// epoch cannot be continued by this or any other publisher; the worker owner must exit.
	ErrFatal = errors.New("worker: publisher fatal")

	errFull = errors.New("worker: track full")
)

const (
	netDeadline   = 2 * time.Second
	retryInterval = 100 * time.Millisecond
	reasonFull    = "full"
	reasonBusy    = "busy" // the supervisor serves another connection of this worker: retry the handshake
)

// PublisherOptions describe the supervisor to publish to: the absolute path of its IPC2 unixpacket
// socket, the uid its process must have, and the scope and configuration of this worker's source.
type PublisherOptions struct {
	Socket        string
	Run           string
	Source        string
	Config        ipc.Config
	SupervisorUID int
}

// Publisher is one source's IPC2 client. The handshake is lazy: the first Publish dials, receives
// the epoch and its descriptors and then publishes. One operation runs at a time under a global
// mutex; it owns a copy of the caller's bytes and one immutable in-flight entry, and survives
// disconnects by reconnecting every 100 ms, context-bound, with the same operation. Any outcome
// the publisher cannot prove poisons it: every later Publish returns the same ErrFatal.
type Publisher struct {
	opts PublisherOptions
	pid  int

	mu    sync.Mutex // the global publication mutex: one operation at a time, and the handles
	conn  *net.UnixConn
	epoch uint64
	ids   []ipc.FDIdentity // the epoch's descriptors, in track order
	fds   []int            // owned writable duplicates, in track order; nil before the setup welcome
	high  uint64           // last acknowledged entry seq
	fatal error

	lc       sync.Mutex // lifecycle: Close against the active operation
	closed   bool
	cancel   context.CancelFunc
	active   chan struct{}
	done     chan struct{} // closed once the first Close has finished its cleanup
	closeErr error         // that cleanup's result, shared by every Close caller
}

// operation is one Publish in flight.
type operation struct {
	entry    ipc.Entry // stamped once the epoch is known; span set once written
	data     []byte    // owned copy
	digest   string
	byteKind bool
	grant    *ipc.Grant
	written  bool
	sent     bool // the publication left this process at least once: the supervisor may hold it
}

// NewPublisher validates o and returns a publisher that has not yet connected.
func NewPublisher(o PublisherOptions) (*Publisher, error) {
	switch {
	case !filepath.IsAbs(o.Socket):
		return nil, fmt.Errorf("%w: socket %q is not absolute", ErrInvalidOptions, o.Socket)
	case o.SupervisorUID < 0:
		return nil, fmt.Errorf("%w: supervisor uid %d is negative", ErrInvalidOptions, o.SupervisorUID)
	}
	pid := os.Getpid()
	hello := ipc.Hello{Version: ipc.Version, Run: o.Run, Source: o.Source, PID: pid, Config: o.Config}
	if err := ipc.ValidatePacket(ipc.Packet{Type: ipc.TypeHello, Hello: &hello}); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidOptions, err)
	}
	return &Publisher{opts: o, pid: pid}, nil
}

// Publish publishes e with data and returns the entry as acknowledged. The caller provides kind,
// track and the kind's native fields; version, run, source, epoch, seq, offset, length and sha256
// must be zero and are filled here. An init or part carries 1..capacity bytes, which are copied,
// digested, written into the granted extent and published; a phases or terminal carries none. On
// ctx ending mid-operation the publisher is poisoned, because the supervisor's view of the
// operation can no longer be reconciled.
func (p *Publisher) Publish(ctx context.Context, e ipc.Entry, data []byte) (ipc.Entry, error) {
	op, err := p.prepare(e, data)
	if err != nil {
		return ipc.Entry{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fatal != nil {
		return ipc.Entry{}, p.fatal
	}
	opCtx, err := p.begin(ctx)
	if err != nil {
		return ipc.Entry{}, err
	}
	defer p.end()
	out, err := p.run(opCtx, op)
	if err != nil {
		if !errors.Is(err, ErrClosed) {
			p.fatal = err
		}
		return ipc.Entry{}, err
	}
	return out, nil
}

// Close cancels and joins the active operation, even while its Publish is blocked, then closes
// the connection and the track descriptors. The first caller performs the cleanup; every
// concurrent or later caller waits for it to finish and receives the same result. Nothing is said
// to the supervisor: a pending grant stays pending for a future hello that will never come.
func (p *Publisher) Close() error {
	p.lc.Lock()
	if p.closed {
		done := p.done
		p.lc.Unlock()
		<-done
		return p.closeErr
	}
	p.closed = true
	p.done = make(chan struct{})
	cancel, active, done := p.cancel, p.active, p.done
	p.lc.Unlock()
	if cancel != nil {
		cancel()
	}
	if active != nil {
		<-active
	}
	p.mu.Lock()
	var errs []error
	if p.conn != nil {
		if err := p.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
		p.conn = nil
	}
	for i, fd := range p.fds {
		if err := unix.Close(fd); err != nil {
			errs = append(errs, fmt.Errorf("worker: close track %d: %w", i, err))
		}
	}
	p.fds = nil
	p.mu.Unlock()
	p.closeErr = errors.Join(errs...)
	close(done)
	return p.closeErr
}

// begin registers the operation's context for Close to cancel. The caller holds mu.
func (p *Publisher) begin(ctx context.Context) (context.Context, error) {
	p.lc.Lock()
	defer p.lc.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	opCtx, cancel := context.WithCancel(ctx)
	p.cancel, p.active = cancel, make(chan struct{})
	return opCtx, nil
}

// end releases what begin registered and lets a waiting Close proceed.
func (p *Publisher) end() {
	p.lc.Lock()
	cancel, active := p.cancel, p.active
	p.cancel, p.active = nil, nil
	p.lc.Unlock()
	cancel()
	close(active)
}

func (p *Publisher) isClosed() bool {
	p.lc.Lock()
	defer p.lc.Unlock()
	return p.closed
}

// prepare checks the caller's entry and bytes and builds the operation, before anything is sent.
func (p *Publisher) prepare(e ipc.Entry, data []byte) (*operation, error) {
	cfg := p.opts.Config
	if e.Version != 0 || e.Run != "" || e.Source != "" || e.Epoch != 0 || e.Seq != 0 || e.Offset != 0 || e.Length != 0 || e.SHA256 != "" {
		return nil, fmt.Errorf("%w: version, run, source, epoch, seq, offset, length and sha256 must be zero", ErrInvalidEntry)
	}
	op := &operation{entry: e}
	switch e.Kind {
	case ipc.KindInit, ipc.KindPart:
		switch {
		case len(data) == 0:
			return nil, fmt.Errorf("%w: %s entry without bytes", ErrInvalidEntry, e.Kind)
		case int64(len(data)) > cfg.Capacity:
			return nil, fmt.Errorf("%w: %d bytes exceed the track capacity %d", ErrInvalidEntry, len(data), cfg.Capacity)
		}
		op.byteKind = true
		op.data = append([]byte(nil), data...)
		sum := sha256.Sum256(op.data)
		op.digest = hex.EncodeToString(sum[:])
	case ipc.KindPhases, ipc.KindTerminal:
		if len(data) != 0 {
			return nil, fmt.Errorf("%w: %s entry carries %d bytes", ErrInvalidEntry, e.Kind, len(data))
		}
	default:
		return nil, fmt.Errorf("%w: unsupported kind %q", ErrInvalidEntry, e.Kind)
	}
	if e.Kind != ipc.KindTerminal && (e.Track < 0 || e.Track >= cfg.Tracks) {
		return nil, fmt.Errorf("%w: track %d is not one of the %d configured", ErrInvalidEntry, e.Track, cfg.Tracks)
	}
	// Probe the final shape once; only the epoch, seq and offset can differ later.
	probe := p.stamped(e, 1, 1)
	if op.byteKind {
		probe.Offset, probe.Length, probe.SHA256 = 0, int64(len(op.data)), op.digest
	}
	if err := ipc.ValidatePacket(ipc.Packet{Type: ipc.TypeEntry, Entry: &probe}); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEntry, err)
	}
	return op, nil
}

// stamped returns e with the publisher's scope, version, epoch and seq.
func (p *Publisher) stamped(e ipc.Entry, epoch, seq uint64) ipc.Entry {
	e.Version, e.Run, e.Source, e.Epoch, e.Seq = ipc.Version, p.opts.Run, p.opts.Source, epoch, seq
	return e
}

// run drives op to its acknowledgement, reconnecting with the same operation as often as the
// connection is lost. Every other failure is final.
func (p *Publisher) run(ctx context.Context, op *operation) (ipc.Entry, error) {
	for {
		if p.conn == nil {
			if err := p.connect(ctx, op); err != nil {
				return ipc.Entry{}, err
			}
		}
		if op.entry.Seq == 0 {
			op.entry = p.stamped(op.entry, p.epoch, p.high+1)
		}
		err := p.withConn(ctx, func(conn *net.UnixConn) error { return p.step(ctx, conn, op) })
		if err == nil {
			p.high = op.entry.Seq
			return op.entry, nil
		}
		if !isDisconnect(err) {
			return ipc.Entry{}, err
		}
		p.drop()
		if err := p.pause(ctx); err != nil {
			return ipc.Entry{}, err
		}
	}
}

// connect dials and completes a handshake, retrying every 100 ms while the supervisor cannot be
// reached, until a connection is established or the failure is final.
func (p *Publisher) connect(ctx context.Context, op *operation) error {
	for {
		conn, err := p.dial(ctx)
		if err == nil {
			p.conn = conn
			err = p.withConn(ctx, func(conn *net.UnixConn) error {
				if p.fds == nil {
					return p.setup(conn)
				}
				return p.reconnect(conn, op)
			})
			if err == nil {
				return nil
			}
			p.drop()
		}
		if !isDisconnect(err) {
			return err
		}
		if err := p.pause(ctx); err != nil {
			return err
		}
	}
}

// dial connects to the supervisor's socket and checks that its peer runs as the supervisor uid.
func (p *Publisher) dial(ctx context.Context) (*net.UnixConn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "unixpacket", p.opts.Socket)
	if err != nil {
		if ctx.Err() != nil {
			return nil, p.cancelled(ctx)
		}
		return nil, disconnectError{err}
	}
	conn, ok := c.(*net.UnixConn)
	if !ok {
		c.Close()
		return nil, fatalf("dial returned a %T", c)
	}
	uid, _, err := ipc.PeerCredentials(conn)
	switch {
	case err != nil:
		conn.Close()
		return nil, fatalf("peer credentials: %v", err)
	case uid != p.opts.SupervisorUID:
		conn.Close()
		return nil, fatalf("socket peer is uid %d, want %d", uid, p.opts.SupervisorUID)
	}
	return conn, nil
}

// withConn runs f on the current connection while a watcher closes it should ctx end. A context
// that has already ended is refused before f runs, and the watcher is stopped and joined before
// withConn returns. A failure after ctx ended is reported as the cancellation.
func (p *Publisher) withConn(ctx context.Context, f func(*net.UnixConn) error) error {
	if ctx.Err() != nil {
		return p.cancelled(ctx)
	}
	conn := p.conn
	stop, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-ctx.Done():
			conn.Close()
		case <-stop:
		}
	}()
	err := f(conn)
	close(stop)
	<-joined
	if err != nil && ctx.Err() != nil {
		return p.cancelled(ctx)
	}
	return err
}

// setup runs the setup handshake: a hello for epoch 0 and a welcome that assigns the epoch and
// delivers its descriptors. The whole received set is adopted through partstore.AdoptEpoch, which
// verifies each against the identity the welcome describes, the canonical name, size, access mode
// and seals, and consumes every delivered descriptor on every path; only owned duplicates of a
// fully verified set are kept, and the temporary store is closed.
func (p *Publisher) setup(conn *net.UnixConn) error {
	cfg := p.opts.Config
	hello := ipc.Hello{Version: ipc.Version, Run: p.opts.Run, Source: p.opts.Source, PID: p.pid, Config: cfg}
	if err := p.write(conn, ipc.PublicationPacket{Version: ipc.PublicationVersion, Type: ipc.TypeHello, Hello: &hello}); err != nil {
		return err
	}
	pk, fds, err := ipc.ReadPublicationMsg(conn, cfg.Tracks, time.Now().Add(netDeadline))
	if err != nil {
		return classify(err)
	}
	w, err := p.welcome(pk, fds)
	if err != nil {
		return err
	}
	st := w.State
	switch {
	case !st.Setup:
		closeUnique(fds)
		return fatalf("setup welcome without descriptors")
	case st.Applied != 0 || w.Pending != nil:
		closeUnique(fds)
		return fatalf("setup welcome with applied %d and a pending grant", st.Applied)
	case len(st.Tracks) != cfg.Tracks:
		closeUnique(fds)
		return fatalf("setup welcome describes %d tracks, config has %d", len(st.Tracks), cfg.Tracks)
	}
	// Order the received set by track, as AdoptEpoch expects; the welcome names each track once.
	ids := make([]ipc.FDIdentity, cfg.Tracks)
	ordered := make([]int, cfg.Tracks)
	for i := range ordered {
		ordered[i] = -1
	}
	for i, d := range st.Tracks {
		if d.Track < 0 || d.Track >= cfg.Tracks || ordered[d.Track] >= 0 {
			closeUnique(fds)
			return fatalf("setup welcome describes track %d twice or outside the configuration", d.Track)
		}
		ids[d.Track], ordered[d.Track] = d, fds[i]
	}
	store, err := partstore.AdoptEpoch(p.opts.Run, p.opts.Source, st.Epoch, cfg, ordered, ids)
	if err != nil {
		return fatalf("adopt the epoch's descriptors: %v", err)
	}
	owned, err := store.DupFDs()
	if cerr := store.Close(); err == nil && cerr != nil {
		closeUnique(owned)
		err = cerr
	}
	if err != nil {
		return fatalf("duplicate the epoch's descriptors: %v", err)
	}
	p.epoch, p.ids, p.fds = st.Epoch, ids, owned
	return nil
}

// reconnect runs the handshake of an assigned epoch and reconciles op with what the welcome says
// the supervisor holds.
func (p *Publisher) reconnect(conn *net.UnixConn, op *operation) error {
	high := p.high
	if op.sent {
		high = op.entry.Seq
	}
	hello := ipc.Hello{Version: ipc.Version, Run: p.opts.Run, Source: p.opts.Source, Epoch: p.epoch, PID: p.pid,
		Config: p.opts.Config, Tracks: append([]ipc.FDIdentity(nil), p.ids...), High: high}
	if err := p.write(conn, ipc.PublicationPacket{Version: ipc.PublicationVersion, Type: ipc.TypeHello, Hello: &hello}); err != nil {
		return err
	}
	pk, _, err := ipc.ReadPublicationMsg(conn, 0, time.Now().Add(netDeadline))
	if err != nil {
		return classify(err)
	}
	w, err := p.welcome(pk, nil)
	if err != nil {
		return err
	}
	switch {
	case w.State.Setup:
		return fatalf("setup welcome on a reconnect")
	case w.State.Epoch != p.epoch:
		return fatalf("welcome for epoch %d, this publisher holds epoch %d", w.State.Epoch, p.epoch)
	}
	return p.reconcile(w, op)
}

// welcome checks the common part of a handshake answer. On a reject or a foreign scope the
// delivered descriptors are closed. A "busy" reject means the supervisor still serves an earlier
// connection of this worker: the handshake is retried like a lost connection.
func (p *Publisher) welcome(pk ipc.PublicationPacket, fds []int) (*ipc.PublicationWelcome, error) {
	switch pk.Type {
	case ipc.TypeWelcome:
	case ipc.TypeReject:
		closeUnique(fds)
		if pk.Reason == reasonBusy {
			return nil, disconnectError{errors.New("supervisor busy with an earlier connection")}
		}
		return nil, fatalf("hello rejected: %s", pk.Reason)
	default:
		closeUnique(fds)
		return nil, fatalf("expected a welcome, got a %s packet", pk.Type)
	}
	w := pk.Welcome
	if w.State.Run != p.opts.Run || w.State.Source != p.opts.Source {
		closeUnique(fds)
		return nil, fatalf("welcome for run %q source %q, want run %q source %q", w.State.Run, w.State.Source, p.opts.Run, p.opts.Source)
	}
	return w, nil
}

// reconcile compares the welcome with the operation in flight. Applied is either this publisher's
// acknowledged high or, if the publication was sent and its ack lost, one more: then the identical
// publication is sent again and compared by the supervisor, never skipped. A pending grant must
// be exactly the one held, or the exact answer to the reserve whose grant was lost.
func (p *Publisher) reconcile(w *ipc.PublicationWelcome, op *operation) error {
	seq := p.high + 1
	applied, pending := w.State.Applied, w.Pending
	switch {
	case applied == seq:
		if !op.sent || pending != nil {
			return fatalf("supervisor holds seq %d, which this publisher never published", seq)
		}
		return nil
	case applied != p.high:
		return fatalf("welcome applied %d, this publisher acknowledged %d", applied, p.high)
	}
	switch {
	case op.grant != nil:
		if pending == nil || *pending != *op.grant {
			return fatalf("grant %d of seq %d is no longer pending", op.grant.ID, seq)
		}
	case op.byteKind && op.entry.Seq != 0 && pending != nil:
		if pending.Seq != seq || pending.Kind != op.entry.Kind || pending.Track != op.entry.Track || pending.Length != int64(len(op.data)) ||
			pending.Offset > p.opts.Config.Capacity-pending.Length {
			return fatalf("pending grant %+v does not answer the reserve of seq %d", *pending, seq)
		}
		g := *pending
		op.grant = &g
	case pending != nil:
		return fatalf("welcome names pending grant %d this publisher does not hold", pending.ID)
	}
	return nil
}

// step runs op on conn from wherever it stands: reserve until granted, write once, publish.
func (p *Publisher) step(ctx context.Context, conn *net.UnixConn, op *operation) error {
	if op.byteKind {
		for op.grant == nil {
			g, err := p.reserve(conn, op)
			switch {
			case errors.Is(err, errFull):
				if err := p.pause(ctx); err != nil {
					return err
				}
			case err != nil:
				return err
			default:
				op.grant = g
			}
		}
		if !op.written {
			if err := pwriteFull(p.fds[op.grant.Track], op.data, op.grant.Offset); err != nil {
				return fmt.Errorf("%w: write of %d bytes at %d in track %d is ambiguous: %v", ErrFatal, len(op.data), op.grant.Offset, op.grant.Track, err)
			}
			op.written = true
			op.entry.Offset, op.entry.Length, op.entry.SHA256 = op.grant.Offset, op.grant.Length, op.digest
		}
	}
	return p.publish(conn, op)
}

// reserve asks for op's extent and returns the grant, errFull while nothing fits.
func (p *Publisher) reserve(conn *net.UnixConn, op *operation) (*ipc.Grant, error) {
	req := ipc.ReserveRequest{Seq: op.entry.Seq, Kind: op.entry.Kind, Track: op.entry.Track, Length: int64(len(op.data))}
	if err := p.write(conn, ipc.PublicationPacket{Version: ipc.PublicationVersion, Type: ipc.TypeReserve, Reserve: &req}); err != nil {
		return nil, err
	}
	pk, _, err := ipc.ReadPublicationMsg(conn, 0, time.Now().Add(netDeadline))
	if err != nil {
		return nil, classify(err)
	}
	switch pk.Type {
	case ipc.TypeGrant:
		g := *pk.Grant
		if g.Seq != req.Seq || g.Kind != req.Kind || g.Track != req.Track || g.Length != req.Length || g.Offset > p.opts.Config.Capacity-g.Length {
			return nil, fatalf("grant %+v does not answer reserve %+v", g, req)
		}
		return &g, nil
	case ipc.TypeReject:
		if pk.Reason == reasonFull {
			return nil, errFull
		}
		return nil, fatalf("reserve of seq %d rejected: %s", req.Seq, pk.Reason)
	}
	return nil, fatalf("expected a grant, got a %s packet", pk.Type)
}

// publish sends op's publication and awaits its acknowledgement.
func (p *Publisher) publish(conn *net.UnixConn, op *operation) error {
	pub := ipc.Publication{Entry: op.entry}
	if op.byteKind {
		g := *op.grant
		pub.Grant = &g
	}
	op.sent = true
	if err := p.write(conn, ipc.PublicationPacket{Version: ipc.PublicationVersion, Type: ipc.TypePublish, Publish: &pub}); err != nil {
		return err
	}
	pk, _, err := ipc.ReadPublicationMsg(conn, 0, time.Now().Add(netDeadline))
	if err != nil {
		return classify(err)
	}
	switch pk.Type {
	case ipc.TypeAck:
		if pk.Ack.Seq != op.entry.Seq {
			return fatalf("ack for seq %d, published seq %d", pk.Ack.Seq, op.entry.Seq)
		}
		return nil
	case ipc.TypeReject:
		return fatalf("publication of seq %d rejected: %s", op.entry.Seq, pk.Reason)
	}
	return fatalf("expected an ack, got a %s packet", pk.Type)
}

func (p *Publisher) write(conn *net.UnixConn, pk ipc.PublicationPacket) error {
	return classify(ipc.WritePublicationMsg(conn, pk, nil, time.Now().Add(netDeadline)))
}

// drop closes and forgets the connection. A connection the watcher already closed is fine.
func (p *Publisher) drop() {
	if p.conn != nil {
		_ = p.conn.Close() // net.ErrClosed is benign here
		p.conn = nil
	}
}

// pause waits the retry interval, or returns the cancellation when ctx ends first.
func (p *Publisher) pause(ctx context.Context) error {
	t := time.NewTimer(retryInterval)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return p.cancelled(ctx)
	case <-t.C:
		return nil
	}
}

// cancelled is the error of an operation cut short: ErrClosed when Close did it, otherwise the
// poisoning cancellation.
func (p *Publisher) cancelled(ctx context.Context) error {
	if p.isClosed() {
		return ErrClosed
	}
	return fmt.Errorf("%w: operation cancelled: %w", ErrFatal, ctx.Err())
}

// disconnectError marks a connection lost: the operation continues on the next connection.
type disconnectError struct{ err error }

func (e disconnectError) Error() string { return "worker: connection lost: " + e.err.Error() }
func (e disconnectError) Unwrap() error { return e.err }

func isDisconnect(err error) bool {
	var d disconnectError
	return errors.As(err, &d)
}

// classify sorts a transport error: a packet that breaks the contract is the supervisor's
// protocol violation and final; anything else (EOF, a deadline, a closed socket) is a lost
// connection.
func classify(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ipc.ErrInvalidPacket), errors.Is(err, ipc.ErrUnsupportedVersion),
		errors.Is(err, ipc.ErrInvalidTransport), errors.Is(err, ipc.ErrPacketTooLarge):
		return fmt.Errorf("%w: %w", ErrFatal, err)
	}
	return disconnectError{err}
}

func fatalf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrFatal, fmt.Sprintf(format, args...))
}

// pwriteFull writes all of b at off, retrying short writes and EINTR.
func pwriteFull(fd int, b []byte, off int64) error {
	for len(b) > 0 {
		n, err := unix.Pwrite(fd, b, off)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		b, off = b[n:], off+int64(n)
	}
	return nil
}

// closeUnique closes each non-negative descriptor in fds exactly once, however often it is listed.
func closeUnique(fds []int) {
	for i, fd := range fds {
		if fd < 0 {
			continue
		}
		seen := false
		for _, prev := range fds[:i] {
			if prev == fd {
				seen = true
				break
			}
		}
		if !seen {
			_ = unix.Close(fd)
		}
	}
}
