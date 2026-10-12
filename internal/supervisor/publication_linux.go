//go:build linux

package supervisor

// The IPC2 session of a Source with its worker. One connection is active at a time; a later
// connection, even from the same authenticated worker process, is refused with "busy" and the
// active session is left untouched, until that session has ended. The session position is the ring's: the entry
// high-water and the single pending grant, both durable, so a connection ending (EOF, timeout,
// cancellation, Close) changes nothing. While a grant is pending the session admits only its
// exact reserve repeat, the publication that fills it, or an exact retained duplicate; a full
// track is a "full" reject that keeps the connection, every other refusal rejects and closes.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"golang.org/x/sys/unix"
)

const (
	netDeadline = 2 * time.Second

	reasonFull         = "full"
	reasonBusy         = "busy"
	reasonMismatch     = "mismatch"
	reasonInvalid      = "invalid"
	reasonRefused      = "refused"
	reasonUnauthorized = "unauthorized"

	hookReserveDurable = "reserve_durable"
	hookCommitDurable  = "commit_durable"
)

// reply is the session's answer to one request and whether the connection ends after it.
type reply struct {
	packet ipc.PublicationPacket
	close  bool
	err    error
}

// ServeConn runs the IPC2 session on conn until the worker goes away, the session is broken, ctx
// ends or the Source closes. It consumes conn: the connection is closed before it returns on every
// path. The peer must be exactly the epoch's worker process; while another session is active the
// connection is refused with "busy" (ErrBusy) and the active one is untouched. ctx ending closes
// exactly this connection; its watcher is stopped and joined before the session is released. The
// worker's clean close is a nil result; a broken session wraps ErrProtocol.
func (s *Source) ServeConn(ctx context.Context, conn *net.UnixConn) error {
	if conn == nil {
		return fmt.Errorf("%w: nil connection", ErrInvalidOptions)
	}
	defer conn.Close() // net.ErrClosed is benign: the watcher or Close may have closed it first
	id, err := ipc.PeerProcessIdentity(conn)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	unix.Close(id.Pidfd) // the identity is compared, never kept
	if peer := (WorkerOwner{UID: id.UID, PID: id.PID, Ino: id.Ino}); peer != s.owner {
		s.reject(conn, reasonUnauthorized)
		return fmt.Errorf("%w: uid %d pid %d inode %d", ErrUnauthorized, id.UID, id.PID, id.Ino)
	}
	sess := &session{conn: conn, done: make(chan struct{})}
	if err := s.admit(sess); err != nil {
		reason := reasonRefused
		if errors.Is(err, ErrBusy) {
			reason = reasonBusy
		}
		s.reject(conn, reason)
		return err
	}
	stop, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-ctx.Done():
			conn.Close()
		case <-stop:
		}
	}()
	err = s.serve(ctx, conn, id.PID)
	close(stop)
	<-joined
	s.release(sess)
	return err
}

// admit makes sess the active session when none is; an occupied Source refuses with ErrBusy and
// leaves the active session as it is.
func (s *Source) admit(sess *session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.closed:
		return ErrClosed
	case s.refusing.Load():
		return ErrRefusing
	case s.active != nil:
		return ErrBusy
	}
	s.active = sess
	return nil
}

// release clears sess as the active session and marks it done.
func (s *Source) release(sess *session) {
	s.mu.Lock()
	if s.active == sess {
		s.active = nil
	}
	s.mu.Unlock()
	close(sess.done)
}

// serve runs the handshake and then answers requests until the connection ends.
func (s *Source) serve(ctx context.Context, conn *net.UnixConn, peerPID int) error {
	p, _, err := ipc.ReadPublicationMsg(conn, 0, time.Now().Add(netDeadline))
	if err != nil {
		return s.connErr(ctx, err)
	}
	if p.Type != ipc.TypeHello {
		s.reject(conn, reasonInvalid)
		return fmt.Errorf("%w: expected a hello, got a %s packet", ErrProtocol, p.Type)
	}
	h := *p.Hello
	if h.Run != s.run || h.Source != s.source || h.Config != s.cfg || h.PID != peerPID {
		s.reject(conn, reasonMismatch)
		return fmt.Errorf("%w: hello names run %q source %q pid %d config %+v, this epoch is run %q source %q pid %d config %+v",
			ErrProtocol, h.Run, h.Source, h.PID, h.Config, s.run, s.source, peerPID, s.cfg)
	}
	w, dups, err := s.welcome(h)
	if err != nil {
		s.reject(conn, reasonFor(err))
		return err
	}
	err = ipc.WritePublicationMsg(conn, ipc.PublicationPacket{Version: ipc.PublicationVersion, Type: ipc.TypeWelcome, Welcome: &w}, dups, time.Now().Add(netDeadline))
	closeUnique(dups)
	if err != nil {
		return s.connErr(ctx, err)
	}
	for {
		p, _, err := ipc.ReadPublicationMsg(conn, 0, time.Now().Add(netDeadline))
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) && ctx.Err() == nil && !s.isClosed() {
				continue
			}
			return s.connErr(ctx, err)
		}
		var r reply
		switch p.Type {
		case ipc.TypeReserve:
			r = s.handleReserve(*p.Reserve)
		case ipc.TypePublish:
			r = s.handlePublish(*p.Publish)
		default:
			r = reply{packet: rejectPacket(reasonInvalid), close: true,
				err: fmt.Errorf("%w: unexpected %s packet after the handshake", ErrProtocol, p.Type)}
		}
		if err := ipc.WritePublicationMsg(conn, r.packet, nil, time.Now().Add(netDeadline)); err != nil {
			return s.connErr(ctx, err)
		}
		if r.close {
			return r.err
		}
	}
}

// welcome decides the answer to an authenticated hello, under op. A setup hello (epoch 0, high 0)
// is answered with fresh duplicates of the descriptors only while no record has been persisted;
// a reconnect hello must name the epoch, its exact descriptor identities and a high-water of
// EntryHigh or EntryHigh+1, and is told the pending grant for EntryHigh+1 when one exists. The
// returned duplicates are the caller's to close after the send.
func (s *Source) welcome(h ipc.Hello) (ipc.PublicationWelcome, []int, error) {
	s.op.Lock()
	defer s.op.Unlock()
	if s.refusing.Load() {
		return ipc.PublicationWelcome{}, nil, ErrRefusing
	}
	state := ipc.Welcome{Version: ipc.Version, Run: s.run, Source: s.source, Epoch: s.epoch}
	if h.Epoch == 0 {
		switch {
		case h.High != 0:
			return ipc.PublicationWelcome{}, nil, fmt.Errorf("%w: setup hello with high %d", ErrProtocol, h.High)
		case !s.fresh:
			return ipc.PublicationWelcome{}, nil, fmt.Errorf("%w: setup hello after the epoch's descriptors were issued and used", ErrProtocol)
		}
		dups, err := s.store.DupFDs()
		if err != nil {
			return ipc.PublicationWelcome{}, nil, err
		}
		state.Setup, state.Tracks = true, append([]ipc.FDIdentity(nil), s.ids...)
		return ipc.PublicationWelcome{State: state}, dups, nil
	}
	switch {
	case h.Epoch != s.epoch:
		return ipc.PublicationWelcome{}, nil, fmt.Errorf("%w: hello for epoch %d, this source is epoch %d", ErrProtocol, h.Epoch, s.epoch)
	case !s.sameIdentities(h.Tracks):
		return ipc.PublicationWelcome{}, nil, fmt.Errorf("%w: hello descriptor identities differ from the epoch's", ErrProtocol)
	case h.High != s.high && h.High != s.high+1:
		return ipc.PublicationWelcome{}, nil, fmt.Errorf("%w: hello high %d, epoch high is %d", ErrProtocol, h.High, s.high)
	}
	state.Applied = s.high
	w := ipc.PublicationWelcome{State: state}
	if s.pending != nil {
		g := s.grant(*s.pending)
		w.Pending = &g
	}
	return w, nil, nil
}

// sameIdentities reports whether ds describes exactly the epoch's descriptors, each track once.
func (s *Source) sameIdentities(ds []ipc.FDIdentity) bool {
	if len(ds) != len(s.ids) {
		return false
	}
	seen := make([]bool, len(s.ids))
	for _, d := range ds {
		if d.Track < 0 || d.Track >= len(s.ids) || seen[d.Track] || d != s.ids[d.Track] {
			return false
		}
		seen[d.Track] = true
	}
	return true
}

// grant is the wire form of the pending reservation, for entry high+1.
func (s *Source) grant(res partstore.Reservation) ipc.Grant {
	return ipc.Grant{Seq: s.high + 1, ID: res.ID, Kind: res.Kind, Track: res.Track, Offset: res.Offset, Length: res.Length}
}

// handleReserve answers a reserve: the pending grant again for its exact repeat, a new durable
// grant for the next sequence on a configured track, "full" without closing when nothing fits.
func (s *Source) handleReserve(r ipc.ReserveRequest) reply {
	s.op.Lock()
	defer s.op.Unlock()
	if s.refusing.Load() {
		return refuse(ErrRefusing)
	}
	if s.pending != nil {
		g := s.grant(*s.pending)
		if r.Seq == g.Seq && r.Kind == g.Kind && r.Track == g.Track && r.Length == g.Length {
			return reply{packet: grantPacket(g)}
		}
		return refuse(fmt.Errorf("%w: reserve %+v while grant %d is pending", ErrProtocol, r, g.ID))
	}
	switch {
	case r.Seq != s.high+1:
		return refuse(fmt.Errorf("%w: reserve for seq %d, want %d", ErrProtocol, r.Seq, s.high+1))
	case r.Track >= s.cfg.Tracks:
		return refuse(fmt.Errorf("%w: reserve on track %d, %d configured", ErrProtocol, r.Track, s.cfg.Tracks))
	}
	res, err := s.ring.Reserve(r.Kind, r.Track, r.Length)
	switch {
	case errors.Is(err, partstore.ErrFull):
		return reply{packet: rejectPacket(reasonFull)}
	case err != nil:
		return refuse(s.ringErr(err))
	}
	s.fresh = false
	s.pending = &res
	s.callHook(hookReserveDurable)
	return reply{packet: grantPacket(s.grant(res))}
}

// handlePublish answers a publication: an exact retained duplicate is acknowledged again, the
// publication that fills the pending grant is committed, a phases or terminal entry without any
// pending grant is applied; a closed segment is compacted before the acknowledgement.
func (s *Source) handlePublish(pb ipc.Publication) reply {
	e := pb.Entry
	s.op.Lock()
	defer s.op.Unlock()
	if s.refusing.Load() {
		return refuse(ErrRefusing)
	}
	if e.Run != s.run || e.Source != s.source || e.Epoch != s.epoch {
		return refuse(fmt.Errorf("%w: entry %d belongs to another run, source or epoch", ErrProtocol, e.Seq))
	}
	if e.Seq <= s.high {
		if err := s.ring.Apply(e); err != nil {
			return refuse(s.ringErr(err))
		}
		return reply{packet: ackPacket(e.Seq)}
	}
	if e.Seq != s.high+1 {
		return refuse(fmt.Errorf("%w: publication of seq %d, want %d", ErrProtocol, e.Seq, s.high+1))
	}
	byteKind := e.Kind == ipc.KindInit || e.Kind == ipc.KindPart
	var err error
	switch {
	case s.pending != nil && !byteKind:
		return refuse(fmt.Errorf("%w: %s entry %d while grant %d is pending", ErrProtocol, e.Kind, e.Seq, s.pending.ID))
	case s.pending != nil:
		if g := s.grant(*s.pending); pb.Grant == nil || *pb.Grant != g {
			return refuse(fmt.Errorf("%w: publication of seq %d does not carry pending grant %d", ErrProtocol, e.Seq, g.ID))
		}
		err = s.ring.Commit(*s.pending, e)
	case byteKind:
		return refuse(fmt.Errorf("%w: %s entry %d without a pending grant", ErrProtocol, e.Kind, e.Seq))
	default:
		err = s.ring.Apply(e)
	}
	if err != nil {
		return refuse(s.ringErr(err))
	}
	s.fresh = false
	s.high, s.pending = e.Seq, nil
	defer s.notify() // observers wake on the committed entry, or on the refusal below
	if closesSegment(e) {
		if err := s.journal.Compact(); err != nil {
			s.refusing.Store(true)
			return refuse(fmt.Errorf("%w: compact after seq %d: %w", ErrRefusing, e.Seq, err))
		}
	}
	s.callHook(hookCommitDurable)
	return reply{packet: ackPacket(e.Seq)}
}

// closesSegment reports whether committing e closed a segment: a part that starts a new segment
// after the first, or the terminal.
func closesSegment(e ipc.Entry) bool {
	return (e.Kind == ipc.KindPart && e.FragmentStart && e.MSN > 1) || e.Kind == ipc.KindTerminal
}

// ringErr maps a ring refusal: an ambiguous outcome makes the Source refuse from now on, anything
// else is the worker's protocol violation.
func (s *Source) ringErr(err error) error {
	if errors.Is(err, partstore.ErrPoisoned) {
		s.refusing.Store(true)
		s.notify()
		return fmt.Errorf("%w: %w", ErrRefusing, err)
	}
	if errors.Is(err, partstore.ErrClosed) {
		return fmt.Errorf("%w: %w", ErrClosed, err)
	}
	return fmt.Errorf("%w: %w", ErrProtocol, err)
}

// refuse is a reject that ends the connection, with the reason class of err.
func refuse(err error) reply {
	return reply{packet: rejectPacket(reasonFor(err)), close: true, err: err}
}

// reasonFor maps an error to the reject reason the worker sees.
func reasonFor(err error) string {
	switch {
	case errors.Is(err, ErrRefusing), errors.Is(err, ErrClosed):
		return reasonRefused
	case errors.Is(err, ErrProtocol) && !errors.Is(err, partstore.ErrInvalidPublication) &&
		!errors.Is(err, partstore.ErrReservation) && !errors.Is(err, partstore.ErrNotFound):
		return reasonMismatch
	}
	return reasonInvalid
}

// connErr maps a connection error: ctx ending is reported as such, the worker's clean close as
// nil, anything else as it is.
func (s *Source) connErr(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// reject sends a reject with reason; the caller closes the connection. A failed send changes
// nothing: the connection is ending either way.
func (s *Source) reject(conn *net.UnixConn, reason string) {
	_ = ipc.WritePublicationMsg(conn, rejectPacket(reason), nil, time.Now().Add(netDeadline))
}

func (s *Source) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Source) callHook(point string) {
	if s.hook != nil {
		s.hook(point)
	}
}

func rejectPacket(reason string) ipc.PublicationPacket {
	return ipc.PublicationPacket{Version: ipc.PublicationVersion, Type: ipc.TypeReject, Reason: reason}
}

func grantPacket(g ipc.Grant) ipc.PublicationPacket {
	return ipc.PublicationPacket{Version: ipc.PublicationVersion, Type: ipc.TypeGrant, Grant: &g}
}

func ackPacket(seq uint64) ipc.PublicationPacket {
	return ipc.PublicationPacket{Version: ipc.PublicationVersion, Type: ipc.TypeAck, Ack: &ipc.Ack{Seq: seq}}
}
