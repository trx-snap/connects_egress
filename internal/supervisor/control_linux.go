//go:build linux

package supervisor

// Control: the private control API the backend drives runs through, an http.Handler over a Node
// on its own listener, never the origin's. Every request carries the one bearer token, compared
// in constant time and never logged. Routes:
//
//	POST   /control/v1/runs           {run_id, room_id, livekit_room, host_identity, livekit_url}
//	GET    /control/v1/runs/{run_id}
//	DELETE /control/v1/runs/{run_id}
//
// A run is created once per run_id (201) and returned as it is on an exact retry (200); a
// run_id with other parameters or a room with a run that has not ended is 409, and a room
// whose run is over but whose terminal record is not durable yet or whose worker unit is not
// gone yet is waited for, bounded by the request; invalid input is 400 before any side effect;
// a worker that cannot be started is 500 with nothing registered. A start without an
// established outcome is 500 with the run, which is registered and held in starting: every
// retry of it is 500 again until it settles, and GET reports it with uncertain set. GET and
// DELETE of a run still being started wait for the launch to settle. A stop is idempotent
// (200); a stop the unit refuses or systemd cannot take is 500 with the run unchanged, so
// DELETE can be retried, also for a run that is over but still owes its unit a stop; an
// unknown run is 404. Every answer is JSON: run_id, room_id, state, the stable playlist URL,
// the timestamps, a bounded reason, uncertain while held and, with a failure that carries a
// run, the error.

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	controlPrefix  = "/control/v1/runs"
	maxControlBody = 64 << 10
)

// Control is the control API handler. See the file comment.
type Control struct {
	n     *Node
	token [sha256.Size]byte
	base  string
}

// NewControl returns the control API of n requiring token, with stable playlist URLs under
// base, an http or https URL without query or fragment.
func NewControl(n *Node, token, base string) (*Control, error) {
	if n == nil {
		return nil, fmt.Errorf("%w: nil node", ErrInvalidOptions)
	}
	if token == "" {
		return nil, fmt.Errorf("%w: empty control token", ErrInvalidOptions)
	}
	u, err := url.Parse(base)
	switch {
	case err != nil:
		return nil, fmt.Errorf("%w: playlist base: %v", ErrInvalidOptions, err)
	case (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "":
		return nil, fmt.Errorf("%w: playlist base %q is not an http(s) URL without query or fragment", ErrInvalidOptions, base)
	}
	return &Control{n: n, token: sha256.Sum256([]byte(token)), base: strings.TrimRight(base, "/")}, nil
}

// CheckControlAddr requires a host:port with a literal loopback, private or link-local IP and a
// nonzero port: the control API is never exposed on a public or unspecified address.
func CheckControlAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%w: control address: %v", ErrInvalidOptions, err)
	}
	ip := net.ParseIP(host)
	switch p, err := strconv.Atoi(port); {
	case err != nil || p <= 0 || p > 65535:
		return fmt.Errorf("%w: control address %q has no valid port", ErrInvalidOptions, addr)
	case ip == nil:
		return fmt.Errorf("%w: control address %q is not a literal IP address", ErrInvalidOptions, addr)
	case !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast():
		return fmt.Errorf("%w: control address %q is not loopback, private or link-local", ErrInvalidOptions, addr)
	}
	return nil
}

// PlaylistURL returns the stable playlist URL of room.
func (c *Control) PlaylistURL(room string) string {
	return c.base + "/live/" + url.PathEscape(room) + "/master.m3u8"
}

// runRequest is the body of a start.
type runRequest struct {
	RunID        string `json:"run_id"`
	RoomID       string `json:"room_id"`
	LiveKitRoom  string `json:"livekit_room"`
	HostIdentity string `json:"host_identity"`
	LiveKitURL   string `json:"livekit_url"`
}

// runResponse describes a run.
type runResponse struct {
	RunID       string `json:"run_id"`
	RoomID      string `json:"room_id"`
	State       string `json:"state"`
	PlaylistURL string `json:"playlist_url"`
	CreatedAt   string `json:"created_at"`
	StartedAt   string `json:"started_at,omitempty"`
	EndedAt     string `json:"ended_at,omitempty"`
	Reason      string `json:"reason,omitempty"`
	Uncertain   bool   `json:"uncertain,omitempty"`
	Error       string `json:"error,omitempty"`
}

// ServeHTTP implements http.Handler.
func (c *Control) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !c.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="connects-egress"`)
		c.fail(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	path := r.URL.Path
	switch {
	case path == controlPrefix:
		if r.Method != http.MethodPost {
			c.disallow(w, "POST")
			return
		}
		c.start(w, r)
	case strings.HasPrefix(path, controlPrefix+"/"):
		id := strings.TrimPrefix(path, controlPrefix+"/")
		if id == "" || strings.Contains(id, "/") {
			c.fail(w, http.StatusNotFound, "not found")
			return
		}
		switch r.Method {
		case http.MethodGet:
			c.status(w, r, id)
		case http.MethodDelete:
			c.stop(w, r, id)
		default:
			c.disallow(w, "GET, DELETE")
		}
	default:
		c.fail(w, http.StatusNotFound, "not found")
	}
}

// authorized compares the request's bearer token with the configured one in constant time.
func (c *Control) authorized(r *http.Request) bool {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return subtle.ConstantTimeCompare(sum[:], c.token[:]) == 1
}

func (c *Control) start(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxControlBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		c.fail(w, http.StatusBadRequest, "malformed request body: "+boundReason(err.Error()))
		return
	}
	// Exactly one value: the next decode must meet EOF, not a second value and not a stray
	// delimiter, which More would pass over.
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		c.fail(w, http.StatusBadRequest, "malformed request body: trailing data")
		return
	}
	spec := RunSpec{RunID: req.RunID, RoomID: req.RoomID, LiveKitRoom: req.LiveKitRoom, HostIdentity: req.HostIdentity, LiveKitURL: req.LiveKitURL}
	st, created, err := c.n.Start(r.Context(), spec)
	switch {
	case errors.Is(err, ErrInvalidOptions):
		c.fail(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrRunConflict):
		c.fail(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrRunLaunchUncertain):
		c.reply(w, http.StatusInternalServerError, st, boundReason(err.Error())) // the run is held, not rolled back
	case errors.Is(err, ErrRunLaunch):
		c.fail(w, http.StatusInternalServerError, err.Error())
	case errors.Is(err, ErrNodeClosed):
		c.fail(w, http.StatusServiceUnavailable, err.Error())
	case err != nil:
		c.fail(w, http.StatusInternalServerError, boundReason(err.Error()))
	case created:
		c.reply(w, http.StatusCreated, st, "")
	default:
		c.reply(w, http.StatusOK, st, "")
	}
}

func (c *Control) status(w http.ResponseWriter, r *http.Request, id string) {
	st, err := c.n.Status(r.Context(), id)
	switch {
	case errors.Is(err, ErrRunUnknown):
		c.fail(w, http.StatusNotFound, err.Error())
	case err != nil:
		c.fail(w, http.StatusInternalServerError, boundReason(err.Error()))
	default:
		c.reply(w, http.StatusOK, st, "")
	}
}

func (c *Control) stop(w http.ResponseWriter, r *http.Request, id string) {
	st, err := c.n.Stop(r.Context(), id)
	switch {
	case errors.Is(err, ErrRunUnknown):
		c.fail(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrRunStop):
		c.reply(w, http.StatusInternalServerError, st, boundReason(err.Error())) // unchanged, or over and still owing the stop: retry
	case err != nil:
		c.fail(w, http.StatusInternalServerError, boundReason(err.Error()))
	default:
		c.reply(w, http.StatusOK, st, "")
	}
}

// reply writes the run with code; errMsg, when set, is the failure the run is returned with.
func (c *Control) reply(w http.ResponseWriter, code int, st RunStatus, errMsg string) {
	resp := runResponse{RunID: st.Spec.RunID, RoomID: st.Spec.RoomID, State: string(st.State), PlaylistURL: c.PlaylistURL(st.Spec.RoomID),
		CreatedAt: stamp(st.CreatedAt), StartedAt: stamp(st.StartedAt), EndedAt: stamp(st.EndedAt), Reason: st.Reason,
		Uncertain: st.Uncertain, Error: errMsg}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

func (c *Control) fail(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (c *Control) disallow(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	c.fail(w, http.StatusMethodNotAllowed, "method not allowed")
}

// stamp renders t in UTC RFC 3339 with nanoseconds, "" when zero.
func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
