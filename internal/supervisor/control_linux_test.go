//go:build linux

package supervisor

// The control API over a node rig: authentication, validation before any side effect, the
// idempotent start, conflicts, concurrent starts, every state a GET can show, the idempotent
// stop, the launch failure, the held launch, the refused stop and requests during a launch. The
// worker is the fake of node_linux_test.go.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/systemd"
)

const (
	ctlToken = "s3cret-token-value"
	ctlBase  = "https://egress.example"
)

// controlRig is a node rig behind the control API on a test server.
type controlRig struct {
	*nodeRig
	srv *httptest.Server
}

func newControlRig(t *testing.T) *controlRig {
	t.Helper()
	r := &controlRig{nodeRig: newNodeRig(t)}
	c, err := NewControl(r.n, ctlToken, ctlBase+"/")
	if err != nil {
		t.Fatalf("NewControl: %v", err)
	}
	r.srv = httptest.NewServer(c)
	t.Cleanup(r.srv.Close)
	return r
}

// call sends method path with body (JSON when not nil) and token, and returns the status and the
// decoded JSON object.
func (r *controlRig) call(method, path string, body any, token string) (int, map[string]any) {
	r.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		data, err := json.Marshal(b)
		if err != nil {
			r.t.Fatalf("marshal: %v", err)
		}
		rd = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, r.srv.URL+path, rd)
	if err != nil {
		r.t.Fatalf("request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			r.t.Fatalf("%s %s: body %q is not a JSON object", method, path, data)
		}
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		r.t.Fatalf("%s %s: Content-Type %q", method, path, ct)
	}
	return resp.StatusCode, out
}

func (r *controlRig) post(body any) (int, map[string]any) {
	return r.call(http.MethodPost, "/control/v1/runs", body, ctlToken)
}

func (r *controlRig) get(id string) (int, map[string]any) {
	return r.call(http.MethodGet, "/control/v1/runs/"+id, nil, ctlToken)
}

func (r *controlRig) del(id string) (int, map[string]any) {
	return r.call(http.MethodDelete, "/control/v1/runs/"+id, nil, ctlToken)
}

func (r *controlRig) body(spec RunSpec) map[string]string {
	return map[string]string{"run_id": spec.RunID, "room_id": spec.RoomID, "livekit_room": spec.LiveKitRoom,
		"host_identity": spec.HostIdentity, "livekit_url": spec.LiveKitURL}
}

// wantRun checks the run fields of a response.
func (r *controlRig) wantRun(got map[string]any, spec RunSpec, state RunState) {
	r.t.Helper()
	want := map[string]any{"run_id": spec.RunID, "room_id": spec.RoomID, "state": string(state),
		"playlist_url": ctlBase + "/live/" + spec.RoomID + "/master.m3u8"}
	for k, v := range want {
		if got[k] != v {
			r.t.Fatalf("response %v: %s = %v, want %v", got, k, got[k], v)
		}
	}
	if s, _ := got["created_at"].(string); s == "" {
		r.t.Fatalf("response %v lacks created_at", got)
	}
}

func TestControlAuthentication(t *testing.T) {
	r := newControlRig(t)
	spec := r.spec("run-30", "room_a")
	for _, token := range []string{"", "wrong", ctlToken + "x", strings.ToUpper(ctlToken)} {
		code, body := r.call(http.MethodPost, "/control/v1/runs", r.body(spec), token)
		if code != http.StatusUnauthorized || body["error"] != "unauthorized" {
			t.Fatalf("token %q: %d %v", token, code, body)
		}
	}
	req, _ := http.NewRequest(http.MethodGet, r.srv.URL+"/control/v1/runs/run-30", nil)
	req.Header.Set("Authorization", "Basic "+ctlToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || !strings.HasPrefix(resp.Header.Get("WWW-Authenticate"), "Bearer") {
		t.Fatalf("basic scheme: %d %q", resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
	}
	if n := len(r.l.starts()); n != 0 {
		t.Fatalf("%d launches without authentication", n)
	}
	req, _ = http.NewRequest(http.MethodGet, r.srv.URL+"/control/v1/runs/run-30", nil)
	req.Header.Set("Authorization", "bearer "+ctlToken)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("lower-case scheme: %d", resp.StatusCode)
	}
	if code, _ := r.post(r.body(spec)); code != http.StatusCreated {
		t.Fatalf("authenticated start: %d", code)
	}
	if logs := r.logs.String(); strings.Contains(logs, ctlToken) {
		t.Fatalf("the token was logged:\n%s", logs)
	}
	for _, c := range []struct{ token, base string }{{"", ctlBase}, {ctlToken, ""}, {ctlToken, "egress.example"}, {ctlToken, "ftp://x"}, {ctlToken, "http://x/?a=b"}} {
		if _, err := NewControl(r.n, c.token, c.base); !errors.Is(err, ErrInvalidOptions) {
			t.Errorf("NewControl(%q, %q) = %v", c.token, c.base, err)
		}
	}
	if _, err := NewControl(nil, ctlToken, ctlBase); !errors.Is(err, ErrInvalidOptions) {
		t.Errorf("NewControl(nil) = %v", err)
	}
}

func TestControlValidationBeforeSideEffects(t *testing.T) {
	r := newControlRig(t)
	good := r.body(r.spec("run-31", "room_b"))
	for _, c := range []struct {
		name string
		body any
	}{
		{"not json", "run-31"},
		{"array", "[]"},
		{"unknown field", map[string]string{"run_id": "run-31", "room_id": "room_b", "livekit_room": "lk", "host_identity": "h", "livekit_url": nodeTestURL, "token": "x"}},
		{"trailing data", `{"run_id":"run-31","room_id":"room_b","livekit_room":"lk","host_identity":"h","livekit_url":"ws://h"} {}`},
		{"trailing brace", `{"run_id":"run-31","room_id":"room_b","livekit_room":"lk","host_identity":"h","livekit_url":"ws://h"}}`},
		{"trailing bracket", `{"run_id":"run-31","room_id":"room_b","livekit_room":"lk","host_identity":"h","livekit_url":"ws://h"}]`},
		{"trailing text", `{"run_id":"run-31","room_id":"room_b","livekit_room":"lk","host_identity":"h","livekit_url":"ws://h"} x`},
		{"two values", `{"run_id":"run-31","room_id":"room_b","livekit_room":"lk","host_identity":"h","livekit_url":"ws://h"}{"run_id":"run-32"}`},
		{"missing fields", map[string]string{"run_id": "run-31"}},
		{"bad run id", map[string]string{"run_id": "run 31", "room_id": "room_b", "livekit_room": "lk", "host_identity": "h", "livekit_url": nodeTestURL}},
		{"bad url", map[string]string{"run_id": "run-31", "room_id": "room_b", "livekit_room": "lk", "host_identity": "h", "livekit_url": "ftp://h"}},
		{"secret in json", map[string]string{"run_id": "run-31", "room_id": "room_b", "livekit_room": "lk", "host_identity": "h", "livekit_url": nodeTestURL, "livekit_api_secret": "s"}},
	} {
		code, body := r.post(c.body)
		if code != http.StatusBadRequest || body["error"] == "" {
			t.Errorf("%s: %d %v", c.name, code, body)
		}
	}
	if n := len(r.l.starts()); n != 0 {
		t.Fatalf("%d launches for invalid requests", n)
	}
	if code, _ := r.post(strings.Repeat(" ", maxControlBody+1) + "{}"); code != http.StatusBadRequest {
		t.Fatalf("oversized body: %d", code)
	}
	code, body := r.post(good)
	if code != http.StatusCreated {
		t.Fatalf("valid request: %d %v", code, body)
	}
	if code, body := r.post(` {"run_id":"run-32","room_id":"room_c","livekit_room":"lk","host_identity":"h","livekit_url":"ws://h"}  ` + "\n\t"); code != http.StatusCreated {
		t.Fatalf("whitespace around the value: %d %v", code, body)
	}
}

func TestControlStartIdempotentAndConflicting(t *testing.T) {
	r := newControlRig(t)
	spec := r.spec("run-32", "room_c")
	code, body := r.post(r.body(spec))
	if code != http.StatusCreated {
		t.Fatalf("start: %d %v", code, body)
	}
	r.wantRun(body, spec, RunStarting)
	code, again := r.post(r.body(spec))
	if code != http.StatusOK || again["created_at"] != body["created_at"] {
		t.Fatalf("exact retry: %d %v", code, again)
	}
	r.wantRun(again, spec, RunStarting)
	other := r.body(spec)
	other["livekit_url"] = "wss://other.example"
	if code, body := r.post(other); code != http.StatusConflict || !strings.Contains(body["error"].(string), "other parameters") {
		t.Fatalf("reused id: %d %v", code, body)
	}
	if code, body := r.post(r.body(r.spec("run-33", "room_c"))); code != http.StatusConflict || !strings.Contains(body["error"].(string), "room \"room_c\"") {
		t.Fatalf("second run for the room: %d %v", code, body)
	}
	if n := len(r.l.starts()); n != 1 {
		t.Fatalf("%d launches", n)
	}
	// The retry of an ended run returns the ended run, never a new one.
	pub := r.worker("run-32")
	r.publishStart(pub, spec)
	r.waitState("run-32", RunRunning)
	r.publishTerminal(pub, true, "host left")
	r.waitState("run-32", RunEnded)
	code, ended := r.post(r.body(spec))
	if code != http.StatusOK || ended["state"] != "ended" || ended["reason"] != "host left" || ended["ended_at"] == "" {
		t.Fatalf("retry after the end: %d %v", code, ended)
	}
	// The room is free once the worker's unit is gone: a new id then starts a new run.
	_ = pub.Close()
	r.l.unit("run-32").gone(systemd.Exit{})
	if code, body := r.post(r.body(r.spec("run-33", "room_c"))); code != http.StatusCreated {
		t.Fatalf("new run after the end: %d %v", code, body)
	}
}

func TestControlConcurrentSameRoomStarts(t *testing.T) {
	r := newControlRig(t)
	const clients = 8
	var wg sync.WaitGroup
	var created, conflicts atomic.Int32
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			code, _ := r.post(r.body(r.spec(fmt.Sprintf("run-34-%d", i), "room_d")))
			switch code {
			case http.StatusCreated:
				created.Add(1)
			case http.StatusConflict:
				conflicts.Add(1)
			default:
				t.Errorf("client %d: %d", i, code)
			}
		}(i)
	}
	wg.Wait()
	if created.Load() != 1 || conflicts.Load() != clients-1 || len(r.l.starts()) != 1 {
		t.Fatalf("%d created, %d conflicts, %d launches", created.Load(), conflicts.Load(), len(r.l.starts()))
	}
}

func TestControlRunStates(t *testing.T) {
	r := newControlRig(t)
	if code, body := r.get("run-none"); code != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("unknown run: %d %v", code, body)
	}
	spec := r.spec("run-35", "room_e")
	r.post(r.body(spec))
	code, body := r.get("run-35")
	if code != http.StatusOK || body["started_at"] != nil || body["ended_at"] != nil {
		t.Fatalf("starting: %d %v", code, body)
	}
	r.wantRun(body, spec, RunStarting)
	pub := r.worker("run-35")
	r.publishStart(pub, spec)
	r.waitState("run-35", RunRunning)
	if _, body := r.get("run-35"); body["state"] != "running" || body["started_at"] == nil {
		t.Fatalf("running: %v", body)
	}
	r.publishTerminal(pub, true, "host left")
	r.waitState("run-35", RunEnded)
	if _, body := r.get("run-35"); body["state"] != "ended" || body["ended_at"] == nil || body["reason"] != "host left" || body["uncertain"] != nil {
		t.Fatalf("ended: %v", body)
	}

	spec2 := r.spec("run-36", "room_f")
	r.post(r.body(spec2))
	r.l.unit("run-36").gone(systemd.Exit{Known: true, Result: "exit-code", Code: "exited", Status: 2})
	r.waitState("run-36", RunFailed)
	if _, body := r.get("run-36"); body["state"] != "failed" || !strings.Contains(body["reason"].(string), "exit-code") {
		t.Fatalf("failed: %v", body)
	}

	spec3 := r.spec("run-37", "room_g")
	r.post(r.body(spec3))
	pub3 := r.worker("run-37")
	r.publishStart(pub3, spec3)
	r.waitState("run-37", RunRunning)
	r.l.unit("run-37").setStopErr(fmt.Errorf("%w: stop fake-run-37.service: Access denied", systemd.ErrUnitControl))
	if code, body := r.del("run-37"); code != http.StatusInternalServerError || body["state"] != "running" || !strings.Contains(body["error"].(string), "Access denied") {
		t.Fatalf("refused stop: %d %v", code, body)
	}
	r.expect("/live/room_g/master.m3u8", http.StatusOK)
	r.l.unit("run-37").setStopErr(nil)
	if code, body := r.del("run-37"); code != http.StatusOK || body["state"] != "stopping" {
		t.Fatalf("stop: %d %v", code, body)
	}
	if code, body := r.del("run-37"); code != http.StatusOK || body["state"] != "stopping" {
		t.Fatalf("repeated stop: %d %v", code, body)
	}
	r.expect("/live/room_g/master.m3u8", http.StatusServiceUnavailable)
	r.publishTerminal(pub3, true, "stop requested")
	r.waitState("run-37", RunStopped)
	if code, body := r.del("run-37"); code != http.StatusOK || body["state"] != "stopped" || body["reason"] != "stop requested" {
		t.Fatalf("stop after stopped: %d %v", code, body)
	}
	if code, body := r.del("run-35"); code != http.StatusOK || body["state"] != "ended" {
		t.Fatalf("stop after the end: %d %v", code, body)
	}
	if code, _ := r.del("run-none"); code != http.StatusNotFound {
		t.Fatalf("stop of an unknown run: %d", code)
	}
}

func TestControlStartFailure(t *testing.T) {
	r := newControlRig(t)
	r.l.mu.Lock()
	r.l.startErr = errors.New("Failed to start transient service unit: Access denied")
	r.l.mu.Unlock()
	spec := r.spec("run-38", "room_h")
	code, body := r.post(r.body(spec))
	if code != http.StatusInternalServerError || !strings.Contains(body["error"].(string), "Access denied") {
		t.Fatalf("launch failure: %d %v", code, body)
	}
	if code, _ := r.get("run-38"); code != http.StatusNotFound {
		t.Fatalf("after the rollback: %d", code)
	}
	if code, _ := r.del("run-38"); code != http.StatusNotFound {
		t.Fatalf("stop after the rollback: %d", code)
	}
	r.l.mu.Lock()
	r.l.startErr = nil
	r.l.mu.Unlock()
	if code, _ := r.post(r.body(spec)); code != http.StatusCreated {
		t.Fatalf("retry after the rollback: %d", code)
	}
}

func TestControlHeldLaunch(t *testing.T) {
	r := newControlRig(t)
	r.l.mu.Lock()
	r.l.startUncertain = true
	r.l.mu.Unlock()
	spec := r.spec("run-39", "room_i")
	code, body := r.post(r.body(spec))
	if code != http.StatusInternalServerError || body["state"] != "starting" || body["uncertain"] != true || body["error"] == nil {
		t.Fatalf("held start: %d %v", code, body)
	}
	r.wantRun(body, spec, RunStarting)
	r.l.mu.Lock()
	r.l.startUncertain = false
	r.l.mu.Unlock()
	if code, body := r.post(r.body(spec)); code != http.StatusInternalServerError || body["uncertain"] != true {
		t.Fatalf("retry of a held start: %d %v", code, body)
	}
	if code, body := r.get("run-39"); code != http.StatusOK || body["state"] != "starting" || body["uncertain"] != true || body["error"] != nil {
		t.Fatalf("GET of a held run: %d %v", code, body)
	}
	if code, _ := r.post(r.body(r.spec("run-40", "room_i"))); code != http.StatusConflict {
		t.Fatalf("a second run for a held room: %d", code)
	}
	if code, body := r.del("run-39"); code != http.StatusInternalServerError || body["state"] != "starting" {
		t.Fatalf("DELETE of a held run: %d %v", code, body)
	}
	r.l.unit("run-39").gone(systemd.Exit{})
	r.waitState("run-39", RunFailed)
	if code, body := r.get("run-39"); code != http.StatusOK || body["state"] != "failed" || body["uncertain"] != nil {
		t.Fatalf("GET after the held run settled: %d %v", code, body)
	}
	if code, _ := r.post(r.body(r.spec("run-40", "room_i"))); code != http.StatusCreated {
		t.Fatalf("a new run after the held run settled: %d", code)
	}
}

func TestControlStopOfRunOwingItsUnitAStop(t *testing.T) {
	r := newControlRig(t)
	spec := r.spec("run-43", "room_k")
	r.post(r.body(spec))
	pub := r.worker("run-43")
	r.publishStart(pub, spec)
	r.waitState("run-43", RunRunning)
	r.l.unit("run-43").setStopErr(fmt.Errorf("%w: stop fake-run-43.service: Access denied", systemd.ErrUnitControl))
	r.publishTerminal(pub, true, "host left")
	r.waitState("run-43", RunEnded)
	r.waitLog("unit fake-run-43.service: stop not taken")
	// The run is over but owes its unit the stop: DELETE retries it and reports the refusal
	// with the ended run, then the stop taken, then nothing more.
	if code, body := r.del("run-43"); code != http.StatusInternalServerError || body["state"] != "ended" || !strings.Contains(body["error"].(string), "Access denied") {
		t.Fatalf("DELETE while the stop is owed: %d %v", code, body)
	}
	r.l.unit("run-43").setStopErr(nil)
	if code, body := r.del("run-43"); code != http.StatusOK || body["state"] != "ended" || body["error"] != nil {
		t.Fatalf("DELETE retrying the owed stop: %d %v", code, body)
	}
	r.waitStops("run-43", 1)
	if code, body := r.del("run-43"); code != http.StatusOK || body["state"] != "ended" {
		t.Fatalf("DELETE after the stop was taken: %d %v", code, body)
	}
	time.Sleep(20 * time.Millisecond)
	if n := r.l.unit("run-43").stops.Load(); n != 1 {
		t.Fatalf("unit stopped %d times, want 1", n)
	}
	if code, body := r.get("run-43"); code != http.StatusOK || body["state"] != "ended" {
		t.Fatalf("GET: %d %v", code, body)
	}
}

func TestControlRequestsDuringLaunch(t *testing.T) {
	r := newControlRig(t)
	gate := make(chan struct{})
	r.l.mu.Lock()
	r.l.gate = gate
	r.l.mu.Unlock()
	spec := r.spec("run-41", "room_j")
	var wg sync.WaitGroup
	results := make([]int, 3)
	wg.Add(3)
	go func() { defer wg.Done(); results[0], _ = r.post(r.body(spec)) }()
	time.Sleep(50 * time.Millisecond)
	go func() { defer wg.Done(); results[1], _ = r.get("run-41") }()
	go func() { defer wg.Done(); results[2], _ = r.post(r.body(spec)) }()
	time.Sleep(50 * time.Millisecond)
	if code, _ := r.post(r.body(r.spec("run-42", "room_j"))); code != http.StatusConflict {
		t.Fatalf("another run for a room being started: %d", code)
	}
	close(gate)
	wg.Wait()
	if results[0] != http.StatusCreated || results[1] != http.StatusOK || results[2] != http.StatusOK {
		t.Fatalf("during the launch: start %d, GET %d, retry %d", results[0], results[1], results[2])
	}
}

func TestControlRoutes(t *testing.T) {
	r := newControlRig(t)
	for _, c := range []struct {
		method, path string
		code         int
	}{
		{http.MethodGet, "/control/v1/runs", http.StatusMethodNotAllowed},
		{http.MethodPut, "/control/v1/runs/x", http.StatusMethodNotAllowed},
		{http.MethodPost, "/control/v1/runs/x", http.StatusMethodNotAllowed},
		{http.MethodGet, "/control/v1/runs/", http.StatusNotFound},
		{http.MethodGet, "/control/v1/runs/a/b", http.StatusNotFound},
		{http.MethodGet, "/control/v1/other", http.StatusNotFound},
		{http.MethodGet, "/live/room_a/master.m3u8", http.StatusNotFound},
	} {
		if code, _ := r.call(c.method, c.path, nil, ctlToken); code != c.code {
			t.Errorf("%s %s = %d, want %d", c.method, c.path, code, c.code)
		}
	}
}

func TestCheckControlAddr(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8081", "[::1]:8081", "10.0.0.5:9000", "192.168.1.2:1", "172.16.0.1:65535", "[fd00::1]:8081", "[fe80::1]:8081"} {
		if err := CheckControlAddr(addr); err != nil {
			t.Errorf("%s refused: %v", addr, err)
		}
	}
	for _, addr := range []string{"", ":8081", "0.0.0.0:8081", "[::]:8081", "8.8.8.8:8081", "localhost:8081", "127.0.0.1", "127.0.0.1:0", "127.0.0.1:70000", "[2001:db8::1]:80"} {
		if err := CheckControlAddr(addr); !errors.Is(err, ErrInvalidOptions) {
			t.Errorf("%s accepted: %v", addr, err)
		}
	}
}
