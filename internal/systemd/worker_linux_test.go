//go:build linux

package systemd

// The launcher against fake systemd-run and systemctl executables: what it refuses before
// running anything, the exact argv it runs, how a start failure is reconciled by the launch id
// systemd records, how foreign, malformed and flickering unit states are only observed, how
// stops, waits and adoptions decide ownership, and how a handle never established settles
// absence and is established by a reading. None of this proves the real systemd boundary.
//
// The fake systemd-run records its argv and, when run.mirror exists, writes a show.out of a
// loaded, running unit carrying the Description it was given, as systemd would report it. The
// fake systemctl appends every argv to ctl.args, answers show from show.out (or the next line of
// show.seq, one reading per call, when that exists), stop from stop.exit and stop.stderr, and
// after a stop or reset replaces show.out with show.after when that exists.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	fakeRun = `#!/bin/sh
printf '%s\n' "$@" > "$CE_FAKE_DIR/run.args"
if [ -f "$CE_FAKE_DIR/run.mirror" ]; then
  for a in "$@"; do case "$a" in --description=*)
    printf 'LoadState=loaded\nActiveState=active\nSubState=running\nResult=success\nExecMainStatus=0\nExecMainCode=0\nMainPID=4242\nDescription=%s\n' "${a#--description=}" > "$CE_FAKE_DIR/show.out";;
  esac; done
fi
[ -f "$CE_FAKE_DIR/run.stderr" ] && cat "$CE_FAKE_DIR/run.stderr" >&2
exit $(cat "$CE_FAKE_DIR/run.exit" 2>/dev/null || echo 0)
`
	fakeCtl = `#!/bin/sh
printf '%s\n' "$@" >> "$CE_FAKE_DIR/ctl.args"
printf -- '--\n' >> "$CE_FAKE_DIR/ctl.args"
case "$1" in
show)
  if [ -f "$CE_FAKE_DIR/show.seq" ]; then
    line=$(head -n 1 "$CE_FAKE_DIR/show.seq")
    if [ -n "$line" ]; then
      tail -n +2 "$CE_FAKE_DIR/show.seq" > "$CE_FAKE_DIR/show.seq.tmp" && mv "$CE_FAKE_DIR/show.seq.tmp" "$CE_FAKE_DIR/show.seq"
      [ "$line" = "FAIL" ] && { echo "Failed to connect to bus" >&2; exit 1; }
      printf '%s\n' "$line" | tr ';' '\n'
      exit 0
    fi
  fi
  [ -f "$CE_FAKE_DIR/show.out" ] && cat "$CE_FAKE_DIR/show.out"
  exit 0;;
stop)
  [ -f "$CE_FAKE_DIR/stop.stderr" ] && cat "$CE_FAKE_DIR/stop.stderr" >&2
  rc=$(cat "$CE_FAKE_DIR/stop.exit" 2>/dev/null || echo 0)
  [ "$rc" = 0 ] && [ -f "$CE_FAKE_DIR/show.after" ] && cp "$CE_FAKE_DIR/show.after" "$CE_FAKE_DIR/show.out"
  exit $rc;;
reset-failed)
  rc=$(cat "$CE_FAKE_DIR/reset.exit" 2>/dev/null || echo 0)
  [ "$rc" = 0 ] && [ -f "$CE_FAKE_DIR/show.after" ] && cp "$CE_FAKE_DIR/show.after" "$CE_FAKE_DIR/show.out"
  [ "$rc" = 0 ] || echo "Failed to reset failed state: Access denied" >&2
  exit $rc;;
esac
exit 1
`
	notFound    = "LoadState=not-found\nActiveState=inactive\nSubState=dead\nResult=success\nExecMainStatus=0\nExecMainCode=0\nMainPID=0\nDescription=none\n"
	testLaunch  = "0123456789abcdef0123456789abcdef"
	otherLaunch = "ffffffffffffffffffffffffffffffff"
	testUnit    = "connects-egress-worker-r-" + testLaunch + ".service"
)

// launcherRig holds fake executables in dir, which the fakes find through CE_FAKE_DIR.
type launcherRig struct {
	dir  string
	opts LauncherOptions
}

func newLauncherRig(t *testing.T) *launcherRig {
	t.Helper()
	dir := t.TempDir()
	write := func(name, content string, mode os.FileMode) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), mode); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return p
	}
	r := &launcherRig{dir: dir}
	r.opts = LauncherOptions{
		SystemdRun:      write("systemd-run", fakeRun, 0o755),
		Systemctl:       write("systemctl", fakeCtl, 0o755),
		Executable:      write("connects-egress", "#!/bin/sh\nexit 0\n", 0o755),
		EnvironmentFile: write("worker.env", "LIVEKIT_API_KEY=k\nLIVEKIT_API_SECRET=s\n", 0o600),
		UID:             os.Geteuid(),
		PollInterval:    20 * time.Millisecond,
		StopTimeout:     time.Second,
	}
	t.Setenv("CE_FAKE_DIR", dir)
	return r
}

func (r *launcherRig) set(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(r.dir, name), []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func (r *launcherRig) read(t *testing.T, name string) (string, bool) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(r.dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return "", false
	}
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b), true
}

func (r *launcherRig) launcher(t *testing.T) *Launcher {
	t.Helper()
	l, err := NewLauncher(r.opts)
	if err != nil {
		t.Fatalf("NewLauncher: %v", err)
	}
	return l
}

// loaded renders a show.out of a loaded unit in state active with description desc.
func loaded(active, desc string, mainPID int) string {
	return "LoadState=loaded\nActiveState=" + active + "\nSubState=x\nResult=success\nExecMainStatus=0\nExecMainCode=0\nMainPID=" +
		strconv.Itoa(mainPID) + "\nDescription=" + desc + "\n"
}

// ctlCalls counts systemctl invocations of verb.
func (r *launcherRig) ctlCalls(t *testing.T, verb string) int {
	t.Helper()
	got, _ := r.read(t, "ctl.args")
	return strings.Count(got, "\n--\n"+verb+"\n") + boolInt(strings.HasPrefix(got, verb+"\n"))
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// handle returns a handle of l's unit for name and launch, established when owned.
func handle(l *Launcher, name, launch string, owned bool) *Unit {
	u := &Unit{l: l, name: l.UnitName(name, launch), launch: launch}
	u.owned.Store(owned)
	return u
}

// seq renders show.seq lines: one reading per line, with its newlines folded to ';'.
func seq(readings ...string) string {
	var b strings.Builder
	for _, r := range readings {
		b.WriteString(strings.ReplaceAll(r, "\n", ";") + "\n")
	}
	return b.String()
}

func TestLaunchID(t *testing.T) {
	a, err := NewLaunchID()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewLaunchID()
	if a == b || CheckLaunchID(a) != nil || CheckLaunchID(b) != nil {
		t.Fatalf("launch ids %q %q", a, b)
	}
	for _, id := range []string{"", "abc", strings.Repeat("g", 32), strings.ToUpper(testLaunch), testLaunch + "0"} {
		if err := CheckLaunchID(id); !errors.Is(err, ErrInvalidLauncher) {
			t.Errorf("CheckLaunchID(%q) = %v", id, err)
		}
	}
}

func TestLauncherOptionsRejected(t *testing.T) {
	r := newLauncherRig(t)
	good := r.opts
	for _, c := range []struct {
		name string
		mod  func(*LauncherOptions)
	}{
		{"relative systemd-run", func(o *LauncherOptions) { o.SystemdRun = "systemd-run" }},
		{"missing systemctl", func(o *LauncherOptions) { o.Systemctl = filepath.Join(r.dir, "nope") }},
		{"relative executable", func(o *LauncherOptions) { o.Executable = "connects-egress" }},
		{"executable without execute bit", func(o *LauncherOptions) { o.Executable = o.EnvironmentFile }},
		{"executable is a directory", func(o *LauncherOptions) { o.Executable = r.dir }},
		{"relative environment file", func(o *LauncherOptions) { o.EnvironmentFile = "worker.env" }},
		{"missing environment file", func(o *LauncherOptions) { o.EnvironmentFile = filepath.Join(r.dir, "none.env") }},
		{"environment file is a directory", func(o *LauncherOptions) { o.EnvironmentFile = r.dir }},
		{"negative uid", func(o *LauncherOptions) { o.UID = -1 }},
		{"prefix with a slash", func(o *LauncherOptions) { o.UnitPrefix = "a/b-" }},
		{"prefix starting with a dot", func(o *LauncherOptions) { o.UnitPrefix = ".a-" }},
		{"slice without suffix", func(o *LauncherOptions) { o.Slice = "media" }},
		{"slice with a space", func(o *LauncherOptions) { o.Slice = "me dia.slice" }},
		{"stop timeout too short", func(o *LauncherOptions) { o.StopTimeout = 500 * time.Millisecond }},
		{"poll interval too short", func(o *LauncherOptions) { o.PollInterval = time.Millisecond }},
	} {
		o := good
		c.mod(&o)
		if _, err := NewLauncher(o); !errors.Is(err, ErrInvalidLauncher) {
			t.Errorf("%s: NewLauncher = %v, want ErrInvalidLauncher", c.name, err)
		}
	}
	r.opts.StopTimeout = 0
	l := r.launcher(t)
	if l.o.UnitPrefix != DefaultUnitPrefix || l.o.StopTimeout != DefaultStopTimeout {
		t.Fatalf("defaults not applied: %+v", l.o)
	}
	if got := l.UnitName("r1", testLaunch); got != "connects-egress-worker-r1-"+testLaunch+".service" {
		t.Fatalf("UnitName = %q", got)
	}
}

func TestLauncherStartArgv(t *testing.T) {
	r := newLauncherRig(t)
	r.opts.Slice = "connects-media.slice"
	l := r.launcher(t)
	args := []string{"worker", "--run=run-1", "--url=ws://127.0.0.1:7880", "--room=room a", "--room-id=room_a", "--host=h"}
	u, err := l.Start(context.Background(), "run-1", testLaunch, args)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if u.Name() != "connects-egress-worker-run-1-"+testLaunch+".service" || u.LaunchID() != testLaunch || !u.owned.Load() {
		t.Fatalf("unit %+v", u)
	}
	got, ok := r.read(t, "run.args")
	if !ok {
		t.Fatalf("systemd-run was not run")
	}
	want := strings.Join([]string{"--unit=connects-egress-worker-run-1-" + testLaunch + ".service", "--quiet", "--description=" + descriptionPrefix + testLaunch,
		"--property=Type=exec", "--property=User=" + strconv.Itoa(os.Geteuid()), "--property=EnvironmentFile=" + r.opts.EnvironmentFile,
		"--property=TimeoutStopSec=1", "--slice=connects-media.slice", "--", r.opts.Executable,
		"worker", "--run=run-1", "--url=ws://127.0.0.1:7880", "--room=room a", "--room-id=room_a", "--host=h", ""}, "\n")
	if got != want {
		t.Fatalf("systemd-run argv:\n%s\nwant:\n%s", got, want)
	}
	if strings.Contains(got, "LIVEKIT") || strings.Contains(got, "=k\n") {
		t.Fatalf("argv carries credentials:\n%s", got)
	}
	if n := r.ctlCalls(t, "show"); n != 0 {
		t.Fatalf("a successful start queried systemctl %d times", n)
	}
}

func TestLauncherStartRefusedBeforeExec(t *testing.T) {
	r := newLauncherRig(t)
	l := r.launcher(t)
	for _, name := range []string{"", "a/b", ".x", "-x", "_x", "a b", "a.b", strings.Repeat("a", 65), "ü"} {
		if _, err := l.Start(context.Background(), name, testLaunch, []string{"worker"}); !errors.Is(err, ErrInvalidLauncher) {
			t.Errorf("name %q: Start = %v, want ErrInvalidLauncher", name, err)
		}
	}
	if _, err := l.Start(context.Background(), "run", "nope", []string{"worker"}); !errors.Is(err, ErrInvalidLauncher) {
		t.Errorf("bad launch id: Start = %v", err)
	}
	for _, arg := range []string{"a\nb", "a\x00b", "a\tb", "\xff"} {
		if _, err := l.Start(context.Background(), "run", testLaunch, []string{"worker", arg}); !errors.Is(err, ErrInvalidLauncher) {
			t.Errorf("arg %q: Start = %v, want ErrInvalidLauncher", arg, err)
		}
	}
	if _, ok := r.read(t, "run.args"); ok {
		t.Fatalf("systemd-run ran for a refused start")
	}
	if _, ok := r.read(t, "ctl.args"); ok {
		t.Fatalf("systemctl ran for a refused start")
	}
	if err := CheckUnitBase(strings.Repeat("a", 64)); err != nil {
		t.Fatalf("64-byte name refused: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.Start(ctx, "r2", testLaunch, []string{"worker"}); !errors.Is(err, context.Canceled) || !errors.Is(err, ErrLaunch) {
		t.Fatalf("Start with a cancelled context = %v", err)
	}
	if _, ok := r.read(t, "run.args"); ok {
		t.Fatalf("systemd-run ran with a cancelled context")
	}
}

// TestLauncherStartFailure covers a failed systemd-run against every unit state the
// reconciliation can meet.
func TestLauncherStartFailure(t *testing.T) {
	t.Run("own unit stopped until absent", func(t *testing.T) {
		r := newLauncherRig(t)
		l := r.launcher(t)
		r.set(t, "run.exit", "1\n")
		r.set(t, "run.stderr", "Failed to start transient service unit: Connection timed out\n")
		r.set(t, "run.mirror", "")
		r.set(t, "show.after", notFound)
		u, err := l.Start(context.Background(), "r", testLaunch, []string{"worker"})
		switch {
		case !errors.Is(err, ErrLaunch) || errors.Is(err, ErrLaunchUncertain):
			t.Fatalf("Start = %v, want a definite ErrLaunch", err)
		case u != nil:
			t.Fatalf("a definite failure returned a unit")
		case !strings.Contains(err.Error(), "Connection timed out") || !strings.Contains(err.Error(), testUnit):
			t.Fatalf("Start error lacks the reason: %v", err)
		}
		if n := r.ctlCalls(t, "stop"); n != 1 {
			t.Fatalf("own unit stopped %d times, want 1", n)
		}
		got, _ := r.read(t, "ctl.args")
		if !strings.Contains(got, "stop\n--no-block\n--\n"+testUnit+"\n") {
			t.Fatalf("stop argv:\n%s", got)
		}
	})
	t.Run("own failed unit reset", func(t *testing.T) {
		r := newLauncherRig(t)
		l := r.launcher(t)
		r.set(t, "run.exit", "1\n")
		r.set(t, "run.stderr", "Job for connects-egress-worker-r.service failed\n")
		r.set(t, "show.out", loaded("failed", descriptionPrefix+testLaunch, 0))
		r.set(t, "show.after", notFound)
		u, err := l.Start(context.Background(), "r", testLaunch, []string{"worker"})
		if !errors.Is(err, ErrLaunch) || errors.Is(err, ErrLaunchUncertain) || u != nil {
			t.Fatalf("Start = %v, unit %v", err, u)
		}
		if n := r.ctlCalls(t, "reset-failed"); n != 1 {
			t.Fatalf("failed unit reset %d times, want 1", n)
		}
		if n := r.ctlCalls(t, "stop"); n != 0 {
			t.Fatalf("failed unit stopped %d times", n)
		}
	})
	t.Run("reset-failed failure is uncertain", func(t *testing.T) {
		r := newLauncherRig(t)
		l := r.launcher(t)
		r.set(t, "run.exit", "1\n")
		r.set(t, "show.out", loaded("failed", descriptionPrefix+testLaunch, 0))
		r.set(t, "show.after", notFound)
		r.set(t, "reset.exit", "1\n")
		u, err := l.Start(context.Background(), "r", testLaunch, []string{"worker"})
		if !errors.Is(err, ErrLaunchUncertain) || u == nil || !strings.Contains(err.Error(), "reset-failed") || !strings.Contains(err.Error(), "Access denied") {
			t.Fatalf("Start = %v, unit %v", err, u)
		}
		if n := r.ctlCalls(t, "reset-failed"); n != 1 {
			t.Fatalf("reset issued %d times, want 1", n)
		}
	})
	t.Run("reset-failed that leaves the unit loaded is uncertain", func(t *testing.T) {
		r := newLauncherRig(t)
		l := r.launcher(t)
		r.set(t, "run.exit", "1\n")
		r.set(t, "show.out", loaded("failed", descriptionPrefix+testLaunch, 0)) // no show.after: the reset changes nothing
		u, err := l.Start(context.Background(), "r", testLaunch, []string{"worker"})
		if !errors.Is(err, ErrLaunchUncertain) || u == nil || !strings.Contains(err.Error(), "still loaded/failed after reset-failed") {
			t.Fatalf("Start = %v, unit %v", err, u)
		}
	})
	t.Run("foreign unit left untouched", func(t *testing.T) {
		r := newLauncherRig(t)
		l := r.launcher(t)
		r.set(t, "run.exit", "1\n")
		r.set(t, "run.stderr", "Failed to start transient service unit: Unit connects-egress-worker-r.service was already loaded or has a fragment file.\n")
		r.set(t, "show.out", loaded("active", descriptionPrefix+otherLaunch, 77))
		u, err := l.Start(context.Background(), "r", testLaunch, []string{"worker"})
		switch {
		case !errors.Is(err, ErrLaunchUncertain) || !errors.Is(err, ErrLaunch):
			t.Fatalf("Start = %v, want ErrLaunchUncertain", err)
		case u == nil || u.Name() != testUnit || u.owned.Load():
			t.Fatalf("uncertain start returned unit %+v", u)
		case !strings.Contains(err.Error(), "already loaded") || !strings.Contains(err.Error(), "another launch"):
			t.Fatalf("Start error lacks the reasons: %v", err)
		}
		if n := r.ctlCalls(t, "stop") + r.ctlCalls(t, "reset-failed"); n != 0 {
			t.Fatalf("a foreign unit was acted on %d times", n)
		}
	})
	t.Run("unreadable state is uncertain", func(t *testing.T) {
		for name, out := range map[string]string{"empty": "", "no load state": "ActiveState=active\nDescription=" + descriptionPrefix + testLaunch + "\n",
			"other load state": "LoadState=error\nActiveState=inactive\n"} {
			r := newLauncherRig(t)
			l := r.launcher(t)
			r.set(t, "run.exit", "1\n")
			r.set(t, "show.out", out)
			u, err := l.Start(context.Background(), "r", testLaunch, []string{"worker"})
			if !errors.Is(err, ErrLaunchUncertain) || u == nil || u.owned.Load() {
				t.Errorf("%s: Start = %v, unit %+v, want ErrLaunchUncertain with an unowned unit", name, err, u)
			}
			if n := r.ctlCalls(t, "stop") + r.ctlCalls(t, "reset-failed"); n != 0 {
				t.Errorf("%s: acted on %d times", name, n)
			}
		}
	})
	t.Run("systemctl failure is uncertain", func(t *testing.T) {
		r := newLauncherRig(t)
		l := r.launcher(t)
		r.set(t, "run.exit", "1\n")
		r.set(t, "show.seq", "FAIL\n")
		u, err := l.Start(context.Background(), "r", testLaunch, []string{"worker"})
		if !errors.Is(err, ErrLaunchUncertain) || u == nil || !strings.Contains(err.Error(), "cannot be asked") {
			t.Fatalf("Start = %v, unit %v", err, u)
		}
	})
	t.Run("one absent reading is not absence", func(t *testing.T) {
		r := newLauncherRig(t)
		l := r.launcher(t)
		r.set(t, "run.exit", "1\n")
		r.set(t, "show.seq", strings.ReplaceAll(notFound, "\n", ";")+"\n") // one absent reading, then the unit appears
		r.set(t, "show.out", loaded("active", descriptionPrefix+testLaunch, 5))
		r.set(t, "show.after", notFound)
		u, err := l.Start(context.Background(), "r", testLaunch, []string{"worker"})
		if !errors.Is(err, ErrLaunch) || errors.Is(err, ErrLaunchUncertain) || u != nil {
			t.Fatalf("Start = %v, unit %v", err, u)
		}
		if n := r.ctlCalls(t, "stop"); n != 1 {
			t.Fatalf("the unit that appeared after an absent reading was stopped %d times, want 1", n)
		}
		if n := r.ctlCalls(t, "show"); n < reconcileSettle+2 {
			t.Fatalf("%d readings, want the appearance and then %d consecutive absences", n, reconcileSettle)
		}
	})
	t.Run("own unit outlasting the budget is uncertain", func(t *testing.T) {
		r := newLauncherRig(t)
		l := r.launcher(t)
		r.set(t, "run.exit", "1\n")
		r.set(t, "show.out", loaded("active", descriptionPrefix+testLaunch, 5))
		start := time.Now()
		u, err := l.Start(context.Background(), "r", testLaunch, []string{"worker"})
		if !errors.Is(err, ErrLaunchUncertain) || u == nil || !strings.Contains(err.Error(), "still loaded") {
			t.Fatalf("Start = %v, unit %v", err, u)
		}
		if el := time.Since(start); el < l.o.StopTimeout+reconcileGrace || el > l.o.StopTimeout+reconcileGrace+2*time.Second {
			t.Fatalf("reconciliation took %v, want about the budget %v", el, l.o.StopTimeout+reconcileGrace)
		}
	})
}

func TestUnitOwnsAndStop(t *testing.T) {
	r := newLauncherRig(t)
	l := r.launcher(t)
	u := handle(l, "r", testLaunch, true)
	r.set(t, "show.out", loaded("active", descriptionPrefix+testLaunch, 4242))
	for pid, want := range map[int]bool{4242: true, 4243: false, 0: false, -1: false} {
		if got, err := u.Owns(context.Background(), pid); err != nil || got != want {
			t.Errorf("Owns(%d) = %v, %v, want %v", pid, got, err, want)
		}
	}
	r.set(t, "show.out", loaded("active", descriptionPrefix+otherLaunch, 4242))
	if got, err := u.Owns(context.Background(), 4242); err != nil || got {
		t.Fatalf("Owns of a foreign unit = %v, %v", got, err)
	}
	if err := u.Stop(context.Background()); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("Stop of a foreign unit = %v, want ErrNotOwned", err)
	}
	r.set(t, "show.out", "LoadState=\n")
	if _, err := u.Owns(context.Background(), 4242); !errors.Is(err, ErrUnitControl) {
		t.Fatalf("Owns with an unreadable state = %v, want ErrUnitControl", err)
	}
	if err := u.Stop(context.Background()); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("Stop with an unreadable state = %v, want ErrNotOwned", err)
	}
	if n := r.ctlCalls(t, "stop"); n != 0 {
		t.Fatalf("stop was issued %d times without ownership", n)
	}

	r.set(t, "show.out", loaded("active", descriptionPrefix+testLaunch, 4242))
	if err := u.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	got, _ := r.read(t, "ctl.args")
	if !strings.HasSuffix(got, "stop\n--no-block\n--\n"+testUnit+"\n--\n") {
		t.Fatalf("systemctl argv:\n%s", got)
	}
	r.set(t, "show.out", notFound)
	if err := u.Stop(context.Background()); err != nil {
		t.Fatalf("Stop of an absent unit: %v", err)
	}
	r.set(t, "show.out", loaded("active", descriptionPrefix+testLaunch, 4242))
	r.set(t, "stop.exit", "5\n")
	r.set(t, "stop.stderr", "Failed to stop "+testUnit+": Unit "+testUnit+" not loaded.\n")
	if err := u.Stop(context.Background()); err != nil {
		t.Fatalf("Stop racing the unit's exit: %v", err)
	}
	r.set(t, "stop.exit", "1\n")
	r.set(t, "stop.stderr", "Failed to stop "+testUnit+": Access denied\n")
	if err := u.Stop(context.Background()); !errors.Is(err, ErrUnitControl) || !strings.Contains(err.Error(), "Access denied") {
		t.Fatalf("Stop refused = %v, want ErrUnitControl with the reason", err)
	}
}

func TestUnitWait(t *testing.T) {
	r := newLauncherRig(t)
	l := r.launcher(t)
	u := handle(l, "r", testLaunch, true)
	active := loaded("active", descriptionPrefix+testLaunch, 5)
	failed := "LoadState=loaded\nActiveState=failed\nSubState=failed\nResult=exit-code\nExecMainStatus=3\nExecMainCode=exited\nMainPID=0\nDescription=" + descriptionPrefix + testLaunch + "\n"
	r.set(t, "show.out", active)
	r.set(t, "show.after", notFound)
	go func() {
		time.Sleep(100 * time.Millisecond)
		r.set(t, "show.out", failed)
	}()
	start := time.Now()
	e, err := u.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if want := (Exit{Known: true, Result: "exit-code", Code: "exited", Status: 3}); e != want || time.Since(start) < 100*time.Millisecond {
		t.Fatalf("Wait = %+v after %v, want %+v after the poll that saw the failure", e, time.Since(start), want)
	}
	if e.String() != "result exit-code, exited 3" {
		t.Fatalf("Exit.String = %q", e.String())
	}
	if n := r.ctlCalls(t, "reset-failed"); n != 1 {
		t.Fatalf("a failed unit was reset %d times, want 1", n)
	}
	// A reset that fails, or that leaves the unit loaded, is not an exit: the caller retries.
	r.set(t, "show.out", failed)
	r.set(t, "reset.exit", "1\n")
	if _, err := u.Wait(context.Background()); !errors.Is(err, ErrUnitControl) || !strings.Contains(err.Error(), "reset-failed") {
		t.Fatalf("Wait with a failing reset = %v, want ErrUnitControl", err)
	}
	r.set(t, "reset.exit", "0\n")
	if err := os.Remove(filepath.Join(r.dir, "show.after")); err != nil {
		t.Fatal(err)
	}
	if _, err := u.Wait(context.Background()); !errors.Is(err, ErrUnitControl) || !strings.Contains(err.Error(), "after reset-failed") {
		t.Fatalf("Wait with a reset that changed nothing = %v, want ErrUnitControl", err)
	}
	r.set(t, "show.after", notFound)
	if e, err := u.Wait(context.Background()); err != nil || !e.Known || e.Status != 3 {
		t.Fatalf("Wait after the reset succeeds = %+v, %v", e, err)
	}

	r.set(t, "show.out", notFound)
	if e, err := u.Wait(context.Background()); err != nil || e.Known || e.String() != "unit gone" {
		t.Fatalf("Wait of an unloaded unit = %+v, %v", e, err)
	}

	// The name taken by another launch: ours is gone, the other is not touched.
	r.set(t, "show.out", "LoadState=loaded\nActiveState=failed\nSubState=failed\nResult=exit-code\nExecMainStatus=1\nExecMainCode=exited\nMainPID=0\nDescription="+descriptionPrefix+otherLaunch+"\n")
	if e, err := u.Wait(context.Background()); err != nil || !e.Replaced || e.String() != "unit name taken by another launch" {
		t.Fatalf("Wait with the name taken = %+v, %v", e, err)
	}
	if n := r.ctlCalls(t, "reset-failed"); n != 4 {
		t.Fatalf("a foreign failed unit was reset (%d resets, want the 4 of this launch's)", n)
	}
	// A handle never established waits for the name to be free.
	watcher := handle(l, "r", testLaunch, false)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	if _, err := watcher.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unestablished handle returned on a foreign unit: %v", err)
	}
	cancel()
	r.set(t, "show.out", notFound)
	if e, err := watcher.Wait(context.Background()); err != nil || e.Known || watcher.owned.Load() {
		t.Fatalf("unestablished handle with the name free = %+v, %v, owned %v", e, err, watcher.owned.Load())
	}

	r.set(t, "show.out", "LoadState=\n")
	if _, err := u.Wait(context.Background()); !errors.Is(err, ErrUnitControl) {
		t.Fatalf("Wait with an unreadable state = %v, want ErrUnitControl", err)
	}
	r.set(t, "show.out", active)
	ctx, cancel = context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := u.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait past the context = %v", err)
	}
}

// TestUnitWaitUnestablished: a handle never established takes the name as free only after
// reconcileSettle consecutive absent readings, any loaded or failed reading starting the count
// over, is established by a reading that finds the unit carrying its launch id, and then
// reports the unit's end as an established handle does.
func TestUnitWaitUnestablished(t *testing.T) {
	r := newLauncherRig(t)
	l := r.launcher(t)
	ours := loaded("active", descriptionPrefix+testLaunch, 5)
	failed := "LoadState=loaded\nActiveState=failed\nSubState=failed\nResult=exit-code\nExecMainStatus=3\nExecMainCode=exited\nMainPID=0\nDescription=" + descriptionPrefix + testLaunch + "\n"

	// One absent reading, then a unit of another launch appears: not gone, only watched.
	w := handle(l, "r", testLaunch, false)
	r.set(t, "show.seq", seq(notFound))
	r.set(t, "show.out", loaded("active", descriptionPrefix+otherLaunch, 7))
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err := w.Wait(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || w.owned.Load() {
		t.Fatalf("Wait after one absent reading and a foreign unit = %v, owned %v", err, w.owned.Load())
	}
	if n := r.ctlCalls(t, "show"); n < 3 {
		t.Fatalf("%d readings, want the absence and then the foreign unit watched", n)
	}
	if n := r.ctlCalls(t, "stop") + r.ctlCalls(t, "reset-failed"); n != 0 {
		t.Fatalf("the foreign unit was acted on %d times", n)
	}

	// Two absent readings, then the unit appears as this launch's: the handle is established
	// and its end, a failure, is reset and reported.
	r.set(t, "show.seq", seq(notFound, notFound))
	r.set(t, "show.out", ours)
	r.set(t, "show.after", notFound)
	go func() {
		time.Sleep(100 * time.Millisecond)
		r.set(t, "show.out", failed)
	}()
	e, err := w.Wait(context.Background())
	if err != nil || !e.Known || e.Status != 3 || !w.owned.Load() {
		t.Fatalf("Wait of the unit appearing after two absences = %+v, %v, owned %v", e, err, w.owned.Load())
	}
	if n := r.ctlCalls(t, "reset-failed"); n != 1 {
		t.Fatalf("the established unit's failure was reset %d times, want 1", n)
	}
	// Established: the name taken by another launch is this unit gone.
	r.set(t, "show.out", loaded("active", descriptionPrefix+otherLaunch, 8))
	if e, err := w.Wait(context.Background()); err != nil || !e.Replaced {
		t.Fatalf("Wait of the established handle on the name taken = %+v, %v", e, err)
	}

	// Three consecutive absent readings are absence, after exactly reconcileSettle readings.
	w2 := handle(l, "r", testLaunch, false)
	r.set(t, "show.out", notFound)
	before := r.ctlCalls(t, "show")
	if e, err := w2.Wait(context.Background()); err != nil || e.Known || w2.owned.Load() {
		t.Fatalf("Wait of a settled absence = %+v, %v, owned %v", e, err, w2.owned.Load())
	}
	if n := r.ctlCalls(t, "show") - before; n != reconcileSettle {
		t.Fatalf("absence settled after %d readings, want %d", n, reconcileSettle)
	}

	// A reading that fails is reported, and the next Wait starts the count over.
	r.set(t, "show.seq", seq(notFound, notFound, "FAIL"))
	if _, err := w2.Wait(context.Background()); !errors.Is(err, ErrUnitControl) {
		t.Fatalf("Wait with a failed reading after two absences = %v, want ErrUnitControl", err)
	}
	before = r.ctlCalls(t, "show")
	if e, err := w2.Wait(context.Background()); err != nil || e.Known {
		t.Fatalf("Wait after the failed reading = %+v, %v", e, err)
	}
	if n := r.ctlCalls(t, "show") - before; n != reconcileSettle {
		t.Fatalf("absence after a failed reading settled after %d readings, want %d", n, reconcileSettle)
	}
	if n := r.ctlCalls(t, "stop"); n != 0 {
		t.Fatalf("Wait stopped a unit %d times", n)
	}
}

// TestUnitStopUnestablished: a handle never established stops only a unit a reading proves is
// this launch's, which establishes it; a foreign unit is refused; the name is taken as free
// only after reconcileSettle consecutive absent readings.
func TestUnitStopUnestablished(t *testing.T) {
	r := newLauncherRig(t)
	l := r.launcher(t)
	w := handle(l, "r", testLaunch, false)
	r.set(t, "show.out", loaded("active", descriptionPrefix+otherLaunch, 7))
	if err := w.Stop(context.Background()); !errors.Is(err, ErrNotOwned) || w.owned.Load() {
		t.Fatalf("Stop of a foreign unit = %v, owned %v", err, w.owned.Load())
	}
	// Two absent readings, then the unit appears as this launch's: stopped, and established.
	r.set(t, "show.seq", seq(notFound, notFound))
	r.set(t, "show.out", loaded("active", descriptionPrefix+testLaunch, 5))
	if err := w.Stop(context.Background()); err != nil || !w.owned.Load() {
		t.Fatalf("Stop of the unit appearing after two absences = %v, owned %v", err, w.owned.Load())
	}
	if n := r.ctlCalls(t, "stop"); n != 1 {
		t.Fatalf("stop issued %d times, want 1", n)
	}
	got, _ := r.read(t, "ctl.args")
	if !strings.HasSuffix(got, "stop\n--no-block\n--\n"+testUnit+"\n--\n") {
		t.Fatalf("systemctl argv:\n%s", got)
	}
	// Never established: absence settles after reconcileSettle readings, with no stop issued.
	w2 := handle(l, "r", testLaunch, false)
	r.set(t, "show.out", notFound)
	before := r.ctlCalls(t, "show")
	if err := w2.Stop(context.Background()); err != nil || w2.owned.Load() {
		t.Fatalf("Stop with the name free = %v, owned %v", err, w2.owned.Load())
	}
	if n := r.ctlCalls(t, "show") - before; n != reconcileSettle {
		t.Fatalf("absence settled after %d readings, want %d", n, reconcileSettle)
	}
	if n := r.ctlCalls(t, "stop"); n != 1 {
		t.Fatalf("stop issued %d times in all, want the one of the established unit", n)
	}
	// A reading that fails is reported; a context that ends during the readings is too.
	r.set(t, "show.seq", seq(notFound, "FAIL"))
	if err := w2.Stop(context.Background()); !errors.Is(err, ErrUnitControl) {
		t.Fatalf("Stop with a failed reading = %v, want ErrUnitControl", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	r.set(t, "show.out", notFound)
	if err := w2.Stop(ctx); !errors.Is(err, ErrUnitControl) {
		t.Fatalf("Stop past the context = %v", err)
	}
}

// TestUnitNameIsTheAttempt: two launches of one run never share a unit name, the name is
// derived from the run and launch id alone, and a stop or reset by one handle names only its
// own unit, whatever is loaded under the other's name between the ownership read and the act.
func TestUnitNameIsTheAttempt(t *testing.T) {
	r := newLauncherRig(t)
	l := r.launcher(t)
	a := handle(l, "r", testLaunch, true)
	b := handle(l, "r", otherLaunch, true)
	if a.Name() == b.Name() || a.Name() != testUnit || b.Name() != "connects-egress-worker-r-"+otherLaunch+".service" {
		t.Fatalf("names %q %q", a.Name(), b.Name())
	}
	if l.UnitName("r", testLaunch) != a.Name() {
		t.Fatal("the name is not derivable from run and launch id")
	}
	// Both units are loaded; stopping b names b only. The ownership read and the stop both
	// address b's name, so nothing loaded under a's name can be acted on by b.
	r.set(t, "show.out", loaded("active", descriptionPrefix+otherLaunch, 8))
	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("Stop b: %v", err)
	}
	got, _ := r.read(t, "ctl.args")
	if strings.Contains(got, testUnit) || !strings.Contains(got, "stop\n--no-block\n--\n"+b.Name()+"\n") {
		t.Fatalf("b's stop addressed another launch's unit:\n%s", got)
	}
	// A unit found under a's name that carries b's id is not a's: a refuses to act on it.
	if err := a.Stop(context.Background()); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("a.Stop on b's id = %v, want ErrNotOwned", err)
	}
	r.set(t, "show.out", loaded("failed", descriptionPrefix+otherLaunch, 0))
	if e, err := a.Wait(context.Background()); err != nil || !e.Replaced {
		t.Fatalf("a.Wait on b's id = %+v, %v", e, err)
	}
	if n := r.ctlCalls(t, "reset-failed"); n != 0 {
		t.Fatalf("a reset another launch's failed unit %d times", n)
	}
	if _, err := l.Start(context.Background(), "r", "", []string{"worker"}); !errors.Is(err, ErrInvalidLauncher) {
		t.Fatalf("Start without a launch id = %v", err)
	}
}

func TestLauncherAdopt(t *testing.T) {
	r := newLauncherRig(t)
	l := r.launcher(t)
	r.set(t, "show.out", loaded("active", descriptionPrefix+testLaunch, 9))
	u, present, err := l.Adopt(context.Background(), "r", testLaunch)
	if err != nil || !present || u == nil || !u.owned.Load() || u.LaunchID() != testLaunch || u.Name() != testUnit {
		t.Fatalf("Adopt of own unit = %v, %v, %+v", present, err, u)
	}
	r.set(t, "show.out", loaded("active", descriptionPrefix+otherLaunch, 9))
	u, present, err = l.Adopt(context.Background(), "r", testLaunch)
	if !errors.Is(err, ErrLaunchUncertain) || present || u == nil || u.owned.Load() {
		t.Fatalf("Adopt of a foreign unit = %v, %v, %+v", present, err, u)
	}
	r.set(t, "show.out", notFound)
	before := r.ctlCalls(t, "show")
	u, present, err = l.Adopt(context.Background(), "r", testLaunch)
	if err != nil || present || u == nil {
		t.Fatalf("Adopt of an absent unit = %v, %v, %+v", present, err, u)
	}
	if n := r.ctlCalls(t, "show") - before; n != reconcileSettle {
		t.Fatalf("absence settled after %d readings, want %d", n, reconcileSettle)
	}
	r.set(t, "show.out", "garbage\n")
	if _, _, err := l.Adopt(context.Background(), "r", testLaunch); !errors.Is(err, ErrLaunchUncertain) {
		t.Fatalf("Adopt with an unreadable state = %v", err)
	}
	if _, _, err := l.Adopt(context.Background(), "r", "bad"); !errors.Is(err, ErrInvalidLauncher) {
		t.Fatalf("Adopt with a bad launch id = %v", err)
	}
	if n := r.ctlCalls(t, "stop") + r.ctlCalls(t, "reset-failed"); n != 0 {
		t.Fatalf("Adopt acted on a unit %d times", n)
	}
}
