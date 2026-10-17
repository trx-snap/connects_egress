//go:build linux

package systemd

// Worker launcher: one transient systemd service per worker, started, stopped and watched
// through the systemd-run and systemctl executables. Both are invoked directly with a fixed
// argv, never through a shell; the unit runs one fixed executable as one fixed uid and takes
// its credentials from an operator-configured EnvironmentFile, so nothing secret passes through
// argv, unit properties, this package's errors or its logs.
//
// Every launch carries an unpredictable launch id that is part of the unit's name, so no two
// launches, of the same run or of different runs, ever share a unit name and no stop, reset or
// wait can act on another launch's unit: the name itself is the attempt. The id is also
// recorded by systemd as the unit's Description and read back before anything acts, so a
// loaded unit that does not carry it, or whose state cannot be read, is only ever observed and
// reported; one transient absence is never taken as stable absence, and a cleanup counts as
// done only once the unit's absence is read back. A handle that was never established as the
// launch's, because the start or the adoption could not prove it, is established by the first
// reading that finds the unit carrying the id, by Wait or by Stop, and until then takes the
// name as free only after consecutive readings find no unit. The unit outlives this process: a
// supervisor restart derives the same name from the run's record and re-adopts it. Nothing
// here is proven against systemd by the package tests, which run the launcher against fake
// executables.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	// ErrInvalidLauncher reports LauncherOptions, a unit name, a launch id or an argument the
	// launcher refuses before running anything.
	ErrInvalidLauncher = errors.New("systemd: invalid launcher options")

	// ErrLaunch reports a start that did not succeed. Without ErrLaunchUncertain the unit does
	// not exist: nothing was created, or what this launch created is gone.
	ErrLaunch = errors.New("systemd: transient unit not started")

	// ErrLaunchUncertain reports a start or an adoption whose outcome is not established: a unit
	// of the name exists that carries another launch id, or exists past the reconciliation
	// budget, or its state cannot be read, or systemd cannot be asked. It always wraps ErrLaunch
	// and comes with the Unit, which the caller watches instead of retrying the start.
	ErrLaunchUncertain = errors.New("systemd: launch outcome uncertain")

	// ErrNotOwned reports a stop or reset refused because the unit of the name is loaded with
	// another launch id, or its state cannot be read: nothing was done to it.
	ErrNotOwned = errors.New("systemd: unit is not this launch's")

	// ErrUnitControl reports a systemctl operation that failed.
	ErrUnitControl = errors.New("systemd: unit control failed")
)

const (
	DefaultSystemdRun  = "/usr/bin/systemd-run"
	DefaultSystemctl   = "/usr/bin/systemctl"
	DefaultUnitPrefix  = "connects-egress-worker-"
	DefaultStopTimeout = 20 * time.Second // SIGTERM to SIGKILL: the worker's drain, readers and terminal fit

	maxUnitBase    = 64                     // the caller's part of a unit name
	maxOutput      = 512                    // bytes of a command's output kept for an error
	commandTimeout = 30 * time.Second       // systemd-run waits for the start job; systemctl is quick
	defaultPoll    = 500 * time.Millisecond // the cadence of Wait, Adopt and reconcile

	// reconcileSettle is how many consecutive readings must find the unit absent before an
	// absence counts: a start request still in flight on the bus can load the unit after a
	// first reading. reconcileGrace is what reconcile has beyond the stop timeout, which bounds
	// a started worker's exit.
	reconcileSettle = 3
	reconcileGrace  = 5 * time.Second

	launchIDBytes = 16
	// descriptionPrefix precedes the launch id in the unit's Description, the property systemd
	// records the id in and reports back.
	descriptionPrefix = "connects-egress worker launch "
	showProperties    = "LoadState,ActiveState,SubState,Result,ExecMainStatus,ExecMainCode,MainPID,Description"
)

// NewLaunchID returns an unpredictable launch id: 32 lowercase hexadecimal characters.
func NewLaunchID() (string, error) {
	var b [launchIDBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("systemd: launch id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// CheckLaunchID reports whether id has the form NewLaunchID produces.
func CheckLaunchID(id string) error {
	if len(id) != 2*launchIDBytes {
		return fmt.Errorf("%w: launch id of %d characters, want %d", ErrInvalidLauncher, len(id), 2*launchIDBytes)
	}
	for i := 0; i < len(id); i++ {
		if c := id[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return fmt.Errorf("%w: launch id holds byte %#x at %d", ErrInvalidLauncher, c, i)
		}
	}
	return nil
}

// LauncherOptions configure a Launcher. Executable is the absolute path of the worker binary
// and EnvironmentFile the absolute path of the file the unit reads its credentials from; the
// launcher never reads it. UID is the uid the unit runs as, normally this process's. Slice, when
// set, is the slice unit the services are placed in. SystemdRun, Systemctl, UnitPrefix,
// StopTimeout and PollInterval take their defaults when zero.
type LauncherOptions struct {
	SystemdRun      string
	Systemctl       string
	Executable      string
	EnvironmentFile string
	UnitPrefix      string
	Slice           string
	UID             int
	StopTimeout     time.Duration
	PollInterval    time.Duration
}

// Launcher starts, adopts, stops and watches transient worker services. See the file comment.
type Launcher struct {
	o LauncherOptions
}

// NewLauncher validates o: every path absolute, the executable a regular file with an execute
// bit, the environment file a regular file (its content is not read), the uid non-negative and
// the unit prefix and slice well-formed unit name parts.
func NewLauncher(o LauncherOptions) (*Launcher, error) {
	if o.SystemdRun == "" {
		o.SystemdRun = DefaultSystemdRun
	}
	if o.Systemctl == "" {
		o.Systemctl = DefaultSystemctl
	}
	if o.UnitPrefix == "" {
		o.UnitPrefix = DefaultUnitPrefix
	}
	if o.StopTimeout == 0 {
		o.StopTimeout = DefaultStopTimeout
	}
	if o.PollInterval == 0 {
		o.PollInterval = defaultPoll
	}
	for _, p := range []struct{ what, path string }{
		{"systemd-run", o.SystemdRun}, {"systemctl", o.Systemctl}, {"executable", o.Executable}, {"environment file", o.EnvironmentFile},
	} {
		if !filepath.IsAbs(p.path) {
			return nil, fmt.Errorf("%w: %s %q is not an absolute path", ErrInvalidLauncher, p.what, p.path)
		}
	}
	for _, p := range []struct{ what, path string }{{"systemd-run", o.SystemdRun}, {"systemctl", o.Systemctl}, {"executable", o.Executable}} {
		if err := checkExecutable(p.path); err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrInvalidLauncher, p.what, err)
		}
	}
	st, err := os.Stat(o.EnvironmentFile)
	switch {
	case err != nil:
		return nil, fmt.Errorf("%w: environment file: %v", ErrInvalidLauncher, err)
	case !st.Mode().IsRegular():
		return nil, fmt.Errorf("%w: environment file %q is not a regular file", ErrInvalidLauncher, o.EnvironmentFile)
	}
	switch {
	case o.UID < 0:
		return nil, fmt.Errorf("%w: uid %d is negative", ErrInvalidLauncher, o.UID)
	case o.StopTimeout < time.Second || o.StopTimeout > time.Hour:
		return nil, fmt.Errorf("%w: stop timeout %v is not within 1s..1h", ErrInvalidLauncher, o.StopTimeout)
	case o.PollInterval < 10*time.Millisecond || o.PollInterval > time.Minute:
		return nil, fmt.Errorf("%w: poll interval %v is not within 10ms..1m", ErrInvalidLauncher, o.PollInterval)
	}
	if err := checkUnitChars("unit prefix", o.UnitPrefix); err != nil {
		return nil, err
	}
	if o.Slice != "" {
		if err := checkUnitChars("slice", strings.TrimSuffix(o.Slice, ".slice")); err != nil {
			return nil, err
		}
		if !strings.HasSuffix(o.Slice, ".slice") {
			return nil, fmt.Errorf("%w: slice %q does not end in .slice", ErrInvalidLauncher, o.Slice)
		}
	}
	return &Launcher{o: o}, nil
}

// checkExecutable requires a regular file with an execute bit.
func checkExecutable(path string) error {
	st, err := os.Stat(path)
	switch {
	case err != nil:
		return err
	case !st.Mode().IsRegular():
		return fmt.Errorf("%q is not a regular file", path)
	case st.Mode().Perm()&0o111 == 0:
		return fmt.Errorf("%q is not executable", path)
	}
	return nil
}

// checkUnitChars requires a nonempty ASCII unit name part of letters, digits, '-', '_', '.' and
// ':' with no leading '.', the characters systemd accepts without escaping.
func checkUnitChars(what, s string) error {
	if s == "" || strings.HasPrefix(s, ".") {
		return fmt.Errorf("%w: %s %q is empty or starts with a dot", ErrInvalidLauncher, what, s)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == ':') {
			return fmt.Errorf("%w: %s %q holds byte %#x at %d", ErrInvalidLauncher, what, s, c, i)
		}
	}
	return nil
}

// CheckUnitBase reports whether name may name a unit after the prefix: 1..64 bytes of ASCII
// letters, digits, '-' and '_' starting with a letter or digit.
func CheckUnitBase(name string) error {
	if name == "" || len(name) > maxUnitBase {
		return fmt.Errorf("%w: unit name of %d bytes is not within 1..%d", ErrInvalidLauncher, len(name), maxUnitBase)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alnum && (i == 0 || (c != '-' && c != '_')) {
			return fmt.Errorf("%w: unit name %q holds byte %#x at %d", ErrInvalidLauncher, name, c, i)
		}
	}
	return nil
}

// checkArg requires an argument systemd-run passes through unchanged: valid UTF-8 without
// control characters.
func checkArg(i int, s string) error {
	if !utf8.ValidString(s) || strings.ContainsFunc(s, unicode.IsControl) {
		return fmt.Errorf("%w: argument %d is not valid UTF-8 or holds a control character", ErrInvalidLauncher, i)
	}
	return nil
}

// UnitName returns the service unit the launcher starts for name with launchID: the prefix,
// the name, the launch id and ".service", unique to that launch.
func (l *Launcher) UnitName(name, launchID string) string {
	return l.o.UnitPrefix + name + "-" + launchID + ".service"
}

// Start runs the executable with args in a new transient service named after name and
// launchID, and returns once systemd has started it (Type=exec: the executable has been
// exec'd) or refused it. The name, the launch id and every argument are validated before
// anything runs.
//
// A failure once systemd-run has been invoked is ambiguous: the unit may have been loaded, be
// starting or be running when systemd-run failed or was killed at its deadline. Start then
// reconciles: a unit of the launch's name carrying its id is stopped or reset until its absence
// is read back, and the failure is ErrLaunch alone with nothing of the start left. A unit
// carrying another id, a unit still loaded when the budget ends, a reset that fails, a state
// that cannot be read or a systemd that cannot be asked leaves everything untouched and is
// ErrLaunchUncertain, returned with the Unit so the caller can watch it.
func (l *Launcher) Start(ctx context.Context, name, launchID string, args []string) (*Unit, error) {
	if err := CheckUnitBase(name); err != nil {
		return nil, err
	}
	if err := CheckLaunchID(launchID); err != nil {
		return nil, err
	}
	for i, a := range args {
		if err := checkArg(i, a); err != nil {
			return nil, err
		}
	}
	u := &Unit{l: l, name: l.UnitName(name, launchID), launch: launchID}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrLaunch, u.name, err) // nothing was invoked
	}
	argv := []string{"--unit=" + u.name, "--quiet", "--description=" + u.description(), "--property=Type=exec",
		"--property=User=" + strconv.Itoa(l.o.UID), "--property=EnvironmentFile=" + l.o.EnvironmentFile,
		"--property=TimeoutStopSec=" + strconv.FormatInt(int64(l.o.StopTimeout/time.Second), 10)}
	if l.o.Slice != "" {
		argv = append(argv, "--slice="+l.o.Slice)
	}
	argv = append(argv, "--", l.o.Executable)
	argv = append(argv, args...)
	if _, err := l.command(ctx, l.o.SystemdRun, argv); err != nil {
		if rerr := u.reconcile(); rerr != nil {
			return u, fmt.Errorf("%w: %s: %w; %w", ErrLaunch, u.name, err, rerr)
		}
		return nil, fmt.Errorf("%w: %s: %w", ErrLaunch, u.name, err)
	}
	u.owned.Store(true)
	return u, nil
}

// Adopt returns the handle of the unit of name and launchID, as a restarted supervisor derives
// it from the run's record: present is true when the unit is loaded with that id, false when
// consecutive readings find no unit of the name. A unit carrying another id, a state that cannot
// be read or a systemd that cannot be asked is ErrLaunchUncertain, with the handle.
func (l *Launcher) Adopt(ctx context.Context, name, launchID string) (*Unit, bool, error) {
	if err := CheckUnitBase(name); err != nil {
		return nil, false, err
	}
	if err := CheckLaunchID(launchID); err != nil {
		return nil, false, err
	}
	u := &Unit{l: l, name: l.UnitName(name, launchID), launch: launchID}
	t := time.NewTicker(l.o.PollInterval)
	defer t.Stop()
	absent := 0
	for {
		st, err := u.show(ctx)
		switch {
		case err != nil:
			return u, false, fmt.Errorf("%w: %w: %s: systemd cannot be asked: %v", ErrLaunch, ErrLaunchUncertain, u.name, err)
		case u.owns(st):
			u.owned.Store(true)
			return u, true, nil
		case st.loaded():
			return u, false, fmt.Errorf("%w: %w: %s carries another launch; left untouched", ErrLaunch, ErrLaunchUncertain, u.name)
		case st.absent():
			absent++
			if absent >= reconcileSettle {
				return u, false, nil
			}
		default:
			return u, false, fmt.Errorf("%w: %w: %s: load state %q; left untouched", ErrLaunch, ErrLaunchUncertain, u.name, st["LoadState"])
		}
		select {
		case <-ctx.Done():
			return u, false, fmt.Errorf("%w: %w: %s: %v", ErrLaunch, ErrLaunchUncertain, u.name, ctx.Err())
		case <-t.C:
		}
	}
}

// command runs path with argv and returns its standard output. A failure carries the bounded
// combined output.
func (l *Launcher) command(ctx context.Context, path string, argv []string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, argv...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.Stdin = nil
	err := cmd.Run()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = strings.TrimSpace(stdout.String())
		}
		if len(msg) > maxOutput {
			msg = msg[:maxOutput]
		}
		if msg == "" {
			return nil, fmt.Errorf("%s: %v", filepath.Base(path), err)
		}
		return nil, fmt.Errorf("%s: %v: %s", filepath.Base(path), err, msg)
	}
	return stdout.Bytes(), nil
}

// unitState is one reading of a unit's showProperties.
type unitState map[string]string

func (st unitState) loaded() bool { return st["LoadState"] == "loaded" }
func (st unitState) absent() bool { return st["LoadState"] == "not-found" }

// Unit is the handle of one transient service: its name, which holds the launch id, and the
// id itself. owned records that the unit was established as this launch's at some point: by a
// successful start, by reconciliation, by adoption, or by a reading of Wait or Stop that found
// it carrying the id. A handle that never was can only watch the name and stop what a reading
// proves is its own. Wait and Stop may run concurrently on one handle.
type Unit struct {
	l      *Launcher
	name   string
	launch string
	owned  atomic.Bool
}

// Name returns the service unit name.
func (u *Unit) Name() string { return u.name }

// LaunchID returns the launch id the unit carries.
func (u *Unit) LaunchID() string { return u.launch }

// description is the Description systemd records for this launch.
func (u *Unit) description() string { return descriptionPrefix + u.launch }

// owns reports whether st shows the unit of the name loaded with this launch's id.
func (u *Unit) owns(st unitState) bool { return st.loaded() && st["Description"] == u.description() }

// Owns reports whether pid is the main process of this launch's unit: the unit is loaded with
// the launch id and systemd names pid as its MainPID. A state that cannot be read is an error,
// never false.
func (u *Unit) Owns(ctx context.Context, pid int) (bool, error) {
	st, err := u.show(ctx)
	if err != nil {
		return false, err
	}
	if !st.loaded() && !st.absent() {
		return false, fmt.Errorf("%w: %s: load state %q", ErrUnitControl, u.name, st["LoadState"])
	}
	main, err := strconv.Atoi(st["MainPID"])
	if err != nil {
		return false, fmt.Errorf("%w: %s: MainPID %q", ErrUnitControl, u.name, st["MainPID"])
	}
	return u.owns(st) && pid > 0 && main == pid, nil
}

// Stop asks systemd to stop this launch's unit and returns without waiting for it: the unit's
// stop timeout then bounds the worker's exit. The stop is issued only on a reading that finds
// the unit loaded with this launch's id, which establishes the handle. A unit no longer loaded
// counts as stopped, for a handle never established only once reconcileSettle consecutive
// readings find none. A unit loaded with another launch id, or whose state cannot be read, is
// ErrNotOwned and untouched; a systemd that cannot be asked is ErrUnitControl.
func (u *Unit) Stop(ctx context.Context) error {
	t := time.NewTicker(u.l.o.PollInterval)
	defer t.Stop()
	absent := 0
	for {
		st, err := u.show(ctx)
		switch {
		case err != nil:
			return err
		case st.absent():
			absent++
			if u.owned.Load() || absent >= reconcileSettle {
				return nil
			}
		case !u.owns(st):
			return fmt.Errorf("%w: %s: load state %q, description %q", ErrNotOwned, u.name, st["LoadState"], st["Description"])
		default:
			u.owned.Store(true)
			_, err = u.l.command(ctx, u.l.o.Systemctl, []string{"stop", "--no-block", "--", u.name})
			if err == nil || strings.Contains(err.Error(), "not loaded") {
				return nil
			}
			return fmt.Errorf("%w: stop %s: %v", ErrUnitControl, u.name, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s: %w", ErrUnitControl, u.name, ctx.Err())
		case <-t.C:
		}
	}
}

// Exit is how a unit ended: the systemd Result and the main process's exit code or signal, as
// far as systemd still reported them. Known is false when the unit was already unloaded, which
// a clean exit is at once, or when the name was taken by another launch (Replaced).
type Exit struct {
	Known    bool
	Replaced bool
	Result   string
	Code     string // ExecMainCode: exited, killed, dumped
	Status   int    // the exit status or signal number
}

// String describes the exit for a log line or a run's failure reason.
func (e Exit) String() string {
	switch {
	case e.Replaced:
		return "unit name taken by another launch"
	case !e.Known:
		return "unit gone"
	}
	return fmt.Sprintf("result %s, %s %d", e.Result, e.Code, e.Status)
}

// Wait polls until this launch's unit is gone and returns how it ended: no unit of the name,
// the unit inactive or failed (a failed unit of this launch is reset, and its absence read
// back, so systemd has unloaded it), or, for a handle established as this launch's, the name
// taken by another launch. A handle never established is established here by the first
// reading that finds the unit loaded with this launch's id, and until then takes the name as
// free only once reconcileSettle consecutive readings find no unit; a reading that finds one,
// or that fails, starts the count over. It returns ctx's error when ctx ends first and
// ErrUnitControl when the state cannot be read or a reset fails or does not unload the unit,
// so the caller can retry.
func (u *Unit) Wait(ctx context.Context) (Exit, error) {
	t := time.NewTicker(u.l.o.PollInterval)
	defer t.Stop()
	absent := 0
	for {
		st, err := u.show(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return Exit{}, ctx.Err()
			}
			return Exit{}, err
		}
		switch {
		case st.absent():
			absent++
			if u.owned.Load() || absent >= reconcileSettle {
				return Exit{}, nil
			}
		case !st.loaded():
			return Exit{}, fmt.Errorf("%w: %s: load state %q", ErrUnitControl, u.name, st["LoadState"])
		case !u.owns(st):
			absent = 0
			if u.owned.Load() {
				return Exit{Replaced: true}, nil
			}
		default:
			absent = 0
			u.owned.Store(true)
			if st["ActiveState"] == "inactive" || st["ActiveState"] == "failed" {
				e := Exit{Known: true, Result: st["Result"], Code: st["ExecMainCode"]}
				e.Status, _ = strconv.Atoi(st["ExecMainStatus"])
				if st["ActiveState"] == "failed" {
					if err := u.resetFailed(ctx); err != nil {
						return Exit{}, err
					}
				}
				return e, nil
			}
		}
		select {
		case <-ctx.Done():
			return Exit{}, ctx.Err()
		case <-t.C:
		}
	}
}

// reconcile undoes an ambiguous start, with its own budget since the start's context may already
// be over: every reading that finds the unit loaded with this launch's id stops it (which also
// replaces a pending start job) or resets it when failed, and the start counts as undone only
// once reconcileSettle consecutive readings find no unit of the name. A unit with another
// launch id, a unit still loaded when the budget ends, a reset that fails, a state that cannot
// be read or a systemd that cannot be asked is ErrLaunchUncertain, with nothing touched from
// then on.
func (u *Unit) reconcile() error {
	budget := u.l.o.StopTimeout + reconcileGrace
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	t := time.NewTicker(u.l.o.PollInterval)
	defer t.Stop()
	absent := 0
	for {
		st, err := u.show(ctx)
		switch {
		case err != nil && ctx.Err() != nil:
			return fmt.Errorf("%w: %s is still loaded after %v", ErrLaunchUncertain, u.name, budget)
		case err != nil:
			return fmt.Errorf("%w: %s: systemd cannot be asked: %v", ErrLaunchUncertain, u.name, err)
		case st.absent():
			absent++
			if absent >= reconcileSettle {
				return nil
			}
		case u.owns(st):
			absent = 0
			u.owned.Store(true)
			if st["ActiveState"] == "failed" {
				if err := u.resetFailed(ctx); err != nil {
					return fmt.Errorf("%w: %s: %v", ErrLaunchUncertain, u.name, err)
				}
			} else if err := u.Stop(ctx); err != nil {
				return fmt.Errorf("%w: %s: %v", ErrLaunchUncertain, u.name, err)
			}
		case st.loaded():
			return fmt.Errorf("%w: %s carries another launch; left untouched", ErrLaunchUncertain, u.name)
		default:
			return fmt.Errorf("%w: %s: load state %q; left untouched", ErrLaunchUncertain, u.name, st["LoadState"])
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s is still loaded after %v", ErrLaunchUncertain, u.name, budget)
		case <-t.C:
		}
	}
}

// resetFailed resets this launch's failed unit and reads back that systemd unloaded it; a reset
// that fails or leaves the unit loaded is ErrUnitControl.
func (u *Unit) resetFailed(ctx context.Context) error {
	if _, err := u.l.command(ctx, u.l.o.Systemctl, []string{"reset-failed", "--", u.name}); err != nil {
		return fmt.Errorf("%w: reset-failed %s: %v", ErrUnitControl, u.name, err)
	}
	st, err := u.show(ctx)
	switch {
	case err != nil:
		return err
	case !st.absent():
		return fmt.Errorf("%w: %s is still %s after reset-failed", ErrUnitControl, u.name, st["LoadState"]+"/"+st["ActiveState"])
	}
	return nil
}

// show reads the unit's showProperties.
func (u *Unit) show(ctx context.Context) (unitState, error) {
	out, err := u.l.command(ctx, u.l.o.Systemctl, []string{"show", "--no-pager", "--property=" + showProperties, "--", u.name})
	if err != nil {
		return nil, fmt.Errorf("%w: show %s: %v", ErrUnitControl, u.name, err)
	}
	st := unitState{}
	for _, line := range strings.Split(string(out), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			st[k] = strings.TrimSpace(v)
		}
	}
	return st, nil
}
