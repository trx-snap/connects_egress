//go:build linux

package partstore

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"golang.org/x/sys/unix"
)

// Journal is the durable record history of exactly one Ring, kept as journal.bin in a private
// directory the caller owns: one checkpoint header followed by every record after the
// checkpoint's RecordHigh. Its private append is the Ring's Persist: a record is written and
// synced before the Ring applies it, so a crash never applies what is not on disk, and a
// complete record whose acknowledgement was lost is replayed on open. Any ambiguous write,
// sync, rename or close poisons the journal; the Ring then poisons itself on the next
// mutation. Lock order is Ring then Journal: append runs under the Ring's write lock, Close
// disables the Ring before taking the journal mutex, and Compact exports the checkpoint
// before taking it. Nothing here calls the Ring while the journal mutex is held.
//
// hook, when set by a test in this package, runs under the journal mutex at the named
// durability points; an error poisons the journal. It must not call back into the Journal or
// the Ring.
type Journal struct {
	mu       sync.Mutex
	ring     *Ring
	dir      int
	lock     int
	file     int
	offset   int64   // size of the active file: next append position
	base     uint64  // RecordHigh of the active header
	high     uint64  // last durable record
	starts   []int64 // frame start of each suffix record, base+1 first
	run      string
	source   string
	epoch    uint64
	cfg      ipc.Config
	policy   Policy // effective
	ids      []ipc.FDIdentity
	bootID   string
	closed   bool
	poisoned bool
	hook     func(string) error
}

// CreateJournal starts a fresh Ring over store and journals it in dir, which must be an
// existing absolute private directory. The store must be fresh and journal.bin absent: the
// empty checkpoint header is written to a temporary file, synced and published by an atomic
// no-replace rename before the Ring is returned, and an existing journal.bin is refused and
// never overwritten. The store is borrowed and untouched on every error.
func CreateJournal(dir string, store *EpochStore, run, source string, epoch uint64, cfg ipc.Config, policy Policy) (*Journal, *Ring, error) {
	j, err := newJournal(dir, store, run, source, epoch, cfg, policy)
	if err != nil {
		return nil, nil, err
	}
	r, err := NewRing(store, run, source, epoch, cfg, policy, j.append)
	if err != nil {
		return nil, nil, err
	}
	if err := j.enter(dir); err != nil {
		return nil, nil, err
	}
	if err := j.bootstrap(r); err != nil {
		j.discard()
		return nil, nil, err
	}
	j.ring = r
	return j, r, nil
}

// bootstrap publishes the empty checkpoint of r as a new journal.bin.
func (j *Journal) bootstrap(r *Ring) error {
	var st unix.Stat_t
	if err := unix.Fstatat(j.dir, journalFile, &st, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		return fmt.Errorf("%w: %s", os.ErrExist, journalFile)
	} else if !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("%w: stat %s: %v", ErrJournalInvalid, journalFile, err)
	}
	cp, err := r.ExportCheckpoint()
	if err != nil {
		return err
	}
	hdr, err := j.encodeHeader(cp)
	if err != nil {
		return err
	}
	content := append(journalPrefix(), hdr...)
	fd, name, err := j.createTemp()
	if err != nil {
		return err
	}
	err = writeSynced(fd, content)
	if err == nil {
		err = unix.Renameat2(j.dir, name, j.dir, journalFile, unix.RENAME_NOREPLACE)
	}
	if err != nil {
		_ = unix.Unlinkat(j.dir, name, 0)
		_ = unix.Close(fd)
		return fmt.Errorf("%w: publish %s: %v", ErrJournalInvalid, journalFile, err)
	}
	if err := unix.Fsync(j.dir); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("%w: sync directory: %v", ErrJournalInvalid, err)
	}
	j.file, j.offset = fd, int64(len(content))
	return nil
}

// OpenJournal restores the Ring of an existing journal.bin in dir, which must have been
// written in this boot for exactly the store's memfds, run, source, epoch, cfg and effective
// policy. A missing journal wraps os.ErrNotExist and is never replaced. The header must be
// complete and valid, and so must every complete record frame; only an incomplete final
// record frame is dropped, and only after RestoreCheckpoint has accepted the checkpoint and
// the complete records: the file is then truncated and synced before the Ring is returned.
// Any other defect leaves the file unchanged. The store is borrowed and untouched on error.
func OpenJournal(dir string, store *EpochStore, run, source string, epoch uint64, cfg ipc.Config, policy Policy) (*Journal, *Ring, error) {
	j, err := newJournal(dir, store, run, source, epoch, cfg, policy)
	if err != nil {
		return nil, nil, err
	}
	if err := j.enter(dir); err != nil {
		return nil, nil, err
	}
	r, err := j.recover(store)
	if err != nil {
		j.discard()
		return nil, nil, err
	}
	j.ring = r
	return j, r, nil
}

// recover reads, validates and replays journal.bin, then trims an incomplete tail.
func (j *Journal) recover(store *EpochStore) (*Ring, error) {
	fd, err := unix.Openat(j.dir, journalFile, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil, fmt.Errorf("%w: %s", os.ErrNotExist, journalFile)
		}
		return nil, fmt.Errorf("%w: open %s: %v", ErrJournalInvalid, journalFile, err)
	}
	j.file = fd
	st, err := privateFile(fd, journalFile)
	if err != nil {
		return nil, err
	}
	if st.Size > maxJournalFile {
		return nil, fmt.Errorf("%w: %s is %d bytes, limit %d", ErrJournalLimit, journalFile, st.Size, maxJournalFile)
	}
	buf := make([]byte, st.Size)
	if err := preadFull(fd, buf, 0); err != nil {
		return nil, fmt.Errorf("%w: read %s: %v", ErrJournalInvalid, journalFile, err)
	}
	hdr, pos, err := j.parseHeader(buf)
	if err != nil {
		return nil, err
	}
	recs, starts, end, err := j.parseRecords(buf, pos, hdr.Checkpoint.RecordHigh)
	if err != nil {
		return nil, err
	}
	r, err := RestoreCheckpoint(store, hdr.Checkpoint, recs, j.append)
	if err != nil {
		return nil, err
	}
	if end < len(buf) {
		err = unix.Ftruncate(fd, int64(end))
		if err == nil {
			err = unix.Fsync(fd)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: truncate incomplete tail of %s: %v", ErrJournalInvalid, journalFile, err)
		}
	}
	j.offset, j.base, j.high, j.starts = int64(end), hdr.Checkpoint.RecordHigh, hdr.Checkpoint.RecordHigh, starts
	if n := len(recs); n > 0 {
		j.high = recs[n-1].Seq
	}
	return r, nil
}

// newJournal validates the arguments and captures the scope, effective policy, store
// identities and boot id, before anything on disk is touched.
func newJournal(dir string, store *EpochStore, run, source string, epoch uint64, cfg ipc.Config, policy Policy) (*Journal, error) {
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("%w: journal directory %q is not absolute", ErrInvalidConfig, dir)
	}
	if _, err := setup(run, source, epoch, cfg); err != nil {
		return nil, err
	}
	pol, err := policy.effective()
	if err != nil {
		return nil, err
	}
	ids, err := store.Identities()
	if err != nil {
		return nil, err
	}
	boot, err := currentBootID()
	if err != nil {
		return nil, err
	}
	return &Journal{dir: -1, lock: -1, file: -1, run: run, source: source, epoch: epoch, cfg: cfg, policy: pol, ids: ids, bootID: boot}, nil
}

// enter opens the private directory and takes its stable lock file exclusively for the life
// of the journal. The lock is never unlinked. On error nothing is left open.
func (j *Journal) enter(dir string) error {
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("%w: open directory: %v", ErrJournalInvalid, err)
	}
	j.dir = fd
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		j.discard()
		return fmt.Errorf("%w: fstat directory: %v", ErrJournalInvalid, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != uint32(unix.Geteuid()) || st.Mode&0o777 != 0o700 {
		j.discard()
		return fmt.Errorf("%w: directory is not a private 0700 directory of uid %d", ErrJournalInvalid, unix.Geteuid())
	}
	lock, err := unix.Openat(fd, journalLock, unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		j.discard()
		return fmt.Errorf("%w: open %s: %v", ErrJournalInvalid, journalLock, err)
	}
	j.lock = lock
	if _, err := privateFile(lock, journalLock); err != nil {
		j.discard()
		return err
	}
	if err := unix.Flock(lock, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		j.discard()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return fmt.Errorf("%w: %s is held", ErrJournalBusy, journalLock)
		}
		return fmt.Errorf("%w: flock %s: %v", ErrJournalInvalid, journalLock, err)
	}
	return nil
}

// privateFile requires fd to be a regular 0600 file of the effective uid with one link.
func privateFile(fd int, name string) (unix.Stat_t, error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return st, fmt.Errorf("%w: fstat %s: %v", ErrJournalInvalid, name, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != uint32(unix.Geteuid()) || st.Nlink != 1 || st.Mode&0o777 != 0o600 {
		return st, fmt.Errorf("%w: %s is not a private 0600 regular file of uid %d with one link", ErrJournalInvalid, name, unix.Geteuid())
	}
	return st, nil
}

// createTemp opens a new exclusive 0600 temporary file with a random name in the directory.
func (j *Journal) createTemp() (int, string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return -1, "", fmt.Errorf("%w: random temp name: %v", ErrJournalInvalid, err)
	}
	name := ".journal." + hex.EncodeToString(b[:]) + ".tmp"
	fd, err := unix.Openat(j.dir, name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return -1, "", fmt.Errorf("%w: create %s: %v", ErrJournalInvalid, name, err)
	}
	return fd, name, nil
}

// append is the Ring's Persist: it validates rec as the next record of this journal, writes
// its frame and syncs the file before returning nil. Limits fail closed without writing. A
// failed write or sync, or a hook error, poisons the journal.
func (j *Journal) append(rec Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.mutable(); err != nil {
		return err
	}
	if j.high == ^uint64(0) {
		return fmt.Errorf("%w: record sequence exhausted", ErrJournalLimit)
	}
	if err := j.checkRecord(rec, j.high+1); err != nil {
		return err
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("%w: encode record %d: %v", ErrJournalInvalid, rec.Seq, err)
	}
	switch {
	case len(data) > maxRecordJSON:
		return fmt.Errorf("%w: record %d of %d bytes exceeds %d", ErrJournalLimit, rec.Seq, len(data), maxRecordJSON)
	case len(j.starts) >= maxSuffixRecords:
		return fmt.Errorf("%w: suffix of %d records is full, compact first", ErrJournalLimit, maxSuffixRecords)
	case j.offset > maxJournalFile-int64(frameHeadLen+len(data)):
		return fmt.Errorf("%w: record %d would grow %s past %d bytes", ErrJournalLimit, rec.Seq, journalFile, maxJournalFile)
	}
	frame := encodeFrame(frameRecord, data)
	if err := pwriteFull(j.file, frame, j.offset); err != nil {
		return j.poison(fmt.Errorf("write record %d: %v", rec.Seq, err))
	}
	if err := j.callHook("append_written"); err != nil {
		return err
	}
	if err := unix.Fsync(j.file); err != nil {
		return j.poison(fmt.Errorf("sync record %d: %v", rec.Seq, err))
	}
	if err := j.callHook("append_synced"); err != nil {
		return err
	}
	j.starts = append(j.starts, j.offset)
	j.offset += int64(len(frame))
	j.high = rec.Seq
	return nil
}

// Compact replaces journal.bin by the Ring's current checkpoint followed by exactly the
// records after it, including any appended since the export. The owner calls it once per
// closed segment, never from Persist. The new representation is written to a temporary file,
// synced, renamed over journal.bin and the directory synced; the journal then switches to the
// new file. A checkpoint no newer than the current header is a no-op. Until the rename the
// old file is untouched; any ambiguity at or after it poisons the journal, and on restart
// either whole valid representation is accepted. A nil, zero or closed journal is ErrClosed.
func (j *Journal) Compact() error {
	if j == nil || j.ring == nil {
		return ErrClosed
	}
	cp, err := j.ring.ExportCheckpoint()
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.mutable(); err != nil {
		return err
	}
	switch {
	case cp.RecordHigh <= j.base:
		return nil
	case cp.RecordHigh > j.high:
		return fmt.Errorf("%w: checkpoint record high %d beyond journal high %d", ErrJournalInvalid, cp.RecordHigh, j.high)
	}
	keep := int(cp.RecordHigh - j.base) // index of the first record to keep
	from := j.offset
	if keep < len(j.starts) {
		from = j.starts[keep]
	}
	suffix := make([]byte, j.offset-from)
	if err := preadFull(j.file, suffix, from); err != nil {
		return j.poison(fmt.Errorf("read suffix: %v", err))
	}
	recs, _, end, err := j.parseRecords(suffix, 0, cp.RecordHigh)
	if err == nil && (end != len(suffix) || uint64(len(recs)) != j.high-cp.RecordHigh) {
		err = fmt.Errorf("%w: suffix on disk does not match the journal state", ErrJournalInvalid)
	}
	if err != nil {
		return j.poison(err)
	}
	hdr, err := j.encodeHeader(cp)
	if err != nil {
		return err
	}
	content := append(journalPrefix(), hdr...)
	if int64(len(content)) > maxJournalFile-int64(len(suffix)) {
		return fmt.Errorf("%w: compacted journal would exceed %d bytes", ErrJournalLimit, maxJournalFile)
	}
	headerLen := int64(len(content))
	content = append(content, suffix...)
	fd, name, err := j.createTemp()
	if err != nil {
		return err
	}
	if err := writeSynced(fd, content); err != nil {
		_ = unix.Unlinkat(j.dir, name, 0)
		_ = unix.Close(fd)
		return fmt.Errorf("%w: write %s: %v", ErrJournalInvalid, name, err)
	}
	for _, point := range []string{"compact_temp_synced", "compact_before_rename"} {
		if err := j.callHook(point); err != nil {
			_ = unix.Unlinkat(j.dir, name, 0)
			_ = unix.Close(fd)
			return err
		}
	}
	if err := unix.Renameat2(j.dir, name, j.dir, journalFile, 0); err != nil {
		_ = unix.Close(fd)
		return j.poison(fmt.Errorf("rename %s over %s: %v", name, journalFile, err))
	}
	old := j.file
	j.file, j.offset, j.base = fd, int64(len(content)), cp.RecordHigh
	starts := make([]int64, 0, len(j.starts)-keep)
	for _, s := range j.starts[keep:] {
		starts = append(starts, s-from+headerLen)
	}
	j.starts = starts
	if err := j.callHook("compact_renamed"); err != nil {
		_ = unix.Close(old)
		return err
	}
	if err := unix.Fsync(j.dir); err != nil {
		_ = unix.Close(old)
		return j.poison(fmt.Errorf("sync directory: %v", err))
	}
	if err := j.callHook("compact_dir_synced"); err != nil {
		_ = unix.Close(old)
		return err
	}
	if err := unix.Close(old); err != nil {
		return j.poison(fmt.Errorf("close old %s: %v", journalFile, err))
	}
	return nil
}

// Close disables the Ring, then the journal, and closes its file, lock and directory. The
// lock file stays in place. It is idempotent and safe on a nil or zero journal.
func (j *Journal) Close() error {
	if j == nil {
		return nil
	}
	if j.ring != nil {
		_ = j.ring.Close()
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	if j.ring == nil {
		return nil
	}
	return j.discard()
}

// discard closes every descriptor the journal holds once; it never unlinks anything.
func (j *Journal) discard() error {
	var errs []error
	for _, d := range []struct {
		fd   *int
		name string
	}{{&j.file, journalFile}, {&j.lock, journalLock}, {&j.dir, "directory"}} {
		if *d.fd >= 0 {
			if err := unix.Close(*d.fd); err != nil {
				errs = append(errs, fmt.Errorf("partstore: close journal %s: %w", d.name, err))
			}
			*d.fd = -1
		}
	}
	return errors.Join(errs...)
}

// mutable reports whether the journal may be written.
func (j *Journal) mutable() error {
	switch {
	case j.closed:
		return ErrClosed
	case j.poisoned:
		return ErrPoisoned
	}
	return nil
}

// poison marks the journal's durable state unknown and returns err as ErrPoisoned.
func (j *Journal) poison(err error) error {
	j.poisoned = true
	return fmt.Errorf("%w: %w", ErrPoisoned, err)
}

// callHook runs the test hook at point; an error poisons the journal.
func (j *Journal) callHook(point string) error {
	if j.hook == nil {
		return nil
	}
	if err := j.hook(point); err != nil {
		return j.poison(fmt.Errorf("hook %s: %w", point, err))
	}
	return nil
}

// writeSynced writes all of b at the start of fd and syncs it.
func writeSynced(fd int, b []byte) error {
	if err := pwriteFull(fd, b, 0); err != nil {
		return err
	}
	return unix.Fsync(fd)
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

// preadFull fills b from off, retrying short reads and EINTR.
func preadFull(fd int, b []byte, off int64) error {
	for len(b) > 0 {
		n, err := unix.Pread(fd, b, off)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrUnexpectedEOF
		}
		b, off = b[n:], off+int64(n)
	}
	return nil
}
