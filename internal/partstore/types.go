// Package partstore is the supervisor-owned backing memory of the Part Store: one sealed-size
// memfd per configured track of a source epoch, allocated by the supervisor, written by the
// worker through its own duplicates, and read here through read-only mappings.
//
// This is the raw memory primitive only. Which spans are published, how the ring is laid out,
// what is retained and when bytes are reused is decided by the callers that hold the
// publication protocol; ReadSpan copies whatever is in the span and nothing here keeps a
// writer from overwriting it. The Linux implementation is partstore_linux.go.
package partstore

import "errors"

var (
	// ErrInvalidConfig reports a run, source, epoch or Config the store refuses before touching
	// any descriptor: it fails the ipc hello validation, the epoch is zero, the capacity does not
	// fit a mapping, or a canonical memfd name is too long for the kernel.
	ErrInvalidConfig = errors.New("partstore: invalid config")

	// ErrIdentityMismatch reports descriptors offered for adoption that are not exactly the
	// epoch's memfds: wrong count, negative or repeated numbers, aliased inodes, or a descriptor
	// whose expected identity, kernel identity, size, type, access mode, seals or name differ
	// from what this package allocates.
	ErrIdentityMismatch = errors.New("partstore: identity mismatch")

	// ErrSpanOutOfBounds reports a ReadSpan track or byte span outside the track's capacity.
	ErrSpanOutOfBounds = errors.New("partstore: span out of bounds")

	// ErrClosed reports an operation on a closed, nil or never-opened store.
	ErrClosed = errors.New("partstore: store closed")
)
