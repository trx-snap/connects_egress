//go:build linux

package ipc

// The Linux transport of IPC2: one PublicationPacket per SOCK_SEQPACKET datagram over the same
// raw framing as the version 1 helpers, with the same ownership rules. Only a setup welcome
// carries descriptors, exactly those of its state's tracks.

import (
	"fmt"
	"net"
	"time"
)

// WritePublicationMsg sends p as one datagram with fds attached as SCM_RIGHTS, by deadline. A
// setup welcome carries exactly len(Welcome.State.Tracks) descriptors, in that array's order;
// every other packet carries none. The descriptors stay the caller's: borrowed for the send,
// never closed here. Everything is checked before anything is written, and a partial send is an
// error that is not retried.
func WritePublicationMsg(conn *net.UnixConn, p PublicationPacket, fds []int, deadline time.Time) error {
	if deadline.IsZero() {
		return fmt.Errorf("%w: zero deadline", ErrInvalidTransport)
	}
	data, err := EncodePublicationPacket(p)
	if err != nil {
		return err
	}
	return writeDatagram(conn, p.Type, data, fds, publicationRightsFor(p), deadline)
}

// ReadPublicationMsg receives one datagram by deadline and returns its IPC2 packet with the
// descriptors it carried, at most maxFDs (0 to 5) of them, close-on-exec. The descriptors become
// the caller's only with a nil error: on any error the packet is zero, the list nil and every
// descriptor the datagram delivered has been closed. Errors are those of ReadMsg, with the IPC2
// codec's own for a packet that breaks its contract.
func ReadPublicationMsg(conn *net.UnixConn, maxFDs int, deadline time.Time) (PublicationPacket, []int, error) {
	data, fds, err := readDatagram(conn, maxFDs, deadline)
	if err != nil {
		return PublicationPacket{}, nil, err
	}
	p, err := DecodePublicationPacket(data)
	if err != nil {
		closeFDs(fds...)
		return PublicationPacket{}, nil, err
	}
	if want := publicationRightsFor(p); len(fds) != want {
		closeFDs(fds...)
		return PublicationPacket{}, nil, fmt.Errorf("%w: %s packet with %d descriptors, want %d", ErrInvalidTransport, p.Type, len(fds), want)
	}
	return p, fds, nil
}

// publicationRightsFor returns the number of descriptors a valid IPC2 packet carries: a setup
// welcome's tracks, otherwise none.
func publicationRightsFor(p PublicationPacket) int {
	if p.Type == TypeWelcome && p.Welcome != nil && p.Welcome.State.Setup {
		return len(p.Welcome.State.Tracks)
	}
	return 0
}
