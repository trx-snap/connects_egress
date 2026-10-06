package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "0.1.0-dev"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	role := os.Args[1]
	switch role {
	case "supervisor":
		runSupervisor(os.Args[2:])
	case "worker":
		runWorker(os.Args[2:])
	case "bridge":
		runBridge(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Printf("connects-egress v%s\n", version)
	default:
		fmt.Fprintf(os.Stderr, "unknown role: %s\n\n", role)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `connects-egress - Unified Media Packager & Egress Engine

Usage:
  connects-egress <role> [options]

Roles:
  supervisor   Runs node supervisor (memfd Part Store, Room Timelines, LL-HLS origin)
  worker       Runs media pipeline worker for a source (ingest, compose, encode, mix)
  bridge       Runs WebRTC stage bridge for cross-node / cross-region relay

Use "connects-egress <role> --help" for role-specific options.
`)
}

func runSupervisor(args []string) {
	fs := flag.NewFlagSet("supervisor", flag.ExitOnError)
	httpPort := fs.Int("http-port", 8080, "HTTP origin port for LL-HLS")
	ipcSocket := fs.String("ipc-socket", "/run/connects-egress/supervisor.sock", "Unix socket for worker IPC")
	fs.Parse(args)

	fmt.Printf("[supervisor] starting connects-egress supervisor v%s on port %d (IPC: %s)...\n", version, *httpPort, *ipcSocket)
	// TODO: Phase 1 implementation per plan §17
}

func runWorker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	sourceID := fs.String("source-id", "", "Unique source ID (solo room or match)")
	mode := fs.String("mode", "solo", "Media mode: solo | 1v1 | tryon | quadro")
	ipcSocket := fs.String("ipc-socket", "/run/connects-egress/supervisor.sock", "Supervisor Unix socket")
	fs.Parse(args)

	if *sourceID == "" {
		fmt.Fprintln(os.Stderr, "error: --source-id is required")
		os.Exit(1)
	}

	fmt.Printf("[worker] starting connects-egress worker for source %s (mode: %s, IPC: %s)...\n", *sourceID, *mode, *ipcSocket)
	// TODO: Phase 1 implementation per plan §17
}

func runBridge(args []string) {
	fs := flag.NewFlagSet("bridge", flag.ExitOnError)
	matchID := fs.String("match-id", "", "Match ID to bridge")
	anchorURL := fs.String("anchor-url", "", "Anchor LiveKit SFU URL")
	spokeURL := fs.String("spoke-url", "", "Spoke LiveKit SFU URL")
	fs.Parse(args)

	if *matchID == "" {
		fmt.Fprintln(os.Stderr, "error: --match-id is required")
		os.Exit(1)
	}

	fmt.Printf("[bridge] starting connects-egress bridge for match %s (anchor: %s, spoke: %s)...\n", *matchID, *anchorURL, *spokeURL)
	// TODO: Phase 1 implementation per plan §17
}
