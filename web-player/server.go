// Package main provides a standalone, zero-dependency Go web server and streaming reverse proxy
// for the Connects Egress LL-HLS Web Player.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Config struct {
	mu             sync.RWMutex
	upstreamOrigin string
	proxy          *httputil.ReverseProxy
}

func (c *Config) SetUpstream(raw string) error {
	trimmed := strings.TrimRight(raw, "/")
	u, err := url.Parse(trimmed)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid upstream origin: %q", raw)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.upstreamOrigin = trimmed
	c.proxy = httputil.NewSingleHostReverseProxy(u)
	// Customize director to maintain path and add CORS
	originalDirector := c.proxy.Director
	c.proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = u.Host
	}
	c.proxy.ModifyResponse = func(res *http.Response) error {
		res.Header.Set("Access-Control-Allow-Origin", "*")
		res.Header.Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
		res.Header.Set("Access-Control-Allow-Headers", "*")
		res.Header.Set("Access-Control-Expose-Headers", "*")
		if strings.Contains(res.Request.URL.Path, ".m3u8") {
			res.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
		}
		return nil
	}
	return nil
}

func (c *Config) GetUpstream() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.upstreamOrigin
}

func (c *Config) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	proxy := c.proxy
	c.mu.RUnlock()
	proxy.ServeHTTP(w, r)
}

func main() {
	port := flag.Int("port", 3050, "Port for web player server")
	upstream := flag.String("upstream", "http://127.0.0.1:18080", "Upstream connects-egress HTTP origin")
	staticDir := flag.String("static", ".", "Path to web-player static assets directory")
	flag.Parse()

	if env := os.Getenv("EGRESS_ORIGIN"); env != "" {
		*upstream = env
	}

	cfg := &Config{}
	if err := cfg.SetUpstream(*upstream); err != nil {
		log.Fatalf("invalid upstream origin: %v", err)
	}

	absStatic, err := filepath.Abs(*staticDir)
	if err != nil {
		absStatic = "."
	}
	fileServer := http.FileServer(http.Dir(absStatic))

	mux := http.NewServeMux()

	// CORS wrapper
	withCORS := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			w.Header().Set("Access-Control-Expose-Headers", "*")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	// 1. Upstream configuration API
	mux.HandleFunc("/api/upstream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			json.NewEncoder(w).Encode(map[string]any{"upstream": cfg.GetUpstream()})
			return
		}
		if r.Method == http.MethodPost {
			var body struct {
				Upstream string `json:"upstream"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Upstream == "" {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": "invalid payload"})
				return
			}
			if err := cfg.SetUpstream(body.Upstream); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			log.Printf("[Config] Upstream origin updated to: %s", cfg.GetUpstream())
			json.NewEncoder(w).Encode(map[string]any{"success": true, "upstream": cfg.GetUpstream()})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	// 2. Health & Status probe API
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		room := r.URL.Query().Get("room")
		if room == "" {
			room = "room_android_smoke"
		}
		currentUpstream := cfg.GetUpstream()
		probeURL := fmt.Sprintf("%s/live/%s/master.m3u8", currentUpstream, room)
		t0 := time.Now()

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"status": "error", "error": err.Error()})
			return
		}
		res, err := http.DefaultClient.Do(req)
		pingMs := time.Since(t0).Milliseconds()
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{
				"status":   "error",
				"upstream": currentUpstream,
				"probeUrl": probeURL,
				"online":   false,
				"error":    err.Error(),
			})
			return
		}
		defer res.Body.Close()

		json.NewEncoder(w).Encode(map[string]any{
			"status":     "ok",
			"upstream":   currentUpstream,
			"probeUrl":   probeURL,
			"statusCode": res.StatusCode,
			"online":     res.StatusCode == http.StatusOK,
			"pingMs":     pingMs,
		})
	})

	// 3. Egress Reverse Proxy routes
	mux.Handle("/live/", cfg)
	mux.Handle("/media/", cfg)

	// 4. Static Files
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".m3u8") || strings.HasSuffix(r.URL.Path, ".m4s") {
			cfg.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-cache")
		fileServer.ServeHTTP(w, r)
	})

	addr := fmt.Sprintf(":%d", *port)
	fmt.Println("================================================================")
	fmt.Println("   CONNECTS EGRESS LOW-LATENCY HLS (< 2.0s) WEB PLAYER (GO)    ")
	fmt.Println("================================================================")
	fmt.Printf(" Web Player UI:       http://localhost:%d\n", *port)
	fmt.Printf(" Egress Upstream:     %s\n", cfg.GetUpstream())
	fmt.Printf(" Smart Proxy Prefix:  http://localhost:%d/live/...\n", *port)
	fmt.Println("================================================================")
	fmt.Println(" Ready for sub-2-second LL-HLS playback.")

	if err := http.ListenAndServe(addr, withCORS(mux)); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
