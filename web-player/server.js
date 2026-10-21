#!/usr/bin/env node
/**
 * Connects Egress LL-HLS Web Player Server
 *
 * Provides:
 * 1. Static asset serving (HTML, CSS, JS, vendor files)
 * 2. High-performance streaming reverse proxy for `/live/*` and `/media/*` with full CORS support
 * 3. Dynamic upstream configuration and health check APIs
 *
 * Zero external dependencies — runs directly on Node.js standard library.
 */

const http = require('node:http');
const https = require('node:https');
const url = require('node:url');
const fs = require('node:fs');
const path = require('node:path');

// Configuration
let UPSTREAM_ORIGIN = process.env.EGRESS_ORIGIN || 'http://127.0.0.1:18080';
const PORT = parseInt(process.env.PORT || '3050', 10);
const HOST = process.env.HOST || '0.0.0.0';
const STATIC_DIR = __dirname;

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.mp4': 'video/mp4',
  '.m4s': 'video/mp4',
  '.m3u8': 'application/vnd.apple.mpegurl',
  '.ts': 'video/mp2t',
  '.map': 'application/json',
};

function setCorsHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Range, _HLS_msn, _HLS_part, _HLS_skip');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Content-Type, Age, Date, Server');
}

/**
 * Streaming proxy for LL-HLS playlist and media requests.
 * Preserves query params, chunked transfer encoding, and sets CORS headers.
 */
function proxyRequest(req, res, targetUrl) {
  setCorsHeaders(res);

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsedTarget = url.parse(targetUrl);
  const clientModule = parsedTarget.protocol === 'https:' ? https : http;

  const clientReqOptions = {
    protocol: parsedTarget.protocol,
    hostname: parsedTarget.hostname,
    port: parsedTarget.port || (parsedTarget.protocol === 'https:' ? 443 : 80),
    path: parsedTarget.path,
    method: req.method,
    headers: {
      ...req.headers,
      host: parsedTarget.host,
    },
  };

  // Remove hop-by-hop headers if needed
  delete clientReqOptions.headers['accept-encoding']; // Keep chunked/uncompressed for lowest latency

  const proxyReq = clientModule.request(clientReqOptions, (proxyRes) => {
    // Merge headers from upstream
    const responseHeaders = { ...proxyRes.headers };
    // Ensure CORS
    responseHeaders['access-control-allow-origin'] = '*';
    responseHeaders['access-control-allow-methods'] = 'GET, HEAD, OPTIONS';
    responseHeaders['access-control-allow-headers'] = '*';
    responseHeaders['access-control-expose-headers'] = '*';

    // Disable caching for playlists, cache media parts
    if (parsedTarget.path.includes('.m3u8')) {
      responseHeaders['cache-control'] = 'no-cache, no-store, must-revalidate';
    }

    res.writeHead(proxyRes.statusCode, responseHeaders);
    proxyRes.pipe(res);
  });

  proxyReq.on('error', (err) => {
    console.error(`[Proxy Error] ${req.method} ${targetUrl}:`, err.message);
    if (!res.headersSent) {
      setCorsHeaders(res);
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        error: 'Bad Gateway: failed to connect to upstream egress',
        upstream: UPSTREAM_ORIGIN,
        details: err.message,
      }));
    }
  });

  req.pipe(proxyReq);
}

/**
 * Serve static files from web-player folder.
 */
function serveStatic(req, res, pathname) {
  let relativePath = pathname === '/' ? '/index.html' : pathname;
  // Normalize and prevent path traversal
  const safePath = path.normalize(relativePath).replace(/^(\.\.[\/\\])+/, '');
  const filePath = path.join(STATIC_DIR, safePath);

  fs.stat(filePath, (err, stats) => {
    if (err || !stats.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('404 Not Found');
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    setCorsHeaders(res);
    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Length', stats.size);
    // Don't cache during development
    res.setHeader('Cache-Control', 'no-cache');

    const stream = fs.createReadStream(filePath);
    stream.pipe(res);
  });
}

// Create HTTP server
const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    setCorsHeaders(res);
    res.writeHead(204);
    res.end();
    return;
  }

  // 1. API: Upstream configuration
  if (pathname === '/api/upstream') {
    setCorsHeaders(res);
    if (req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ upstream: UPSTREAM_ORIGIN }));
      return;
    }
    if (req.method === 'POST') {
      let body = '';
      req.on('data', chunk => { body += chunk; });
      req.on('end', () => {
        try {
          const data = JSON.parse(body || '{}');
          if (data.upstream && typeof data.upstream === 'string') {
            UPSTREAM_ORIGIN = data.upstream.replace(/\/+$/, '');
            console.log(`[Config] Upstream origin updated to: ${UPSTREAM_ORIGIN}`);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, upstream: UPSTREAM_ORIGIN }));
          } else {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Missing or invalid "upstream" string' }));
          }
        } catch (e) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Malformed JSON' }));
        }
      });
      return;
    }
  }

  // 2. API: Health and status probe
  if (pathname === '/api/status') {
    setCorsHeaders(res);
    const checkRoom = parsedUrl.query.room || 'room_android_smoke';
    const probeUrl = `${UPSTREAM_ORIGIN}/live/${checkRoom}/master.m3u8`;
    const t0 = Date.now();

    const clientModule = probeUrl.startsWith('https:') ? https : http;
    const probeReq = clientModule.get(probeUrl, (probeRes) => {
      const pingMs = Date.now() - t0;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        upstream: UPSTREAM_ORIGIN,
        probeUrl,
        statusCode: probeRes.statusCode,
        online: probeRes.statusCode === 200,
        pingMs,
      }));
    });

    probeReq.on('error', (err) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'error',
        upstream: UPSTREAM_ORIGIN,
        probeUrl,
        online: false,
        error: err.message,
      }));
    });
    return;
  }

  // 3. Egress Reverse Proxy routes: `/live/*` and `/media/*`
  if (pathname.startsWith('/live/') || pathname.startsWith('/media/')) {
    const targetUrl = `${UPSTREAM_ORIGIN}${req.url}`;
    proxyRequest(req, res, targetUrl);
    return;
  }

  // 4. Static File Serving
  serveStatic(req, res, pathname);
});

server.listen(PORT, HOST, () => {
  console.log('================================================================');
  console.log('   CONNECTS EGRESS LOW-LATENCY HLS (< 2.0s) WEB PLAYER SERVER   ');
  console.log('================================================================');
  console.log(` Web Player UI:       http://localhost:${PORT}`);
  console.log(` Egress Upstream:     ${UPSTREAM_ORIGIN}`);
  console.log(` Smart Proxy Prefix:  http://localhost:${PORT}/live/...`);
  console.log('================================================================');
  console.log(' Ready for sub-2-second LL-HLS playback.');
});
