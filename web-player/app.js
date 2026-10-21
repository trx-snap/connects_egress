/**
 * Connects Egress LL-HLS Web Player Controller
 * 
 * Strict sub-2-second Low-Latency HLS playback using Hls.js with part-level streaming,
 * live-sync clock regulation, real-time telemetry, and canvas sparkline visualization.
 */

// DOM Elements
const video = document.getElementById('videoPlayer');
const connectBtn = document.getElementById('connectBtn');
const egressOriginInput = document.getElementById('egressOriginInput');
const roomIdInput = document.getElementById('roomIdInput');
const connectionModeSelect = document.getElementById('connectionModeSelect');
const latencyProfileSelect = document.getElementById('latencyProfileSelect');
const upstreamStatusText = document.getElementById('upstreamStatusText');
const radarDot = document.getElementById('radarDot');
const videoLiveDot = document.getElementById('videoLiveDot');
const headerLatencyBadge = document.getElementById('headerLatencyBadge');
const liveJumpBtn = document.getElementById('liveJumpBtn');
const liveJumpText = document.getElementById('liveJumpText');
const overlayLatencyVal = document.getElementById('overlayLatencyVal');
const playPauseBtn = document.getElementById('playPauseBtn');
const playIcon = document.getElementById('playIcon');
const pauseIcon = document.getElementById('pauseIcon');
const muteBtn = document.getElementById('muteBtn');
const volumeHighIcon = document.getElementById('volumeHighIcon');
const volumeMuteIcon = document.getElementById('volumeMuteIcon');
const volumeSlider = document.getElementById('volumeSlider');
const abrSelect = document.getElementById('abrSelect');
const fullscreenBtn = document.getElementById('fullscreenBtn');
const activePlaylistDisplay = document.getElementById('activePlaylistDisplay');
const currentTimeDisplay = document.getElementById('currentTimeDisplay');
const syncStatusDisplay = document.getElementById('syncStatusDisplay');

// KPI Elements
const kpiLatencyCard = document.getElementById('kpiLatencyCard');
const kpiLatencyVal = document.getElementById('kpiLatencyVal');
const kpiLatencyNote = document.getElementById('kpiLatencyNote');
const kpiSpeedVal = document.getElementById('kpiSpeedVal');
const kpiSpeedStatus = document.getElementById('kpiSpeedStatus');
const kpiBufferVal = document.getElementById('kpiBufferVal');
const kpiResolutionVal = document.getElementById('kpiResolutionVal');
const kpiBitrateVal = document.getElementById('kpiBitrateVal');
const specPdtAgeVal = document.getElementById('specPdtAgeVal');
const specCodecsDisplay = document.getElementById('specCodecsDisplay');
const eventTerminal = document.getElementById('eventTerminal');
const latencyCanvas = document.getElementById('latencyChart');

// Latency Profiles for strictly maintaining < 2.0s
const PROFILES = {
  ultra: {
    target: 1.0,      // 1.0s target latency
    max: 1.4,         // catch-up triggers at 1.4s
    maxRate: 1.15,    // 15% acceleration catch-up
    minRate: 0.95,    // 5% deceleration brake
  },
  balanced: {
    target: 1.25,     // 1.25s target latency
    max: 1.8,         // catch-up triggers at 1.8s (strictly < 2.0s)
    maxRate: 1.12,    // 12% acceleration catch-up
    minRate: 0.95,
  },
  safe: {
    target: 1.9,      // 1.9s target latency
    max: 2.4,
    maxRate: 1.08,
    minRate: 0.95,
  },
};

let hls = null;
let telemetryTimer = null;
let statusProbeTimer = null;
const latencyHistory = [];
const MAX_HISTORY_POINTS = 60; // 60 data points (~12 seconds history)

// Chart rendering setup
const ctx = latencyCanvas.getContext('2d');
function resizeCanvas() {
  const rect = latencyCanvas.parentElement.getBoundingClientRect();
  latencyCanvas.width = rect.width * window.devicePixelRatio;
  latencyCanvas.height = rect.height * window.devicePixelRatio;
  ctx.scale(window.devicePixelRatio, window.devicePixelRatio);
}
window.addEventListener('resize', resizeCanvas);
resizeCanvas();

/**
 * Log message to internal terminal UI
 */
function logEvent(tag, message, type = 'info') {
  const line = document.createElement('div');
  line.className = 'terminal-line';

  const timeStr = new Date().toTimeString().split(' ')[0];
  const tagClass = `terminal-tag ${type}`;

  line.innerHTML = `
    <span class="terminal-time">[${timeStr}]</span>
    <span class="${tagClass}">${tag}</span>
    <span class="terminal-msg">${message}</span>
  `;

  eventTerminal.appendChild(line);
  // Auto-scroll
  eventTerminal.scrollTop = eventTerminal.scrollHeight;

  // Trim to max 100 lines
  if (eventTerminal.children.length > 100) {
    eventTerminal.removeChild(eventTerminal.children[0]);
  }
}

/**
 * Calculate forward buffer length in seconds
 */
function getForwardBuffer() {
  if (!video || !video.buffered || video.buffered.length === 0) return 0;
  const current = video.currentTime;
  for (let i = 0; i < video.buffered.length; i++) {
    const start = video.buffered.start(i);
    const end = video.buffered.end(i);
    if (current >= start && current <= end) {
      return end - current;
    }
  }
  return 0;
}

/**
 * Build stream URL based on mode
 */
function getStreamUrl() {
  const origin = egressOriginInput.value.trim().replace(/\/+$/, '');
  const room = roomIdInput.value.trim();
  const mode = connectionModeSelect.value;

  if (mode === 'proxy') {
    // Relative URL to our local proxy
    return `/live/${encodeURIComponent(room)}/master.m3u8`;
  }
  return `${origin}/live/${encodeURIComponent(room)}/master.m3u8`;
}

/**
 * Sync upstream setting with local server
 */
async function syncUpstreamServer() {
  const origin = egressOriginInput.value.trim().replace(/\/+$/, '');
  try {
    const res = await fetch('/api/upstream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ upstream: origin }),
    });
    if (res.ok) {
      logEvent('PROXY', `Upstream origin synchronized: ${origin}`);
    }
  } catch (err) {
    // Running in direct static file mode without Node proxy
  }
}

/**
 * Health check probe
 */
async function probeEgressStatus() {
  const room = roomIdInput.value.trim();
  try {
    const res = await fetch(`/api/status?room=${encodeURIComponent(room)}`);
    if (res.ok) {
      const data = await res.json();
      if (data.online) {
        radarDot.className = 'radar-dot live';
        upstreamStatusText.textContent = `Online (${data.pingMs}ms)`;
        upstreamStatusText.style.color = 'var(--accent-emerald)';
      } else {
        radarDot.className = 'radar-dot';
        upstreamStatusText.textContent = data.statusCode ? `Origin HTTP ${data.statusCode}` : 'Waiting for stream';
        upstreamStatusText.style.color = 'var(--text-secondary)';
      }
    }
  } catch (e) {
    upstreamStatusText.textContent = 'Proxy active';
  }
}

/**
 * Initialize Hls.js with strict low-latency parameters
 */
function initHls() {
  if (hls) {
    hls.destroy();
    hls = null;
  }

  const profileKey = latencyProfileSelect.value;
  const profile = PROFILES[profileKey] || PROFILES.balanced;
  const streamUrl = getStreamUrl();

  logEvent('HLS_INIT', `Configuring LL-HLS engine (Target: ${profile.target}s, Max: ${profile.max}s)`);

  const hlsConfig = {
    enableWorker: true,
    // CRITICAL: Low Latency HLS mode enables EXT-X-PART loading and delivery directives
    lowLatencyMode: true,
    liveSyncDuration: profile.target,
    liveMaxLatencyDuration: profile.max,
    maxLiveSyncPlaybackRate: profile.maxRate,
    minLiveSyncPlaybackRate: profile.minRate,
    liveDurationInfinity: true,
    backBufferLength: 8,
    maxBufferLength: 3,
    maxMaxBufferLength: 5,
    highBufferWatchdogPeriod: 1,
    nudgeOffset: 0.05,
    nudgeMaxRetry: 5,
    manifestLoadingTimeOut: 8000,
    manifestLoadingMaxRetry: 4,
    levelLoadingTimeOut: 8000,
    fragLoadingTimeOut: 12000,
  };

  if (!Hls.isSupported()) {
    if (video.canPlayType('application/vnd.apple.mpegurl')) {
      logEvent('SAFARI', 'Using native Safari LL-HLS engine');
      video.src = streamUrl;
      video.play().catch(e => console.log('Auto-play muted wait:', e));
      return;
    }
    logEvent('ERROR', 'Hls.js is not supported on this browser', 'err');
    alert('MSE/Hls.js is not supported in this browser.');
    return;
  }

  hls = new Hls(hlsConfig);
  hls.attachMedia(video);

  hls.on(Hls.Events.MEDIA_ATTACHED, () => {
    logEvent('ATTACH', `Loading manifest: ${streamUrl}`);
    activePlaylistDisplay.textContent = streamUrl.split('/').pop() || 'master.m3u8';
    hls.loadSource(streamUrl);
  });

  hls.on(Hls.Events.MANIFEST_PARSED, (event, data) => {
    logEvent('MANIFEST', `Manifest parsed with ${data.levels.length} renditions`, 'seg');

    // Populate ABR Selector
    abrSelect.innerHTML = '<option value="-1">AUTO (ABR)</option>';
    data.levels.forEach((lvl, idx) => {
      const opt = document.createElement('option');
      opt.value = idx;
      const height = lvl.height || (idx === 0 ? 1280 : (idx === 1 ? 1024 : 640));
      const width = lvl.width || (idx === 0 ? 720 : (idx === 1 ? 576 : 360));
      const kbps = Math.round(lvl.bitrate / 1000) || (idx === 0 ? 2500 : (idx === 1 ? 1600 : 800));
      const name = idx === 0 ? 'vhi' : (idx === 1 ? 'vmid' : 'vlo');
      opt.textContent = `${name} (${width}x${height} @ ${kbps}k)`;
      abrSelect.appendChild(opt);
    });

    if (data.levels[0]) {
      const vCodec = data.levels[0].videoCodec || 'avc1.64001f';
      const aCodec = data.levels[0].audioCodec || 'mp4a.40.2';
      specCodecsDisplay.textContent = `${vCodec}, ${aCodec}`;
    }

    video.play().then(() => {
      updatePlayPauseIcons();
    }).catch((err) => {
      logEvent('AUTOPLAY', 'Playback muted waiting for user interaction', 'warn');
    });
  });

  hls.on(Hls.Events.LEVEL_SWITCHED, (event, data) => {
    const level = hls.levels[data.level];
    if (level) {
      const w = level.width || 720;
      const h = level.height || 1280;
      const kbps = Math.round(level.bitrate / 1000);
      const name = data.level === 0 ? 'vhi' : (data.level === 1 ? 'vmid' : 'vlo');
      kpiResolutionVal.textContent = `${w}\u00D7${h}`;
      kpiBitrateVal.textContent = `${kbps.toLocaleString()} kbps (${name})`;
      logEvent('ABR', `Active rung: ${name} (${w}x${h} @ ${kbps}kbps)`);
    }
  });

  // Track LL-HLS Part delivery
  hls.on(Hls.Events.FRAG_LOADED, (event, data) => {
    const frag = data.frag;
    const isPart = frag.part !== undefined && frag.part >= 0;
    const duration = frag.duration.toFixed(3);
    const msn = frag.sn;
    const partIdx = frag.part;

    if (isPart) {
      logEvent('PART', `Part ${msn}.${partIdx} loaded (${duration}s)`, 'part');
    } else {
      logEvent('SEG', `Full segment ${msn} loaded (${duration}s)`, 'seg');
    }
    checkProtocol();
  });

function checkProtocol() {
  try {
    const entries = performance.getEntriesByType('resource');
    for (let i = entries.length - 1; i >= 0; i--) {
      const entry = entries[i];
      if (entry.name.includes('.m3u8') || entry.name.includes('.m4s') || entry.name.includes('localhost') || entry.name.includes('192.168')) {
        const proto = entry.nextHopProtocol;
        if (proto) {
          const protocolBadge = document.getElementById('protocolBadge');
          const specTransportVal = document.getElementById('specTransportVal');
          if (proto.startsWith('h3')) {
            if (protocolBadge) {
              protocolBadge.textContent = 'HTTP/3 (QUIC)';
              protocolBadge.className = 'badge-pill badge-h3';
            }
            if (specTransportVal) specTransportVal.textContent = 'HTTP/3 (QUIC / UDP)';
          } else if (proto === 'h2') {
            if (protocolBadge) protocolBadge.textContent = 'HTTP/2';
            if (specTransportVal) specTransportVal.textContent = 'HTTP/2 (TLS)';
          }
          break;
        }
      }
    }
  } catch (e) {}
}


  hls.on(Hls.Events.ERROR, (event, data) => {
    if (data.fatal) {
      logEvent('FATAL', `Fatal error: ${data.type} - ${data.details}`, 'err');
      switch (data.type) {
        case Hls.ErrorTypes.NETWORK_ERROR:
          logEvent('RETRY', 'Attempting network recovery in 1s...', 'warn');
          setTimeout(() => hls && hls.startLoad(), 1000);
          break;
        case Hls.ErrorTypes.MEDIA_ERROR:
          logEvent('RETRY', 'Attempting media recovery...', 'warn');
          hls.recoverMediaError();
          break;
        default:
          initHls();
          break;
      }
    } else {
      if (data.details.includes('bufferStalledError')) {
        logEvent('STALL', 'Buffer depleted: playhead caught live edge', 'warn');
      }
    }
  });
}

/**
 * Real-time telemetry monitoring loop (runs at 5Hz = 200ms)
 */
function updateTelemetry() {
  if (!video) return;

  currentTimeDisplay.textContent = `${video.currentTime.toFixed(2)}s`;

  let currentLatency = null;
  if (hls && typeof hls.latency === 'number' && !isNaN(hls.latency)) {
    currentLatency = hls.latency;
  } else if (hls && hls.liveSyncPosition) {
    currentLatency = Math.max(0, hls.liveSyncPosition - video.currentTime);
  }

  const forwardBuffer = getForwardBuffer();
  kpiBufferVal.textContent = forwardBuffer.toFixed(2);

  // Playback catch-up speed status
  const speed = video.playbackRate;
  kpiSpeedVal.textContent = speed.toFixed(2);
  if (speed > 1.02) {
    kpiSpeedStatus.textContent = 'Catching Up (Accelerating)';
    kpiSpeedStatus.style.color = 'var(--accent-cyan)';
  } else if (speed < 0.98) {
    kpiSpeedStatus.textContent = 'Braking (Decelerating)';
    kpiSpeedStatus.style.color = 'var(--accent-amber)';
  } else {
    kpiSpeedStatus.textContent = 'Target Lock (Synchronized)';
    kpiSpeedStatus.style.color = 'var(--accent-emerald)';
  }

  // Program Date Time (PDT) wall-clock latency
  if (hls && hls.playingDate) {
    const pdtTime = hls.playingDate.getTime();
    const wallLatency = Math.max(0, (Date.now() - pdtTime) / 1000);
    specPdtAgeVal.textContent = `${wallLatency.toFixed(2)} s`;
  }

  // Latency Evaluation
  if (currentLatency !== null) {
    const latStr = currentLatency.toFixed(2);
    kpiLatencyVal.textContent = latStr;
    overlayLatencyVal.textContent = `${latStr} s`;

    // Visual classification against < 2.0s ceiling
    if (currentLatency < 1.5) {
      kpiLatencyCard.className = 'kpi-card hero-latency glass';
      kpiLatencyVal.style.color = 'var(--accent-emerald)';
      overlayLatencyVal.style.color = 'var(--accent-emerald)';
      headerLatencyBadge.className = 'badge-pill badge-target';
      headerLatencyBadge.textContent = `TARGET MET (${latStr}s)`;
      kpiLatencyNote.innerHTML = `<span style="color: var(--accent-emerald);">&bull; Optimal LL-HLS performance (&lt; 1.5s)</span>`;
      liveJumpBtn.classList.remove('behind');
    } else if (currentLatency <= 2.0) {
      kpiLatencyCard.className = 'kpi-card hero-latency warning glass';
      kpiLatencyVal.style.color = 'var(--accent-amber)';
      overlayLatencyVal.style.color = 'var(--accent-amber)';
      headerLatencyBadge.className = 'badge-pill badge-target warning';
      headerLatencyBadge.textContent = `UNDER 2.0s (${latStr}s)`;
      kpiLatencyNote.innerHTML = `<span style="color: var(--accent-amber);">&bull; Within sub-2s threshold</span>`;
      liveJumpBtn.classList.remove('behind');
    } else {
      kpiLatencyCard.className = 'kpi-card hero-latency danger glass';
      kpiLatencyVal.style.color = 'var(--accent-rose)';
      overlayLatencyVal.style.color = 'var(--accent-rose)';
      headerLatencyBadge.className = 'badge-pill badge-target danger';
      headerLatencyBadge.textContent = `EXCEEDING (${latStr}s)`;
      kpiLatencyNote.innerHTML = `<span style="color: var(--accent-rose);">&bull; Catch-up clock accelerating...</span>`;
      liveJumpBtn.classList.add('behind');
    }

    // Push into chart history
    latencyHistory.push(currentLatency);
    if (latencyHistory.length > MAX_HISTORY_POINTS) {
      latencyHistory.shift();
    }
    drawLatencyChart();
  }
}

/**
 * Draw the real-time latency chart on HTML5 canvas
 */
function drawLatencyChart() {
  const w = latencyCanvas.parentElement.clientWidth;
  const h = latencyCanvas.parentElement.clientHeight;
  ctx.clearRect(0, 0, w, h);

  if (latencyHistory.length < 2) return;

  const maxLatencyScale = 3.0; // Y axis top = 3.0s
  const thresholdY = h - (2.0 / maxLatencyScale) * (h - 20) - 10;

  // 1. Grid background
  ctx.strokeStyle = 'rgba(255, 255, 255, 0.05)';
  ctx.lineWidth = 1;
  for (let s = 0.5; s <= 2.5; s += 0.5) {
    const y = h - (s / maxLatencyScale) * (h - 20) - 10;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(w, y);
    ctx.stroke();

    ctx.fillStyle = 'rgba(255, 255, 255, 0.25)';
    ctx.font = '10px "JetBrains Mono"';
    ctx.fillText(`${s.toFixed(1)}s`, 8, y - 4);
  }

  // 2. Critical 2.0s Red Ceiling Line
  ctx.strokeStyle = '#f43f5e';
  ctx.lineWidth = 1.5;
  ctx.setLineDash([4, 4]);
  ctx.beginPath();
  ctx.moveTo(0, thresholdY);
  ctx.lineTo(w, thresholdY);
  ctx.stroke();
  ctx.setLineDash([]);

  ctx.fillStyle = '#f43f5e';
  ctx.font = 'bold 10px "JetBrains Mono"';
  ctx.fillText('2.0s CEILING', w - 75, thresholdY - 4);

  // 3. Latency Line & Gradient Area
  const stepX = w / (MAX_HISTORY_POINTS - 1);
  const startOffset = MAX_HISTORY_POINTS - latencyHistory.length;

  ctx.beginPath();
  latencyHistory.forEach((val, idx) => {
    const x = (startOffset + idx) * stepX;
    const clamped = Math.min(val, maxLatencyScale);
    const y = h - (clamped / maxLatencyScale) * (h - 20) - 10;
    if (idx === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });

  // Stroke line
  ctx.strokeStyle = '#06b6d4';
  ctx.lineWidth = 2.5;
  ctx.stroke();

  // Gradient fill under curve
  const lastX = (startOffset + latencyHistory.length - 1) * stepX;
  const firstX = startOffset * stepX;
  ctx.lineTo(lastX, h);
  ctx.lineTo(firstX, h);
  ctx.closePath();

  const grad = ctx.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, 'rgba(6, 182, 212, 0.25)');
  grad.addColorStop(1, 'rgba(6, 182, 212, 0)');
  ctx.fillStyle = grad;
  ctx.fill();

  // Current point highlight
  const latestVal = latencyHistory[latencyHistory.length - 1];
  const latestY = h - (Math.min(latestVal, maxLatencyScale) / maxLatencyScale) * (h - 20) - 10;
  ctx.beginPath();
  ctx.arc(lastX, latestY, 4, 0, Math.PI * 2);
  ctx.fillStyle = latestVal <= 2.0 ? '#10b981' : '#f43f5e';
  ctx.fill();
  ctx.strokeStyle = '#ffffff';
  ctx.lineWidth = 1.5;
  ctx.stroke();
}

/**
 * Instant Jump to Live Edge
 */
function jumpToLiveEdge() {
  if (!hls || !video) return;
  const targetTime = hls.liveSyncPosition || (video.duration - 1.2);
  if (targetTime > 0) {
    logEvent('SEEK', `Snapping playhead to live edge (${targetTime.toFixed(2)}s)`);
    video.currentTime = targetTime;
    video.playbackRate = 1.0;
  }
}

/**
 * UI Controls Binding
 */
function updatePlayPauseIcons() {
  if (video.paused) {
    playIcon.style.display = 'block';
    pauseIcon.style.display = 'none';
  } else {
    playIcon.style.display = 'none';
    pauseIcon.style.display = 'block';
  }
}

playPauseBtn.addEventListener('click', () => {
  if (video.paused) {
    video.play();
  } else {
    video.pause();
  }
  updatePlayPauseIcons();
});

video.addEventListener('play', updatePlayPauseIcons);
video.addEventListener('pause', updatePlayPauseIcons);

muteBtn.addEventListener('click', () => {
  video.muted = !video.muted;
  if (video.muted) {
    volumeHighIcon.style.display = 'none';
    volumeMuteIcon.style.display = 'block';
    volumeSlider.value = 0;
  } else {
    volumeHighIcon.style.display = 'block';
    volumeMuteIcon.style.display = 'none';
    volumeSlider.value = video.volume || 1;
  }
});

volumeSlider.addEventListener('input', (e) => {
  const val = parseFloat(e.target.value);
  video.volume = val;
  video.muted = val === 0;
  if (video.muted) {
    volumeHighIcon.style.display = 'none';
    volumeMuteIcon.style.display = 'block';
  } else {
    volumeHighIcon.style.display = 'block';
    volumeMuteIcon.style.display = 'none';
  }
});

abrSelect.addEventListener('change', (e) => {
  if (!hls) return;
  const levelIdx = parseInt(e.target.value, 10);
  hls.currentLevel = levelIdx;
  logEvent('ABR_MANUAL', levelIdx === -1 ? 'AUTO ladder selection enabled' : `Manual lock to level index ${levelIdx}`);
});

fullscreenBtn.addEventListener('click', () => {
  const wrapper = video.parentElement;
  if (!document.fullscreenElement) {
    if (wrapper.requestFullscreen) wrapper.requestFullscreen();
    else if (video.webkitEnterFullscreen) video.webkitEnterFullscreen();
  } else {
    document.exitFullscreen();
  }
});

liveJumpBtn.addEventListener('click', jumpToLiveEdge);

connectBtn.addEventListener('click', async () => {
  await syncUpstreamServer();
  initHls();
});

latencyProfileSelect.addEventListener('change', () => {
  initHls();
});

// Preset Chips
document.querySelectorAll('.chip').forEach(chip => {
  chip.addEventListener('click', async () => {
    document.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
    chip.classList.add('active');

    egressOriginInput.value = chip.dataset.origin;
    roomIdInput.value = chip.dataset.room;
    await syncUpstreamServer();
    initHls();
  });
});

// Initialization
(async function startup() {
  await syncUpstreamServer();
  initHls();

  // Start telemetry loop (200ms)
  telemetryTimer = setInterval(updateTelemetry, 200);

  // Start health status probe (3s)
  probeEgressStatus();
  statusProbeTimer = setInterval(probeEgressStatus, 3000);
})();
