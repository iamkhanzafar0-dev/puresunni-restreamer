const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const express = require('express');
const NodeMediaServer = require('node-media-server');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const RTMP_PORT = Number(process.env.RTMP_PORT) || 1935;
const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;
const INGEST_KEY = process.env.INGEST_STREAM_KEY || crypto.randomBytes(18).toString('hex');
const DESTINATIONS_FILE = path.resolve(process.env.DESTINATIONS_FILE || path.join(__dirname, 'data', 'destinations.json'));

if (!ADMIN_PASSWORD || ADMIN_PASSWORD.length < 12) {
  console.error('Set ADMIN_PASSWORD to a value at least 12 characters long before starting the server.');
  process.exit(1);
}

const app = express();
const httpServer = require('node:http').createServer(app);
const wsServer = new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 * 1024, perMessageDeflate: false });
let destinations = [];
let currentSource = null;
let outputPaused = false;
let sourceStartedAt = null;
let activeWebSocket = null;
const outputRunners = new Map();

function readDestinations() {
  try {
    const parsed = JSON.parse(fs.readFileSync(DESTINATIONS_FILE, 'utf8'));
    if (!Array.isArray(parsed)) throw new Error('Expected a JSON array');
    destinations = parsed.map(validateDestination);
  } catch (error) {
    if (error.code === 'ENOENT') {
      fs.mkdirSync(path.dirname(DESTINATIONS_FILE), { recursive: true });
      fs.writeFileSync(DESTINATIONS_FILE, '[]\n', { mode: 0o600 });
      destinations = [];
      return;
    }
    throw new Error(`Could not load destinations file: ${error.message}`);
  }
}

function saveDestinations(items = destinations) {
  fs.mkdirSync(path.dirname(DESTINATIONS_FILE), { recursive: true });
  const tempFile = `${DESTINATIONS_FILE}.${process.pid}.tmp`;
  fs.writeFileSync(tempFile, `${JSON.stringify(items, null, 2)}\n`, { mode: 0o600 });
  fs.renameSync(tempFile, DESTINATIONS_FILE);
}

function validateDestination(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Each destination must be an object');
  }
  const { id, platform, name, url, key, active } = value;
  if (typeof id !== 'string' || !/^[\w-]{1,80}$/.test(id)) throw new Error('Invalid destination id');
  if (typeof platform !== 'string' || platform.length > 40) throw new Error('Invalid platform');
  if (typeof name !== 'string' || !name.trim() || name.length > 100) throw new Error('Channel name is required (max 100 characters)');
  if (typeof url !== 'string' || url.length > 2048) throw new Error('Invalid RTMP URL');
  let parsedUrl;
  try {
    parsedUrl = new URL(url);
  } catch {
    throw new Error('Enter a valid RTMP or RTMPS server URL');
  }
  if (!['rtmp:', 'rtmps:'].includes(parsedUrl.protocol) || parsedUrl.username || parsedUrl.password) {
    throw new Error('Only RTMP/RTMPS destination URLs without embedded credentials are allowed');
  }
  if (typeof key !== 'string' || !key.trim() || key.length > 512 || /[\r\n]/.test(key)) {
    throw new Error('Stream key is required (max 512 characters)');
  }
  return {
    id,
    platform: platform.trim(),
    name: name.trim(),
    url: url.trim(),
    key: key.trim(),
    active: active === true
  };
}

function authorized(req, res, next) {
  const header = req.headers.authorization || '';
  const match = header.match(/^Basic\s+(.+)$/i);
  if (match) {
    try {
      const [username, ...passwordParts] = Buffer.from(match[1], 'base64').toString().split(':');
      const password = passwordParts.join(':');
      const userOk = username === ADMIN_USER;
      const expected = Buffer.from(ADMIN_PASSWORD);
      const received = Buffer.from(password);
      const passwordOk = expected.length === received.length && crypto.timingSafeEqual(expected, received);
      if (userOk && passwordOk) return next();
    } catch {
      console.warn('Rejected malformed authorization header');
    }
  }
  res.set('WWW-Authenticate', 'Basic realm="PureSunni Studio", charset="UTF-8"');
  res.status(401).send('Authentication required');
}

function outputUrl(destination) {
  return `${destination.url.replace(/\/+$/, '')}/${encodeURIComponent(destination.key)}`;
}

function getPublicStatus(req) {
  const runners = destinations.map((destination) => {
    const runner = outputRunners.get(destination.id);
    return {
      id: destination.id,
      state: !destination.active ? 'OFF' : runner?.state || 'OFF',
      error: runner?.error || null,
      fps: runner?.fps ?? null,
      bitrate: runner?.bitrate ?? null
    };
  });
  const activeRunners = runners.filter((runner) => runner.state === 'LIVE');
  return {
    online: Boolean(currentSource),
    source: currentSource?.type || null,
    startedAt: sourceStartedAt,
    paused: outputPaused,
    ingestKey: INGEST_KEY,
    rtmpUrl: process.env.PUBLIC_RTMP_URL || `rtmp://${req.hostname}:${RTMP_PORT}/live`,
    websocketUrl: process.env.PUBLIC_WS_URL || `${req.get('x-forwarded-proto')?.split(',')[0].trim() === 'https' ? 'wss' : 'ws'}://${req.get('host')}/ingest/ws?key=${encodeURIComponent(INGEST_KEY)}`,
    outputs: runners,
    stats: {
      fps: activeRunners.length ? Math.round(activeRunners.reduce((sum, item) => sum + (item.fps || 0), 0) / activeRunners.length) : null,
      bitrate: activeRunners.reduce((sum, item) => sum + (item.bitrate || 0), 0)
    }
  };
}

function launchOutput(destination, source) {
  const existing = outputRunners.get(destination.id);
  if (existing?.child || existing?.retryTimer || !destination.active || outputPaused || currentSource !== source) return;

  const runner = existing || { state: 'CONNECTING', error: null, fps: null, bitrate: null, retries: 0 };
  outputRunners.set(destination.id, runner);
  runner.state = 'CONNECTING';
  runner.error = null;
  runner.fps = null;
  runner.bitrate = null;
  runner.totalSize = null;
  runner.outTimeUs = null;

  const args = ['-hide_banner', '-loglevel', 'warning'];
  if (source.type === 'rtmp') {
    args.push('-i', source.url);
  } else {
    args.push('-f', 'mpegts', '-i', 'pipe:0');
  }
  args.push(
    '-map', '0:v?', '-map', '0:a?',
    '-c:v', 'copy', '-c:a', 'copy',
    '-f', 'flv',
    '-stats_period', '1',
    '-progress', 'pipe:1',
    outputUrl(destination)
  );

  let child;
  try {
    child = spawn('ffmpeg', args, { stdio: ['pipe', 'pipe', 'pipe'] });
  } catch (error) {
    runner.error = error.message;
    runner.state = 'ERROR';
    scheduleRetry(destination, source, runner);
    return;
  }
  runner.child = child;
  runner.destinationConfig = JSON.stringify(destination);
  if (source.type === 'ws') {
    child.stdin.on('error', (error) => {
      runner.error = error.message;
      runner.state = 'ERROR';
    });
  }
  if (source.type === 'ws' && source.latestChunk) child.stdin.write(source.latestChunk);

  let progressBuffer = '';
  child.stdout.on('data', (chunk) => {
    progressBuffer += chunk.toString();
    const lines = progressBuffer.split(/\r?\n/);
    progressBuffer = lines.pop() || '';
    for (const line of lines) {
      const separator = line.indexOf('=');
      if (separator < 0) continue;
      const key = line.slice(0, separator);
      const value = line.slice(separator + 1);
      if (key === 'fps' && Number.isFinite(Number(value))) runner.fps = Number(value);
      if (key === 'bitrate') {
        const match = value.match(/^([\d.]+)\s*kbits\/s$/);
        if (match) runner.bitrate = Number(match[1]);
      }
      if (key === 'total_size' && Number.isFinite(Number(value))) runner.totalSize = Number(value);
      if ((key === 'out_time_us' || key === 'out_time_ms') && Number(value) > 0) runner.outTimeUs = Number(value);
      if (key === 'progress' && !runner.bitrate && runner.totalSize > 0 && runner.outTimeUs > 0) {
        runner.bitrate = (runner.totalSize * 8000) / runner.outTimeUs;
      }
      if (key === 'frame' && Number(value) > 0) {
        runner.state = 'LIVE';
        runner.retries = 0;
      }
    }
  });
  child.stderr.on('data', (chunk) => {
    const text = chunk.toString();
    const relevant = text.split(/\r?\n/).filter(Boolean).pop();
    if (relevant) runner.error = relevant.slice(0, 300);
  });
  child.on('error', (error) => {
    runner.error = error.message;
    runner.state = 'ERROR';
  });
  child.on('close', (code) => {
    if (runner.child !== child) return;
    runner.child = null;
    if (!currentSource || currentSource !== source || outputPaused || !destination.active) {
      runner.state = 'OFF';
      return;
    }
    if (code !== 0) runner.error = runner.error || `FFmpeg exited with code ${code}`;
    scheduleRetry(destination, source, runner);
  });
}

function scheduleRetry(destination, source, runner) {
  if (!currentSource || currentSource !== source || outputPaused || !destination.active) return;
  runner.state = 'ERROR';
  const delay = Math.min(30000, 1000 * (2 ** Math.min(runner.retries, 5)));
  runner.retries += 1;
  runner.retryTimer = setTimeout(() => {
    runner.retryTimer = null;
    launchOutput(destination, source);
  }, delay);
}

function startOutputs() {
  if (!currentSource || outputPaused) return;
  for (const destination of destinations) launchOutput(destination, currentSource);
}

function stopOutputs() {
  for (const runner of outputRunners.values()) {
    if (runner.retryTimer) clearTimeout(runner.retryTimer);
    runner.retryTimer = null;
    if (runner.child) {
      runner.child.stdin?.destroy();
      runner.child.kill('SIGTERM');
      runner.child = null;
    }
    runner.state = 'OFF';
  }
}

function beginSource(source) {
  if (currentSource) {
    console.warn(`Ignored additional ${source.type} source while one is already active`);
    return false;
  }
  currentSource = source;
  outputPaused = false;
  sourceStartedAt = new Date().toISOString();
  startOutputs();
  console.log(`Incoming ${source.type} stream started`);
  return true;
}

function endSource(source) {
  if (currentSource !== source) return;
  stopOutputs();
  currentSource = null;
  outputPaused = false;
  sourceStartedAt = null;
  console.log('Incoming stream ended');
}

function validRequestBody(req, res, next) {
  express.json({ limit: '128kb' })(req, res, (error) => {
    if (error) return res.status(400).json({ error: 'Invalid JSON body' });
    next();
  });
}

readDestinations();
app.get('/health', (_req, res) => res.json({ ok: true }));
app.use(authorized);
app.use(express.static(path.join(__dirname, 'public'), { index: false }));
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.get('/api/status', (req, res) => res.json(getPublicStatus(req)));
app.get('/api/destinations', (_req, res) => res.json(destinations));
app.put('/api/destinations', validRequestBody, (req, res) => {
  try {
    if (!Array.isArray(req.body)) return res.status(400).json({ error: 'Expected a destination array' });
    const updated = req.body.map(validateDestination);
    if (new Set(updated.map((item) => item.id)).size !== updated.length) {
      return res.status(400).json({ error: 'Destination IDs must be unique' });
    }
    const previousDestinations = destinations;
    saveDestinations(updated);
    destinations = updated;
    for (const [id, runner] of outputRunners) {
      const previous = previousDestinations.find((item) => item.id === id);
      const current = destinations.find((item) => item.id === id);
      if (!current?.active || !previous || JSON.stringify(previous) !== JSON.stringify(current)) {
        if (runner.retryTimer) clearTimeout(runner.retryTimer);
        runner.retryTimer = null;
        if (runner.child) {
          runner.child.stdin?.destroy();
          runner.child.kill('SIGTERM');
          runner.child = null;
        }
        runner.state = 'OFF';
      }
    }
    for (const id of outputRunners.keys()) {
      if (!destinations.some((item) => item.id === id)) outputRunners.delete(id);
    }
    if (currentSource && !outputPaused) startOutputs();
    res.json({ ok: true });
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
});
app.post('/api/start', validRequestBody, (_req, res) => {
  if (!currentSource) return res.status(409).json({ error: 'Start an incoming stream first' });
  outputPaused = false;
  startOutputs();
  res.json({ ok: true });
});
app.post('/api/stop', validRequestBody, (_req, res) => {
  outputPaused = true;
  stopOutputs();
  res.json({ ok: true });
});

httpServer.on('upgrade', (req, socket, head) => {
  let pathname;
  try {
    pathname = new URL(req.url, 'http://localhost').pathname;
  } catch {
    socket.destroy();
    return;
  }
  if (pathname !== '/ingest/ws') {
    socket.destroy();
    return;
  }
  wsServer.handleUpgrade(req, socket, head, (ws) => wsServer.emit('connection', ws, req));
});

wsServer.on('connection', (ws, req) => {
  const key = new URL(req.url, 'http://localhost').searchParams.get('key');
  if (key !== INGEST_KEY) {
    ws.close(1008, 'Invalid ingest key');
    return;
  }
  if (currentSource) {
    ws.close(1013, 'A stream is already active');
    return;
  }
  const source = { type: 'ws', latestChunk: null };
  if (!beginSource(source)) {
    ws.close(1013, 'A stream is already active');
    return;
  }
  activeWebSocket = ws;
  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      ws.close(1003, 'Send binary MPEG-TS data only');
      return;
    }
    source.latestChunk = Buffer.from(data);
    for (const runner of outputRunners.values()) {
      if (runner.child?.stdin?.writable) runner.child.stdin.write(data);
    }
  });
  ws.on('close', () => {
    if (activeWebSocket === ws) activeWebSocket = null;
    endSource(source);
  });
  ws.on('error', (error) => console.warn(`WebSocket ingest error: ${error.message}`));
});

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`Dashboard listening on port ${PORT}`);
  console.log(`RTMP ingest listening on port ${RTMP_PORT}`);
});

const mediaServer = new NodeMediaServer({
  rtmp: { port: RTMP_PORT, chunk_size: 60000, gop_cache: true, ping: 30, ping_timeout: 60 },
  http: { port: 0, allow_origin: '*' },
  logType: 0
});
mediaServer.on('postPublish', (id, streamPath) => {
  if (!streamPath.startsWith('/live/')) return;
  const key = streamPath.slice('/live/'.length);
  if (key !== INGEST_KEY) {
    mediaServer.getSession(id)?.reject();
    return;
  }
  const source = { type: 'rtmp', url: `rtmp://127.0.0.1:${RTMP_PORT}${streamPath}` };
  if (!beginSource(source)) mediaServer.getSession(id)?.reject();
  else currentSource.sessionId = id;
});
mediaServer.on('donePublish', (id, streamPath) => {
  if (currentSource?.type === 'rtmp' && currentSource.sessionId === id && streamPath.startsWith('/live/')) {
    endSource(currentSource);
  }
});
mediaServer.run();

function shutdown() {
  stopOutputs();
  if (activeWebSocket?.readyState === WebSocket.OPEN) activeWebSocket.close(1001, 'Server shutting down');
  mediaServer.stop();
  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
