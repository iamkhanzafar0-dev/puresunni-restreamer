const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Transform } = require('node:stream');
const { pipeline } = require('node:stream/promises');
const { spawn } = require('node:child_process');
const express = require('express');

const PORT = Number(process.env.PORT) || 3000;
const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;
const GRAPH_API_VERSION = 'v22.0';
const CONFIG_FILE = path.resolve(process.env.CONFIG_FILE || path.join(__dirname, 'data', 'publisher-config.enc'));
const QUEUE_DIR = path.resolve(process.env.QUEUE_DATA_DIR || path.join(__dirname, 'data', 'video-queue'));
const QUEUE_STATE_FILE = path.join(QUEUE_DIR, 'queue_state.json');
const QUEUE_SOURCE_DIR = path.join(QUEUE_DIR, 'sources');
const QUEUE_OUTPUT_DIR = path.join(QUEUE_DIR, 'processed');
const QUEUE_TEMP_DIR = path.join(QUEUE_DIR, 'temp');
const CONFIG_ENCRYPTION_KEY = process.env.CONFIG_ENCRYPTION_KEY;
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL;
const FFMPEG_PATH = process.env.FFMPEG_PATH || 'ffmpeg';
const INSTAGRAM_MEDIA_DIR = path.join(os.tmpdir(), `puresunni-instagram-media-${process.pid}`);
const INSTAGRAM_MEDIA_TTL = 24 * 60 * 60 * 1000;
const MAX_INSTAGRAM_FILE_SIZE = 1024 ** 3 - 1;
const CONFIG_FIELDS = [
  'YOUTUBE_CLIENT_ID',
  'YOUTUBE_CLIENT_SECRET',
  'YOUTUBE_REFRESH_TOKEN',
  'FB_PAGE_ID',
  'FB_PAGE_ACCESS_TOKEN',
  'IG_USER_ID'
];

if (!ADMIN_PASSWORD || ADMIN_PASSWORD.length < 12) {
  console.error('Set ADMIN_PASSWORD to a value at least 12 characters long before starting the server.');
  process.exit(1);
}

if (CONFIG_ENCRYPTION_KEY && !/^[a-f\d]{64}$/i.test(CONFIG_ENCRYPTION_KEY)) {
  console.error('CONFIG_ENCRYPTION_KEY must be exactly 64 hexadecimal characters (32 bytes).');
  process.exit(1);
}

const app = express();
let savedConfig = {};
let cachedYouTubeToken = null;
const instagramMedia = new Map();
let queueState;
let isQueueProcessing = false;

fs.mkdirSync(INSTAGRAM_MEDIA_DIR, { recursive: true, mode: 0o700 });
fs.mkdirSync(QUEUE_SOURCE_DIR, { recursive: true, mode: 0o700 });
fs.mkdirSync(QUEUE_OUTPUT_DIR, { recursive: true, mode: 0o700 });
fs.mkdirSync(QUEUE_TEMP_DIR, { recursive: true, mode: 0o700 });

function saveQueueState() {
  const tempFile = `${QUEUE_STATE_FILE}.${process.pid}.tmp`;
  const descriptor = fs.openSync(tempFile, 'w', 0o600);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify(queueState, null, 2)}\n`);
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  fs.renameSync(tempFile, QUEUE_STATE_FILE);
  if (process.platform !== 'win32') {
    const directory = fs.openSync(QUEUE_DIR, 'r');
    try {
      fs.fsyncSync(directory);
    } finally {
      fs.closeSync(directory);
    }
  }
}

function loadQueueState() {
  if (!fs.existsSync(QUEUE_STATE_FILE)) return { logoEnabled: true, items: [] };
  const state = JSON.parse(fs.readFileSync(QUEUE_STATE_FILE, 'utf8'));
  if (!state || typeof state !== 'object' || !Array.isArray(state.items) ||
      state.items.some((item) => !item || typeof item !== 'object' ||
        typeof item.id !== 'string' || !/^[a-f\d-]{36}$/i.test(item.id))) {
    throw new Error('Saved video queue has an invalid format.');
  }
  state.logoEnabled = state.logoEnabled !== false;
  return state;
}

function queuePaths(id) {
  return {
    source: path.join(QUEUE_SOURCE_DIR, `${id}.input`),
    uploadTemp: path.join(QUEUE_TEMP_DIR, `${id}.upload`),
    output: path.join(QUEUE_OUTPUT_DIR, `${id}.mp4`),
    outputTemp: path.join(QUEUE_TEMP_DIR, `${id}.mp4`)
  };
}

function publicJob(item) {
  const job = { ...item };
  if (fs.existsSync(queuePaths(item.id).output)) {
    job.outputUrl = `/api/queue/jobs/${encodeURIComponent(item.id)}/output`;
  } else {
    job.outputUrl = '';
  }
  return job;
}

function recoverQueue() {
  for (const entry of fs.readdirSync(QUEUE_DIR)) {
    if (/^queue_state\.json\.\d+\.tmp$/.test(entry)) {
      fs.rmSync(path.join(QUEUE_DIR, entry), { force: true });
    }
  }
  for (const entry of fs.readdirSync(QUEUE_TEMP_DIR)) {
    fs.rmSync(path.join(QUEUE_TEMP_DIR, entry), { force: true, recursive: true });
  }
  for (const item of queueState.items) {
    const files = queuePaths(item.id);
    if (item.status === 'processing') {
      item.status = fs.existsSync(files.output) ? 'processed' : fs.existsSync(files.source) ? 'pending' : 'failed';
      if (item.status === 'failed') item.error = 'Processing was interrupted and the source file is unavailable.';
    } else if (item.status === 'uploading_source' || item.status === 'awaiting_upload') {
      item.status = fs.existsSync(files.source) ? 'pending' : 'failed';
      if (item.status === 'failed') item.error = 'Source upload was interrupted. Add the file again to retry.';
    } else if (item.status === 'uploading') {
      for (const [platform, state] of Object.entries(item.platformState || {})) {
        if (['Preparing', 'Uploading', 'Retrying', 'Publishing', 'Hosting'].includes(state)) {
          item.platformState[platform] = 'Failed';
          item.platformError[platform] = 'Publishing was interrupted; retry this platform after checking its status.';
        }
      }
      item.status = fs.existsSync(files.output) ? 'processed' : fs.existsSync(files.source) ? 'pending' : 'failed';
    } else if (item.status === 'processed' && !fs.existsSync(files.output)) {
      item.status = fs.existsSync(files.source) ? 'pending' : 'failed';
      if (item.status === 'failed') item.error = 'Processed output and source file are unavailable.';
    }
    if (item.status === 'processed' || ['complete', 'partial'].includes(item.status)) {
      fs.rmSync(files.source, { force: true });
    }
  }
  saveQueueState();
}

function processSingleJob(item) {
  return new Promise((resolve, reject) => {
    const files = queuePaths(item.id);
    if (!fs.existsSync(files.source)) return reject(new Error('Source file is unavailable.'));
    fs.rmSync(files.outputTemp, { force: true });
    const hasLogo = item.logoEnabled && fs.existsSync(path.join(__dirname, 'public', 'logo.png'));
    const args = ['-y', '-i', files.source];
    if (hasLogo) {
      args.push(
        '-i', path.join(__dirname, 'public', 'logo.png'),
        '-filter_complex', '[1:v]scale=110:-1[lg];[0:v][lg]overlay=W-w-24:24[vout]',
        '-map', '[vout]'
      );
    } else {
      args.push('-map', '0:v');
    }
    args.push(
      '-map', '0:a?',
      '-map_metadata', '0',
      '-c:v', 'libx264',
      '-preset', 'ultrafast',
      '-crf', '22',
      '-c:a', 'aac',
      '-b:a', '128k',
      '-movflags', '+faststart',
      files.outputTemp
    );

    const ffmpeg = spawn(FFMPEG_PATH, args, { stdio: ['ignore', 'ignore', 'pipe'] });
    let errLog = '';
    let settled = false;
    ffmpeg.stderr.on('data', (data) => {
      errLog = `${errLog}${data.toString()}`.slice(-4000);
    });
    ffmpeg.on('error', (error) => {
      if (settled) return;
      settled = true;
      reject(new Error(`Could not start FFmpeg (${FFMPEG_PATH}): ${error.message}`));
    });
    ffmpeg.on('close', (code) => {
      if (settled) return;
      settled = true;
      if (code === 0 && fs.existsSync(files.outputTemp) && fs.statSync(files.outputTemp).size > 0) {
        try {
          fs.renameSync(files.outputTemp, files.output);
          fs.chmodSync(files.output, 0o600);
          resolve();
        } catch (error) {
          reject(error);
        }
      } else {
        reject(new Error(errLog.slice(-400) || `FFmpeg exited with code ${code}.`));
      }
    });
  });
}

async function runQueue() {
  if (isQueueProcessing) return;
  isQueueProcessing = true;
  try {
    while (true) {
      const item = queueState.items.find((job) => job.status === 'pending');
      if (!item) break;
      item.status = 'processing';
      item.error = '';
      saveQueueState();
      try {
        await processSingleJob(item);
        item.status = 'processed';
        item.error = '';
        saveQueueState();
        try {
          fs.rmSync(queuePaths(item.id).source, { force: true });
        } catch (error) {
          console.error(`Could not remove processed source for job ${item.id}: ${error.message}`);
        }
      } catch (error) {
        fs.rmSync(queuePaths(item.id).outputTemp, { force: true });
        item.status = 'failed';
        item.error = error.message;
        saveQueueState();
      }
    }
  } finally {
    isQueueProcessing = false;
  }
}

function scheduleQueue() {
  runQueue().catch((error) => {
    console.error(`Video queue stopped: ${error.message}`);
    isQueueProcessing = false;
  });
}

queueState = loadQueueState();
recoverQueue();

function readConfig() {
  if (!fs.existsSync(CONFIG_FILE)) return;
  if (!CONFIG_ENCRYPTION_KEY) {
    throw new Error('CONFIG_ENCRYPTION_KEY is required to decrypt the saved publisher settings.');
  }
  const [version, ivText, tagText, encryptedText] = fs.readFileSync(CONFIG_FILE, 'utf8').trim().split('.');
  if (version !== 'v1' || !ivText || !tagText || !encryptedText) {
    throw new Error('Saved publisher settings have an invalid format.');
  }
  const decipher = crypto.createDecipheriv(
    'aes-256-gcm',
    Buffer.from(CONFIG_ENCRYPTION_KEY, 'hex'),
    Buffer.from(ivText, 'base64url')
  );
  decipher.setAuthTag(Buffer.from(tagText, 'base64url'));
  const plaintext = Buffer.concat([
    decipher.update(Buffer.from(encryptedText, 'base64url')),
    decipher.final()
  ]).toString('utf8');
  savedConfig = validateConfig(JSON.parse(plaintext));
}

function validateConfig(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Settings must be an object');
  }
  const validated = {};
  for (const field of CONFIG_FIELDS) {
    if (value[field] === undefined || value[field] === '') continue;
    if (typeof value[field] !== 'string' || value[field].length > 4096 || /[\r\n]/.test(value[field])) {
      throw new Error(`Invalid value for ${field}`);
    }
    validated[field] = value[field].trim();
  }
  return validated;
}

function effectiveConfig() {
  return Object.fromEntries(CONFIG_FIELDS.map((field) => [field, savedConfig[field] || process.env[field] || '']));
}

function saveConfig(value) {
  if (!CONFIG_ENCRYPTION_KEY) {
    const error = new Error('Set CONFIG_ENCRYPTION_KEY on the server before saving encrypted settings.');
    error.status = 503;
    throw error;
  }
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', Buffer.from(CONFIG_ENCRYPTION_KEY, 'hex'), iv);
  const encrypted = Buffer.concat([cipher.update(JSON.stringify(value), 'utf8'), cipher.final()]);
  const contents = [
    'v1',
    iv.toString('base64url'),
    cipher.getAuthTag().toString('base64url'),
    encrypted.toString('base64url')
  ].join('.');
  fs.mkdirSync(path.dirname(CONFIG_FILE), { recursive: true });
  const tempFile = `${CONFIG_FILE}.${process.pid}.tmp`;
  fs.writeFileSync(tempFile, `${contents}\n`, { mode: 0o600 });
  fs.renameSync(tempFile, CONFIG_FILE);
}

function authorized(req, res, next) {
  const match = (req.headers.authorization || '').match(/^Basic\s+(.+)$/i);
  if (match) {
    try {
      const decoded = Buffer.from(match[1], 'base64').toString();
      const separator = decoded.indexOf(':');
      const username = separator < 0 ? '' : decoded.slice(0, separator);
      const password = separator < 0 ? '' : decoded.slice(separator + 1);
      const expected = Buffer.from(ADMIN_PASSWORD);
      const received = Buffer.from(password);
      const userOk = username === ADMIN_USER;
      const passwordOk = expected.length === received.length && crypto.timingSafeEqual(expected, received);
      if (userOk && passwordOk) return next();
    } catch {
      // Malformed credentials are handled like any other failed login.
    }
  }
  res.set('WWW-Authenticate', 'Basic realm="PureSunni Publisher", charset="UTF-8"');
  res.status(401).send('Authentication required');
}

function requireFields(config, fields) {
  const missing = fields.filter((field) => !config[field]);
  if (missing.length) {
    const error = new Error(`Configure ${missing.join(', ')} in Settings or as server environment variables.`);
    error.status = 400;
    throw error;
  }
}

function validateMetadata(body, { fileSizeRequired = true } = {}) {
  const { title, description = '', tags = [], visibility = 'public', scheduledAt = null, fileSize } = body || {};
  if (typeof title !== 'string' || !title.trim() || title.length > 100) {
    throw Object.assign(new Error('Title is required and must be at most 100 characters.'), { status: 400 });
  }
  if (typeof description !== 'string' || description.length > 5000) {
    throw Object.assign(new Error('Description must be at most 5,000 characters.'), { status: 400 });
  }
  if (!Array.isArray(tags) || tags.length > 50 || tags.some((tag) => typeof tag !== 'string' || tag.length > 100)) {
    throw Object.assign(new Error('Tags must be a list of up to 50 values, each at most 100 characters.'), { status: 400 });
  }
  if (tags.reduce((length, tag) => length + tag.trim().length, 0) > 500) {
    throw Object.assign(new Error('Combined tags must be at most 500 characters.'), { status: 400 });
  }
  if (!['public', 'unlisted', 'private'].includes(visibility)) {
    throw Object.assign(new Error('Choose a valid visibility.'), { status: 400 });
  }
  let schedule = null;
  if (scheduledAt) {
    const time = Date.parse(scheduledAt);
    if (!Number.isFinite(time) || time <= Date.now()) {
      throw Object.assign(new Error('Schedule time must be a valid future date and time.'), { status: 400 });
    }
    schedule = new Date(time).toISOString();
  }
  if (fileSizeRequired && (!Number.isSafeInteger(fileSize) || fileSize <= 0)) {
    throw Object.assign(new Error('A valid file size is required.'), { status: 400 });
  }
  return {
    title: title.trim(),
    description,
    tags: tags.map((tag) => tag.trim()).filter(Boolean),
    visibility,
    scheduledAt: schedule,
    fileSize
  };
}

async function readJsonResponse(response, service) {
  const text = await response.text();
  let payload = {};
  try {
    payload = text ? JSON.parse(text) : {};
  } catch {
    throw Object.assign(new Error(`${service} returned an invalid response (HTTP ${response.status}).`), { status: 502 });
  }
  if (!response.ok) {
    const message = payload.error?.message || payload.error_description || `${service} request failed (HTTP ${response.status}).`;
    throw Object.assign(new Error(message), { status: 502 });
  }
  return payload;
}

async function getYouTubeToken(config) {
  if (cachedYouTubeToken && cachedYouTubeToken.expiresAt > Date.now() + 60_000) {
    return cachedYouTubeToken.value;
  }
  const response = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: config.YOUTUBE_CLIENT_ID,
      client_secret: config.YOUTUBE_CLIENT_SECRET,
      refresh_token: config.YOUTUBE_REFRESH_TOKEN,
      grant_type: 'refresh_token'
    }),
    signal: AbortSignal.timeout(20_000)
  });
  const payload = await readJsonResponse(response, 'YouTube OAuth');
  if (!payload.access_token) throw Object.assign(new Error('YouTube OAuth did not return an access token.'), { status: 502 });
  cachedYouTubeToken = {
    value: payload.access_token,
    expiresAt: Date.now() + (Number(payload.expires_in) || 3600) * 1000
  };
  return cachedYouTubeToken.value;
}

function graphUrl(pathname, params = {}, host = 'graph.facebook.com') {
  const url = new URL(`https://${host}/${GRAPH_API_VERSION}/${pathname.replace(/^\/+/, '')}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, String(value));
  }
  return url;
}

function hashtagsFor(tags) {
  return tags
    .map((tag) => tag.replace(/^#+/, '').replace(/\s+/g, ''))
    .filter(Boolean)
    .map((tag) => `#${tag}`)
    .join(' ');
}

async function graphRequest(url, options = {}) {
  const response = await fetch(url, { ...options, signal: AbortSignal.timeout(30_000) });
  const payload = await readJsonResponse(response, 'Meta Graph API');
  if (payload.error) throw Object.assign(new Error(payload.error.message || 'Meta Graph API request failed.'), { status: 502 });
  return payload;
}

function noStore(_req, res, next) {
  res.set('Cache-Control', 'no-store');
  next();
}

app.disable('x-powered-by');
app.use(noStore);
app.get('/health', (_req, res) => res.json({ ok: true }));
app.get('/media/:token', (req, res) => {
  const media = instagramMedia.get(req.params.token);
  if (!media || media.expiresAt <= Date.now()) {
    instagramMedia.delete(req.params.token);
    return res.sendStatus(404);
  }
  res.type('video/mp4');
  res.set('Cache-Control', 'public, max-age=3600');
  res.sendFile(media.filePath, (error) => {
    if (error && !res.headersSent) res.sendStatus(404);
  });
});
app.use(authorized);
app.use(express.json({ limit: '64kb' }));
app.use(express.static(path.join(__dirname, 'public'), { index: false }));
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.get('/api/queue', (_req, res) => {
  res.json({
    logoEnabled: queueState.logoEnabled,
    isProcessing: isQueueProcessing,
    hasLogoFile: fs.existsSync(path.join(__dirname, 'public', 'logo.png')),
    items: queueState.items.map(publicJob)
  });
});

app.put('/api/queue/logo', (req, res) => {
  if (typeof req.body?.logoEnabled !== 'boolean') {
    return res.status(400).json({ error: 'logoEnabled must be a boolean.' });
  }
  queueState.logoEnabled = req.body.logoEnabled;
  saveQueueState();
  res.json({ ok: true, logoEnabled: queueState.logoEnabled });
});

app.post('/api/queue/jobs', (req, res, next) => {
  try {
    const metadata = validateMetadata(req.body);
    const { sourceName, platforms } = req.body || {};
    if (typeof sourceName !== 'string' || !sourceName.trim() || sourceName.length > 255) {
      throw Object.assign(new Error('A valid source filename is required.'), { status: 400 });
    }
    if (!platforms || typeof platforms !== 'object' ||
        ['youtube', 'facebook', 'instagram'].some((platform) => typeof platforms[platform] !== 'boolean') ||
        !Object.values(platforms).some(Boolean)) {
      throw Object.assign(new Error('Select at least one supported platform.'), { status: 400 });
    }
    const { fileSize: sourceSize, ...jobMetadata } = metadata;
    const item = {
      id: crypto.randomUUID(),
      sourceName: path.win32.basename(sourceName.trim()),
      sourceSize,
      metadata: jobMetadata,
      platforms: {
        youtube: platforms.youtube,
        facebook: platforms.facebook,
        instagram: platforms.instagram
      },
      logoEnabled: queueState.logoEnabled,
      status: 'awaiting_upload',
      platformState: {},
      platformError: {},
      error: '',
      createdAt: new Date().toISOString()
    };
    queueState.items.push(item);
    saveQueueState();
    res.status(201).json(publicJob(item));
  } catch (error) {
    next(error);
  }
});

app.put('/api/queue/jobs/:id/source', async (req, res, next) => {
  const item = queueState.items.find((job) => job.id === req.params.id);
  if (!item) return res.sendStatus(404);
  if (item.status !== 'awaiting_upload') {
    return res.status(409).json({ error: 'This job is not waiting for a source upload.' });
  }
  const contentLength = req.headers['content-length'] === undefined ? null : Number(req.headers['content-length']);
  if (contentLength !== null && (!Number.isSafeInteger(contentLength) || contentLength !== item.sourceSize)) {
    return res.status(400).json({ error: 'The uploaded file size does not match the queued source.' });
  }
  const files = queuePaths(item.id);
  let received = 0;
  const limiter = new Transform({
    transform(chunk, _encoding, callback) {
      received += chunk.length;
      if (received > item.sourceSize) {
        callback(Object.assign(new Error('Uploaded source exceeds its declared file size.'), { status: 413 }));
        return;
      }
      callback(null, chunk);
    }
  });
  item.status = 'uploading_source';
  saveQueueState();
  try {
    await pipeline(req, limiter, fs.createWriteStream(files.uploadTemp, { flags: 'wx', mode: 0o600 }));
    if (received !== item.sourceSize) {
      throw Object.assign(new Error('The source upload ended before the complete file was received.'), { status: 400 });
    }
    fs.renameSync(files.uploadTemp, files.source);
    item.status = 'pending';
    item.error = '';
    saveQueueState();
    scheduleQueue();
    res.status(202).json(publicJob(item));
  } catch (error) {
    fs.rmSync(files.uploadTemp, { force: true });
    item.status = 'failed';
    item.error = error.message;
    saveQueueState();
    next(error);
  }
});

app.get('/api/queue/jobs/:id/output', (req, res) => {
  const item = queueState.items.find((job) => job.id === req.params.id);
  if (!item) return res.sendStatus(404);
  const output = queuePaths(item.id).output;
  if (!fs.existsSync(output)) return res.sendStatus(404);
  res.type('video/mp4');
  res.sendFile(output, (error) => {
    if (error && !res.headersSent) res.status(error.statusCode || 500).json({ error: 'Could not read the processed video.' });
  });
});

app.get('/api/queue/jobs/:id', (req, res) => {
  const item = queueState.items.find((job) => job.id === req.params.id);
  if (!item) return res.sendStatus(404);
  res.json(publicJob(item));
});

app.patch('/api/queue/jobs/:id', (req, res) => {
  const item = queueState.items.find((job) => job.id === req.params.id);
  if (!item) return res.sendStatus(404);
  const { status, platformState, platformError, error } = req.body || {};
  if (!['uploading', 'complete', 'partial', 'failed'].includes(status) ||
      !platformState || typeof platformState !== 'object' || Array.isArray(platformState) ||
      !platformError || typeof platformError !== 'object' || Array.isArray(platformError)) {
    return res.status(400).json({ error: 'A valid publishing checkpoint is required.' });
  }
  item.status = status;
  item.platformState = Object.fromEntries(
    Object.entries(platformState)
      .filter(([platform, value]) => ['youtube', 'facebook', 'instagram'].includes(platform) &&
        typeof value === 'string' && value.length <= 40)
      .map(([platform, value]) => [platform, value])
  );
  item.platformError = Object.fromEntries(
    Object.entries(platformError)
      .filter(([platform, value]) => ['youtube', 'facebook', 'instagram'].includes(platform) &&
        typeof value === 'string' && value.length <= 1500)
      .map(([platform, value]) => [platform, value])
  );
  item.error = typeof error === 'string' ? error.slice(0, 1500) : '';
  saveQueueState();
  res.json(publicJob(item));
});

app.post('/api/queue/retry-failed', (_req, res) => {
  for (const item of queueState.items) {
    const interruptedPlatform = Object.values(item.platformState || {}).includes('Failed');
    if (!['failed', 'partial'].includes(item.status) &&
        !(interruptedPlatform && ['processed', 'uploading'].includes(item.status))) continue;
    const files = queuePaths(item.id);
    if (fs.existsSync(files.output)) {
      item.status = 'processed';
    } else if (fs.existsSync(files.source)) {
      item.status = 'pending';
    } else {
      item.error = 'The source file is unavailable. Add the file again to retry.';
      continue;
    }
    item.error = '';
    for (const [platform, state] of Object.entries(item.platformState || {})) {
      if (state === 'Failed') {
        delete item.platformState[platform];
        delete item.platformError[platform];
      }
    }
  }
  saveQueueState();
  scheduleQueue();
  res.json({ ok: true, items: queueState.items.map(publicJob) });
});

app.post('/api/queue/clear-done', (_req, res) => {
  const removed = queueState.items.filter((item) => item.status === 'complete');
  queueState.items = queueState.items.filter((item) => item.status !== 'complete');
  saveQueueState();
  for (const item of removed) {
    const files = queuePaths(item.id);
    fs.rmSync(files.source, { force: true });
    fs.rmSync(files.output, { force: true });
  }
  res.json({ ok: true });
});

app.delete('/api/queue/jobs/:id', (req, res) => {
  const index = queueState.items.findIndex((job) => job.id === req.params.id);
  if (index < 0) return res.sendStatus(404);
  const item = queueState.items[index];
  if (['processing', 'uploading_source', 'uploading'].includes(item.status)) {
    return res.status(409).json({ error: 'Wait until this job finishes before removing it.' });
  }
  queueState.items.splice(index, 1);
  saveQueueState();
  const files = queuePaths(item.id);
  for (const file of Object.values(files)) fs.rmSync(file, { force: true });
  res.json({ ok: true });
});

app.post('/api/instagram/media', async (req, res, next) => {
  const token = crypto.randomBytes(32).toString('base64url');
  const filePath = path.join(INSTAGRAM_MEDIA_DIR, `${token}.mp4`);
  let fileSize = 0;
  try {
    if (!PUBLIC_BASE_URL) {
      throw Object.assign(new Error('Set PUBLIC_BASE_URL to the app public HTTPS URL to publish Instagram Reels.'), { status: 503 });
    }
    let publicBase;
    try {
      publicBase = new URL(PUBLIC_BASE_URL);
    } catch {
      throw Object.assign(new Error('PUBLIC_BASE_URL must be a valid public HTTPS URL.'), { status: 503 });
    }
    if (publicBase.protocol !== 'https:' || publicBase.pathname !== '/' ||
        publicBase.username || publicBase.password || publicBase.search || publicBase.hash) {
      throw Object.assign(new Error('PUBLIC_BASE_URL must be a public HTTPS origin without a path.'), { status: 503 });
    }
    if (req.headers['content-type'] !== 'video/mp4') {
      throw Object.assign(new Error('A processed MP4 video is required.'), { status: 400 });
    }
    const limiter = new Transform({
      transform(chunk, _encoding, callback) {
        fileSize += chunk.length;
        if (fileSize > MAX_INSTAGRAM_FILE_SIZE) {
          callback(Object.assign(new Error('Instagram Reels require a watermarked video smaller than 1 GB.'), { status: 413 }));
          return;
        }
        callback(null, chunk);
      }
    });
    await pipeline(req, limiter, fs.createWriteStream(filePath, { flags: 'wx', mode: 0o600 }));
    if (!fileSize) throw Object.assign(new Error('The processed video is empty.'), { status: 400 });
    const publicUrl = new URL(`media/${token}`, publicBase.href.endsWith('/') ? publicBase : `${publicBase.href}/`);
    instagramMedia.set(token, { filePath, expiresAt: Date.now() + INSTAGRAM_MEDIA_TTL });
    res.status(201).json({ videoUrl: publicUrl.href, fileSize });
  } catch (error) {
    fs.promises.rm(filePath, { force: true }).catch((cleanupError) => console.error(cleanupError.message));
    next(error);
  }
});

setInterval(() => {
  for (const [token, media] of instagramMedia) {
    if (media.expiresAt <= Date.now()) {
      instagramMedia.delete(token);
      fs.promises.rm(media.filePath, { force: true }).catch((error) => console.error(error.message));
    }
  }
}, 15 * 60 * 1000).unref();

app.get('/api/settings', (_req, res) => {
  const config = effectiveConfig();
  res.json({
    configured: {
      youtube: ['YOUTUBE_CLIENT_ID', 'YOUTUBE_CLIENT_SECRET', 'YOUTUBE_REFRESH_TOKEN'].every((field) => Boolean(config[field])),
      facebook: Boolean(config.FB_PAGE_ID && config.FB_PAGE_ACCESS_TOKEN),
      instagram: Boolean(config.IG_USER_ID && config.FB_PAGE_ACCESS_TOKEN)
    },
    encryptedSettingsAvailable: Boolean(CONFIG_ENCRYPTION_KEY)
  });
});

app.put('/api/settings', (req, res, next) => {
  try {
    const updates = validateConfig(req.body);
    if (!Object.keys(updates).length) {
      return res.status(400).json({ error: 'Enter at least one credential. Blank fields are left unchanged.' });
    }
    const updatedConfig = { ...savedConfig, ...updates };
    saveConfig(updatedConfig);
    savedConfig = updatedConfig;
    cachedYouTubeToken = null;
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.post('/api/youtube/init-upload', async (req, res, next) => {
  try {
    const metadata = validateMetadata(req.body);
    const config = effectiveConfig();
    requireFields(config, ['YOUTUBE_CLIENT_ID', 'YOUTUBE_CLIENT_SECRET', 'YOUTUBE_REFRESH_TOKEN']);
    const token = await getYouTubeToken(config);
    const response = await fetch('https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json; charset=UTF-8',
        'X-Upload-Content-Length': String(metadata.fileSize),
        'X-Upload-Content-Type': 'application/octet-stream'
      },
      body: JSON.stringify({
        snippet: { title: metadata.title, description: metadata.description, tags: metadata.tags, categoryId: '22' },
        status: {
          privacyStatus: metadata.scheduledAt ? 'private' : metadata.visibility,
          ...(metadata.scheduledAt ? { publishAt: metadata.scheduledAt } : {})
        }
      }),
      signal: AbortSignal.timeout(30_000)
    });
    await readJsonResponse(response, 'YouTube Data API');
    const uploadUrl = response.headers.get('location');
    if (!uploadUrl) throw Object.assign(new Error('YouTube did not return a resumable upload URL.'), { status: 502 });
    res.json({ uploadUrl, accessToken: token });
  } catch (error) {
    next(error);
  }
});

app.post('/api/youtube/check-upload', async (req, res, next) => {
  try {
    const { uploadUrl, fileSize } = req.body || {};
    let parsedUploadUrl;
    try {
      parsedUploadUrl = new URL(uploadUrl);
    } catch {
      throw Object.assign(new Error('A valid YouTube upload session is required.'), { status: 400 });
    }
    if (parsedUploadUrl.protocol !== 'https:' ||
        parsedUploadUrl.hostname !== 'www.googleapis.com' ||
        parsedUploadUrl.pathname !== '/upload/youtube/v3/videos' ||
        parsedUploadUrl.username || parsedUploadUrl.password || parsedUploadUrl.hash ||
        parsedUploadUrl.searchParams.get('uploadType') !== 'resumable' ||
        !parsedUploadUrl.searchParams.get('upload_id') ||
        !Number.isSafeInteger(fileSize) || fileSize <= 0) {
      throw Object.assign(new Error('A valid YouTube upload session and file size are required.'), { status: 400 });
    }
    const config = effectiveConfig();
    requireFields(config, ['YOUTUBE_CLIENT_ID', 'YOUTUBE_CLIENT_SECRET', 'YOUTUBE_REFRESH_TOKEN']);
    const token = await getYouTubeToken(config);
    const response = await fetch(parsedUploadUrl, {
      method: 'PUT',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Length': '0',
        'Content-Range': `bytes */${fileSize}`
      },
      body: '',
      redirect: 'error',
      signal: AbortSignal.timeout(30_000)
    });
    const text = await response.text();
    let payload = {};
    try {
      payload = text ? JSON.parse(text) : {};
    } catch {
      // Final upload status is authoritative even when YouTube's response body is unreadable.
    }
    if (response.status === 200 || response.status === 201) {
      return res.json({ status: response.status, videoId: typeof payload.id === 'string' ? payload.id : null });
    }
    if (response.status === 308) {
      return res.json({ status: response.status, range: response.headers.get('range') });
    }
    throw Object.assign(new Error(payload.error?.message || `YouTube upload status check failed (HTTP ${response.status}).`), { status: 502 });
  } catch (error) {
    next(error);
  }
});

app.post('/api/facebook/init-upload', async (req, res, next) => {
  try {
    const metadata = validateMetadata(req.body);
    const config = effectiveConfig();
    requireFields(config, ['FB_PAGE_ID', 'FB_PAGE_ACCESS_TOKEN']);
    const params = {
      upload_phase: 'start',
      file_size: metadata.fileSize,
      access_token: config.FB_PAGE_ACCESS_TOKEN
    };
    const payload = await graphRequest(graphUrl(`${encodeURIComponent(config.FB_PAGE_ID)}/videos`, params, 'graph-video.facebook.com'), {
      method: 'POST'
    });
    if (!payload.video_id || !payload.upload_session_id || payload.start_offset === undefined) {
      throw Object.assign(new Error('Meta did not return a valid resumable upload session.'), { status: 502 });
    }
    res.json({
      uploadUrl: `https://graph-video.facebook.com/${GRAPH_API_VERSION}/${encodeURIComponent(payload.video_id)}`,
      accessToken: config.FB_PAGE_ACCESS_TOKEN,
      videoId: payload.video_id,
      uploadSessionId: payload.upload_session_id,
      startOffset: Number(payload.start_offset),
      endOffset: Number(payload.end_offset)
    });
  } catch (error) {
    next(error);
  }
});

app.post('/api/facebook/finish-upload', async (req, res, next) => {
  try {
    const metadata = validateMetadata(req.body, { fileSizeRequired: false });
    const { videoId, uploadSessionId } = req.body || {};
    if (typeof videoId !== 'string' || !/^\d+$/.test(videoId) ||
        typeof uploadSessionId !== 'string' || uploadSessionId.length > 256) {
      return res.status(400).json({ error: 'A valid Facebook upload session is required.' });
    }
    const config = effectiveConfig();
    requireFields(config, ['FB_PAGE_ID', 'FB_PAGE_ACCESS_TOKEN']);
    const facebookDescription = [metadata.description, hashtagsFor(metadata.tags)].filter(Boolean).join('\n\n');
    if (facebookDescription.length > 5000) {
      return res.status(400).json({ error: 'Facebook description and hashtags must be at most 5,000 characters.' });
    }
    const params = {
      upload_phase: 'finish',
      upload_session_id: uploadSessionId,
      title: metadata.title,
      description: facebookDescription,
      access_token: config.FB_PAGE_ACCESS_TOKEN,
      published: metadata.scheduledAt ? 'false' : 'true',
      ...(metadata.scheduledAt ? { scheduled_publish_time: Math.floor(Date.parse(metadata.scheduledAt) / 1000) } : {})
    };
    const payload = await graphRequest(graphUrl(`${encodeURIComponent(config.FB_PAGE_ID)}/videos`, params, 'graph-video.facebook.com'), { method: 'POST' });
    res.json({ ok: payload.success !== false, videoId });
  } catch (error) {
    next(error);
  }
});

app.post('/api/instagram/publish', async (req, res, next) => {
  try {
    const metadata = validateMetadata(req.body, { fileSizeRequired: false });
    const { videoUrl, fileSize } = req.body || {};
    if (typeof videoUrl !== 'string' || videoUrl.length > 2048) {
      return res.status(400).json({ error: 'A public HTTPS video URL is required for Instagram Reels.' });
    }
    let parsedVideoUrl;
    try {
      parsedVideoUrl = new URL(videoUrl);
    } catch {
      return res.status(400).json({ error: 'Enter a valid public HTTPS video URL.' });
    }
    if (parsedVideoUrl.protocol !== 'https:' || !Number.isSafeInteger(fileSize) || fileSize <= 0 || fileSize >= 1024 ** 3) {
      return res.status(400).json({ error: 'Instagram Reels require a public HTTPS video URL and a video smaller than 1 GB.' });
    }
    const config = effectiveConfig();
    requireFields(config, ['IG_USER_ID', 'FB_PAGE_ACCESS_TOKEN']);
    const caption = [metadata.title, metadata.description, hashtagsFor(metadata.tags)]
      .filter(Boolean)
      .join('\n\n');
    if (caption.length > 2200) {
      return res.status(400).json({ error: 'Instagram caption must be at most 2,200 characters.' });
    }
    const token = config.FB_PAGE_ACCESS_TOKEN;
    const createUrl = graphUrl(`${encodeURIComponent(config.IG_USER_ID)}/media`, {
      media_type: 'REELS',
      video_url: videoUrl,
      caption,
      access_token: token
    });
    const container = await graphRequest(createUrl, { method: 'POST' });
    if (!container.id) throw Object.assign(new Error('Instagram did not return a media container ID.'), { status: 502 });

    const deadline = Date.now() + 8 * 60_000;
    let status = 'IN_PROGRESS';
    while (status === 'IN_PROGRESS') {
      if (Date.now() >= deadline) {
        throw Object.assign(new Error('Instagram is still processing the Reel. Retry publishing after the video finishes processing.'), { status: 504 });
      }
      await new Promise((resolve) => setTimeout(resolve, 5000));
      const statusUrl = graphUrl(encodeURIComponent(container.id), {
        fields: 'status_code',
        access_token: token
      });
      const statusPayload = await graphRequest(statusUrl);
      status = statusPayload.status_code;
      if (status === 'ERROR' || status === 'EXPIRED') {
        throw Object.assign(new Error(`Instagram video processing failed (${status}).`), { status: 502 });
      }
    }
    if (status !== 'FINISHED') {
      throw Object.assign(new Error(`Instagram returned an unexpected processing status (${status || 'unknown'}).`), { status: 502 });
    }
    const published = await graphRequest(graphUrl(`${encodeURIComponent(config.IG_USER_ID)}/media_publish`, {
      creation_id: container.id,
      access_token: token
    }), { method: 'POST' });
    res.json({ ok: true, mediaId: published.id || null });
  } catch (error) {
    next(error);
  }
});

app.use((error, _req, res, _next) => {
  const status = Number.isInteger(error.status) && error.status >= 400 && error.status <= 599 ? error.status : 500;
  if (status >= 500) console.error(error.message);
  res.status(status).json({ error: error.message || 'Request failed.' });
});

readConfig();
app.listen(PORT, '0.0.0.0', () => {
  console.log(`PureSunni Publisher listening on port ${PORT}`);
  scheduleQueue();
});
