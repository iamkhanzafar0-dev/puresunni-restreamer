const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { setTimeout: delay } = require('node:timers/promises');
const { after, test } = require('node:test');

const ROOT = fs.mkdtempSync(path.join(os.tmpdir(), 'puresunni-queue-tests-'));
const SERVER_PATH = path.join(__dirname, '..', 'server.js');
const LOGO_PATH = path.join(__dirname, '..', 'public', 'logo.png');
const AUTHORIZATION = `Basic ${Buffer.from('admin:test-password-123').toString('base64')}`;
const servers = new Set();

after(async () => {
  for (const server of servers) await stopServer(server);
  fs.rmSync(ROOT, { recursive: true, force: true });
});

async function unusedPort() {
  const listener = net.createServer();
  await new Promise((resolve) => listener.listen(0, '127.0.0.1', resolve));
  const { port } = listener.address();
  await new Promise((resolve, reject) => listener.close((error) => error ? reject(error) : resolve()));
  return port;
}

function createFakeFFmpeg(directory) {
  const executable = path.join(directory, 'fake-ffmpeg.js');
  fs.writeFileSync(executable, `#!/usr/bin/env node
const fs = require('node:fs');
const output = process.argv.at(-1);
fs.appendFileSync(process.env.FFMPEG_TEST_ARGS, JSON.stringify(process.argv.slice(2)) + '\\n');
try {
  fs.writeFileSync(process.env.FFMPEG_TEST_LOCK, String(process.pid), { flag: 'wx' });
} catch {
  fs.writeFileSync(process.env.FFMPEG_TEST_OVERLAP, 'overlap');
  process.exit(1);
}
setTimeout(() => {
  fs.writeFileSync(output, 'processed-video');
  fs.unlinkSync(process.env.FFMPEG_TEST_LOCK);
}, 100);
`);
  fs.chmodSync(executable, 0o700);
  return executable;
}

async function startServer(directory, options = {}) {
  const port = await unusedPort();
  const queueDir = path.join(directory, 'queue');
  fs.mkdirSync(queueDir, { recursive: true });
  const ffmpeg = createFakeFFmpeg(directory);
  const child = spawn(process.execPath, [SERVER_PATH], {
    env: {
      ...process.env,
      PORT: String(port),
      ADMIN_PASSWORD: 'test-password-123',
      QUEUE_DATA_DIR: queueDir,
      FFMPEG_PATH: ffmpeg,
      FFMPEG_TEST_LOCK: path.join(directory, 'ffmpeg.lock'),
      FFMPEG_TEST_OVERLAP: path.join(directory, 'ffmpeg-overlap'),
      FFMPEG_TEST_ARGS: path.join(directory, 'ffmpeg-args.log'),
      ...options.env
    },
    stdio: ['ignore', 'ignore', 'pipe']
  });
  let stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (data) => { stderr += data; });
  const server = { child, port, queueDir, directory, get stderr() { return stderr; } };
  servers.add(server);
  server.baseUrl = `http://127.0.0.1:${port}`;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (child.exitCode !== null) throw new Error(`Server exited during startup: ${stderr}`);
    try {
      const response = await fetch(`${server.baseUrl}/health`);
      if (response.ok) return server;
    } catch {}
    await delay(50);
  }
  throw new Error(`Server did not start: ${stderr}`);
}

async function stopServer(server) {
  if (!server || server.child.exitCode !== null) return;
  const exited = new Promise((resolve) => server.child.once('exit', resolve));
  server.child.kill('SIGTERM');
  await exited;
  servers.delete(server);
}

async function api(server, pathname, options = {}) {
  const response = await fetch(`${server.baseUrl}${pathname}`, {
    ...options,
    headers: {
      Authorization: AUTHORIZATION,
      ...(options.body && !(options.body instanceof Buffer) ? { 'Content-Type': 'application/json' } : {}),
      ...(options.headers || {})
    }
  });
  const body = response.headers.get('content-type')?.includes('application/json')
    ? await response.json()
    : await response.text();
  return { response, body };
}

async function createJob(server, sourceName = 'input.mp4') {
  const source = Buffer.from('fake source video');
  const { response, body } = await api(server, '/api/queue/jobs', {
    method: 'POST',
    body: JSON.stringify({
      sourceName,
      fileSize: source.length,
      title: sourceName,
      description: '',
      tags: [],
      visibility: 'public',
      scheduledAt: null,
      platforms: { youtube: true, facebook: false, instagram: false }
    })
  });
  assert.equal(response.status, 201, JSON.stringify(body));
  return { job: body, source };
}

test('persists logo preference and processes uploaded jobs sequentially', async () => {
  const directory = path.join(ROOT, 'sequential');
  fs.mkdirSync(directory);
  const server = await startServer(directory);
  const logo = await api(server, '/api/queue/logo', {
    method: 'PUT',
    body: JSON.stringify({ logoEnabled: false })
  });
  assert.equal(logo.response.status, 200);
  assert.equal(logo.body.logoEnabled, false);

  const jobs = await Promise.all([
    createJob(server, 'first.mp4'),
    createJob(server, 'second.mp4')
  ]);
  const uploads = await Promise.all(jobs.map(({ job, source }) => api(server, `/api/queue/jobs/${job.id}/source`, {
    method: 'PUT',
    headers: { 'Content-Type': 'video/mp4' },
    body: source
  })));
  assert.ok(uploads.every(({ response }) => response.status === 202));

  let state;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    ({ body: state } = await api(server, '/api/queue'));
    if (state.items.every((item) => item.status === 'processed')) break;
    await delay(50);
  }
  assert.ok(state.items.every((item) => item.status === 'processed'), server.stderr);
  assert.ok(state.items.every((item) => item.logoEnabled === false));
  assert.equal(fs.existsSync(path.join(directory, 'ffmpeg-overlap')), false);
  assert.ok(state.items.every((item) => fs.existsSync(path.join(server.queueDir, 'processed', `${item.id}.mp4`))));
  assert.deepEqual(fs.readdirSync(path.join(server.queueDir, 'temp')), []);
  const ffmpegArgs = fs.readFileSync(path.join(directory, 'ffmpeg-args.log'), 'utf8').trim().split('\n').map(JSON.parse);
  assert.ok(ffmpegArgs.every((args) => !args.includes(LOGO_PATH)));

  const checkpoint = JSON.parse(fs.readFileSync(path.join(server.queueDir, 'queue_state.json'), 'utf8'));
  assert.equal(checkpoint.logoEnabled, false);
  assert.ok(checkpoint.items.every((item) => item.status === 'processed'));

  const interruptedJob = state.items[0];
  await api(server, `/api/queue/jobs/${interruptedJob.id}`, {
    method: 'PATCH',
    body: JSON.stringify({
      status: 'uploading',
      platformState: { youtube: 'Failed' },
      platformError: { youtube: 'Publishing was interrupted.' },
      error: 'Publishing was interrupted.'
    })
  });
  const retried = await api(server, '/api/queue/retry-failed', { method: 'POST', body: '{}' });
  const restored = retried.body.items.find((item) => item.id === interruptedJob.id);
  assert.equal(restored.status, 'processed');
  assert.deepEqual(restored.platformState, {});
  await stopServer(server);
});

test('recovers interrupted processing and removes abandoned temporary files on restart', async () => {
  const directory = path.join(ROOT, 'recovery');
  const queueDir = path.join(directory, 'queue');
  const sourceDir = path.join(queueDir, 'sources');
  const tempDir = path.join(queueDir, 'temp');
  fs.mkdirSync(sourceDir, { recursive: true });
  fs.mkdirSync(tempDir, { recursive: true });
  fs.writeFileSync(path.join(queueDir, 'queue_state.json.123.tmp'), 'incomplete checkpoint');
  const id = crypto.randomUUID();
  fs.writeFileSync(path.join(sourceDir, `${id}.input`), 'recoverable source');
  fs.writeFileSync(path.join(tempDir, 'abandoned.partial'), 'partial output');
  fs.writeFileSync(path.join(queueDir, 'queue_state.json'), JSON.stringify({
    logoEnabled: true,
    items: [{
      id,
      sourceName: 'recover.mp4',
      sourceSize: 18,
      metadata: { title: 'recover.mp4', description: '', tags: [], visibility: 'public', scheduledAt: null },
      platforms: { youtube: true, facebook: false, instagram: false },
      logoEnabled: true,
      status: 'processing',
      platformState: {},
      platformError: {},
      error: ''
    }]
  }));
  const server = await startServer(directory, { env: { QUEUE_DATA_DIR: queueDir } });

  let job;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    ({ body: job } = await api(server, `/api/queue/jobs/${id}`));
    if (job.status === 'processed') break;
    await delay(50);
  }
  assert.equal(job.status, 'processed', server.stderr);
  assert.deepEqual(fs.readdirSync(tempDir), []);
  assert.equal(fs.existsSync(path.join(queueDir, 'queue_state.json.123.tmp')), false);
  assert.equal(fs.existsSync(path.join(sourceDir, `${id}.input`)), false);
  assert.equal(fs.existsSync(path.join(queueDir, 'processed', `${id}.mp4`)), true);
  const ffmpegArgs = JSON.parse(fs.readFileSync(path.join(directory, 'ffmpeg-args.log'), 'utf8').trim());
  assert.ok(ffmpegArgs.includes(LOGO_PATH));
  await stopServer(server);
});
