# PureSunni Multi-Stream Studio

A self-hosted dashboard that accepts one live stream and forwards it to enabled RTMP/RTMPS destinations using FFmpeg stream copy (`-c:v copy -c:a copy`). It does not transcode; the incoming audio/video codecs and container must be accepted by every selected platform.

## Run locally

Requirements: Node.js 20+, FFmpeg, and an available TCP port 1935 for RTMP ingest.

```sh
npm install
ADMIN_PASSWORD='replace-this-with-a-long-random-password' npm start
```

Open `http://localhost:3000`. The default dashboard username is `admin`; set `ADMIN_USER` to change it. Configure OBS with the displayed RTMP server URL and stream key. Set `INGEST_STREAM_KEY` to keep that key stable across restarts; otherwise a new random key is created at each startup.

## Render deployment (Docker)

Create a Render **Web Service** from this repository and select the Docker runtime. Set these environment variables in Render:

| Variable | Required | Purpose |
| --- | --- | --- |
| `ADMIN_PASSWORD` | Yes | Dashboard/API password (at least 12 characters). |
| `ADMIN_USER` | No | Dashboard username; defaults to `admin`. |
| `INGEST_STREAM_KEY` | Recommended | Stable, high-entropy ingest key. If omitted, one is generated on each startup. |
| `DESTINATIONS_FILE` | No | Defaults to `/data/destinations.json` in Docker. |
| `PUBLIC_WS_URL` | No | Override the external WebSocket ingest URL if the proxy hostname cannot be inferred. Use `wss://your-service.onrender.com/ingest/ws`. |

Attach a persistent Render disk mounted at `/data` to retain destination settings and stream keys across deploys. Without a persistent disk, Render's container filesystem is ephemeral. Treat `destinations.json` as a secret: it contains destination stream keys.

The web dashboard and WebSocket ingest share Render's `PORT`. **Render web services do not provide public raw TCP forwarding for RTMP port 1935**, so the RTMP URL shown in the studio is for hosts where that port is reachable, not for Render ingress. Render can receive MPEG-TS over the displayed `wss://.../ingest/ws?key=...` endpoint. A sender must implement a WebSocket client and send binary MPEG-TS messages. Standard OBS RTMP output cannot publish directly to this WebSocket endpoint; use a WebSocket-capable sender or run an FFmpeg relay near OBS. This app's websocket receiver is a byte-stream transport, not browser-camera/WebRTC ingest.

The image installs FFmpeg with `apk add --no-cache ffmpeg`, exposes HTTP and RTMP ports, and stores the destination file under `/data`.

## Destinations and controls

Add a platform, channel name, RTMP/RTMPS server, and stream key in the dashboard. Platform presets are provided for YouTube, Facebook, and Instagram. Each active destination gets its own FFmpeg connection, automatic retry with backoff, per-output state, FPS, and bitrate. Start All and Stop All control output connections while an input is live; a new incoming stream automatically starts enabled destinations.

Destination keys are saved in cleartext in the local JSON file and are visible to authenticated dashboard users. Use a private deployment, a strong admin password, HTTPS, and restricted disk access. YouTube, Meta, and Instagram account eligibility, live permissions, and key issuance are managed by those platforms and are not provided by this application.

## Endpoints

- `GET /health` — unauthenticated Render health check.
- `GET /api/status`, `GET /api/destinations`, `PUT /api/destinations`, `POST /api/start`, `POST /api/stop` — HTTP Basic authenticated.
- `/ingest/ws?key=...` via WebSocket upgrade — binary MPEG-TS ingress authenticated by the ingest key.
- RTMP publish: `rtmp://HOST:1935/live` with the ingest key as the stream name.