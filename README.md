# PureSunni Multi-Platform Bulk Video Publisher

A bulk publisher for YouTube, Facebook Pages, and Instagram Reels. Videos are uploaded to the server, processed sequentially with FFmpeg, and checkpointed to `queue_state.json`. Logo watermarking can be turned on or off for each new queue item. YouTube and Facebook receive the processed file from the browser in 16 MiB chunks. Instagram's API requires a public video URL, so its processed file is temporarily hosted by this app for Meta to fetch.

## Run locally

Requirements: Node.js 20+ and FFmpeg available on `PATH`.

```sh
npm install
ADMIN_PASSWORD='replace-this-with-a-long-random-password' npm start
```

Open `http://localhost:3000` and authenticate with Basic Auth. The default username is `admin`; set `ADMIN_USER` to change it. `ADMIN_PASSWORD` is required and must be at least 12 characters. Use HTTPS for any deployment exposed to the internet.

The default queue directory is `data/video-queue`. Set `QUEUE_DATA_DIR` to move it. Mount this directory on persistent storage in deployments; it contains the queue checkpoint and processed outputs. The Docker image installs FFmpeg.

## Render deployment

Create a Render Web Service using the Docker runtime. Configure:

| Variable | Required | Purpose |
| --- | --- | --- |
| `ADMIN_PASSWORD` | Yes | Basic Auth password (at least 12 characters). |
| `ADMIN_USER` | No | Basic Auth username; defaults to `admin`. |
| `PORT` | No | HTTP port; defaults to `3000` and is normally supplied by Render. |
| `YOUTUBE_CLIENT_ID` | For YouTube | OAuth client ID for a Google project with YouTube Data API v3 enabled. |
| `YOUTUBE_CLIENT_SECRET` | For YouTube | OAuth client secret. |
| `YOUTUBE_REFRESH_TOKEN` | For YouTube | Refresh token authorized for the channel and `youtube.upload` scope. |
| `FB_PAGE_ID` | For Facebook | Facebook Page ID. |
| `FB_PAGE_ACCESS_TOKEN` | For Facebook/Instagram | Page access token with the required video and publishing permissions. |
| `IG_USER_ID` | For Instagram | Instagram professional account ID linked to the Page. |
| `PUBLIC_BASE_URL` | For Instagram | Public HTTPS base URL for this app, used to create temporary video URLs Meta can fetch. |
| `CONFIG_ENCRYPTION_KEY` | For settings form | Optional 32-byte key encoded as exactly 64 hexadecimal characters. Enables encrypted credential settings saved to disk. |
| `CONFIG_FILE` | No | Encrypted settings file location; defaults to `data/publisher-config.enc`. |
| `QUEUE_DATA_DIR` | No | Persistent video queue directory; defaults to `data/video-queue`. |
| `FFMPEG_PATH` | No | FFmpeg executable; defaults to `ffmpeg`. |

Credentials can be supplied as environment variables, or entered in Settings and saved encrypted when `CONFIG_ENCRYPTION_KEY` is configured. The encryption key itself must be kept in Render's environment settings, not in the repository. Attach persistent storage for `QUEUE_DATA_DIR` so queued work and processed outputs survive restarts. If encrypted settings are used, also attach a persistent disk at the parent directory of `CONFIG_FILE`; otherwise they are lost when the service restarts. Environment-variable credentials work without a persistent disk.

## Upload behavior and platform requirements

- Add any number of video files to the queue. Source files are uploaded to the authenticated server, encoded one at a time as H.264 with the ultrafast preset and CRF 22, and then published sequentially to the selected platforms.
- The Logo switch is saved in `queue_state.json`; its value is captured when a video enters the queue, so changing it does not alter jobs already queued. When enabled and `public/logo.png` exists, FFmpeg overlays it near the top-right corner.
- Queue state is written atomically to `queue_state.json` after state transitions. After a restart, unfinished FFmpeg processing resumes from its saved source; incomplete uploads to external platforms are marked for manual review before retrying. Temporary and partial FFmpeg files are removed during recovery.
- Original source files are removed after successful processing. Processed outputs remain available for retry until the job is removed or completed jobs are cleared.
- YouTube and Facebook receive the processed file in 16 MiB browser-to-platform chunks with retry for network drops. If the final YouTube response is lost, the app checks the resumable session before reporting failure. Platform errors are available on the status badge tooltip.
- Instagram's API requires a public HTTPS video URL. Set `PUBLIC_BASE_URL` to the app's public HTTPS origin; the processed MP4 is streamed to the app and made available at a random, unguessable URL for up to 24 hours, then deleted. The app must be reachable by Meta over HTTPS.
- The authenticated browser receives the YouTube resumable URL and short-lived OAuth access token, and the Facebook upload session details and Page token, so it can upload directly. The browser also downloads the processed video from the authenticated app before platform publishing. Protect the app with a strong password and HTTPS.
- YouTube uses the unified title, description, tags, visibility, and optional publish time. Scheduled YouTube videos are created as private with `publishAt`.
- Facebook uses the unified title/description, adds the unified tags as hashtags, and supports optional scheduled publish time. Meta API access, permissions, app review, and Page eligibility are controlled by Meta.
- Instagram Reels use the unified title, description, and tags as the caption. The processed file must be smaller than 1 GB; the app hosts it temporarily at a public HTTPS URL for Instagram to fetch. Reels publish immediately; the Graph API does not provide scheduled Reels publishing through this app.
- Visibility and scheduled publishing have platform-specific behavior; for example, Instagram's API does not support the visibility selector or scheduled Reels in this workflow.
- YouTube Data API quota and Google/Meta account eligibility still apply. The app itself uses free/open-source software and makes no charge for uploads.

## API

All routes except `/health` and the unguessable temporary `/media/:token` URLs require HTTP Basic Auth.

- `GET /health` — unauthenticated service health check.
- `GET /api/settings` — reports configured platforms without returning secrets.
- `PUT /api/settings` — saves provided credentials encrypted at rest; requires `CONFIG_ENCRYPTION_KEY`.
- `GET /api/queue` — returns the saved queue and logo-switch setting.
- `PUT /api/queue/logo` — persists the logo switch for new queue jobs.
- `POST /api/queue/jobs` and `PUT /api/queue/jobs/:id/source` — create a job and upload its source video.
- `GET /api/queue/jobs/:id` and `GET /api/queue/jobs/:id/output` — inspect a job and retrieve its processed MP4.
- `PATCH /api/queue/jobs/:id` — save platform-publishing checkpoints.
- `POST /api/queue/retry-failed`, `POST /api/queue/clear-done`, and `DELETE /api/queue/jobs/:id` — retry, clear, or remove jobs.
- `POST /api/youtube/init-upload` — refreshes a YouTube token and creates a resumable upload session.
- `POST /api/facebook/init-upload` — creates a Facebook Page video upload session.
- `POST /api/facebook/finish-upload` — completes and optionally schedules a Facebook upload after browser transfer.
- `POST /api/instagram/media` — temporarily hosts a processed MP4 for Instagram to fetch. Requires `PUBLIC_BASE_URL`.
- `GET /media/:token` — serves a temporary Instagram video by its unguessable URL token.
- `POST /api/instagram/publish` — creates, processes, and publishes an Instagram Reel from a public video URL.

Source uploads, processed outputs, and queue checkpoints are stored under `QUEUE_DATA_DIR`. Instagram media remains temporary and is deleted after up to 24 hours.
