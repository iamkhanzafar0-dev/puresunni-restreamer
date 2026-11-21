# PureSunni Multi-Platform Bulk Video Publisher

A browser-based bulk publisher for YouTube, Facebook Pages, and Instagram Reels. Large video files are **never sent through the Node.js server**: YouTube and Facebook uploads are streamed from the authenticated browser in 16 MiB chunks. The queue runs at most two videos simultaneously, keeping the server's memory use independent of video size.

## Run locally

Requirements: Node.js 20+.

```sh
npm install
ADMIN_PASSWORD='replace-this-with-a-long-random-password' npm start
```

Open `http://localhost:3000` and authenticate with Basic Auth. The default username is `admin`; set `ADMIN_USER` to change it. `ADMIN_PASSWORD` is required and must be at least 12 characters. Use HTTPS for any deployment exposed to the internet.

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
| `CONFIG_ENCRYPTION_KEY` | For settings form | Optional 32-byte key encoded as exactly 64 hexadecimal characters. Enables encrypted credential settings saved to disk. |
| `CONFIG_FILE` | No | Encrypted settings file location; defaults to `data/publisher-config.enc`. |

Credentials can be supplied as environment variables, or entered in Settings and saved encrypted when `CONFIG_ENCRYPTION_KEY` is configured. The encryption key itself must be kept in Render's environment settings, not in the repository. If encrypted settings are used, attach a persistent disk at the parent directory of `CONFIG_FILE`; otherwise they are lost when the service restarts. Environment variables work without a persistent disk.

## Upload behavior and platform requirements

- Add any number of video files to the browser queue. Only two videos are processed concurrently; selected platforms for one video are uploaded sequentially.
- YouTube and Facebook receive 16 MiB browser-to-platform chunks with retry for network drops. The browser reports per-platform progress, bytes sent, and average speed. Render handles only small session/API requests.
- The authenticated browser receives the YouTube resumable URL and short-lived OAuth access token, and the Facebook upload session details and Page token, so it can upload directly. Protect the app with a strong password and HTTPS.
- YouTube uses the unified title, description, tags, visibility, and optional publish time. Scheduled YouTube videos are created as private with `publishAt`.
- Facebook uses the unified title/description, adds the unified tags as hashtags, and supports optional scheduled publish time. Meta API access, permissions, app review, and Page eligibility are controlled by Meta.
- Instagram Reels use the unified title, description, and tags as the caption. The selected file must be smaller than 1 GB and already available at a public HTTPS video URL that Instagram can fetch. Reels publish immediately; the Graph API does not provide scheduled Reels publishing through this app.
- Visibility and scheduled publishing have platform-specific behavior; for example, Instagram's API does not support the visibility selector or scheduled Reels in this workflow.
- YouTube Data API quota and Google/Meta account eligibility still apply. The app itself uses free/open-source software and makes no charge for uploads.

## API

All routes except `/health` require HTTP Basic Auth.

- `GET /health` — unauthenticated service health check.
- `GET /api/settings` — reports configured platforms without returning secrets.
- `PUT /api/settings` — saves provided credentials encrypted at rest; requires `CONFIG_ENCRYPTION_KEY`.
- `POST /api/youtube/init-upload` — refreshes a YouTube token and creates a resumable upload session.
- `POST /api/facebook/init-upload` — creates a Facebook Page video upload session.
- `POST /api/facebook/finish-upload` — completes and optionally schedules a Facebook upload after browser transfer.
- `POST /api/instagram/publish` — creates, processes, and publishes an Instagram Reel from a public video URL.

The server does not accept video upload bodies or write video data to disk.
