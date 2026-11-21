# PureSunni Multi-Platform Bulk Video Publisher

A browser-based bulk publisher for YouTube, Facebook Pages, and Instagram Reels. Each queued video is automatically watermarked in the browser before upload. YouTube and Facebook receive the processed file directly from the browser in 16 MiB chunks. Instagram's API requires a public video URL, so its processed file is temporarily hosted by this app for Meta to fetch. The queue runs at most two videos simultaneously.

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
| `PUBLIC_BASE_URL` | For Instagram | Public HTTPS base URL for this app, used to create temporary video URLs Meta can fetch. |
| `CONFIG_ENCRYPTION_KEY` | For settings form | Optional 32-byte key encoded as exactly 64 hexadecimal characters. Enables encrypted credential settings saved to disk. |
| `CONFIG_FILE` | No | Encrypted settings file location; defaults to `data/publisher-config.enc`. |

Credentials can be supplied as environment variables, or entered in Settings and saved encrypted when `CONFIG_ENCRYPTION_KEY` is configured. The encryption key itself must be kept in Render's environment settings, not in the repository. If encrypted settings are used, attach a persistent disk at the parent directory of `CONFIG_FILE`; otherwise they are lost when the service restarts. Environment variables work without a persistent disk.

## Upload behavior and platform requirements

- Add any number of video files to the browser queue. Only two videos are processed concurrently; selected platforms for one video are uploaded sequentially.
- Before any platform upload begins, the browser applies `public/logo.png` in the top-right corner: 8% of the video width, capped at 120 by 60 pixels while preserving the logo aspect ratio, with a 16-pixel margin. Video is encoded as H.264 with the ultrafast preset and CRF 22; the original audio stream is copied without re-encoding.
- YouTube and Facebook receive the processed file in 16 MiB browser-to-platform chunks with retry for network drops. If the final YouTube response is lost, the app checks the resumable session before reporting failure. Platform errors are available on the status badge tooltip.
- Instagram's API requires a public HTTPS video URL. Set `PUBLIC_BASE_URL` to the app's public HTTPS origin; the processed MP4 is streamed to the app and made available at a random, unguessable URL for up to 24 hours, then deleted. The app must be reachable by Meta over HTTPS.
- The authenticated browser receives the YouTube resumable URL and short-lived OAuth access token, and the Facebook upload session details and Page token, so it can upload directly. Protect the app with a strong password and HTTPS.
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
- `POST /api/youtube/init-upload` — refreshes a YouTube token and creates a resumable upload session.
- `POST /api/facebook/init-upload` — creates a Facebook Page video upload session.
- `POST /api/facebook/finish-upload` — completes and optionally schedules a Facebook upload after browser transfer.
- `POST /api/instagram/media` — temporarily hosts a processed MP4 for Instagram to fetch. Requires `PUBLIC_BASE_URL`.
- `GET /media/:token` — serves a temporary Instagram video by its unguessable URL token.
- `POST /api/instagram/publish` — creates, processes, and publishes an Instagram Reel from a public video URL.

Except for the temporary Instagram media hosting route, the server does not accept video upload bodies or write video data to disk.
