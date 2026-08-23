# 🎵 SyncWave — Listen Together

Real-time synchronized music listening app. Everyone in a room hears the **exact same moment** of a song — host controls playback, guests follow in sync via WebSocket.

## Features

- **Real WebSocket sync** — play, pause, seek, and load events broadcast to every listener with server-side timestamp correction for network lag
- **Real YouTube search** — YouTube Data API v3 proxied through the server (no key exposed to browser)
- **oEmbed fallback** — paste any YouTube URL and it fetches the real title without needing an API key
- **Acoustic similarity engine** — scores songs by BPM proximity, energy, mood, and genre graph distance; auto-queues the best match when a song ends
- **Host system** — host controls playback; guests can vote to skip (majority vote triggers skip); host can transfer crown or kick users
- **Drag-to-reorder queue** — host can drag songs; upvotes from guests re-sort the upcoming queue
- **Real-time chat** — messages, emoji reactions, floating emoji animations

---

## Setup

### 1. Install dependencies

```bash
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```
YOUTUBE_API_KEY=YOUR_KEY_HERE
PORT=3000
```

**Getting a YouTube API key:**
1. Go to https://console.cloud.google.com
2. Create a project → Enable **YouTube Data API v3**
3. Credentials → Create API key → copy it into `.env`
4. Free quota: 10,000 units/day (each search = 100 units → ~100 searches/day free)

> **Without a key:** The app still works. Paste any YouTube URL into the search box — it fetches the title via oEmbed (no quota). Search by text just won't work.

### 3. Run

```bash
# Production
npm start

# Development (auto-restarts on file change — Node 18+)
npm run dev
```

Open http://localhost:3000

---

## How to test real-time sync

1. Start the server
2. Open http://localhost:3000 in Tab 1 — **Create room** → note the room code
3. Open http://localhost:3000 in Tab 2 → **Join room** → enter the code
4. Tab 1 (host) controls playback — Tab 2 follows in real time

Or share the invite link with someone else on your network:
`http://YOUR_LOCAL_IP:3000?room=ROOMCODE`

---

## Project structure

```
syncwave/
├── src/
│   └── server.js       # Express + WebSocket server, YouTube proxy
├── public/
│   └── index.html      # Frontend — YouTube IFrame API, WebSocket client
├── .env.example
├── package.json
└── README.md
```

## WebSocket message protocol

| Type | Direction | Description |
|---|---|---|
| `create` | client→server | Create a new room |
| `join` | client→server | Join existing room |
| `created` | server→client | Room created, includes code |
| `room_state` | server→client | Full state snapshot on join |
| `play` | both | Play with current timestamp |
| `pause` | both | Pause |
| `seek` | both | Seek to timestamp |
| `load_song` | both | Load and play a specific queue index |
| `queue_add` | client→server | Add a song (any user) |
| `queue_remove` | host→server | Remove a song |
| `queue_reorder` | host→server | Drag reorder or shuffle |
| `queue_update` | server→all | Broadcast queue change |
| `chat` | both | Chat message or emoji |
| `skip_vote` | client→server | Cast a skip vote |
| `transfer_host` | host→server | Give host to someone else |
| `kick` | host→server | Remove a user |
| `resync_request` | client→server | Ask server for current time |
| `resync` | server→client | Current timestamp for re-sync |
| `upvote` | client→server | Toggle upvote on a song |

## Deployment

### Railway / Render / Fly.io (recommended)

1. Push to a GitHub repo
2. Connect to Railway/Render
3. Set env var `YOUTUBE_API_KEY`
4. Deploy — WebSocket is supported out of the box

### nginx reverse proxy (VPS)

```nginx
location / {
    proxy_pass http://localhost:3000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";  # Required for WebSocket
    proxy_set_header Host $host;
}
```

## What's NOT included (and why)

- **Persistent rooms** — rooms live in memory; server restart clears them. Add Redis if you need persistence.
- **Auth** — no login system. Add Passport.js or a JWT flow if needed.
- **Mobile app** — this is a web app. React Native + this server would work.
- **Rate limiting** — add `express-rate-limit` for production to protect the YouTube proxy.
