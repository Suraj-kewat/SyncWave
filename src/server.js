require('dotenv').config();
const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const axios     = require('axios');
const path      = require('path');
const crypto    = require('crypto');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

const PORT         = process.env.PORT || 3000;
const YT_API_KEY   = process.env.YOUTUBE_API_KEY || '';
const YT_SEARCH    = 'https://www.googleapis.com/youtube/v3/search';
const YT_VIDEOS    = 'https://www.googleapis.com/youtube/v3/videos';
const YT_OEMBED    = 'https://www.youtube.com/oembed';

// ─── LIMITS ───────────────────────────────────
const MAX_ROOMS        = 500;
const MAX_CLIENTS_ROOM = 50;
const MAX_QUEUE_LEN    = 200;
const MAX_MSG_BYTES    = 8192;   // 8 KB per WS message
const ROOM_TTL_MS      = 6 * 60 * 60 * 1000; // 6 h idle before cleanup
const WS_PING_MS       = 25000;  // heartbeat interval

app.use(express.json({ limit: '50kb' }));
app.use(express.static(path.join(__dirname, '../public')));

// ─── RATE LIMITER (simple in-memory) ──────────
const searchRates = new Map(); // ip → { count, resetAt }
function rateLimit(ip, max = 30, windowMs = 60_000) {
  const now = Date.now();
  let r = searchRates.get(ip);
  if (!r || now > r.resetAt) { r = { count: 0, resetAt: now + windowMs }; searchRates.set(ip, r); }
  r.count++;
  return r.count > max;
}
// Clean rate map every 5 min
setInterval(() => { const now = Date.now(); for (const [k, v] of searchRates) if (now > v.resetAt) searchRates.delete(k); }, 300_000);

// ─── ROOM STORE ────────────────────────────────
const rooms = new Map(); // code → room

function makeRoom(code, hostName, hostWs, hostColor) {
  const room = {
    code,
    host: hostName,
    queue: [],
    currentIdx: 0,
    isPlaying: false,
    startedAt: null,
    elapsed: 0,
    skipVotes: new Set(),
    clients: new Map(),   // ws → {id,name,color,isHost}
    lastActivity: Date.now(),
  };
  room.clients.set(hostWs, { id: crypto.randomUUID(), name: hostName, color: hostColor, isHost: true });
  rooms.set(code, room);
  return room;
}

function roomCurrentTime(room) {
  if (!room.isPlaying || !room.startedAt) return room.elapsed;
  return room.elapsed + (Date.now() - room.startedAt) / 1000;
}

function listenerList(room) {
  return [...room.clients.values()].map(c => ({ id: c.id, name: c.name, color: c.color, isHost: c.isHost }));
}

function roomState(room) {
  return {
    type: 'room_state',
    queue: room.queue,
    currentIdx: room.currentIdx,
    isPlaying: room.isPlaying,
    currentTime: roomCurrentTime(room),
    serverTime: Date.now(),
    host: room.host,
    listeners: listenerList(room),
  };
}

function broadcast(room, msg, exceptWs = null) {
  const data = JSON.stringify(msg);
  for (const [ws] of room.clients) {
    if (ws !== exceptWs && ws.readyState === WebSocket.OPEN) ws.send(data);
  }
}
function broadcastAll(room, msg) { broadcast(room, msg, null); }

// ─── IDLE ROOM CLEANUP ─────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (room.clients.size === 0 || (now - room.lastActivity) > ROOM_TTL_MS) {
      for (const ws of room.clients.keys()) { try { ws.close(); } catch {} }
      rooms.delete(code);
      console.log(`[cleanup] Room ${code} expired`);
    }
  }
}, 60_000);

// ─── WEBSOCKET ─────────────────────────────────
wss.on('connection', (ws, req) => {
  let currentRoom = null;
  let myInfo      = null;

  // Heartbeat — keeps Railway/proxies from timing out idle connections
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    // Guard: oversized message
    if (raw.length > MAX_MSG_BYTES) { ws.send(JSON.stringify({ type: 'error', msg: 'Message too large' })); return; }

    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (currentRoom) currentRoom.lastActivity = Date.now();

    // Sanitise name
    if (msg.name) msg.name = String(msg.name).trim().substring(0, 24);

    switch (msg.type) {

      // ── CREATE ──────────────────────────────
      case 'create': {
        if (rooms.size >= MAX_ROOMS) { ws.send(JSON.stringify({ type: 'error', msg: 'Server full — try again later' })); return; }
        if (!msg.name) { ws.send(JSON.stringify({ type: 'error', msg: 'Name required' })); return; }
        const code  = crypto.randomBytes(3).toString('hex').toUpperCase();
        const room  = makeRoom(code, msg.name, ws, msg.color || ['#7c6df5','#ede8ff']);
        currentRoom = room;
        myInfo      = room.clients.get(ws);
        ws.send(JSON.stringify({ type: 'created', code }));
        ws.send(JSON.stringify(roomState(room)));
        console.log(`[room] Created ${code} by ${msg.name} (total: ${rooms.size})`);
        break;
      }

      // ── JOIN ────────────────────────────────
      case 'join': {
        const code = String(msg.code || '').toUpperCase().trim();
        const room = rooms.get(code);
        if (!room) { ws.send(JSON.stringify({ type: 'error', msg: 'Room not found — check the code' })); return; }
        if (room.clients.size >= MAX_CLIENTS_ROOM) { ws.send(JSON.stringify({ type: 'error', msg: 'Room is full (max 50)' })); return; }
        if (!msg.name) { ws.send(JSON.stringify({ type: 'error', msg: 'Name required' })); return; }
        // Reject duplicate names
        const nameTaken = [...room.clients.values()].some(c => c.name.toLowerCase() === msg.name.toLowerCase());
        if (nameTaken) { ws.send(JSON.stringify({ type: 'error', msg: 'Name already taken in this room' })); return; }

        const info = { id: crypto.randomUUID(), name: msg.name, color: msg.color || ['#3ecf7c','#d1f7e7'], isHost: false };
        room.clients.set(ws, info);
        currentRoom = room; myInfo = info;

        ws.send(JSON.stringify(roomState(room)));
        broadcast(room, { type: 'user_joined', listeners: listenerList(room) }, ws);
        broadcast(room, { type: 'chat', system: true, text: `${info.name} joined the room 👋` });
        console.log(`[room] ${msg.name} joined ${code} (${room.clients.size} listeners)`);
        break;
      }

      // ── PLAYBACK (host only) ─────────────────
      case 'play': {
        if (!currentRoom || !myInfo?.isHost) return;
        currentRoom.elapsed   = typeof msg.currentTime === 'number' ? msg.currentTime : roomCurrentTime(currentRoom);
        currentRoom.isPlaying = true;
        currentRoom.startedAt = Date.now();
        broadcastAll(currentRoom, { type: 'play', currentTime: currentRoom.elapsed, serverTime: Date.now() });
        break;
      }

      case 'pause': {
        if (!currentRoom || !myInfo?.isHost) return;
        currentRoom.elapsed   = roomCurrentTime(currentRoom);
        currentRoom.isPlaying = false;
        currentRoom.startedAt = null;
        broadcastAll(currentRoom, { type: 'pause', currentTime: currentRoom.elapsed });
        break;
      }

      case 'seek': {
        if (!currentRoom || !myInfo?.isHost) return;
        if (typeof msg.currentTime !== 'number') return;
        currentRoom.elapsed   = Math.max(0, msg.currentTime);
        currentRoom.startedAt = currentRoom.isPlaying ? Date.now() : null;
        broadcastAll(currentRoom, { type: 'seek', currentTime: currentRoom.elapsed, serverTime: Date.now(), isPlaying: currentRoom.isPlaying });
        break;
      }

      case 'load_song': {
        if (!currentRoom || !myInfo?.isHost) return;
        const idx = parseInt(msg.idx);
        if (isNaN(idx) || idx < 0 || idx >= currentRoom.queue.length) return;
        currentRoom.currentIdx = idx;
        currentRoom.elapsed    = 0;
        currentRoom.isPlaying  = true;
        currentRoom.startedAt  = Date.now();
        currentRoom.skipVotes.clear();
        broadcastAll(currentRoom, {
          type: 'load_song', idx,
          song: currentRoom.queue[idx],
          queue: currentRoom.queue,
          serverTime: Date.now(),
          reason: msg.reason || 'manual',
        });
        break;
      }

      // ── QUEUE ────────────────────────────────
      case 'queue_add': {
        if (!currentRoom) return;
        if (currentRoom.queue.length >= MAX_QUEUE_LEN) { ws.send(JSON.stringify({ type: 'error', msg: 'Queue full (max 200 songs)' })); return; }
        // Validate song object minimally
        const s = msg.song;
        if (!s || !s.id || typeof s.id !== 'string' || !s.title) return;
        // Sanitise
        const song = {
          id: s.id.replace(/[^a-zA-Z0-9_-]/g, '').substring(0, 11),
          title: String(s.title).substring(0, 120),
          artist: String(s.artist || '').substring(0, 80),
          dur: String(s.dur || '').substring(0, 10),
          thumbnail: typeof s.thumbnail === 'string' ? s.thumbnail.substring(0, 200) : '',
          emoji: String(s.emoji || '🎵').substring(0, 4),
          genre: String(s.genre || 'pop').substring(0, 20),
          bpm: Number(s.bpm) || 120,
          energy: Math.min(1, Math.max(0, Number(s.energy) || 0.5)),
          mood: ['calm','happy','melancholy','energetic'].includes(s.mood) ? s.mood : 'happy',
          addedBy: myInfo.name,
          autoQueued: !!s.autoQueued,
          votes: [],
        };
        currentRoom.queue.push(song);
        broadcastAll(currentRoom, {
          type: 'queue_update',
          queue: currentRoom.queue,
          currentIdx: currentRoom.currentIdx,
          addedBy: myInfo.name,
          song,
        });
        // Only announce in chat if guest added it (avoids spam during host init)
        if (!myInfo.isHost) {
          broadcast(currentRoom, { type: 'chat', system: true, text: `${myInfo.name} added "${song.title}"` });
        }
        break;
      }

      case 'queue_remove': {
        if (!currentRoom || !myInfo?.isHost) return;
        const idx = parseInt(msg.idx);
        if (isNaN(idx) || idx < 0 || idx >= currentRoom.queue.length || idx === currentRoom.currentIdx) return;
        currentRoom.queue.splice(idx, 1);
        if (idx < currentRoom.currentIdx) currentRoom.currentIdx--;
        broadcastAll(currentRoom, { type: 'queue_update', queue: currentRoom.queue, currentIdx: currentRoom.currentIdx });
        break;
      }

      case 'queue_reorder': {
        if (!currentRoom || !myInfo?.isHost) return;
        if (!Array.isArray(msg.queue)) return;
        // Only accept reorders of same songs (no injecting new ones)
        if (msg.queue.length !== currentRoom.queue.length) return;
        currentRoom.queue      = msg.queue;
        currentRoom.currentIdx = parseInt(msg.currentIdx) || 0;
        broadcastAll(currentRoom, { type: 'queue_update', queue: currentRoom.queue, currentIdx: currentRoom.currentIdx });
        break;
      }

      // ── CHAT ─────────────────────────────────
      case 'chat': {
        if (!currentRoom) return;
        const text = String(msg.text || '').substring(0, 300);
        if (!text.trim()) return;
        broadcastAll(currentRoom, {
          type: 'chat', system: false,
          name: myInfo.name, color: myInfo.color,
          text, ts: Date.now(),
        });
        break;
      }

      // ── SKIP VOTE ────────────────────────────
      case 'skip_vote': {
        if (!currentRoom) return;
        currentRoom.skipVotes.add(myInfo.id);
        const needed = Math.max(2, Math.ceil(currentRoom.clients.size / 2));
        broadcastAll(currentRoom, {
          type: 'skip_vote',
          count: currentRoom.skipVotes.size,
          needed,
          voter: myInfo.name,
        });
        if (currentRoom.skipVotes.size >= needed) {
          currentRoom.skipVotes.clear();
          const nextIdx = currentRoom.currentIdx + 1;
          if (nextIdx < currentRoom.queue.length) {
            currentRoom.currentIdx = nextIdx;
            currentRoom.elapsed    = 0;
            currentRoom.startedAt  = Date.now();
            currentRoom.isPlaying  = true;
            broadcastAll(currentRoom, {
              type: 'load_song', idx: nextIdx,
              song: currentRoom.queue[nextIdx],
              queue: currentRoom.queue,
              serverTime: Date.now(),
              reason: 'skip_vote',
            });
          }
        }
        break;
      }

      // ── HOST TRANSFER ────────────────────────
      case 'transfer_host': {
        if (!currentRoom || !myInfo?.isHost) return;
        const target = String(msg.to || '');
        for (const [, info] of currentRoom.clients) {
          if (info.name === target) {
            info.isHost   = true;
            myInfo.isHost = false;
            currentRoom.host = info.name;
            broadcastAll(currentRoom, { type: 'host_changed', newHost: info.name, listeners: listenerList(currentRoom) });
            break;
          }
        }
        break;
      }

      // ── KICK ─────────────────────────────────
      case 'kick': {
        if (!currentRoom || !myInfo?.isHost) return;
        const target = String(msg.name || '');
        for (const [ws2, info] of currentRoom.clients) {
          if (info.name === target && ws2 !== ws) {
            try { ws2.send(JSON.stringify({ type: 'kicked' })); ws2.close(); } catch {}
            break;
          }
        }
        break;
      }

      // ── RE-SYNC ──────────────────────────────
      case 'resync_request': {
        if (!currentRoom) return;
        ws.send(JSON.stringify({
          type: 'resync',
          currentTime: roomCurrentTime(currentRoom),
          serverTime: Date.now(),
          isPlaying: currentRoom.isPlaying,
          idx: currentRoom.currentIdx,
        }));
        break;
      }

      // ── UPVOTE ───────────────────────────────
      case 'upvote': {
        if (!currentRoom) return;
        const song = currentRoom.queue.find(s => s.id === msg.videoId);
        if (!song) return;
        if (!song.votes) song.votes = [];
        const vi = song.votes.indexOf(myInfo.id);
        if (vi > -1) song.votes.splice(vi, 1); else song.votes.push(myInfo.id);
        // Re-sort upcoming songs by votes
        const played = currentRoom.queue.slice(0, currentRoom.currentIdx + 1);
        const rest   = currentRoom.queue.slice(currentRoom.currentIdx + 1)
                         .sort((a, b) => (b.votes?.length || 0) - (a.votes?.length || 0));
        currentRoom.queue = [...played, ...rest];
        broadcastAll(currentRoom, { type: 'queue_update', queue: currentRoom.queue, currentIdx: currentRoom.currentIdx });
        break;
      }
    }
  });

  ws.on('close', () => {
    if (!currentRoom) return;
    currentRoom.clients.delete(ws);
    if (currentRoom.clients.size === 0) {
      rooms.delete(currentRoom.code);
      console.log(`[room] ${currentRoom.code} closed — empty`);
      return;
    }
    // Auto-promote if host left
    if (myInfo?.isHost) {
      const [, newInfo] = currentRoom.clients.entries().next().value;
      newInfo.isHost   = true;
      currentRoom.host = newInfo.name;
      broadcastAll(currentRoom, { type: 'host_changed', newHost: newInfo.name, listeners: listenerList(currentRoom) });
    }
    broadcast(currentRoom, { type: 'user_left', name: myInfo?.name, listeners: listenerList(currentRoom) });
    broadcast(currentRoom, { type: 'chat', system: true, text: `${myInfo?.name} left the room` });
  });

  ws.on('error', (err) => console.error('[ws error]', err.message));
});

// Heartbeat — ping all clients every 25s, drop dead ones
const heartbeat = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, WS_PING_MS);
wss.on('close', () => clearInterval(heartbeat));

// ─── REST: YouTube search proxy ────────────────
app.get('/api/search', async (req, res) => {
  const ip = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress;
  if (rateLimit(ip)) return res.status(429).json({ error: 'Rate limit — slow down' });

  const q = String(req.query.q || '').trim().substring(0, 100);
  if (!q) return res.status(400).json({ error: 'Missing query' });

  if (!YT_API_KEY || YT_API_KEY === 'YOUR_KEY_HERE') {
    return res.status(503).json({ error: 'no_api_key', message: 'Set YOUTUBE_API_KEY env var' });
  }

  try {
    const [searchRes, detailRes] = await Promise.all([
      axios.get(YT_SEARCH, { params: { part: 'snippet', q, type: 'video', videoCategoryId: '10', maxResults: 8, key: YT_API_KEY }, timeout: 8000 }),
      // We fetch details in parallel using a dummy first call — replaced below
    ]).then(async ([sr]) => {
      const ids = sr.data.items.map(i => i.id.videoId).join(',');
      const dr  = await axios.get(YT_VIDEOS, { params: { part: 'contentDetails', id: ids, key: YT_API_KEY }, timeout: 8000 });
      return [sr, dr];
    });

    const durMap = {};
    detailRes.data.items.forEach(v => { durMap[v.id] = parseDuration(v.contentDetails.duration); });

    const results = searchRes.data.items.map(item => ({
      id:        item.id.videoId,
      title:     item.snippet.title,
      artist:    item.snippet.channelTitle,
      thumbnail: item.snippet.thumbnails.medium?.url || '',
      dur:       durMap[item.id.videoId] || '?:??',
      genre: 'pop', bpm: 120, energy: 0.5, mood: 'happy',
    }));

    res.json({ results });
  } catch (err) {
    const status = err.response?.status || 500;
    console.error('[search error]', err.response?.data?.error?.message || err.message);
    res.status(status).json({ error: 'YouTube API error', details: err.response?.data?.error?.message });
  }
});

// oEmbed — get title+thumbnail from a URL, no API key needed
app.get('/api/oembed', async (req, res) => {
  const url = String(req.query.url || '').trim();
  if (!url || !url.includes('youtube')) return res.status(400).json({ error: 'Invalid URL' });
  try {
    const r = await axios.get(YT_OEMBED, { params: { url, format: 'json' }, timeout: 6000 });
    res.json({
      title:     r.data.title,
      author:    r.data.author_name,
      thumbnail: `https://img.youtube.com/vi/${extractId(url)}/mqdefault.jpg`,
    });
  } catch {
    res.status(404).json({ error: 'Video not found or unavailable' });
  }
});

// Room info endpoint
app.get('/api/room/:code', (req, res) => {
  const room = rooms.get(req.params.code.toUpperCase());
  if (!room) return res.status(404).json({ error: 'Room not found' });
  res.json({ code: room.code, host: room.host, listenerCount: room.clients.size, currentSong: room.queue[room.currentIdx] || null });
});

// Health check — Railway uses this
app.get('/health', (_, res) => res.json({ ok: true, rooms: rooms.size, clients: wss.clients.size }));

// ─── HELPERS ───────────────────────────────────
function parseDuration(iso = '') {
  const m = iso.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!m) return '?:??';
  const h = parseInt(m[1] || 0), min = parseInt(m[2] || 0), sec = parseInt(m[3] || 0);
  return h > 0
    ? `${h}:${String(min).padStart(2,'0')}:${String(sec).padStart(2,'0')}`
    : `${min}:${String(sec).padStart(2,'0')}`;
}

function extractId(url = '') {
  const m = url.match(/(?:v=|youtu\.be\/)([a-zA-Z0-9_-]{11})/);
  return m ? m[1] : '';
}

// ─── START ─────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  const isRailway = !!process.env.RAILWAY_PUBLIC_DOMAIN;
  console.log(`\n🎵 SyncWave`);
  console.log(`   Local : http://localhost:${PORT}`);
  if (isRailway) console.log(`   Public: https://${process.env.RAILWAY_PUBLIC_DOMAIN}`);
  console.log(YT_API_KEY && YT_API_KEY !== 'YOUR_KEY_HERE'
    ? `   ✅ YouTube search enabled`
    : `   ⚠  No API key — paste YouTube URLs to add songs`);
  console.log();
});

// Graceful shutdown
process.on('SIGTERM', () => { server.close(() => process.exit(0)); });
process.on('SIGINT',  () => { server.close(() => process.exit(0)); });
