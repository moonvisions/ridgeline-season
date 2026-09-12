#!/usr/bin/env node
/*
 * Ridgeline Season — game server
 * ==================================================================
 * One process does three jobs:
 *
 *   1. ACCOUNTS  — register / login with salted scrypt password hashes,
 *                  bearer tokens, per-IP rate limiting.
 *   2. STATS     — every account's career (harvests, arena wins, best
 *                  scores, campaign stars) stored on disk, plus public
 *                  leaderboards. Arena results are written by the server
 *                  itself, so they cannot be forged by a client.
 *   3. ARENA     — the online match. The SERVER runs the animals and
 *                  decides every hit. Clients only send "I fired from
 *                  here, facing this way". That is what makes the arena
 *                  leaderboard trustworthy.
 *
 * RUN
 *   npm install ws
 *   node server.js               (PORT=8080 by default)
 *   ADMIN_KEY=secret node server.js   enables /api/admin/* endpoints
 *
 * STORAGE
 *   data.json next to this file, written atomically. This is fine for
 *   thousands of accounts. Past that, swap the load()/persist() pair for
 *   SQLite or Postgres — nothing else touches the disk.
 *
 * DEPLOYING FOR REAL
 *   Put this behind nginx or Caddy with HTTPS so the game reaches it at
 *   wss:// and https://. Set ORIGIN to your game's URL to lock CORS down.
 */

'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const ORIGIN = process.env.ORIGIN || '*';          // set to your site in production
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const DATA_FILE = path.join(__dirname, 'data.json');
const TICK_MS = 66;                                // 15 Hz
const MATCH_SECONDS = 180;
const MAX_PER_ROOM = 8;
const MAP_R = 300;
const COLORS = ['#e2542b', '#2b7fe2', '#8e2be2', '#e2c02b', '#2be29a', '#e22b7f', '#5ad6e2', '#e28b2b'];

// ---------------------------------------------------------------- storage
let DB = { users: {}, tokens: {} };
function load() {
  try { DB = Object.assign(DB, JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'))); }
  catch (e) { if (e.code !== 'ENOENT') console.error('load failed', e); }
}
let saveTimer = null;
function persist() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(DB));
    fs.renameSync(tmp, DATA_FILE);
  }, 300);
}
load();

// ---------------------------------------------------------------- accounts
const NAME_RE = /^[A-Za-z0-9_]{3,14}$/;
function hashPassword(pw, salt) {
  salt = salt || crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(pw, salt, 64, { N: 16384, r: 8, p: 1 }).toString('hex');
  return { salt, hash };
}
function verifyPassword(pw, rec) {
  const { hash } = hashPassword(pw, rec.salt);
  return crypto.timingSafeEqual(Buffer.from(hash, 'hex'), Buffer.from(rec.hash, 'hex'));
}
function newUser(name, pw) {
  const { salt, hash } = hashPassword(pw);
  return {
    name, salt, hash, created: Date.now(),
    stats: { harvests: 0, bears: 0, trophies: 0, longest: 0, arenaMatches: 0, arenaWins: 0, bestArena: 0, arenaKills: 0, campaignStars: 0, rank: 0, chalLong: 0, chalKills: 0, chalTags: 0 },
    cloud: null,          // client's campaign save, backed up here
  };
}
function issueToken(name) {
  const tok = crypto.randomBytes(24).toString('base64url');
  DB.tokens[tok] = { name, at: Date.now() };
  persist();
  return tok;
}
function userFromToken(tok) {
  const t = tok && DB.tokens[tok];
  if (!t) return null;
  if (Date.now() - t.at > 30 * 86400e3) { delete DB.tokens[tok]; return null; }
  return DB.users[t.name.toLowerCase()] || null;
}
function publicStats(u) { return { name: u.name, ...u.stats }; }

// ---------------------------------------------------------------- rate limiting
const buckets = new Map();
function limited(key, max, windowMs) {
  const now = Date.now();
  let b = buckets.get(key);
  if (!b || now > b.reset) { b = { n: 0, reset: now + windowMs }; buckets.set(key, b); }
  b.n++;
  return b.n > max;
}
setInterval(() => { const now = Date.now(); for (const [k, b] of buckets) if (now > b.reset) buckets.delete(k); }, 60e3);

// ---------------------------------------------------------------- HTTP API
function json(res, code, obj) {
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': ORIGIN,
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let s = ''; req.on('data', c => { s += c; if (s.length > 64 * 1024) { reject(new Error('too large')); req.destroy(); } });
    req.on('end', () => { try { resolve(s ? JSON.parse(s) : {}); } catch { reject(new Error('bad json')); } });
  });
}
function ipOf(req) { return (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim(); }

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const ip = ipOf(req);
  if (req.method === 'OPTIONS') return json(res, 204, {});
  try {
    if (url.pathname === '/' || url.pathname === '/api/status') {
      return json(res, 200, { ok: true, players: [...rooms.values()].reduce((a, r) => a + r.players.size, 0), rooms: rooms.size, accounts: Object.keys(DB.users).length });
    }
    if (url.pathname === '/api/register' && req.method === 'POST') {
      if (limited('reg:' + ip, 5, 3600e3)) return json(res, 429, { error: 'Too many sign-ups from this address. Try later.' });
      const b = await readBody(req);
      const name = String(b.name || '').trim(), pw = String(b.password || '');
      if (!NAME_RE.test(name)) return json(res, 400, { error: 'Name must be 3–14 letters, numbers or _' });
      if (pw.length < 8) return json(res, 400, { error: 'Password must be at least 8 characters' });
      if (DB.users[name.toLowerCase()]) return json(res, 409, { error: 'That name is taken' });
      DB.users[name.toLowerCase()] = newUser(name, pw);
      const token = issueToken(name);
      console.log(`[account] registered ${name}`);
      return json(res, 200, { token, user: publicStats(DB.users[name.toLowerCase()]) });
    }
    if (url.pathname === '/api/login' && req.method === 'POST') {
      if (limited('login:' + ip, 20, 900e3)) return json(res, 429, { error: 'Too many attempts. Wait 15 minutes.' });
      const b = await readBody(req);
      const u = DB.users[String(b.name || '').toLowerCase()];
      if (!u || !verifyPassword(String(b.password || ''), u)) return json(res, 401, { error: 'Wrong name or password' });
      return json(res, 200, { token: issueToken(u.name), user: publicStats(u), cloud: u.cloud });
    }
    const token = (req.headers.authorization || '').replace(/^Bearer /, '');
    if (url.pathname === '/api/me') {
      const u = userFromToken(token); if (!u) return json(res, 401, { error: 'Not signed in' });
      return json(res, 200, { user: publicStats(u), cloud: u.cloud });
    }
    if (url.pathname === '/api/logout' && req.method === 'POST') { delete DB.tokens[token]; persist(); return json(res, 200, { ok: true }); }
    if (url.pathname === '/api/cloud' && req.method === 'POST') {
      // Campaign save backup. Self-reported (single-player), so it feeds only
      // the "campaignStars" figure which is shown separately from arena stats.
      const u = userFromToken(token); if (!u) return json(res, 401, { error: 'Not signed in' });
      if (limited('cloud:' + u.name, 60, 3600e3)) return json(res, 429, { error: 'Slow down' });
      const b = await readBody(req);
      if (typeof b.save !== 'object' || JSON.stringify(b.save).length > 32 * 1024) return json(res, 400, { error: 'Bad save' });
      u.cloud = b.save;
      const stars = b.save.stars && Object.values(b.save.stars).reduce((a, n) => a + (n | 0), 0);
      u.stats.campaignStars = Math.min(42, stars | 0);
      persist();
      return json(res, 200, { ok: true });
    }
    if (url.pathname === '/api/challenge' && req.method === 'POST') {
      // Challenge runs happen entirely on the client, so these scores are
      // self-reported. They are capped at what is physically achievable in the
      // mode's time limit and rate-limited; the arena board is the one backed by
      // server-side simulation, and the UI says so.
      const u = userFromToken(token);
      if (!u) return json(res, 401, { error: 'Sign in to post a score' });
      if (limited('chal:' + u.name, 40, 3600e3)) return json(res, 429, { error: 'Too many submissions, slow down' });
      const b = await readBody(req);
      const CAPS = { longshot: ['chalLong', 260], harvest: ['chalKills', 45], tagged: ['chalTags', 30] };
      const spec = CAPS[b.mode];
      if (!spec) return json(res, 400, { error: 'Unknown challenge' });
      const [field, cap] = spec;
      const score = Math.round(Number(b.score));
      if (!Number.isFinite(score) || score < 0) return json(res, 400, { error: 'Bad score' });
      if (score > cap) { console.log(`[flag] ${u.name} posted ${score} for ${b.mode} (cap ${cap}) — rejected`); return json(res, 400, { error: 'Score outside the possible range' }); }
      if (score > (u.stats[field] | 0)) { u.stats[field] = score; persist(); }
      return json(res, 200, { ok: true, best: u.stats[field] });
    }
    if (url.pathname === '/api/leaderboard') {
      const key = ['bestArena', 'arenaWins', 'harvests', 'campaignStars', 'chalLong', 'chalKills', 'chalTags'].includes(url.searchParams.get('by')) ? url.searchParams.get('by') : 'bestArena';
      const rows = Object.values(DB.users).map(publicStats).sort((a, b) => b[key] - a[key]).slice(0, 25);
      return json(res, 200, { by: key, rows });
    }
    if (url.pathname.startsWith('/api/admin/')) {
      if (!ADMIN_KEY || req.headers['x-admin-key'] !== ADMIN_KEY) return json(res, 403, { error: 'Forbidden' });
      if (url.pathname === '/api/admin/users') return json(res, 200, Object.values(DB.users).map(u => ({ ...publicStats(u), created: u.created })));
      if (url.pathname === '/api/admin/rooms') return json(res, 200, [...rooms.values()].map(r => ({ name: r.name, players: [...r.players.values()].map(p => p.name), seconds: Math.round(r.timeLeft) })));
    }
    json(res, 404, { error: 'Not found' });
  } catch (e) { json(res, 400, { error: e.message }); }
});

// ---------------------------------------------------------------- arena: species (mirrors the client)
const SP = {
  deer:  { k: 1.15, rx: .7, ry: .28, bodyY: .82, hp: 1,   pts: 100, flee: 7.5, weight: 34, antlers: true },
  boar:  { k: 1.05, rx: .7, ry: .33, bodyY: .6,  hp: 1.5, pts: 150, flee: 8.5, weight: 20, antlers: false },
  elk:   { k: 1.45, rx: .72, ry: .3, bodyY: .86, hp: 1.8, pts: 250, flee: 7,   weight: 16, antlers: true },
  moose: { k: 1.85, rx: .72, ry: .32, bodyY: .9, hp: 2.5, pts: 400, flee: 6,   weight: 7,  antlers: true },
  sheep: { k: 1.1,  rx: .62, ry: .29, bodyY: .75, hp: 1.2, pts: 320, flee: 8,  weight: 8,  antlers: true },
};
const SPK = Object.keys(SP);
const TAU = Math.PI * 2;
const wrap = a => { while (a > Math.PI) a -= TAU; while (a < -Math.PI) a += TAU; return a; };
const rnd = (a, b) => a + Math.random() * (b - a);
function pickSpecies() { let r = Math.random() * SPK.reduce((s, k) => s + SP[k].weight, 0); for (const k of SPK) { r -= SP[k].weight; if (r <= 0) return k; } return 'deer'; }

let animalId = 1;
function spawnAnimal(room) {
  const sp = pickSpecies(), s = SP[sp];
  const ps = [...room.players.values()];
  const around = ps.length ? ps[Math.floor(Math.random() * ps.length)] : { x: 0, z: 0 };
  const a = rnd(0, TAU), d = rnd(45, 140);
  let x = around.x + Math.sin(a) * d, z = around.z + Math.cos(a) * d;
  const dd = Math.hypot(x, z); if (dd > MAP_R - 10) { x *= (MAP_R - 10) / dd; z *= (MAP_R - 10) / dd; }
  return { id: animalId++, sp, x, z, heading: rnd(0, TAU), speed: 0, state: 'graze', t: rnd(2, 6), hp: s.hp, maxhp: s.hp,
           male: sp === 'deer' ? Math.random() < .5 : true, trophy: s.antlers && Math.random() < .12, alert: 0, deadT: 0 };
}
function nearestPlayerDist(room, a) { let d = 1e9; for (const p of room.players.values()) d = Math.min(d, Math.hypot(p.x - a.x, p.z - a.z)); return d; }
function updateAnimal(room, a, dt) {
  const s = SP[a.sp];
  if (a.state === 'dead') { a.deadT += dt; return; }
  a.t -= dt;
  const d = nearestPlayerDist(room, a);
  if (d < 18 && a.state !== 'flee') { a.state = 'flee'; a.t = rnd(4, 7); }
  if (a.state === 'graze') { a.speed = 0; if (a.t <= 0) { a.state = 'walk'; a.heading = rnd(0, TAU); a.t = rnd(3, 8); } }
  else if (a.state === 'walk') { a.speed = .9 * s.k; a.heading += Math.sin(room.time * .5 + a.id) * dt * .4; if (a.t <= 0) { a.state = 'graze'; a.t = rnd(3, 7); } }
  else if (a.state === 'flee') {
    a.speed = s.flee;
    let nearest = null, nd = 1e9;
    for (const p of room.players.values()) { const pd = Math.hypot(p.x - a.x, p.z - a.z); if (pd < nd) { nd = pd; nearest = p; } }
    if (nearest && nd < 60) a.heading = Math.atan2(a.x - nearest.x, a.z - nearest.z) + Math.sin(room.time * 2 + a.id) * .3;
    if (a.t <= 0) { a.state = 'walk'; a.t = rnd(3, 6); }
  }
  a.x += Math.sin(a.heading) * a.speed * dt; a.z += Math.cos(a.heading) * a.speed * dt;
  const dd = Math.hypot(a.x, a.z); if (dd > MAP_R - 12) a.heading = Math.atan2(-a.x, -a.z) + rnd(-.4, .4);
}
function spook(room, x, z, radius) { for (const a of room.animals) { if (a.state === 'dead') continue; if (Math.hypot(a.x - x, a.z - z) < radius) { a.state = 'flee'; a.t = rnd(4, 7); } } }

// The hit test. Everything a client can influence is checked here.
function resolveShot(room, p, m) {
  const now = room.time;
  const weapon = m.weapon === 'bow' ? 'bow' : 'rifle';
  if (now - p.lastShot < (weapon === 'bow' ? 1.0 : 0.8)) return { ok: false, why: 'too fast' };
  p.lastShot = now;
  // Where the client says it fired from must agree with where the server has seen it.
  const cx = +m.x, cz = +m.z;
  if (!Number.isFinite(cx) || !Number.isFinite(cz) || Math.hypot(cx - p.x, cz - p.z) > 8) return { ok: false, why: 'position mismatch' };
  const yaw = +m.yaw, pitch = Math.max(-.7, Math.min(.7, +m.pitch || 0));
  if (!Number.isFinite(yaw)) return { ok: false, why: 'bad aim' };
  spook(room, p.x, p.z, weapon === 'bow' ? 14 : 150);
  let best = null;
  for (const a of room.animals) {
    if (a.state === 'dead') continue;
    const s = SP[a.sp], dx = a.x - p.x, dz = a.z - p.z, dist = Math.hypot(dx, dz);
    if (dist < 1 || dist > 230) continue;
    const dyaw = wrap(Math.atan2(dx, dz) - yaw);
    if (Math.abs(dyaw) > .35) continue;
    const lateral = Math.abs(dist * Math.tan(dyaw));
    let h = 1.6 + dist * Math.tan(pitch);
    if (weapon === 'bow') { const v = m.bowSpeed ? Math.min(80, +m.bowSpeed) : 58; const tf = dist / v; h -= 4.9 * tf * tf; }
    const cy = s.bodyY * s.k, vert = Math.abs(h - cy);
    let zone = null;
    if (lateral < s.rx * s.k * .45 && vert < s.ry * s.k * .65) zone = 'vital';
    else if (lateral < s.rx * s.k && vert < s.ry * s.k * 1.35) zone = 'body';
    else if (lateral < s.rx * s.k && h < cy - s.ry * s.k && h > 0) zone = 'leg';
    else if (lateral < s.rx * s.k * .6 && h > cy + s.ry * s.k && h < cy + s.ry * s.k + .6 * s.k) zone = 'head';
    if (zone && (!best || dist < best.dist)) best = { a, zone, dist };
  }
  if (!best) return { ok: true, hit: null };
  const { a, zone, dist } = best, s = SP[a.sp];
  const magnum = !!m.magnum;
  const dmg = zone === 'vital' ? 10 : zone === 'head' ? (weapon === 'rifle' ? 10 : 3) : zone === 'body' ? (weapon === 'rifle' ? (magnum ? 2.2 : 1.2) : .9) : .5;
  a.hp -= dmg;
  if (a.hp > 0) { a.state = 'flee'; a.t = rnd(6, 9); a.wounded = true; return { ok: true, hit: { id: a.id, zone, dist, killed: false } }; }
  a.state = 'dead'; a.deadT = 0; a.speed = 0;
  const bonus = zone === 'vital' ? 2 : zone === 'head' ? 1.4 : 1;
  const pts = Math.round(s.pts * (1 + dist / 100) * bonus * (weapon === 'bow' ? 1.6 : 1) * (a.trophy ? 2 : 1));
  p.score += pts; p.kills++;
  if (a.trophy) p.trophies++;
  p.longest = Math.max(p.longest, dist);
  setTimeout(() => { if (rooms.has(room.name)) { room.animals = room.animals.filter(x => x.id !== a.id); room.animals.push(spawnAnimal(room)); } }, 5000);
  return { ok: true, hit: { id: a.id, zone, dist, killed: true, pts, species: a.sp, trophy: a.trophy } };
}

// ---------------------------------------------------------------- rooms
const rooms = new Map();
let nextPid = 1;
function getRoom(name) {
  let r = rooms.get(name);
  if (!r) {
    r = { name, seed: (Math.random() * 0xffffffff) >>> 0, players: new Map(), animals: [], time: 0, timeLeft: MATCH_SECONDS, running: false, emptiedAt: 0 };
    rooms.set(name, r);
    console.log(`[room] ${name} created`);
  }
  r.emptiedAt = 0;
  return r;
}
function send(ws, o) { if (ws.readyState === 1) ws.send(JSON.stringify(o)); }
function broadcast(room, o, except) { const m = JSON.stringify(o); for (const p of room.players.values()) if (p.id !== except && p.ws.readyState === 1) p.ws.send(m); }
function startMatch(room) {
  room.running = true; room.time = 0; room.timeLeft = MATCH_SECONDS; room.animals = [];
  for (const p of room.players.values()) { p.score = 0; p.kills = 0; p.trophies = 0; p.longest = 0; }
  for (let i = 0; i < 18; i++) room.animals.push(spawnAnimal(room));
  broadcast(room, { t: 'start', seconds: MATCH_SECONDS });
  console.log(`[match] ${room.name} started with ${room.players.size}`);
}
function endMatch(room) {
  room.running = false;
  const board = [...room.players.values()].map(p => ({ id: p.id, name: p.name, score: p.score, kills: p.kills })).sort((a, b) => b.score - a.score);
  for (const p of room.players.values()) {
    if (!p.user) continue;                                    // guests are not recorded
    const st = p.user.stats; st.arenaMatches++; st.arenaKills += p.kills; st.harvests += p.kills;
    st.trophies += p.trophies; st.longest = Math.max(st.longest, Math.round(p.longest));
    st.bestArena = Math.max(st.bestArena, p.score);
    if (board[0] && board[0].id === p.id && room.players.size > 1) st.arenaWins++;
  }
  persist();
  broadcast(room, { t: 'end', board });
  console.log(`[match] ${room.name} ended: ${board.map(b => b.name + ' ' + b.score).join(', ')}`);
  setTimeout(() => { if (rooms.has(room.name) && room.players.size > 0 && !room.running) startMatch(room); }, 12000);
}

const wss = new WebSocketServer({ server, maxPayload: 4096 });
wss.on('connection', (ws, req) => {
  const ip = ipOf(req);
  if (limited('ws:' + ip, 30, 60e3)) return ws.close();
  let p = null, room = null;
  ws.on('message', raw => {
    if (raw.length > 2048) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'join') {
      if (p) return;
      const user = userFromToken(m.token);
      const roomName = String(m.room || 'RIDGE').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8) || 'RIDGE';
      room = getRoom(roomName);
      if (room.players.size >= MAX_PER_ROOM) { send(ws, { t: 'error', message: 'Room is full' }); return ws.close(); }
      const name = user ? user.name : ('Guest' + (String(m.name || '').replace(/[^A-Za-z0-9_]/g, '').slice(0, 10) || nextPid));
      p = { id: 'p' + (nextPid++), name, user, color: COLORS[room.players.size % COLORS.length], ws,
            x: 0, z: 0, yaw: 0, score: 0, kills: 0, trophies: 0, longest: 0, firing: 0, lastShot: -9, lastState: Date.now() };
      room.players.set(p.id, p);
      send(ws, { t: 'welcome', id: p.id, seed: room.seed, authoritative: true, seconds: room.running ? room.timeLeft : MATCH_SECONDS, guest: !user,
                 players: [...room.players.values()].map(q => ({ id: q.id, name: q.name, color: q.color })) });
      broadcast(room, { t: 'joined', player: { id: p.id, name: p.name, color: p.color } }, p.id);
      if (!room.running) startMatch(room);
      else send(ws, { t: 'start', seconds: room.timeLeft });
      console.log(`[join] ${p.name}${user ? '' : ' (guest)'} -> ${room.name}`);
      return;
    }
    if (!p || !room) return;
    if (m.t === 'state') {
      // Clamp movement to a sane speed so nobody teleports across the map.
      const now = Date.now(), dt = Math.max(.02, (now - p.lastState) / 1000); p.lastState = now;
      const nx = +m.x, nz = +m.z; if (!Number.isFinite(nx) || !Number.isFinite(nz)) return;
      const dx = nx - p.x, dz = nz - p.z, dist = Math.hypot(dx, dz), maxD = 6.5 * dt + .5;
      if (dist > maxD) { p.x += dx / dist * maxD; p.z += dz / dist * maxD; } else { p.x = nx; p.z = nz; }
      const rr = Math.hypot(p.x, p.z); if (rr > MAP_R) { p.x *= MAP_R / rr; p.z *= MAP_R / rr; }
      p.yaw = +m.yaw || 0; p.firing = m.firing ? 1 : 0;
      return;
    }
    if (m.t === 'shot') {
      if (!room.running) return;
      const r = resolveShot(room, p, m);
      send(ws, { t: 'shotResult', ok: r.ok, why: r.why, hit: r.hit, score: p.score });
      if (r.hit && r.hit.killed) broadcast(room, { t: 'kill', id: p.id, name: p.name, species: r.hit.species, points: r.hit.pts, animal: r.hit.id }, p.id);
      return;
    }
    if (m.t === 'chat') broadcast(room, { t: 'chat', name: p.name, text: String(m.text || '').slice(0, 120) }, p.id);
  });
  ws.on('close', () => {
    if (!p || !room) return;
    room.players.delete(p.id);
    broadcast(room, { t: 'left', id: p.id });
    if (room.players.size === 0) { room.emptiedAt = Date.now(); room.running = false; room.seed = (Math.random() * 0xffffffff) >>> 0; }
  });
  ws.on('error', () => {});
});

// Simulation + broadcast tick.
setInterval(() => {
  const dt = TICK_MS / 1000;
  for (const room of rooms.values()) {
    if (room.players.size === 0) continue;
    if (room.running) {
      room.time += dt; room.timeLeft -= dt;
      for (const a of room.animals) updateAnimal(room, a, dt);
      if (room.timeLeft <= 0) endMatch(room);
    }
    broadcast(room, {
      t: 'states',
      seconds: Math.max(0, room.timeLeft),
      players: [...room.players.values()].map(q => ({ id: q.id, x: +q.x.toFixed(2), z: +q.z.toFixed(2), yaw: +q.yaw.toFixed(2), score: q.score, kills: q.kills, firing: q.firing })),
      animals: room.animals.map(a => ({ id: a.id, sp: a.sp, x: +a.x.toFixed(2), z: +a.z.toFixed(2), h: +a.heading.toFixed(2), v: +a.speed.toFixed(1), st: a.state === 'dead' ? 'd' : a.state === 'flee' ? 'f' : a.state === 'walk' ? 'w' : 'g', hp: +(a.hp / a.maxhp).toFixed(2), tr: a.trophy ? 1 : 0, m: a.male ? 1 : 0, wd: a.wounded ? 1 : 0 })),
    });
    for (const q of room.players.values()) q.firing = 0;
  }
}, TICK_MS);

setInterval(() => {
  const now = Date.now();
  for (const [name, r] of rooms) if (r.players.size === 0 && r.emptiedAt && now - r.emptiedAt > 300e3) { rooms.delete(name); console.log(`[room] ${name} removed`); }
  for (const [tok, t] of Object.entries(DB.tokens)) if (now - t.at > 30 * 86400e3) delete DB.tokens[tok];
}, 60e3);

server.listen(PORT, () => {
  console.log(`Ridgeline Season server on port ${PORT}`);
  console.log(`  Game connects to:  ws://localhost:${PORT}   (wss:// behind HTTPS)`);
  console.log(`  API / status:      http://localhost:${PORT}/api/status`);
  console.log(`  Accounts on disk:  ${DATA_FILE}`);
});
