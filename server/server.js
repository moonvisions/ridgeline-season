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
// ---- payments (all optional; set only what you use) ----
const STRIPE_KEY = process.env.STRIPE_SECRET_KEY || '';
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';
const PAYPAL_ID = process.env.PAYPAL_CLIENT_ID || '';
const PAYPAL_SECRET = process.env.PAYPAL_SECRET || '';
const PAYPAL_API = process.env.PAYPAL_LIVE === '1' ? 'https://api-m.paypal.com' : 'https://api-m.sandbox.paypal.com';
const SITE_URL = (process.env.SITE_URL || 'http://localhost:8080').replace(/\/+$/, '');
const PRICE_CENTS = parseInt(process.env.PRICE_CENTS || '499', 10);
const CURRENCY = (process.env.CURRENCY || 'usd').toLowerCase();
const PENDING = new Map();   // paypal order id -> account name
const TICK_MS = 66;                                // 15 Hz
const MATCH_SECONDS = 180;
const MAX_PER_ROOM = 8;
const MAP_R = 300;
const COLORS = ['#e2542b', '#2b7fe2', '#8e2be2', '#e2c02b', '#2be29a', '#e22b7f', '#5ad6e2', '#e28b2b'];

// ---------------------------------------------------------------- storage
let DB = { users: {}, tokens: {}, daily: {}, events: [], seasons: {}, totals: { plays: 0, signups: 0, guestSessions: 0 } };
function load() {
  try {
    DB = Object.assign(DB, JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')));
    migrate();
    console.log(`[data] loaded ${Object.keys(DB.users).length} accounts from ${DATA_FILE} (schema v${DB.schema})`);
  }
  catch (e) {
    if (e.code === 'ENOENT') {
      // No file. If a backup exists this is almost certainly a disk that was
      // wiped by a redeploy — restore rather than silently start from zero.
      const bk = latestBackup();
      if (bk) {
        try { DB = Object.assign(DB, JSON.parse(fs.readFileSync(bk, 'utf8'))); migrate(); console.warn(`[data] data.json was missing — restored ${Object.keys(DB.users).length} accounts from ${path.basename(bk)}`); }
        catch (e2) { console.error('[data] backup restore failed', e2); }
      } else { migrate(); console.log(`[data] fresh start — no accounts yet (schema v${DB.schema})`); }
    } else { console.error('[data] load failed', e); migrate(); }
  }
}
// Every version of this file can read every older data.json. New fields get
// defaults; nothing is ever dropped. Bump SCHEMA when a change needs a step.
const SCHEMA = 3;
function migrate() {
  DB.schema = DB.schema | 0;
  DB.daily = DB.daily || {}; DB.events = DB.events || []; DB.seasons = DB.seasons || {}; DB.tokens = DB.tokens || {};
  DB.totals = Object.assign({ plays: 0, signups: 0, guestSessions: 0 }, DB.totals || {});
  for (const u of Object.values(DB.users)) {
    u.stats = Object.assign({ harvests: 0, bestArena: 0, arenaWins: 0, campaignStars: 0, longest: 0, chalLong: 0, chalKills: 0, chalTags: 0, bestWave: 0 }, u.stats || {});
    if (u.premium == null) u.premium = false;
    if (!u.created) u.created = Date.now();
  }
  if (DB.schema < SCHEMA) { console.log(`[data] migrated schema v${DB.schema} -> v${SCHEMA}`); DB.schema = SCHEMA; }
}
const BACKUP_DIR = path.join(path.dirname(DATA_FILE), 'backups');
function latestBackup() {
  try { const f = fs.readdirSync(BACKUP_DIR).filter(n => n.endsWith('.json')).sort(); return f.length ? path.join(BACKUP_DIR, f[f.length - 1]) : null; }
  catch (e) { return null; }
}
// A dated copy on every boot and every hour, keeping the last 30. Cheap
// insurance against the one mistake that loses everyone's progress.
function backup(reason) {
  try {
    if (!Object.keys(DB.users).length) return;
    fs.mkdirSync(BACKUP_DIR, { recursive: true });
    const name = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19) + '.json';
    fs.writeFileSync(path.join(BACKUP_DIR, name), JSON.stringify(DB));
    const all = fs.readdirSync(BACKUP_DIR).filter(n => n.endsWith('.json')).sort();
    for (const old of all.slice(0, Math.max(0, all.length - 30))) fs.unlinkSync(path.join(BACKUP_DIR, old));
    console.log(`[data] backup written (${reason}) — ${Object.keys(DB.users).length} accounts`);
  } catch (e) { console.error('[data] backup failed', e); }
}
let saveTimer = null;
function persist() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    // Refuse to clobber a populated file with an empty one — that is exactly
    // the shape of a bug that erases everybody.
    try {
      if (!Object.keys(DB.users).length && fs.existsSync(DATA_FILE)) {
        const on = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
        if (on && on.users && Object.keys(on.users).length) { console.error('[data] refused to overwrite a populated data.json with an empty one'); return; }
      }
    } catch (e) {}
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
function makeRecoveryCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let out = '';
  for (let i = 0; i < 12; i++) out += (i && i % 4 === 0 ? '-' : '') + A[crypto.randomInt(A.length)];
  return out;
}
function newUser(name, pw) {
  const { salt, hash } = hashPassword(pw);
  const recovery = makeRecoveryCode();
  const rec = hashPassword(recovery);
  return {
    name, salt, hash, created: Date.now(),
    recSalt: rec.salt, recHash: rec.hash, recovery,   // recovery is stripped before storing
    premium: false, premiumSince: 0,
    stats: { harvests: 0, bears: 0, trophies: 0, longest: 0, arenaMatches: 0, arenaWins: 0, bestArena: 0, arenaKills: 0, campaignStars: 0, rank: 0, chalLong: 0, chalKills: 0, chalTags: 0, bestWave: 0 },
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
function grantPremium(name, how) {
  if (!name) return;
  const u = DB.users[String(name).toLowerCase()];
  if (!u) { console.warn('[pay] paid but no such account:', name); return; }
  u.premium = true; u.premiumSince = Date.now(); u.paidVia = how;
  persist();
  console.log(`[pay] ${u.name} is now ad-free (${how})`);
}
function publicStats(u) { return { name: u.name, premium: !!u.premium, ...u.stats }; }

// ---------------------------------------------------------------- seasons
// A season is one calendar week. Everyone's competitive numbers reset when it
// turns over, and the old week's table is kept. This is what gives people a
// reason to come back on Monday, and what any prize would be awarded from.
function seasonKey(d) {
  const t = d ? new Date(d) : new Date();
  const x = new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate()));
  const day = x.getUTCDay() || 7;
  x.setUTCDate(x.getUTCDate() + 4 - day);                       // ISO: Thursday decides the year
  const y0 = new Date(Date.UTC(x.getUTCFullYear(), 0, 1));
  const week = Math.ceil((((x - y0) / 86400000) + 1) / 7);
  return x.getUTCFullYear() + '-W' + String(week).padStart(2, '0');
}
function seasonEndsAt() {
  const now = new Date();
  const day = now.getUTCDay() || 7;
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + (8 - day)));
  return end.getTime();
}
// Roll a player onto the current season, filing last week's result first.
function ensureSeason(u) {
  const key = seasonKey();
  if (!u.season) { u.season = { key, bestArena: 0, harvests: 0, arenaWins: 0, bestWave: 0, stars: 0 }; return u.season; }
  if (u.season.key !== key) {
    const old = u.season;
    if (old.bestArena || old.harvests || old.arenaWins || old.bestWave) {
      const bucket = DB.seasons[old.key] || (DB.seasons[old.key] = { key: old.key, closed: Date.now(), table: [] });
      bucket.table.push({ name: u.name, bestArena: old.bestArena, harvests: old.harvests, arenaWins: old.arenaWins, bestWave: old.bestWave | 0 });
      bucket.table.sort((a, b) => b.bestArena - a.bestArena);
      bucket.table = bucket.table.slice(0, 100);
    }
    u.season = { key, bestArena: 0, harvests: 0, arenaWins: 0, bestWave: 0, stars: 0 };
  }
  return u.season;
}
function seasonBump(u, field, value, isBest) {
  const sn = ensureSeason(u);
  if (isBest) sn[field] = Math.max(sn[field] | 0, value | 0);
  else sn[field] = (sn[field] | 0) + (value | 0);
}
// ---------------------------------------------------------------- analytics
// Aggregate only: per-day counters plus a short rolling event log. No IPs, no
// personal data. Guests are counted by a random per-tab id that is never stored.
const EVENT_CAP = 4000;
function today() { return new Date().toISOString().slice(0, 10); }
function dayBucket(d) {
  const k = d || today();
  if (!DB.daily[k]) DB.daily[k] = { plays: 0, completions: 0, signups: 0, guestSessions: 0, players: {}, guests: {}, hunts: {}, modes: {} };
  return DB.daily[k];
}
function trackEvent(type, who, data, isGuest, session) {
  const b = dayBucket();
  if (who) b.players[who] = 1; else if (session) b.guests[session] = 1;
  if (type === 'hunt_start') {
    b.plays++; DB.totals.plays++;
    if (data && data.hunt != null) b.hunts[data.hunt] = (b.hunts[data.hunt] | 0) + 1;
  }
  if (type === 'hunt_end') b.completions++;
  if (type === 'signup') { b.signups++; DB.totals.signups++; }
  if (type === 'guest_start') { b.guestSessions++; DB.totals.guestSessions++; }
  if (type === 'mode') b.modes[data && data.mode] = ((b.modes[data && data.mode]) | 0) + 1;
  DB.events.push({ t: Date.now(), type, who: who || null, guest: !!isGuest, data: data || {} });
  if (DB.events.length > EVENT_CAP) DB.events.splice(0, DB.events.length - EVENT_CAP);
  persist();
}
function analytics() {
  const days = Object.keys(DB.daily).sort().slice(-30);
  const series = days.map(d => {
    const b = DB.daily[d];
    return { date: d, plays: b.plays, completions: b.completions, signups: b.signups,
             accounts: Object.keys(b.players).length, guests: Object.keys(b.guests).length,
             guestSessions: b.guestSessions };
  });
  const t = today(), y = new Date(Date.now() - 864e5).toISOString().slice(0, 10);
  const users = Object.values(DB.users);
  const active = k => { const since = Date.now() - k * 864e5; return users.filter(u => (u.lastSeen || 0) > since).length; };
  const huntTotals = {};
  for (const d of days) for (const [h, n] of Object.entries(DB.daily[d].hunts)) huntTotals[h] = (huntTotals[h] | 0) + n;
  return {
    accounts: users.length, premiumAccounts: users.filter(u => u.premium).length, schema: DB.schema, backups: (() => { try { return fs.readdirSync(BACKUP_DIR).length; } catch (e) { return 0; } })(),
    activeToday: active(1), active7: active(7), active30: active(30),
    totals: DB.totals,
    todayRow: series.find(r => r.date === t) || { date: t, plays: 0, completions: 0, signups: 0, accounts: 0, guests: 0, guestSessions: 0 },
    yesterdayRow: series.find(r => r.date === y) || null,
    series,
    popularHunts: Object.entries(huntTotals).sort((a, b) => b[1] - a[1]).slice(0, 8),
    leaders: users.map(u => ({ name: u.name, harvests: u.stats.harvests, bestArena: u.stats.bestArena,
      stars: u.stats.campaignStars, longest: u.stats.longest, created: u.created, lastSeen: u.lastSeen || 0 }))
      .sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0)).slice(0, 60),
    recent: DB.events.slice(-80).reverse(),
  };
}
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
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'no-referrer',
    'Cache-Control': 'no-store',
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


// ---------------------------------------------------------------- admin page
// Served at /admin. Asks for the admin key, keeps it in the tab only, and
// renders whatever /api/admin/stats returns.
const ADMIN_PAGE = `<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ridgeline Season — admin</title>
<style>
:root{--blaze:#ff6a1f;--gold:#ffc24a;--ink:#eef1ec;--dim:#95a396;--faint:#6f7d71;
 --panel:rgba(28,37,30,.9);--line:rgba(255,255,255,.10);
 --disp:'Trebuchet MS','Segoe UI',sans-serif;--type:'Courier New',monospace}
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0b100d;color:var(--ink);font-family:var(--disp);padding:26px 18px 60px}
.wrap{max-width:1100px;margin:0 auto}
h1{font-size:30px;text-transform:uppercase;letter-spacing:-.5px}
h1 span{display:block;font-family:var(--type);font-size:10px;letter-spacing:3.4px;color:var(--blaze);margin-top:8px}
h1::after{content:"";display:block;width:84px;height:4px;background:var(--blaze);border-radius:2px;margin-top:14px}
h2{font-size:14px;text-transform:uppercase;letter-spacing:2px;margin:30px 0 12px;padding-left:12px;
 border-left:3px solid var(--blaze)}
.gate{max-width:380px;margin-top:24px}
input{width:100%;padding:13px;font-size:16px;background:rgba(0,0,0,.45);color:var(--ink);
 border:1px solid var(--line);border-radius:6px;margin-top:8px}
button{margin-top:12px;padding:14px 20px;font-family:var(--disp);font-size:14px;font-weight:bold;
 letter-spacing:1.4px;text-transform:uppercase;background:linear-gradient(180deg,#ff8a3d,var(--blaze));
 color:#180a02;border:0;border-radius:6px;cursor:pointer}
button.ghost{background:rgba(14,20,16,.6);color:#d8e0d7;border:1px solid rgba(255,255,255,.22)}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px}
.card{background:var(--panel);border:1px solid var(--line);border-left:3px solid var(--blaze);
 border-radius:8px;padding:14px}
.card b{display:block;font-size:28px;color:var(--gold)}
.card small{font-family:var(--type);font-size:9.5px;color:var(--faint);text-transform:uppercase;letter-spacing:1.4px}
table{width:100%;border-collapse:collapse;font-size:13px;margin-top:8px}
th{text-align:left;font-family:var(--type);font-size:9.5px;color:var(--faint);text-transform:uppercase;
 letter-spacing:1.2px;padding:8px 6px;border-bottom:1px solid var(--line)}
td{padding:8px 6px;border-bottom:1px solid rgba(255,255,255,.06);color:#c3cec3}
td:first-child{color:#fff}
.bar{height:8px;background:rgba(0,0,0,.5);border-radius:4px;overflow:hidden;min-width:60px}
.bar div{height:100%;background:linear-gradient(90deg,var(--blaze),var(--gold))}
.err{color:#ff9a63;margin-top:10px;font-size:13px}
.muted{color:var(--dim);font-size:13px;margin-top:8px}
.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
</style></head><body><div class="wrap">
<h1>Ridgeline Season<span>Admin &amp; analytics</span></h1>

<div id="gate" class="gate">
  <p class="muted">Enter the admin key you set as <code>ADMIN_KEY</code> on the server.</p>
  <input id="key" type="password" placeholder="admin key" autocomplete="off">
  <button id="go">Open dashboard</button>
  <p id="err" class="err"></p>
</div>

<div id="dash" style="display:none">
  <div class="row"><button id="refresh" class="ghost">Refresh</button><span class="muted" id="stamp"></span></div>
  <h2>Right now</h2><div class="cards" id="now"></div>
  <h2>Last 14 days</h2><div id="series"></div>
  <h2>Most played hunts</h2><div id="hunts"></div>
  <h2>Players</h2><div id="players"></div>
  <h2>Recent activity</h2><div id="recent"></div>
</div>
</div>
<script>
var KEY='';
function esc(s){return String(s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];});}
function ago(t){ if(!t)return 'never'; var s=(Date.now()-t)/1000;
  if(s<60)return Math.round(s)+'s ago'; if(s<3600)return Math.round(s/60)+'m ago';
  if(s<86400)return Math.round(s/3600)+'h ago'; return Math.round(s/86400)+'d ago'; }
function card(v,l){ return '<div class="card"><b>'+v+'</b><small>'+l+'</small></div>'; }
function load(){
  fetch('/api/admin/stats',{headers:{'x-admin-key':KEY}}).then(function(r){
    if(!r.ok)throw new Error(r.status===403?'That key was not accepted.':'Server error '+r.status);
    return r.json();
  }).then(function(d){
    document.getElementById('gate').style.display='none';
    document.getElementById('dash').style.display='';
    document.getElementById('stamp').textContent='updated '+new Date().toLocaleTimeString();
    var t=d.todayRow;
    document.getElementById('now').innerHTML=
      card(d.accounts,'Accounts')+card(d.activeToday,'Active today')+card(d.active7,'Active this week')+
      card(t.plays,'Hunts started today')+card(t.completions,'Hunts finished today')+
      card(t.guests,'Guests today')+card(t.signups,'Sign-ups today')+card(d.totals.plays,'Hunts all time');
    var max=1; d.series.forEach(function(r){ max=Math.max(max,r.plays,r.accounts+r.guests); });
    var rows=d.series.slice(-14).reverse().map(function(r){
      return '<tr><td>'+r.date+'</td><td>'+(r.accounts+r.guests)+'</td><td>'+r.accounts+'</td><td>'+r.guests+
        '</td><td>'+r.plays+'</td><td>'+r.completions+'</td><td>'+r.signups+
        '</td><td><div class="bar"><div style="width:'+Math.round(r.plays/max*100)+'%"></div></div></td></tr>';
    }).join('');
    document.getElementById('series').innerHTML='<table><tr><th>Day</th><th>Players</th><th>Accounts</th><th>Guests</th><th>Started</th><th>Finished</th><th>Sign-ups</th><th></th></tr>'+
      (rows||'<tr><td colspan="8">No activity recorded yet.</td></tr>')+'</table>';
    var hmax=1; d.popularHunts.forEach(function(h){ hmax=Math.max(hmax,h[1]); });
    document.getElementById('hunts').innerHTML='<table><tr><th>Hunt</th><th>Starts</th><th></th></tr>'+
      (d.popularHunts.map(function(h){ return '<tr><td>Hunt '+esc(h[0])+'</td><td>'+h[1]+
        '</td><td><div class="bar"><div style="width:'+Math.round(h[1]/hmax*100)+'%"></div></div></td></tr>'; }).join('')
       ||'<tr><td colspan="3">Nothing yet.</td></tr>')+'</table>';
    document.getElementById('players').innerHTML='<table><tr><th>Name</th><th>Last seen</th><th>Harvests</th><th>Stars</th><th>Best arena</th><th>Longest</th><th>Joined</th></tr>'+
      (d.leaders.map(function(u){ return '<tr><td>'+esc(u.name)+'</td><td>'+ago(u.lastSeen)+'</td><td>'+u.harvests+
        '</td><td>'+u.stars+'/42</td><td>'+u.bestArena+'</td><td>'+u.longest+' m</td><td>'+
        new Date(u.created).toLocaleDateString()+'</td></tr>'; }).join('')
       ||'<tr><td colspan="7">No accounts yet.</td></tr>')+'</table>';
    document.getElementById('recent').innerHTML='<table><tr><th>When</th><th>Who</th><th>Event</th><th>Detail</th></tr>'+
      (d.recent.map(function(e){ return '<tr><td>'+ago(e.t)+'</td><td>'+(e.who?esc(e.who):'<span style="color:#6f7d71">guest</span>')+
        '</td><td>'+esc(e.type)+'</td><td>'+esc(JSON.stringify(e.data))+'</td></tr>'; }).join('')
       ||'<tr><td colspan="4">Nothing yet.</td></tr>')+'</table>';
  }).catch(function(e){ document.getElementById('err').textContent=e.message; });
}
document.getElementById('go').onclick=function(){ KEY=document.getElementById('key').value.trim(); load(); };
document.getElementById('key').onkeydown=function(e){ if(e.key==='Enter')document.getElementById('go').click(); };
document.getElementById('refresh').onclick=load;
setInterval(function(){ if(KEY&&document.getElementById('dash').style.display!=='none')load(); },30000);
</script></body></html>`;

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');

  // Some hosts (DigitalOcean App Platform among them) strip the route prefix
  // before forwarding, so a request for /api/status arrives here as /status and
  // nothing matches. Accept both shapes, so the game works whether or not the
  // proxy trims the path.
  if (!url.pathname.startsWith('/api')) {
    const BARE = ['/status', '/register', '/login', '/me', '/logout', '/cloud',
      '/leaderboard', '/seasons', '/challenge', '/event', '/entitlement',
      '/checkout', '/stripe-webhook', '/paypal-capture', '/my-data', '/delete-account',
      '/admin/users', '/admin/rooms', '/admin/stats', '/admin/premium',
      '/admin/reset', '/admin/backup'];
    if (BARE.includes(url.pathname)) url.pathname = '/api' + url.pathname;
  }
  const ip = ipOf(req);
  if (req.method === 'OPTIONS') return json(res, 204, {});
  try {
    if (url.pathname === '/' || url.pathname === '/api/status') {
      return json(res, 200, { ok: true, players: [...rooms.values()].reduce((a, r) => a + realCount(r), 0), rooms: rooms.size, accounts: Object.keys(DB.users).length, payments: !!(STRIPE_KEY && STRIPE_WEBHOOK_SECRET) || !!(PAYPAL_ID && PAYPAL_SECRET) });
    }
    if (url.pathname === '/api/register' && req.method === 'POST') {
      if (limited('reg:' + ip, 5, 3600e3)) return json(res, 429, { error: 'Too many sign-ups from this address. Try later.' });
      const b = await readBody(req);
      const name = String(b.name || '').trim(), pw = String(b.password || '');
      if (!NAME_RE.test(name)) return json(res, 400, { error: 'Name must be 3–14 letters, numbers or _' });
      if (pw.length < 8) return json(res, 400, { error: 'Password must be at least 8 characters' });
      if (DB.users[name.toLowerCase()]) return json(res, 409, { error: 'That name is taken' });
      const u0 = newUser(name, pw);
      const recoveryCode = u0.recovery; delete u0.recovery;   // shown once, never kept in plain text
      DB.users[name.toLowerCase()] = u0;
      const token = issueToken(name);
      DB.users[name.toLowerCase()].lastSeen = Date.now();
      console.log(`[account] registered ${name}`);
      return json(res, 200, { token, user: publicStats(DB.users[name.toLowerCase()]), recoveryCode });
    }
    if (url.pathname === '/api/login' && req.method === 'POST') {
      if (limited('login:' + ip, 20, 900e3)) return json(res, 429, { error: 'Too many attempts. Wait 15 minutes.' });
      const b = await readBody(req);
      const u = DB.users[String(b.name || '').trim().toLowerCase()];
      if (!u || !verifyPassword(String(b.password || ''), u)) return json(res, 401, { error: 'Wrong name or password' });
      u.lastSeen = Date.now(); persist();
      return json(res, 200, { token: issueToken(u.name), user: publicStats(u), cloud: u.cloud });
    }
    if (url.pathname === '/api/recover' && req.method === 'POST') {
      if (limited('rec:' + ip, 10, 3600e3)) return json(res, 429, { error: 'Too many attempts. Try later.' });
      const b = await readBody(req);
      const u = DB.users[String(b.name || '').trim().toLowerCase()];
      const code = String(b.code || '').toUpperCase().trim();
      const pw = String(b.password || '');
      if (pw.length < 8) return json(res, 400, { error: 'New password must be at least 8 characters' });
      if (!u || !u.recHash) return json(res, 401, { error: 'Wrong name or recovery code' });
      const test = hashPassword(code, u.recSalt);
      let ok = false;
      try { ok = crypto.timingSafeEqual(Buffer.from(test.hash, 'hex'), Buffer.from(u.recHash, 'hex')); } catch (e) { ok = false; }
      if (!ok) return json(res, 401, { error: 'Wrong name or recovery code' });
      const fresh = hashPassword(pw);
      u.salt = fresh.salt; u.hash = fresh.hash;
      const next = makeRecoveryCode(), nh = hashPassword(next);
      u.recSalt = nh.salt; u.recHash = nh.hash;              // old code is spent
      for (const [tok, t] of Object.entries(DB.tokens)) if (t.name === u.name) delete DB.tokens[tok];
      persist();
      console.log(`[account] ${u.name} recovered their password`);
      return json(res, 200, { token: issueToken(u.name), user: publicStats(u), recoveryCode: next });
    }
    const token = (req.headers.authorization || '').replace(/^Bearer /, '');
    if (url.pathname === '/api/me') {
      const u = userFromToken(token); if (!u) return json(res, 401, { error: 'Not signed in' });
      return json(res, 200, { user: publicStats(u), cloud: u.cloud });
    }
    if (url.pathname === '/api/my-data') {
      const u = userFromToken(token); if (!u) return json(res, 401, { error: 'Not signed in' });
      const { salt, hash, recSalt, recHash, ...rest } = u;   // never hand back secrets
      return json(res, 200, { account: rest, note: 'Everything this server holds about you.' });
    }
    if (url.pathname === '/api/delete-account' && req.method === 'POST') {
      const u = userFromToken(token); if (!u) return json(res, 401, { error: 'Not signed in' });
      const b = await readBody(req);
      if (!verifyPassword(String(b.password || ''), u)) return json(res, 401, { error: 'Password does not match' });
      const key = u.name.toLowerCase();
      delete DB.users[key];
      for (const [tok, t] of Object.entries(DB.tokens)) if (t.name === u.name) delete DB.tokens[tok];
      DB.events = DB.events.filter(e => e.who !== u.name);
      for (const d of Object.values(DB.daily)) if (d.players) delete d.players[u.name];
      persist();
      console.log(`[account] ${u.name} deleted their account`);
      return json(res, 200, { ok: true });
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
    // Entitlement. The game asks this on sign-in to decide whether to show ads.
    if (url.pathname === '/api/entitlement') {
      const u = userFromToken(token);
      if (!u) return json(res, 401, { error: 'Not signed in' });
      return json(res, 200, { premium: !!u.premium, since: u.premiumSince || 0 });
    }
    // ---------------------------------------------------------------- payments
    // Stripe and PayPal are both supported and both switch on purely from
    // environment variables. With neither set the endpoint says so plainly
    // rather than pretending to take money.
    if (url.pathname === '/api/checkout' && req.method === 'POST') {
      const u = userFromToken(token);
      if (!u) return json(res, 401, { error: 'Sign in first — a purchase has to attach to an account.' });
      if (u.premium) return json(res, 200, { alreadyPremium: true });
      if (limited('buy:' + u.name, 12, 3600e3)) return json(res, 429, { error: 'Too many attempts, try later' });

      if (STRIPE_KEY) {
        try {
          const params = new URLSearchParams();
          params.set('mode', 'payment');
          params.set('success_url', SITE_URL + '/?paid=1');
          params.set('cancel_url', SITE_URL + '/?paid=0');
          params.set('client_reference_id', u.name);
          params.set('line_items[0][quantity]', '1');
          params.set('line_items[0][price_data][currency]', CURRENCY);
          params.set('line_items[0][price_data][unit_amount]', String(PRICE_CENTS));
          params.set('line_items[0][price_data][product_data][name]', 'Ridgeline Season — ad-free');
          params.set('metadata[account]', u.name);
          const r = await fetch('https://api.stripe.com/v1/checkout/sessions', {
            method: 'POST',
            headers: { Authorization: 'Bearer ' + STRIPE_KEY, 'Content-Type': 'application/x-www-form-urlencoded' },
            body: params,
          });
          const j = await r.json();
          if (j.url) return json(res, 200, { url: j.url });
          console.error('[stripe]', j.error && j.error.message);
          return json(res, 502, { error: 'Could not start checkout. Try again shortly.' });
        } catch (e) {
          console.error('[stripe]', e.message);
          return json(res, 502, { error: 'Could not reach the payment provider.' });
        }
      }
      if (PAYPAL_ID && PAYPAL_SECRET) {
        try {
          const auth = Buffer.from(PAYPAL_ID + ':' + PAYPAL_SECRET).toString('base64');
          const tk = await fetch(PAYPAL_API + '/v1/oauth2/token', {
            method: 'POST',
            headers: { Authorization: 'Basic ' + auth, 'Content-Type': 'application/x-www-form-urlencoded' },
            body: 'grant_type=client_credentials',
          }).then(r => r.json());
          if (!tk.access_token) return json(res, 502, { error: 'Payment provider refused the request.' });
          const order = await fetch(PAYPAL_API + '/v2/checkout/orders', {
            method: 'POST',
            headers: { Authorization: 'Bearer ' + tk.access_token, 'Content-Type': 'application/json' },
            body: JSON.stringify({
              intent: 'CAPTURE',
              purchase_units: [{ custom_id: u.name, description: 'Ridgeline Season — ad-free',
                amount: { currency_code: CURRENCY.toUpperCase(), value: (PRICE_CENTS / 100).toFixed(2) } }],
              application_context: { return_url: SITE_URL + '/?paid=1', cancel_url: SITE_URL + '/?paid=0' },
            }),
          }).then(r => r.json());
          const link = (order.links || []).find(l => l.rel === 'approve');
          if (link) { PENDING.set(order.id, u.name); return json(res, 200, { url: link.href, orderId: order.id }); }
          return json(res, 502, { error: 'Could not start checkout.' });
        } catch (e) {
          console.error('[paypal]', e.message);
          return json(res, 502, { error: 'Could not reach the payment provider.' });
        }
      }
      return json(res, 503, { error: 'Payments are not connected yet on this server.', notConfigured: true });
    }

    // Stripe tells us the money arrived. Webhooks are the only thing we trust to
    // grant ad-free — never the browser saying it paid.
    if (url.pathname === '/api/stripe-webhook' && req.method === 'POST') {
      // No signing secret means we cannot tell a real Stripe event from anyone
      // on the internet posting one. Refuse rather than hand out free upgrades.
      if (!STRIPE_WEBHOOK_SECRET) {
        console.warn('[stripe] webhook refused — STRIPE_WEBHOOK_SECRET is not set');
        return json(res, 503, { error: 'Webhook not configured' });
      }
      const raw = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => r(d)); });
      {
        const sig = req.headers['stripe-signature'] || '';
        const parts = Object.fromEntries(String(sig).split(',').map(kv => kv.split('=')));
        const expected = crypto.createHmac('sha256', STRIPE_WEBHOOK_SECRET).update(parts.t + '.' + raw).digest('hex');
        let ok = false;
        try { ok = crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(parts.v1 || '')); } catch (e) { ok = false; }
        if (!ok) { console.warn('[stripe] rejected a webhook with a bad signature'); return json(res, 400, { error: 'bad signature' }); }
      }
      let ev; try { ev = JSON.parse(raw); } catch (e) { return json(res, 400, { error: 'bad json' }); }
      if (ev.type === 'checkout.session.completed') {
        const who = ev.data.object.client_reference_id || (ev.data.object.metadata || {}).account;
        grantPremium(who, 'stripe');
      }
      return json(res, 200, { received: true });
    }

    // PayPal sends the buyer back here; we capture the order server-side.
    if (url.pathname === '/api/paypal-capture' && req.method === 'POST') {
      const b = await readBody(req);
      const id = String(b.orderId || '');
      const who = PENDING.get(id);
      if (!who) return json(res, 400, { error: 'Unknown order' });
      try {
        const auth = Buffer.from(PAYPAL_ID + ':' + PAYPAL_SECRET).toString('base64');
        const tk = await fetch(PAYPAL_API + '/v1/oauth2/token', { method: 'POST',
          headers: { Authorization: 'Basic ' + auth, 'Content-Type': 'application/x-www-form-urlencoded' },
          body: 'grant_type=client_credentials' }).then(r => r.json());
        const cap = await fetch(PAYPAL_API + '/v2/checkout/orders/' + id + '/capture', { method: 'POST',
          headers: { Authorization: 'Bearer ' + tk.access_token, 'Content-Type': 'application/json' } }).then(r => r.json());
        if (cap.status === 'COMPLETED') { PENDING.delete(id); grantPremium(who, 'paypal'); return json(res, 200, { ok: true }); }
        return json(res, 402, { error: 'Payment not completed' });
      } catch (e) { return json(res, 502, { error: 'Could not confirm the payment' }); }
    }
    if (url.pathname === '/api/event' && req.method === 'POST') {
      if (limited('ev:' + ip, 240, 3600e3)) return json(res, 429, { error: 'Too many events' });
      const b = await readBody(req);
      const u = userFromToken(token);
      if (u) { u.lastSeen = Date.now(); }
      const type = String(b.type || '').slice(0, 24);
      if (!['hunt_start', 'hunt_end', 'signup', 'signin', 'guest_start', 'mode'].includes(type)) return json(res, 400, { error: 'Unknown event' });
      trackEvent(type, u ? u.name : null, b.data, !!b.guest, String(b.session || '').slice(0, 16));
      return json(res, 200, { ok: true });
    }
    if (url.pathname === '/api/leaderboard') {
      const key = ['bestArena', 'arenaWins', 'harvests', 'campaignStars', 'chalLong', 'chalKills', 'chalTags', 'bestWave'].includes(url.searchParams.get('by')) ? url.searchParams.get('by') : 'bestArena';
      const thisWeek = url.searchParams.get('season') === '1';
      let rows;
      if (thisWeek) {
        const sk = ['bestArena', 'harvests', 'arenaWins', 'bestWave'].includes(key) ? key : 'bestArena';
        rows = Object.values(DB.users).map(u => { const sn = ensureSeason(u); return { name: u.name, [key]: sn[sk] | 0 }; })
          .filter(r => r[key] > 0).sort((a, b) => b[key] - a[key]).slice(0, 25);
      } else {
        rows = Object.values(DB.users).map(publicStats).map(r => ({ ...r, [key]: r[key] | 0 })).sort((a, b) => b[key] - a[key]).slice(0, 25);
      }
      return json(res, 200, { by: key, season: thisWeek, seasonKey: seasonKey(), endsAt: seasonEndsAt(), rows });
    }
    // Past weeks, so winners stay on the record even after the reset.
    if (url.pathname === '/api/seasons') {
      const past = Object.values(DB.seasons).sort((a, b) => b.key.localeCompare(a.key)).slice(0, 12)
        .map(s => ({ key: s.key, closed: s.closed, top: s.table.slice(0, 3) }));
      return json(res, 200, { current: seasonKey(), endsAt: seasonEndsAt(), past });
    }
    if (url.pathname.startsWith('/api/admin/')) {
      if (!adminOk(req.headers['x-admin-key'])) return json(res, 403, { error: 'Forbidden' });
      if (url.pathname === '/api/admin/users') return json(res, 200, Object.values(DB.users).map(u => ({ ...publicStats(u), created: u.created })));
      if (url.pathname === '/api/admin/rooms') return json(res, 200, [...rooms.values()].map(r => ({ name: r.name, players: [...r.players.values()].map(p => p.name), seconds: Math.round(r.timeLeft) })));
      if (url.pathname === '/api/admin/stats') return json(res, 200, analytics());
      // One-click backup: everything the server holds, as a file you can save.
      if (url.pathname === '/api/admin/backup') {
        res.writeHead(200, { 'Content-Type': 'application/json',
          'Content-Disposition': 'attachment; filename="ridgeline-backup-' + today() + '.json"' });
        return res.end(JSON.stringify(DB));
      }
      // Grant or revoke ad-free manually — for testers, friends, refunds, and
      // for honouring a purchase taken outside the app.
      // Someone who loses BOTH their password and their recovery code is
      // otherwise locked out for good. This issues a fresh recovery code so you
      // can read it back to them; it never reveals or sets a password.
      if (url.pathname === '/api/admin/reset' && req.method === 'POST') {
        const b = await readBody(req);
        const u = DB.users[String(b.name || '').trim().toLowerCase()];
        if (!u) return json(res, 404, { error: 'No such account' });
        const code = makeRecoveryCode(), h = hashPassword(code);
        u.recSalt = h.salt; u.recHash = h.hash;
        for (const [tok, t] of Object.entries(DB.tokens)) if (t.name === u.name) delete DB.tokens[tok];
        persist();
        console.log(`[admin] issued a new recovery code for ${u.name}`);
        return json(res, 200, { name: u.name, recoveryCode: code,
          note: 'Give this to the player. They use it on the sign-in screen to set a new password.' });
      }
      if (url.pathname === '/api/admin/premium' && req.method === 'POST') {
        const b = await readBody(req);
        const u = DB.users[String(b.name || '').trim().toLowerCase()];
        if (!u) return json(res, 404, { error: 'No such account' });
        u.premium = b.premium !== false;
        u.premiumSince = u.premium ? Date.now() : 0;
        persist();
        console.log(`[premium] ${u.name} -> ${u.premium}`);
        return json(res, 200, { name: u.name, premium: u.premium });
      }
    }
    if (url.pathname === '/admin' || url.pathname === '/admin/' || url.pathname === '/api/admin') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(ADMIN_PAGE);
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
  // predators — only ever spawned by Ridge Defense
  bear:  { k: 1.5,  rx: .75, ry: .40, bodyY: .90, hp: 6,   pts: 210, flee: 0,   weight: 0,  antlers: false, pred: true, charge: 7.2, bite: 13, reach: 2.6 },
  lion:  { k: 1.2,  rx: .60, ry: .28, bodyY: .72, hp: 3.6, pts: 170, flee: 0,   weight: 0,  antlers: false, pred: true, charge: 9.6, bite: 8,  reach: 2.2 },
};
const SPK = Object.keys(SP).filter(k => !SP[k].pred);   // spawnAnimal never picks a predator
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
function resolveShotGeneric(room, p, m) { return resolveShot(room, p, m); }
function resolveShot(room, p, m) {
  const now = room.time;
  const weapon = m.weapon === 'bow' ? 'bow' : 'rifle';
  if (now - p.lastShot < (weapon === 'bow' ? 1.0 : 0.8)) return { ok: false, why: 'too fast' };
  p.lastShot = now;
  // Where the client says it fired from must agree with where the server has seen it.
  const cx = +m.x, cz = +m.z;
  if (!Number.isFinite(cx) || !Number.isFinite(cz)) return { ok: false, why: 'bad position' };
  if (Math.hypot(cx - p.x, cz - p.z) > 8) {
    p.x = cx; p.z = cz; p.drift = 0;          // trust it once, so one hiccup does not cost a whole match
    return { ok: false, why: 'catching up — try that again' };
  }
  const yaw = +m.yaw, pitch = Math.max(-.7, Math.min(.7, +m.pitch || 0));
  if (!Number.isFinite(yaw)) return { ok: false, why: 'bad aim' };
  spook(room, p.x, p.z, weapon === 'bow' ? 14 : 150);
  let best = null;
  for (const a of room.animals) {
    if (a.state === 'dead') continue;
    const s = SP[a.sp], dx = a.x - p.x, dz = a.z - p.z, dist = Math.hypot(dx, dz);
    if (dist < .6 || dist > 230) continue;
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
  if (best.a.pred && room.mode !== 'defense') {      // a roaming arena bear is worth real points
    const a = best.a; a.hp -= best.zone === 'vital' || best.zone === 'head' ? 3 : 1.2;
    if (a.hp > 0) return { ok: true, hit: { id: a.id, zone: best.zone, dist: best.dist, killed: false } };
    a.state = 'dead'; a.deadT = 0; a.speed = 0;
    const pts = Math.round(SP.bear.pts * 3 * (1 + best.dist / 100));
    p.score += pts; p.kills++;
    setTimeout(() => { if (rooms.has(room.name) && room.running) { room.animals = room.animals.filter(x => x.id !== a.id); const b = spawnPredator(room, 'bear', 1); b.roamer = true; b.cool = 12; room.animals.push(b); } }, 12000);
    return { ok: true, hit: { id: a.id, zone: best.zone, dist: best.dist, killed: true, pts, species: 'bear' } };
  }
  if (room.mode === 'defense') return { ok: true, hit: { a: best.a, zone: best.zone, dist: best.dist } };   // caller applies damage
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

// ---------------------------------------------------------------- Ridge Defense
// Co-op. Two to six hunters hold a valley against waves of bears and lions
// that come straight for them. Kills and tags earn points; between waves the
// points buy gear. Everyone down at once and the run is over. Runs on the
// server so every player fights the same animals at the same moment.
const DEF_WARMUP = 12, DEF_BREAK = 7, DEF_MAX = 6;   // a breath between waves, not a shopping trip
const SHOP = {
  ammo:   { name: 'Resupply',          cost: w => 90 + 10 * w,    max: 99, desc: 'Full magazine and quiver' },
  damage: { name: 'Heavier loads',     cost: w => 540 + 160 * w,  max: 5,  desc: '+25% damage per level' },
  rate:   { name: 'Faster action',     cost: w => 460 + 140 * w,  max: 4,  desc: 'Shoot sooner after each shot' },
  health: { name: 'Thicker hide',      cost: w => 560 + 200 * w,  max: 5,  desc: '+40 max health, healed now' },
  revive: { name: 'Second wind',       cost: w => 260 + 90 * w,    max: 2,  desc: 'Get up once on your own' },
};
function isDefense(room) { return room.mode === 'defense'; }
function defPlayerInit(p) {
  p.hp = 100; p.maxHp = 100; p.down = false; p.downT = 0; p.reviveT = 0;
  p.pts = 0; p.up = { damage: 0, rate: 0, health: 0, revive: 0, ammo: 0 };
}
function teamCentre(room) {
  let x = 0, z = 0, n = 0;
  for (const p of room.players.values()) if (!p.bot) { x += p.x; z += p.z; n++; }
  return n ? { x: x / n, z: z / n } : { x: 0, z: 0 };
}
function spawnPredator(room, sp, wave) {
  const s = SP[sp];
  const c = teamCentre(room);
  const a = Math.random() * TAU, d = 85 + Math.random() * 55;         // 85–140 m: past the treeline, not across the county
  const hpMul = 1 + 0.16 * (wave - 1), spMul = Math.min(1.35, 1 + 0.035 * (wave - 1));
  let px = c.x + Math.sin(a) * d, pz = c.z + Math.cos(a) * d; const rr = Math.hypot(px, pz); if (rr > MAP_R - 10) { px *= (MAP_R - 10) / rr; pz *= (MAP_R - 10) / rr; }
  return { id: room.nextAid++, sp, x: px, z: pz, heading: a + Math.PI,
    speed: 0, state: 'stalk', t: 0, hp: s.hp * hpMul, hpMax: s.hp * hpMul, spMul,
    pred: true, wave, cool: 1.5 + Math.random(), retreatT: 0, tagged: false, trophy: false, wounded: false };
}
function waveRoster(n, players) {
  // Solo: 2 bears on wave 1, rising to about 9 by wave 12. A full squad of six
  // faces roughly two and a half times that. Lions arrive at wave 4 and are the
  // fast flankers, never the bulk.
  const team = Math.max(1, players | 0);
  const scale = 1 + .38 * (team - 1);
  let bears = Math.max(1, Math.round((1 + n * .55) * scale));
  let lions = n < 4 ? 0 : Math.max(1, Math.round((n - 3) * .45 * scale));
  // However far the waves climb, no more than this many on the ground at once —
  // past a point it stops being a fight and becomes a wall.
  const capTotal = Math.round(11 * scale);
  if (bears + lions > capTotal) {
    const keepLions = Math.min(lions, Math.floor(capTotal * .4));
    lions = keepLions; bears = Math.max(1, capTotal - keepLions);
  }
  return { bears: Math.min(bears, 12), lions: Math.min(lions, 8) };
}
function defStartWave(room) {
  room.wave++; room.phase = 'wave'; room.running = true;
  // Bots fight, but badly on purpose — they are worth about a third of a person
  // when sizing a wave, or a lobby full of them makes solo play harder than
  // being genuinely alone.
  const r = waveRoster(room.wave, realCount(room) + [...room.players.values()].filter(p => p.bot).length * .3);
  room.animals = room.animals.filter(a => !a.pred);
  for (let i = 0; i < r.bears; i++) room.animals.push(spawnPredator(room, 'bear', room.wave));
  for (let i = 0; i < r.lions; i++) room.animals.push(spawnPredator(room, 'lion', room.wave));
  while (room.animals.filter(a => !a.pred && a.state !== 'dead').length < 6) room.animals.push(spawnAnimal(room));
  broadcast(room, { t: 'wave', n: room.wave, bears: r.bears, lions: r.lions });
  console.log(`[defense] ${room.name} wave ${room.wave}: ${r.bears} bears, ${r.lions} lions`);
}
function defStartBreak(room) {
  room.phase = 'break'; room.breakLeft = DEF_BREAK;
  for (const p of room.players.values()) { p.hp = p.maxHp; p.down = false; }
  broadcast(room, { t: 'break', seconds: DEF_BREAK, wave: room.wave, shop: shopFor(room) });
}
function shopFor(room) {
  const out = {};
  for (const [k, it] of Object.entries(SHOP)) out[k] = { name: it.name, cost: it.cost(room.wave), max: it.max, desc: it.desc };
  return out;
}
function defStartRun(room) {
  balanceBots(room);
  room.phase = 'warm'; room.warmLeft = DEF_WARMUP; room.wave = 0; room.running = false; room.time = 0;
  room.animals = [];
  for (let i = 0; i < 8; i++) room.animals.push(spawnAnimal(room));
  balanceBots(room);
  for (const p of room.players.values()) defPlayerInit(p);
  broadcast(room, { t: 'warmup', seconds: DEF_WARMUP, players: room.players.size, defense: true,
    team: [...room.players.values()].map(q => ({ id: q.id, name: q.name, bot: !!q.bot, color: q.color })) });
}
function defEnd(room) {
  room.phase = 'over'; room.running = false;
  const board = [...room.players.values()]
    .map(p => ({ id: p.id, name: p.name, kills: p.kills, tags: p.tags, pts: p.ptsEarned | 0, bot: !!p.bot }))
    .sort((a, b) => b.pts - a.pts);
  for (const p of room.players.values()) {
    if (p.bot || !p.user) continue;
    const st = p.user.stats; st.bestWave = Math.max(st.bestWave | 0, room.wave); st.harvests += p.kills;
    seasonBump(p.user, 'harvests', p.kills + p.tags, false);
    seasonBump(p.user, 'bestWave', room.wave, true);
    p.user.lastSeen = Date.now();
  }
  persist();
  broadcast(room, { t: 'over', wave: room.wave, board });
  console.log(`[defense] ${room.name} over at wave ${room.wave}`);
  setTimeout(() => { if (rooms.has(room.name) && realCount(room) > 0 && room.phase === 'over') defStartRun(room); }, 15000);
}
function stepPredator(room, a, dt) {
  const s = SP[a.sp];
  if (a.retreatT > 0) { a.retreatT -= dt; a.x += Math.sin(a.heading) * s.charge * .6 * dt; a.z += Math.cos(a.heading) * s.charge * .6 * dt; return; }
  let tgt = null, bd = 1e9;
  for (const p of room.players.values()) { if (p.down) continue; const d = Math.hypot(p.x - a.x, p.z - a.z); if (d < bd) { bd = d; tgt = p; } }
  if (!tgt) { a.heading += (Math.random() - .5) * dt; a.x += Math.sin(a.heading) * 1.5 * dt; a.z += Math.cos(a.heading) * 1.5 * dt; return; }
  const want = Math.atan2(tgt.x - a.x, tgt.z - a.z);
  a.heading += wrap(want - a.heading) * Math.min(1, dt * 3.2);
  if (bd > s.reach) {
    const v = s.charge * a.spMul * (bd < 30 ? 1 : .78);
    a.speed = v; a.x += Math.sin(a.heading) * v * dt; a.z += Math.cos(a.heading) * v * dt;
    return;
  }
  a.speed = 0; a.cool -= dt;
  if (a.cool > 0) return;
  a.cool = a.sp === 'lion' ? 1.35 : 1.7;
  if (a.roamer) {                                    // arena bear: a scare and a scratch, never a kill
    send(tgt.ws, { t: 'mauled', by: a.sp });
    broadcast(room, { t: 'feed', name: tgt.name, text: 'got mauled by a bear' }, tgt.id);
    a.retreatT = 3.5; a.heading += Math.PI; a.cool = 10 + Math.random() * 8;
    return;
  }
  tgt.hp -= Math.round(s.bite * (1 + .05 * (a.wave - 1)));
  send(tgt.ws, { t: 'hp', hp: Math.max(0, tgt.hp), max: tgt.maxHp, by: a.sp });
  if (tgt.hp <= 0 && !tgt.down) {
    if (tgt.up && tgt.up.revive > 0) { tgt.up.revive--; tgt.hp = Math.round(tgt.maxHp * .5); send(tgt.ws, { t: 'secondwind', hp: tgt.hp }); }
    else { tgt.down = true; tgt.downT = 0; tgt.reviveT = 0; broadcast(room, { t: 'down', id: tgt.id, name: tgt.name }); }
  }
  if (Math.random() < (a.sp === 'lion' ? .8 : .5)) { a.retreatT = a.sp === 'lion' ? 2.2 : 1.6; a.heading += Math.PI; }
}
function defTick(room, dt) {
  if (room.phase === 'warm') {
    room.warmLeft -= dt;
    for (const a of room.animals) updateAnimal(room, a, dt);
    if (room.warmLeft <= 0) defStartWave(room);
    return;
  }
  if (room.phase === 'break') {
    room.breakLeft -= dt;
    for (const a of room.animals) if (!a.pred) updateAnimal(room, a, dt);
    if (room.breakLeft <= 0) defStartWave(room);
    return;
  }
  if (room.phase !== 'wave') return;
  room.time += dt;
  for (const a of room.animals) { if (a.state === 'dead') continue; if (a.pred) stepPredator(room, a, dt); else updateAnimal(room, a, dt); }
  // revives: a standing teammate close by for three seconds
  for (const p of room.players.values()) {
    if (!p.down) continue;
    p.downT += dt;
    let helper = false;
    for (const q of room.players.values()) if (q !== p && !q.down && Math.hypot(q.x - p.x, q.z - p.z) < 4) helper = true;
    p.reviveT = helper ? p.reviveT + dt : Math.max(0, p.reviveT - dt * 2);
    if (p.reviveT >= 3) { p.down = false; p.hp = Math.round(p.maxHp * .6); p.reviveT = 0; broadcast(room, { t: 'revived', id: p.id, name: p.name, hp: p.hp }); }
  }
  // bots in defense fight too, crudely
  for (const p of room.players.values()) if (p.bot && !p.down) stepDefenseBot(room, p, dt);
  const alivePred = room.animals.some(a => a.pred && a.state !== 'dead');
  if (!alivePred) { defStartBreak(room); return; }
  const anyoneUp = [...room.players.values()].some(p => !p.down);
  if (!anyoneUp) defEnd(room);
}
function stepDefenseBot(room, b, dt) {
  let best = null, bd = 1e9;
  for (const a of room.animals) { if (!a.pred || a.state === 'dead') continue; const d = Math.hypot(a.x - b.x, a.z - b.z); if (d < bd) { bd = d; best = a; } }
  b.firing = 0;
  if (!best) return;
  const want = Math.atan2(best.x - b.x, best.z - b.z);
  b.yaw += wrap(want - b.yaw) * Math.min(1, dt * 2.5);
  if (bd < 9) { b.x -= Math.sin(b.yaw) * 2.6 * dt; b.z -= Math.cos(b.yaw) * 2.6 * dt; }   // back off
  b.cool -= dt; if (b.cool > 0) return;
  b.cool = 1.6 + Math.random();
  b.firing = 1;
  if (Math.random() < b.skill * .5) {                      // bots chip away; the people finish the job
    best.hp -= .5;
    if (best.hp <= 0) { best.state = 'dead'; best.deadT = 0; predKilled(room, b, best, 'body', bd); }
  }
}
function predKilled(room, p, a, zone, dist) {
  const s = SP[a.sp];
  const pts = Math.round(s.pts * (1 + .16 * (a.wave - 1)) * (zone === 'vital' || zone === 'head' ? 1.3 : 1));
  p.pts = (p.pts | 0) + pts; p.ptsEarned = (p.ptsEarned | 0) + pts; p.kills++;
  broadcast(room, { t: 'kill', id: p.id, name: p.name, species: a.sp, points: pts, animal: a.id, pred: true });
}
function defShot(room, p, m) {
  if (p.down) return { ok: false, why: 'You are down' };
  const rateMul = 1 - .15 * (p.up.rate | 0);
  const weapon = m.weapon === 'bow' ? 'bow' : 'rifle';
  if (room.time - p.lastShot < (weapon === 'bow' ? 1.0 : 0.8) * rateMul) return { ok: false, why: 'too fast' };
  const base = resolveShotGeneric(room, p, m);          // same aim maths as the arena
  if (!base.ok || !base.hit) return base;
  const a = base.hit.a;
  if (!a.pred) {                                        // ordinary game still earns a little
    a.state = 'dead'; a.deadT = 0; a.speed = 0;
    const pts = Math.round(SP[a.sp].pts * .25); p.pts = (p.pts | 0) + pts; p.ptsEarned = (p.ptsEarned | 0) + pts; p.kills++;
    setTimeout(() => { if (rooms.has(room.name)) { room.animals = room.animals.filter(x => x.id !== a.id); room.animals.push(spawnAnimal(room)); } }, 5000);
    return { ok: true, hit: { id: a.id, zone: base.hit.zone, dist: base.hit.dist, killed: true, pts, species: a.sp } };
  }
  const zone = base.hit.zone;
  const dmgMul = 1 + .25 * (p.up.damage | 0);
  const dmg = (zone === 'vital' ? 2.6 : zone === 'head' ? (weapon === 'rifle' ? 2.6 : 1.2) : zone === 'body' ? (m.magnum ? 1.5 : 1.0) : .45) * dmgMul;
  a.hp -= dmg;
  if (a.hp > 0) return { ok: true, hit: { id: a.id, zone, dist: base.hit.dist, killed: false, hpFrac: a.hp / a.hpMax } };
  a.state = 'dead'; a.deadT = 0; a.speed = 0;
  predKilled(room, p, a, zone, base.hit.dist);
  return { ok: true, hit: { id: a.id, zone, dist: base.hit.dist, killed: true, species: a.sp, pts: 0 } };
}
function defBuy(room, p, item) {
  const it = SHOP[item]; if (!it) return { ok: false, why: 'No such item' };
  if (room.phase !== 'break') return { ok: false, why: 'Shop opens between waves' };
  const lvl = p.up[item] | 0; if (lvl >= it.max) return { ok: false, why: 'Maxed out' };
  const cost = it.cost(room.wave); if ((p.pts | 0) < cost) return { ok: false, why: 'Not enough points' };
  p.pts -= cost; p.up[item] = lvl + 1;
  if (item === 'health') { p.maxHp += 40; p.hp = p.maxHp; }
  return { ok: true, item, level: p.up[item], pts: p.pts, hp: p.hp, maxHp: p.maxHp, refill: item === 'ammo' };
}
// ---------------------------------------------------------------- rooms
function adminOk(given) {
  // Constant-time: a plain !== comparison returns faster on an early mismatch,
  // which leaks the key one character at a time to anyone patient.
  if (!ADMIN_KEY || !given) return false;
  const a = Buffer.from(String(given)), b = Buffer.from(ADMIN_KEY);
  if (a.length !== b.length) return false;
  try { return crypto.timingSafeEqual(a, b); } catch (e) { return false; }
}
const BOT_NAMES = ['Dale', 'Marisol', 'Otis', 'Rennick', 'Sable', 'Hutch', 'Wren', 'Cobb'];
const BOT_COLORS = ['#e2542b', '#2b7fe2', '#8e2be2', '#e2c02b', '#2be29a', '#e22b7f'];
const MIN_PARTICIPANTS = 4;           // a match should never feel empty
function realCount(room) { let n = 0; for (const p of room.players.values()) if (!p.bot) n++; return n; }
function addBot(room) {
  const used = new Set([...room.players.values()].map(p => p.name));
  const name = BOT_NAMES.find(n => !used.has(n)) || ('Hunter' + (nextPid % 99));
  const a = Math.random() * Math.PI * 2, d = 40 + Math.random() * 60;
  const b = {
    id: 'b' + (nextPid++), name, bot: true, user: null,
    color: BOT_COLORS[room.players.size % BOT_COLORS.length],
    x: Math.sin(a) * d, z: Math.cos(a) * d, yaw: a,
    score: 0, kills: 0, tags: 0, trophies: 0, longest: 0, firing: 0,
    lastShot: -9, lastState: Date.now(),
    skill: 0.45 + Math.random() * 0.3, cool: 3 + Math.random() * 5, ws: null,
  };
  room.players.set(b.id, b);
  broadcast(room, { t: 'joined', player: { id: b.id, name: b.name, color: b.color, bot: true } });
  return b;
}
function dropBot(room) {
  for (const p of room.players.values()) {
    if (!p.bot) continue;
    room.players.delete(p.id);
    broadcast(room, { t: 'left', id: p.id });
    return true;
  }
  return false;
}
// Keep the headcount sensible: top up with bots, retire them as people arrive.
function balanceBots(room) {
  const real = realCount(room);
  if (real === 0) {                       // nobody here: no need to simulate anyone
    while (dropBot(room));
    return;
  }
  const want = Math.max(0, (room.mode === 'defense' ? 4 : MIN_PARTICIPANTS) - real);
  let bots = room.players.size - real;
  while (bots < want) { addBot(room); bots++; }
  while (bots > want) { if (!dropBot(room)) break; bots--; }
}
function stepBot(room, b, dt) {
  b.firing = 0;
  let best = null, bd = 1e9;
  for (const a of room.animals) {
    if (a.state === 'dead') continue;
    const d = Math.hypot(a.x - b.x, a.z - b.z);
    if (d < bd) { bd = d; best = a; }
  }
  if (!best) { b.x += Math.sin(b.yaw) * 1.6 * dt; b.z += Math.cos(b.yaw) * 1.6 * dt; return; }
  const want = Math.atan2(best.x - b.x, best.z - b.z);
  b.yaw += wrap(want - b.yaw) * Math.min(1, dt * 1.8);
  if (bd > 16) {
    b.x += Math.sin(b.yaw) * 3.6 * dt;
    b.z += Math.cos(b.yaw) * 3.6 * dt;
    const rr = Math.hypot(b.x, b.z);
    if (rr > MAP_R - 10) { b.x *= (MAP_R - 10) / rr; b.z *= (MAP_R - 10) / rr; b.yaw += Math.PI; }
    return;
  }
  b.cool -= dt;
  if (b.cool > 0) return;
  b.cool = 3 + Math.random() * 5;
  b.firing = 1;
  spook(room, b.x, b.z, 90);
  if (Math.random() < b.skill) {
    const sp = SP[best.sp];
    best.state = 'dead'; best.deadT = 0; best.speed = 0;
    const pts = Math.round(sp.pts * (1 + bd / 100));
    b.score += pts; b.kills++;
    broadcast(room, { t: 'kill', id: b.id, name: b.name, species: best.sp, points: pts, animal: best.id });
    setTimeout(() => { if (rooms.has(room.name)) { room.animals = room.animals.filter(x => x.id !== best.id); room.animals.push(spawnAnimal(room)); } }, 5000);
    if (Math.random() < 0.6) { b.tags++; best.tagged = true; }   // bots walk theirs out too, sometimes
  }
}
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
function send(ws, o) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }
function broadcast(room, o, except) { const m = JSON.stringify(o); for (const p of room.players.values()) if (p.id !== except && p.ws && p.ws.readyState === 1) p.ws.send(m); }
const WARMUP_SECONDS = 12;   // same wait as Ridge Defense, so both modes feel alike
// A match should not begin the instant you arrive. Everyone gets a warm-up:
// you are in the world, you can move and look around and watch the others
// turn up, and a shared countdown starts the round for all of you at once.
function startWarmup(room) {
  room.phase = 'warm';
  room.warmLeft = WARMUP_SECONDS;
  room.running = false;
  room.animals = [];
  for (let i = 0; i < 18; i++) room.animals.push(spawnAnimal(room));
  for (const p of room.players.values()) { p.score = 0; p.kills = 0; p.tags = 0; p.trophies = 0; p.longest = 0; }
  broadcast(room, { t: 'warmup', seconds: WARMUP_SECONDS, players: room.players.size });
  console.log(`[match] ${room.name} warm-up (${room.players.size} in the lobby)`);
}
function startMatch(room) {
  room.phase = 'live';
  room.running = true; room.time = 0; room.timeLeft = MATCH_SECONDS; room.animals = [];
  for (const p of room.players.values()) { p.score = 0; p.kills = 0; p.tags = 0; p.trophies = 0; p.longest = 0; }
  for (let i = 0; i < 18; i++) room.animals.push(spawnAnimal(room));
  // Two bears wander the arena for the whole match. Not many — enough that the
  // valley is never entirely safe, and enough that camping one clearing stops
  // being the obvious play.
  for (let i = 0; i < 2; i++) { const b = spawnPredator(room, 'bear', 1); b.roamer = true; b.cool = 6 + Math.random() * 8; room.animals.push(b); }
  broadcast(room, { t: 'start', seconds: MATCH_SECONDS, players: room.players.size });
  console.log(`[match] ${room.name} started with ${room.players.size}`);
}
function endMatch(room) {
  room.running = false; room.phase = 'over';
  const board = [...room.players.values()]
    .map(p => ({ id: p.id, name: p.name, score: p.score, kills: p.kills, tags: p.tags | 0, total: p.kills + (p.tags | 0), bot: !!p.bot }))
    .sort((a, b) => b.total - a.total || b.score - a.score);
  for (const p of room.players.values()) {
    if (p.bot || !p.user) continue;                            // bots and guests are not recorded
    const st = p.user.stats; st.arenaMatches++; st.arenaKills += p.kills; st.harvests += p.kills;
    st.trophies += p.trophies; st.longest = Math.max(st.longest, Math.round(p.longest));
    st.bestArena = Math.max(st.bestArena, p.score);
    seasonBump(p.user, 'bestArena', p.score, true);
    seasonBump(p.user, 'harvests', p.kills + p.tags, false);
    if (board[0] && board[0].id === p.id && room.players.size > 1) { st.arenaWins++; seasonBump(p.user, 'arenaWins', 1, false); }
  }
  persist();
  broadcast(room, { t: 'end', board });
  console.log(`[match] ${room.name} ended: ${board.map(b => b.name + ' ' + b.score).join(', ')}`);
  setTimeout(() => { if (rooms.has(room.name) && realCount(room) > 0 && !room.running) startWarmup(room); }, 12000);
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
      const defense = m.mode === 'defense';
      const roomName = (defense ? 'D_' : '') + (String(m.room || 'RIDGE').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8) || 'RIDGE');
      room = getRoom(roomName);
      if (defense && !room.mode) { room.mode = 'defense'; room.wave = 0; }
      const cap = defense ? DEF_MAX : MAX_PER_ROOM;
      if (room.players.size >= cap) { send(ws, { t: 'error', message: 'Room is full' }); return ws.close(); }
      const name = user ? user.name : ('Guest' + (String(m.name || '').replace(/[^A-Za-z0-9_]/g, '').slice(0, 10) || nextPid));
      p = { id: 'p' + (nextPid++), name, user, color: COLORS[room.players.size % COLORS.length], ws,
            x: 0, z: 0, yaw: 0, score: 0, kills: 0, tags: 0, trophies: 0, longest: 0, firing: 0, lastShot: -9, lastState: Date.now() };
      if (defense) defPlayerInit(p);
      room.players.set(p.id, p);
      send(ws, { t: 'welcome', id: p.id, seed: room.seed, authoritative: true, mode: room.mode || 'arena', seconds: room.running ? room.timeLeft : MATCH_SECONDS, guest: !user,
                 players: [...room.players.values()].map(q => ({ id: q.id, name: q.name, color: q.color, bot: !!q.bot })) });
      broadcast(room, { t: 'joined', player: { id: p.id, name: p.name, color: p.color } }, p.id);
      if (isDefense(room)) {
        if (!room.phase || room.phase === 'over') defStartRun(room);
        else if (room.phase === 'warm') send(ws, { t: 'warmup', seconds: Math.ceil(room.warmLeft), players: room.players.size, defense: true,
          team: [...room.players.values()].map(q => ({ id: q.id, name: q.name, bot: !!q.bot, color: q.color })) });
        else send(ws, { t: 'wave', n: room.wave, late: true });
      }
      else if (room.phase === 'warm') send(ws, { t: 'warmup', seconds: Math.ceil(room.warmLeft), players: room.players.size });
      else if (!room.running) startWarmup(room);
      else send(ws, { t: 'start', seconds: room.timeLeft });
      console.log(`[join] ${p.name}${user ? '' : ' (guest)'} -> ${room.name}`);
      return;
    }
    if (!p || !room) return;
    if (m.t === 'state') {
      // Clamp movement to a sane speed so nobody teleports across the map.
      const now = Date.now(), dt = Math.max(.02, (now - p.lastState) / 1000); p.lastState = now;
      const nx = +m.x, nz = +m.z; if (!Number.isFinite(nx) || !Number.isFinite(nz)) return;
      // Headroom over the client's real top speed (8.4 m/s sprinting) plus slack
      // for network jitter. The old 6.5 limit was BELOW sprint speed, so anyone
      // running online was pulled backwards and their shots were rejected as
      // position mismatches. It still stops teleporting.
      // The first report from a player IS their spawn — take it as given. Until
      // this existed the server kept everyone at the origin and crawled toward
      // wherever they actually were, rejecting every shot as a position
      // mismatch on the way. That looked, from the player's side, like bullets
      // passing straight through animals.
      if (!p.synced) { p.synced = true; p.x = nx; p.z = nz; }
      else {
        const dx = nx - p.x, dz = nz - p.z, dist = Math.hypot(dx, dz), maxD = 10 * dt + 1.2;
        if (dist > maxD) {
          p.x += dx / dist * maxD; p.z += dz / dist * maxD;
          // Persistent disagreement means something is out of step rather than
          // someone cheating — resync instead of refusing their shots forever.
          p.drift = (p.drift || 0) + 1;
          if (p.drift > 45) { p.x = nx; p.z = nz; p.drift = 0; }
        } else { p.x = nx; p.z = nz; p.drift = 0; }
      }
      const rr = Math.hypot(p.x, p.z); if (rr > MAP_R) { p.x *= MAP_R / rr; p.z *= MAP_R / rr; }
      p.yaw = +m.yaw || 0; p.firing = m.firing ? 1 : 0;
      return;
    }
    if (m.t === 'shot' && isDefense(room)) {
      if (room.phase !== 'wave') { send(ws, { t: 'shotResult', ok: false, why: room.phase === 'break' ? 'Between waves — spend your points' : 'Not yet' }); return; }
      const r = defShot(room, p, m); if (r.ok) p.lastShot = room.time;
      send(ws, { t: 'shotResult', ...r, pts: p.pts | 0 });
      if (r.hit && r.hit.killed && r.hit.species) broadcast(room, { t: 'feed', name: p.name, text: 'dropped a ' + r.hit.species }, p.id);
      return;
    }
    if (m.t === 'buy' && isDefense(room)) { send(ws, { t: 'bought', ...defBuy(room, p, String(m.item || '')) }); return; }
    if (m.t === 'shot' && room.phase === 'warm') { send(ws, { t: 'shotResult', ok: false, why: 'Warm-up — the match has not started' }); return; }
    if (m.t === 'shot') {
      if (!room.running) return;
      const r = resolveShot(room, p, m);
      send(ws, { t: 'shotResult', ok: r.ok, why: r.why, hit: r.hit, score: p.score });
      if (r.hit && r.hit.killed) broadcast(room, { t: 'kill', id: p.id, name: p.name, species: r.hit.species, points: r.hit.pts, animal: r.hit.id }, p.id);
      return;
    }
    // Walking out to a carcass is half the work, so it counts toward the win.
    if (m.t === 'tag') {
      if (!room.running) return;
      const a = room.animals.find(x => x.id === (m.id | 0));
      if (!a || a.state !== 'dead' || a.tagged) return;
      if (Math.hypot(a.x - p.x, a.z - p.z) > 6) return;     // must actually be there
      a.tagged = true; p.tags++;
      if (isDefense(room)) { const bonus = Math.round(SP[a.sp].pts * .4 * (a.pred ? (1 + .16 * ((a.wave | 1) - 1)) : .3)); p.pts = (p.pts | 0) + bonus; p.ptsEarned = (p.ptsEarned | 0) + bonus; }
      p.score += Math.round(SP[a.sp].pts * 0.15);
      send(ws, { t: 'tagged', id: a.id, tags: p.tags, score: p.score, pts: p.pts | 0 });
      broadcast(room, { t: 'feed', name: p.name, text: 'tagged a ' + a.sp }, p.id);
      return;
    }
    if (m.t === 'chat') broadcast(room, { t: 'chat', name: p.name, text: String(m.text || '').slice(0, 120) }, p.id);
  });
  ws.on('close', () => {
    if (!p || !room) return;
    if (p.bot) return;
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
    balanceBots(room);
    if (isDefense(room)) { defTick(room, dt); }
    else if (room.phase === 'warm') {
      room.warmLeft -= dt;
      for (const a of room.animals) updateAnimal(room, a, dt);
      for (const p of room.players.values()) if (p.bot) stepBot(room, p, dt);
      if (room.warmLeft <= 0) startMatch(room);
    }
    if (room.running && !isDefense(room)) {
      room.time += dt; room.timeLeft -= dt;
      for (const a of room.animals) if (a.pred) stepPredator(room, a, dt);
      for (const a of room.animals) updateAnimal(room, a, dt);
      for (const p of room.players.values()) if (p.bot) stepBot(room, p, dt);
      if (room.timeLeft <= 0) endMatch(room);
    }
    broadcast(room, {
      t: 'states',
      seconds: Math.max(0, room.phase === 'warm' ? room.warmLeft : room.phase === 'break' ? room.breakLeft : room.timeLeft),
      phase: room.phase || 'live', mode: room.mode || 'arena', wave: room.wave | 0,
      predsLeft: room.mode === 'defense' ? room.animals.filter(a => a.pred && a.state !== 'dead').length : 0,
      players: [...room.players.values()].map(q => ({ id: q.id, x: +q.x.toFixed(2), z: +q.z.toFixed(2), yaw: +q.yaw.toFixed(2), score: q.score, kills: q.kills, tags: q.tags | 0, firing: q.firing, bot: !!q.bot, hp: q.hp | 0, max: q.maxHp | 0, down: !!q.down, pts: q.pts | 0 })),
      animals: room.animals.map(a => ({ id: a.id, sp: a.sp, tg: a.tagged ? 1 : 0, pr: a.pred ? 1 : 0, hf: a.pred ? +(a.hp / a.hpMax).toFixed(2) : 1, x: +a.x.toFixed(2), z: +a.z.toFixed(2), h: +a.heading.toFixed(2), v: +a.speed.toFixed(1), st: a.state === 'dead' ? 'd' : a.state === 'flee' ? 'f' : a.state === 'walk' ? 'w' : 'g', hp: +(a.hp / a.maxhp).toFixed(2), tr: a.trophy ? 1 : 0, m: a.male ? 1 : 0, wd: a.wounded ? 1 : 0 })),
    });
    for (const q of room.players.values()) q.firing = 0;
  }
}, TICK_MS);

setInterval(() => {
  const now = Date.now();
  for (const [name, r] of rooms) if (r.players.size === 0 && r.emptiedAt && now - r.emptiedAt > 300e3) { rooms.delete(name); console.log(`[room] ${name} removed`); }
  for (const [tok, t] of Object.entries(DB.tokens)) if (now - t.at > 30 * 86400e3) delete DB.tokens[tok];
}, 60e3);

backup('boot');
setInterval(() => backup('hourly'), 3600e3);
server.listen(PORT, () => {
  console.log(`Ridgeline Season server on port ${PORT}`);
  // Say plainly what is not yet locked down, rather than failing quietly.
  const warn = [];
  if (!ADMIN_KEY) warn.push('ADMIN_KEY is not set — /admin and the admin API are switched OFF.');
  else if (ADMIN_KEY.length < 16) warn.push('ADMIN_KEY is short. Use 24+ random characters.');
  if (ORIGIN === '*') warn.push('ORIGIN is "*" — any website can call this server. Set it to your game URL.');
  if (STRIPE_KEY && !STRIPE_WEBHOOK_SECRET) warn.push('STRIPE_SECRET_KEY is set but STRIPE_WEBHOOK_SECRET is not — purchases will never complete.');
  if (!STRIPE_KEY && !PAYPAL_ID) warn.push('No payment provider configured — the buy button will say so honestly.');
  if (warn.length) { console.log('\n  ⚠  Before taking this live:'); for (const w of warn) console.log('     - ' + w); console.log(''); }
  console.log(`  Game connects to:  ws://localhost:${PORT}   (wss:// behind HTTPS)`);
  console.log(`  API / status:      http://localhost:${PORT}/api/status`);
  console.log(`  Accounts on disk:  ${DATA_FILE}`);
});
