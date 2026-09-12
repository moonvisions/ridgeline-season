# Ridgeline Season — game server

Accounts, career stats, leaderboards and the authoritative online arena.

**This cannot run on Vercel.** It is a long-lived process holding open WebSocket
connections and simulating animals every 66ms. Vercel's serverless functions shut down
between requests. Use Railway, Render, Fly.io, or any small VPS.

## Run it locally first

```bash
cd server
npm install
npm start
```

It prints the address to use. In the game go to **Play → Open arena**, put
`ws://localhost:8080` in the Server box, pick any room name, and join.

## Deploy it

### Railway
1. railway.app → **New Project** → **Deploy from GitHub repo**
2. Settings → **Root Directory**: `server`
3. It detects Node and runs `npm start` on its own
4. Settings → **Networking** → **Generate Domain**
5. Take the `https://…` address it gives you and use `wss://…` in the game

### Render
1. render.com → **New** → **Web Service** → connect the repo
2. **Root Directory**: `server` · **Build**: `npm install` · **Start**: `npm start`
3. Use the resulting address as `wss://…` in the game

## Environment variables

| Variable | What it does |
|---|---|
| `PORT` | Port to listen on. Most hosts set this for you. |
| `ORIGIN` | Set to your Vercel URL to lock CORS to your own site. Defaults to `*`. |
| `ADMIN_KEY` | Enables `/api/admin/users` and `/api/admin/rooms`, sent as the `x-admin-key` header. |

## Storage

Accounts and stats are written to `data.json` beside the server, saved atomically. That is
fine into the thousands of accounts.

**Most hosts have ephemeral disks** — the file is wiped on redeploy. For anything you care
about keeping, attach a persistent volume (Railway and Render both offer these), or swap
the `load()` / `persist()` pair for SQLite or Postgres. Nothing else in the file touches
the disk.

## Security notes

- Passwords are salted and hashed with scrypt; comparisons are timing-safe
- Bearer tokens expire after 30 days
- Sign-ups, logins, sockets and score submissions are all rate limited per IP
- The arena is authoritative: the server owns the animals, judges every shot, clamps
  movement speed and rejects shots fired from a position it does not agree with
- Challenge scores are client-reported with sanity caps — see the note in the main README

## Next step if it gets popular

`server.js` ends with a comment block describing how to make the whole thing fully
authoritative. The client's network layer already talks in terms of "peers with a position
and a score", so that change stays contained to the arena code.
