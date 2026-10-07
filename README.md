# Cipher Clash server

## Run locally

```powershell
npm run dev
```

The server listens on port 3000 by default. Check it at `http://localhost:3000/api/health`. Set `PORT` in the environment to use another port. Set `CORS_ORIGIN` to a comma-separated list of allowed client origins, or `*` for a public game client.

The `/api/realtime` WebSocket endpoint synchronizes lobby player presence, the matchmaking queue, booth occupancy, and paired matches. A player can also press Join Game on a free booth pad to sit there and wait; the next pad joiner on the other side, or the next queued player, completes that booth immediately. Players join the queue with PLAY; after a short gather window (`QUEUE_DELAY_MS`, default 2500 ms) the server picks two queued players at random, seats them in a random free booth as red and blue, and repeats while booths are free. Separate booths run matches concurrently. Match patterns stay on the server, which validates guesses. Guess results, turn changes and the slots each player has cracked are broadcast to every client, so anyone in the lobby can watch a match. Presence and match state are in memory and are cleared when the server restarts.

## Layout

- `src/index.js` starts the HTTP server.
- `render.yaml` configures the Render service with `server/` as its root directory.

The server exposes the health endpoint and in-memory multiplayer session service; persistent accounts and match history are not implemented.
