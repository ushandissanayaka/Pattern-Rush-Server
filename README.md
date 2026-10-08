# Cipher Clash server

## Run locally

```powershell
npm run dev
```

The server listens on port 3000 by default. Check it at `http://localhost:3000/health` (`/api/health` also works). Set `PORT` in the environment to use another port. Set `CLIENT_ORIGIN` (or the older `CORS_ORIGIN`) to a comma-separated list of allowed client origins, or `*` for a public game client.

The `/api/realtime` WebSocket endpoint synchronizes lobby player presence, the matchmaking queue, booth occupancy, and paired matches. A player can also press Join Game on a free booth pad to sit there and wait; the next pad joiner on the other side, or the next queued player, completes that booth immediately. Players join the queue with PLAY; after a short gather window (`QUEUE_DELAY_MS`, default 2500 ms) the server picks two queued players at random, seats them in a random free booth as red and blue, and repeats while booths are free. Separate booths run matches concurrently. Match patterns stay on the server, which validates guesses. Guess results, turn changes and the slots each player has cracked are broadcast to every client, so anyone in the lobby can watch a match. Presence and match state are in memory and are cleared when the server restarts.

### Boxity messages

- `identify` may carry `token` (`Legion.SDK.auth.getToken()`). The server checks it against `https://api.bloxity.io/v1/auth/me` and publishes the verified account id as `userId` on the player, so clients can match `Legion.SDK.chat.onMessage` messages to characters. Without a valid token the player is a guest (`userId: null`). Avatars accept every equipped slot and all seven proportions (clamped to the SDK's ranges), and skins from `getSkinTextureUrl()`.
- `emote` `{ emoteId }` (a 24-character catalogue id, or `null` to stop) is relayed to everyone else as `player-emote` `{ id, emoteId }`.
- `POST /api/legion-webhook` fulfils Gems purchases. Grants are stored in Mongo keyed by `transactionId` (retries are acknowledged, never granted twice), the server answers 200, and every pod delivers unapplied grants to the verified buyers it holds as `gems-grant` `{ grant: { transactionId, sku, productName, metadata } }`. Set `LEGION_WEBHOOK_SECRET` to require the `x-legion-webhook-secret` header.

### Connecting on Boxity

Clients must not open the socket to `<gameId>.host.bloxity.io`; that host is for HTTP only. On every connect, call `Legion.SDK.net.resolveEndpoint('<gameId>')` and open `${endpoint}/api/realtime?id=<clientId>`. The relay at `play.bloxity.io` strips `/v1/ws/<roomId>` and forwards `/api/realtime` to the pod it pinned. If the resolve or the socket fails, or the socket closes with 1012, resolve again with a backoff.

## Deploying to Bloxity Legion

Pushing to `dev` deploys to the dev channel and pushing to `main` deploys to prod, through `.github/workflows/deploy.yml`. The workflow runs the tests, builds the `Dockerfile`, pushes the image to GHCR and calls the Legion deploy API. It needs the `LEGION_DEPLOY_TOKEN` repository secret, plus `GHCR_PUSH_TOKEN` (a classic PAT with `write:packages`) if the image is published under a different account. After the first push, make the `pattern-rush-server` GHCR package public so Legion can pull it.

Legion injects `PORT`, `CLIENT_ORIGIN` and the other hosting variables. On `SIGTERM` the server stops starting new matches, waits for running matches to finish (up to `DRAIN_TIMEOUT_MS`, default 9 minutes), then closes all connections so clients reconnect to the new pod. Lobby players who are not in a running match are closed with 1012 right away, so they move to the new pod at once. Each pod is its own lobby holding up to `SEAT_CAP` players (the workflow's `seatCap`, baked into the image); the matchmaker fills one pod, then starts the next. Legion injects `MONGODB_URI` for the Gems grant store.

## Layout

- `src/index.js` starts the HTTP and WebSocket server.
- `src/players/identity.js` cleans the avatar a client sends and verifies Boxity tokens.
- `src/progress/gem-grants.js` stores Gems grants (Mongo, or memory locally).
- `src/routes/legion-webhook.js` handles Boxity's purchase webhook.
- `Dockerfile` builds the image Legion runs.

The server exposes the health endpoint and in-memory multiplayer session service; persistent accounts and match history are not implemented.
