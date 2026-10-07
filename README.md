# Cipher Clash server

## Run locally

```powershell
npm run dev
```

The server listens on port 3000 by default. Check it at `http://localhost:3000/health` (`/api/health` also works). Set `PORT` in the environment to use another port. Set `CLIENT_ORIGIN` (or the older `CORS_ORIGIN`) to a comma-separated list of allowed client origins, or `*` for a public game client.

The `/api/realtime` WebSocket endpoint synchronizes lobby player presence, the matchmaking queue, booth occupancy, and paired matches. A player can also press Join Game on a free booth pad to sit there and wait; the next pad joiner on the other side, or the next queued player, completes that booth immediately. Players join the queue with PLAY; after a short gather window (`QUEUE_DELAY_MS`, default 2500 ms) the server picks two queued players at random, seats them in a random free booth as red and blue, and repeats while booths are free. Separate booths run matches concurrently. Match patterns stay on the server, which validates guesses. Guess results, turn changes and the slots each player has cracked are broadcast to every client, so anyone in the lobby can watch a match. Presence and match state are in memory and are cleared when the server restarts.

## Deploying to Bloxity Legion

Pushing to `dev` deploys to the dev channel and pushing to `main` deploys to prod, through `.github/workflows/deploy.yml`. The workflow runs the tests, builds the `Dockerfile`, pushes the image to GHCR and calls the Legion deploy API. It needs the `LEGION_DEPLOY_TOKEN` repository secret, plus `GHCR_PUSH_TOKEN` (a classic PAT with `write:packages`) if the image is published under a different account. After the first push, make the `pattern-rush-server` GHCR package public so Legion can pull it.

Legion injects `PORT`, `CLIENT_ORIGIN` and the other hosting variables. On `SIGTERM` the server stops starting new matches, waits for running matches to finish (up to `DRAIN_TIMEOUT_MS`, default 9 minutes), then closes all connections so clients reconnect to the new pod. Lobby and match state live in memory in one process, so the workflow deploys with `maxReplicas: 1`.

## Layout

- `src/index.js` starts the HTTP and WebSocket server.
- `Dockerfile` builds the image Legion runs.

The server exposes the health endpoint and in-memory multiplayer session service; persistent accounts and match history are not implemented.
