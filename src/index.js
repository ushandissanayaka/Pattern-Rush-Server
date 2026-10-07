import { createServer } from 'node:http';
import { WebSocket, WebSocketServer } from 'ws';
import { cleanIdentity, verifyBoxityToken } from './players/identity.js';
import { createGrantStore } from './progress/gem-grants.js';
import { createWebhookHandler } from './routes/legion-webhook.js';

const port = Number(process.env.PORT || 3000);
// One pod holds at most SEAT_CAP players; this must equal the seatCap the workflow deploys with,
// because Legion's matchmaker fills a pod to that number and then starts another one.
const SEAT_CAP = Math.max(1, Number(process.env.SEAT_CAP || 50));
// Bloxity Legion injects CLIENT_ORIGIN; CORS_ORIGIN still works for local runs.
const allowedOrigins = new Set((process.env.CLIENT_ORIGIN || process.env.CORS_ORIGIN || '*').split(',').map((origin) => origin.trim()));
// Legion waits up to 10 minutes after SIGTERM; leave headroom before it force-kills the pod.
const DRAIN_TIMEOUT_MS = Math.max(0, Number(process.env.DRAIN_TIMEOUT_MS ?? 9 * 60 * 1000));
let draining = false;
const grantStore = createGrantStore();
const handleWebhook = createWebhookHandler({ store: grantStore, onGrant: () => applyGrants() });
const server = createServer((req, res) => {
  const origin = req.headers.origin;
  if (origin && (allowedOrigins.has('*') || allowedOrigins.has(origin))) {
    res.setHeader('Access-Control-Allow-Origin', allowedOrigins.has('*') ? '*' : origin);
    res.setHeader('Vary', 'Origin');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204).end();
    return;
  }
  const path = req.url.split('?')[0];
  // Legion's readiness and liveness probes hit /health; /api/health is kept for existing clients.
  if (req.method === 'GET' && (path === '/health' || path === '/api/health')) {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ status: 'ok', service: 'cipher-clash-server' }));
    return;
  }
  if (req.method === 'POST' && path === '/api/legion-webhook') {
    handleWebhook(req, res).catch((error) => {
      console.error('[gems] webhook failed:', error.message);
      if (!res.headersSent) res.writeHead(500).end();
    });
    return;
  }
  res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify({ error: 'Not found' }));
});

const webSockets = new WebSocketServer({ noServer: true, maxPayload: 32 * 1024 });
const players = new Map();
const clients = new Map();
const stations = new Map();
const stationIds = Array.from({ length: 5 }, (_, i) => [`L${i + 1}`, `R${i + 1}`]).flat();
const TOKEN_IDS = new Set(['grin', 'gem', 'block', 'sunny', 'gloomy', 'winky']);
// Matchmaking: queued players are paired at random. The short gather window lets everyone
// who pressed PLAY at about the same time take part in the draw, not only the first two.
const QUEUE_DELAY_MS = Math.max(0, Number(process.env.QUEUE_DELAY_MS ?? 2500));
const queue = new Set();
let matchmakingTimer = null;

const EMOTE_ID = /^[a-f0-9]{24}$/i;
const GRANT_POLL_MS = 5000;

// userId is the verified Boxity account id (null for guests): clients key chat bubbles by it.
const publicPlayer = (player) => ({
  id: player.id,
  userId: player.userId,
  name: player.name,
  skinUrl: player.skinUrl,
  equipped: player.equipped,
  proportions: player.proportions,
  position: player.position,
  heading: player.heading,
  state: player.state
});

function send(socket, data) {
  if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify(data));
}

function broadcast(data, exceptId = null) {
  const message = JSON.stringify(data);
  for (const [id, socket] of clients) {
    if (id !== exceptId && socket.readyState === WebSocket.OPEN) socket.send(message);
  }
}

// Public view of a booth for every client (players and spectators). Secret patterns are
// never included, only the slots each player has already guessed correctly.
function stationState(stationId) {
  const room = stations.get(stationId);
  const state = {
    type: 'station-updated', stationId, players: {}, names: {}, revealed: {},
    waiting: false, inProgress: false, bot: false, turnId: null, winnerId: null
  };
  if (!room) return state;
  for (const color of ['red', 'blue']) {
    const id = room.players[color];
    if (!id) continue;
    // a player's progress is the part of the OPPONENT's pattern they have already cracked
    const opponentTarget = room.targets[room.players[color === 'red' ? 'blue' : 'red']];
    state.players[color] = id;
    state.names[color] = players.get(id)?.name || 'Player';
    state.revealed[color] = opponentTarget ? opponentTarget.slice(0, room.indexes[id] || 0) : [];
  }
  state.waiting = room.waiting;
  state.inProgress = room.inProgress;
  state.bot = room.bot;
  state.turnId = room.turnId;
  state.winnerId = room.winnerId;
  return state;
}

const stationUpdate = (stationId) => broadcast(stationState(stationId));
const newRoom = () => ({ players: {}, waiting: false, inProgress: false, bot: false, targets: {}, indexes: {}, wrong: {}, turnId: null, winnerId: null });
const freeStationIds = () => stationIds.filter((stationId) => !stations.has(stationId));
const pickRandom = (items) => items[Math.floor(Math.random() * items.length)];

function queueStatus() {
  const size = queue.size;
  for (const id of queue) send(clients.get(id), { type: 'queue-status', queued: true, size });
}

function leaveQueue(player, notify = true) {
  if (!queue.delete(player.id)) return;
  if (notify) send(clients.get(player.id), { type: 'queue-status', queued: false, size: queue.size });
  queueStatus();
}

// A queued player goes straight into a booth where someone is already waiting on a pad.
function fillWaitingBooths() {
  let filled = false;
  for (const stationId of stationIds) {
    const room = stations.get(stationId);
    if (!queue.size) break;
    if (!room?.waiting) continue;
    const id = pickRandom([...queue]);
    queue.delete(id);
    seat(stationId, id, room.players.red ? 'blue' : 'red');
    filled = true;
  }
  if (filled) queueStatus();
}

function scheduleMatchmaking() {
  fillWaitingBooths();
  if (matchmakingTimer || queue.size < 2 || !freeStationIds().length) return;
  matchmakingTimer = setTimeout(() => {
    matchmakingTimer = null;
    matchmake();
  }, QUEUE_DELAY_MS);
  matchmakingTimer.unref?.();
}

function matchmake() {
  while (queue.size >= 2) {
    const free = freeStationIds();
    if (!free.length) break;
    const waiting = [...queue];
    const first = pickRandom(waiting);
    const second = pickRandom(waiting.filter((id) => id !== first));
    queue.delete(first);
    queue.delete(second);
    startMatch(pickRandom(free), Math.random() < 0.5 ? [first, second] : [second, first]);
  }
  queueStatus();
}

// Seats a player on one side of a booth; the match starts as soon as both sides are taken.
function seat(stationId, id, color) {
  let room = stations.get(stationId);
  if (!room) {
    room = newRoom();
    stations.set(stationId, room);
  }
  const player = players.get(id);
  room.players[color] = id;
  player.stationId = stationId;
  player.color = color;
  if (room.players.red && room.players.blue) {
    room.waiting = false;
    room.inProgress = true;
    const roster = ['red', 'blue'].map((side) => ({ ...publicPlayer(players.get(room.players[side])), color: side }));
    for (const playerId of Object.values(room.players)) send(clients.get(playerId), { type: 'match-ready', stationId, players: roster });
  } else {
    room.waiting = true;
  }
  stationUpdate(stationId);
}

function startMatch(stationId, [redId, blueId]) {
  seat(stationId, redId, 'red');
  seat(stationId, blueId, 'blue');
}

// Booth pad: sit down at that booth if the side is free; an opponent (pad or queue) completes it.
function joinStation(player, stationId, color) {
  const socket = clients.get(player.id);
  if (!stationIds.includes(stationId) || !['red', 'blue'].includes(color)) {
    send(socket, { type: 'error', message: 'Invalid booth.' });
    return;
  }
  if (player.stationId) return;
  const room = stations.get(stationId);
  if (room && (!room.waiting || room.players[color])) {
    send(socket, { type: 'join-denied', stationId, color, message: room.waiting ? 'That side is already taken.' : 'That booth is busy.' });
    return;
  }
  leaveQueue(player);
  seat(stationId, player.id, color);
  fillWaitingBooths();
}

// Solo practice against the client-side bot still occupies a booth so no paired match lands on it.
function startBotMatch(player) {
  if (player.stationId) {
    // a player waiting alone on a pad can switch to bot practice in that booth
    const room = stations.get(player.stationId);
    if (!room?.waiting) return;
    room.waiting = false;
    room.inProgress = true;
    room.bot = true;
    send(clients.get(player.id), { type: 'bot-station', stationId: player.stationId, color: player.color });
    stationUpdate(player.stationId);
    return;
  }
  const free = freeStationIds();
  if (!free.length) {
    send(clients.get(player.id), { type: 'join-denied', message: 'All booths are busy. Try again soon.' });
    return;
  }
  leaveQueue(player);
  const stationId = pickRandom(free);
  const room = newRoom();
  room.players = { red: player.id };
  room.inProgress = true;
  room.bot = true;
  stations.set(stationId, room);
  player.stationId = stationId;
  player.color = 'red';
  send(clients.get(player.id), { type: 'bot-station', stationId, color: 'red' });
  stationUpdate(stationId);
}

function removeFromStation(player) {
  if (!player.stationId) return;
  const stationId = player.stationId;
  const room = stations.get(stationId);
  if (room) {
    const otherId = Object.values(room.players).find((id) => id && id !== player.id);
    if (room.inProgress && !room.bot && !room.winnerId) {
      broadcast({ type: 'match-event', stationId, from: player.id, event: { type: 'match-ended', reason: 'Opponent left.' } });
    }
    for (const color of ['red', 'blue']) {
      if (room.players[color] === player.id) delete room.players[color];
    }
    // A paired booth is never refilled; it frees up once the remaining player leaves as well.
    room.waiting = false;
    room.inProgress = false;
    room.targets = {};
    room.indexes = {};
    room.wrong = {};
    room.turnId = null;
    room.winnerId = null;
    if (!otherId) stations.delete(stationId);
  }
  player.stationId = null;
  player.color = null;
  stationUpdate(stationId);
  broadcast({ type: 'player-updated', player: publicPlayer(player) }, player.id);
  scheduleMatchmaking();
}

function closePlayer(playerId) {
  const player = players.get(playerId);
  if (!player) return;
  leaveQueue(player, false);
  removeFromStation(player);
  players.delete(playerId);
  clients.delete(playerId);
  broadcast({ type: 'player-left', id: playerId });
}

// The client sends its Boxity token (Legion.SDK.auth.getToken()) with identify; the account id is
// only ever taken from Boxity's answer, never from the client. No token means a guest.
async function setAccount(player, token) {
  if (typeof token !== 'string' || !token) {
    player.token = null;
    if (!player.userId) return;
    player.userId = null;
    broadcast({ type: 'player-updated', player: publicPlayer(player) });
    return;
  }
  if (token === player.token) return;
  player.token = token;
  const account = await verifyBoxityToken(token);
  if (player.token !== token || !players.has(player.id)) return; // a newer identify won the race
  const userId = account?.userId || null;
  if (userId === player.userId) return;
  player.userId = userId;
  broadcast({ type: 'player-updated', player: publicPlayer(player) });
  if (userId) applyGrants();
}

// Hands Gems purchases recorded by the webhook (on any pod) to the buyers connected to this pod.
let applyingGrants = false;
async function applyGrants() {
  if (applyingGrants) return;
  applyingGrants = true;
  try {
    const online = [...players.values()].filter((player) => player.userId && clients.get(player.id)?.readyState === WebSocket.OPEN);
    const grants = await grantStore.claimFor([...new Set(online.map((player) => player.userId))]);
    for (const grant of grants) {
      const message = {
        type: 'gems-grant',
        grant: { transactionId: grant._id, sku: grant.sku, productName: grant.productName, metadata: grant.metadata ?? null }
      };
      for (const player of online) if (player.userId === grant.userId) send(clients.get(player.id), message);
    }
  } catch (error) {
    console.warn('[gems] could not apply grants:', error.message);
  } finally {
    applyingGrants = false;
  }
}

// Match events go to every client so lobby players can watch any booth.
function matchBroadcast(stationId, from, event) {
  broadcast({ type: 'match-event', stationId, from, event });
}

function handleMatchEvent(player, event) {
  const room = stations.get(player.stationId);
  if (!room?.inProgress || room.bot || !['red', 'blue'].some((color) => room.players[color] === player.id)) return;
  if (!event || typeof event !== 'object') return;
  const playerIds = Object.values(room.players);
  const opponentId = playerIds.find((id) => id !== player.id);
  if (event.type === 'submit-pattern') {
    if (room.targets[player.id]) return;
    if (!Array.isArray(event.pattern) || event.pattern.length !== 9 || !event.pattern.every((token) => TOKEN_IDS.has(token))) return;
    room.targets[player.id] = event.pattern.slice();
    room.indexes[player.id] = 0;
    room.wrong[player.id] = Array.from({ length: 9 }, () => []);
    if (playerIds.length === 2 && playerIds.every((id) => room.targets[id])) {
      room.turnId = pickRandom(playerIds);
      matchBroadcast(player.stationId, player.id, { type: 'game-start', turnPlayerId: room.turnId });
    }
    return;
  }
  if (event.type === 'guess') {
    if (!opponentId || room.winnerId || room.turnId !== player.id || !room.targets[player.id] || !room.targets[opponentId]) return;
    const index = room.indexes[player.id];
    if (!Number.isInteger(index) || index < 0 || index >= 9 || !TOKEN_IDS.has(event.tokenId)) return;
    if (room.wrong[player.id][index].includes(event.tokenId)) return;
    const correct = room.targets[opponentId][index] === event.tokenId;
    if (correct) room.indexes[player.id]++;
    else room.wrong[player.id][index].push(event.tokenId);
    if (room.indexes[player.id] === 9) room.winnerId = player.id;
    if (!correct) room.turnId = opponentId;
    matchBroadcast(player.stationId, player.id, {
      type: 'guess-result',
      playerId: player.id,
      tokenId: event.tokenId,
      correct,
      index,
      winnerId: room.winnerId,
      nextTurnId: room.turnId
    });
    return;
  }
  if (event.type === 'pass' && opponentId && !room.winnerId && room.turnId === player.id) {
    room.turnId = opponentId;
    matchBroadcast(player.stationId, player.id, { type: 'turn-passed', playerId: player.id, nextTurnId: opponentId });
  }
}

const RESTART_MESSAGE = 'The server is restarting. Rejoin in a moment to play.';

function handleMessage(player, socket, message) {
  if (!message || typeof message !== 'object' || typeof message.type !== 'string') {
    send(socket, { type: 'error', message: 'Invalid message.' });
    return;
  }
  if (draining && ['join-queue', 'join-station', 'play-bot'].includes(message.type)) {
    send(socket, { type: 'join-denied', stationId: message.stationId, color: message.color, message: RESTART_MESSAGE });
    return;
  }
  if (message.type === 'identify') {
    cleanIdentity(player, message.player);
    broadcast({ type: 'player-updated', player: publicPlayer(player) }, player.id);
    setAccount(player, message.token).catch((error) => console.warn('[identity] account check failed:', error.message));
    return;
  }
  // Boxity draws the emote picker; the client plays the emote and we relay it to everyone else.
  if (message.type === 'emote') {
    const emoteId = message.emoteId === null ? null : String(message.emoteId ?? '');
    if (emoteId !== null && !EMOTE_ID.test(emoteId)) return;
    broadcast({ type: 'player-emote', id: player.id, emoteId }, player.id);
    return;
  }
  if (message.type === 'move') {
    const p = message.position;
    if (!p || ![p.x, p.y, p.z, message.heading].every((value) => Number.isFinite(value))) return;
    player.position = {
      x: Math.max(-800, Math.min(800, p.x)),
      y: Math.max(-120, Math.min(1000, p.y)),
      z: Math.max(-800, Math.min(800, p.z))
    };
    player.heading = message.heading;
    player.state = ['idle', 'walk', 'airborne'].includes(message.state) ? message.state : 'idle';
    broadcast({ type: 'player-updated', player: publicPlayer(player) }, player.id);
    return;
  }
  if (message.type === 'join-queue') {
    if (player.stationId || queue.has(player.id)) {
      send(socket, { type: 'queue-status', queued: queue.has(player.id), size: queue.size });
      return;
    }
    queue.add(player.id);
    queueStatus();
    scheduleMatchmaking();
    return;
  }
  if (message.type === 'leave-queue') {
    leaveQueue(player);
    return;
  }
  if (message.type === 'join-station') {
    joinStation(player, message.stationId, message.color);
    return;
  }
  if (message.type === 'play-bot') {
    startBotMatch(player);
    return;
  }
  if (message.type === 'leave-station') {
    removeFromStation(player);
    return;
  }
  if (message.type === 'match-event') handleMatchEvent(player, message.event);
}

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname !== '/api/realtime') {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }
  const id = url.searchParams.get('id');
  const origin = req.headers.origin;
  if (!id || !/^[a-zA-Z0-9_-]{1,64}$/.test(id) || (origin && !allowedOrigins.has('*') && !allowedOrigins.has(origin))) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
    socket.destroy();
    return;
  }
  // A reconnecting player keeps their seat; a new one is refused once the pod is full.
  if (!players.has(id) && players.size >= SEAT_CAP) {
    socket.write('HTTP/1.1 503 Service Unavailable\r\n\r\n');
    socket.destroy();
    return;
  }
  webSockets.handleUpgrade(req, socket, head, (ws) => webSockets.emit('connection', ws, req, id));
});

webSockets.on('connection', (socket, _req, id) => {
  const oldSocket = clients.get(id);
  if (oldSocket && oldSocket !== socket) oldSocket.close(4000, 'Reconnected');
  let player = players.get(id);
  if (!player) {
    player = {
      id,
      userId: null,
      token: null,
      name: 'Player',
      skinUrl: 'https://static.bloxity.io/avatars/skins/0.png',
      equipped: {},
      proportions: {},
      position: { x: 0, y: 0, z: 28 },
      heading: Math.PI,
      state: 'idle',
      stationId: null,
      color: null
    };
    players.set(id, player);
  }
  clients.set(id, socket);
  socket.isAlive = true;
  socket.on('pong', () => { socket.isAlive = true; });
  send(socket, {
    type: 'snapshot',
    players: [...players.values()].filter((p) => p.id !== id).map(publicPlayer),
    stations: stationIds.filter((stationId) => stations.has(stationId)).map(stationState)
  });
  broadcast({ type: 'player-joined', player: publicPlayer(player) }, id);

  socket.on('message', (raw) => {
    try {
      handleMessage(player, socket, JSON.parse(raw.toString()));
    } catch (error) {
      console.warn('[multiplayer] rejected malformed message:', error.message);
      send(socket, { type: 'error', message: 'Malformed message.' });
    }
  });
  socket.on('close', () => {
    if (clients.get(id) !== socket) return;
    const timer = setTimeout(() => {
      if (clients.get(id) === socket) closePlayer(id);
    }, 5000);
    timer.unref();
  });
  socket.on('error', (error) => console.warn(`[multiplayer] socket error for ${id}:`, error.message));
});

const heartbeat = setInterval(() => {
  for (const socket of webSockets.clients) {
    if (!socket.isAlive) { socket.terminate(); continue; }
    socket.isAlive = false;
    socket.ping();
  }
}, 30000);
heartbeat.unref();

const grantPoll = setInterval(applyGrants, GRANT_POLL_MS);
grantPoll.unref();

server.listen(port, '0.0.0.0', () => {
  console.log(`Cipher Clash server listening at http://localhost:${port} (seat cap ${SEAT_CAP})`);
});

server.on('close', () => {
  clearInterval(heartbeat);
  clearInterval(grantPoll);
  grantStore.close().catch(() => {});
  clearTimeout(matchmakingTimer);
  for (const socket of webSockets.clients) socket.close();
});

// Graceful drain for deploys and scale-down: matches already running are allowed to finish,
// no new ones start, and everyone is then disconnected so their clients reconnect to a fresh pod.
const isLive = (room) => Boolean(room?.inProgress && !room.winnerId);
const hasLiveMatch = () => [...stations.values()].some(isLive);

// Players who are not in a running match are closed with 1012 straight away, so their client
// resolves a new endpoint through the matchmaker and lands on the fresh pod.
function hopIdlePlayers() {
  for (const player of players.values()) {
    if (isLive(stations.get(player.stationId))) continue;
    const socket = clients.get(player.id);
    if (socket?.readyState === WebSocket.OPEN) socket.close(1012, 'Server restarting');
  }
}

function finishDrain() {
  console.log('[drain] closing remaining connections and exiting');
  for (const socket of webSockets.clients) socket.close(1012, 'Server restarting');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}

process.once('SIGTERM', () => {
  if (draining) return;
  draining = true;
  console.log('[drain] SIGTERM received, waiting for running matches to finish');
  clearTimeout(matchmakingTimer);
  for (const id of [...queue]) {
    queue.delete(id);
    send(clients.get(id), { type: 'queue-status', queued: false, size: 0 });
    send(clients.get(id), { type: 'join-denied', message: RESTART_MESSAGE });
  }
  hopIdlePlayers();
  const deadline = Date.now() + DRAIN_TIMEOUT_MS;
  const check = setInterval(() => {
    hopIdlePlayers();
    if (hasLiveMatch() && Date.now() < deadline) return;
    clearInterval(check);
    finishDrain();
  }, 1000);
  if (!hasLiveMatch()) {
    clearInterval(check);
    finishDrain();
  }
});
