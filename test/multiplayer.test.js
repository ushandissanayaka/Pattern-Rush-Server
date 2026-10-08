import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import { WebSocket } from 'ws';

const serverDirectory = fileURLToPath(new URL('..', import.meta.url));

async function unusedPort() {
  const probe = createServer();
  probe.listen(0, '127.0.0.1');
  await once(probe, 'listening');
  const { port } = probe.address();
  await new Promise((resolve, reject) => probe.close((error) => error ? reject(error) : resolve()));
  return port;
}

async function startServer(env = {}) {
  const port = await unusedPort();
  const child = spawn(process.execPath, ['src/index.js'], {
    cwd: serverDirectory,
    env: { ...process.env, MONGODB_URI: '', PORT: String(port), CORS_ORIGIN: '*', QUEUE_DELAY_MS: '100', ...env },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let output = '';
  child.stdout.on('data', (chunk) => { output += chunk.toString(); });
  child.stderr.on('data', (chunk) => { output += chunk.toString(); });
  const healthUrl = `http://127.0.0.1:${port}/api/health`;
  for (let attempt = 0; attempt < 50; attempt++) {
    if (child.exitCode !== null) throw new Error(`Server exited early:\n${output}`);
    try {
      const response = await fetch(healthUrl);
      if (response.ok) return { child, port };
    } catch {}
    await delay(100);
  }
  child.kill('SIGKILL');
  throw new Error(`Server did not start:\n${output}`);
}

function connect(port, id) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`ws://127.0.0.1:${port}/api/realtime?id=${id}`);
    const client = { socket, messages: [], waiters: [] };
    socket.on('message', (raw) => {
      const message = JSON.parse(raw.toString());
      client.messages.push(message);
      for (const waiter of [...client.waiters]) {
        if (waiter.predicate(message)) {
          client.waiters.splice(client.waiters.indexOf(waiter), 1);
          clearTimeout(waiter.timer);
          waiter.resolve(message);
        }
      }
    });
    socket.once('open', () => resolve(client));
    socket.once('error', reject);
  });
}

function waitFor(client, predicate, timeout = 5000) {
  const existing = client.messages.find(predicate);
  if (existing) return Promise.resolve(existing);
  return new Promise((resolve, reject) => {
    const waiter = {
      predicate,
      resolve,
      timer: setTimeout(() => {
        client.waiters.splice(client.waiters.indexOf(waiter), 1);
        reject(new Error('Timed out waiting for multiplayer event.'));
      }, timeout)
    };
    client.waiters.push(waiter);
  });
}

function send(client, type, data = {}) {
  client.socket.send(JSON.stringify({ type, ...data }));
}

const isGameEvent = (type, stationId) => (message) =>
  message.type === 'match-event' && message.event.type === type && (!stationId || message.stationId === stationId);

test('pairs queued players at random into separate booths that everyone can watch', { timeout: 20000 }, async (t) => {
  const { child, port } = await startServer();
  const clients = [];
  t.after(() => {
    for (const client of clients) client.socket.close();
    child.kill('SIGKILL');
  });

  const ids = ['player-a', 'player-b', 'player-c', 'player-d', 'player-e'];
  for (const id of ids) clients.push(await connect(port, id));
  const byId = Object.fromEntries(ids.map((id, i) => [id, clients[i]]));
  const [a, b, c, d, watcher] = clients;
  await Promise.all(clients.map((client) => waitFor(client, (message) => message.type === 'snapshot')));

  for (const client of [a, b, c, d]) send(client, 'join-queue');
  const ready = await Promise.all([a, b, c, d].map((client) => waitFor(client, (message) => message.type === 'match-ready')));

  // Two booths, each with one red and one blue player, every queued player in exactly one of them.
  const matches = new Map();
  for (const message of ready) matches.set(message.stationId, message.players);
  assert.equal(matches.size, 2);
  const paired = [...matches.values()].flatMap((roster) => roster.map((player) => player.id)).sort();
  assert.deepEqual(paired, ['player-a', 'player-b', 'player-c', 'player-d']);
  for (const roster of matches.values()) assert.deepEqual(roster.map((player) => player.color), ['red', 'blue']);
  assert.equal(watcher.messages.some((message) => message.type === 'match-ready'), false);

  // A player already in a match cannot be queued again.
  a.messages.length = 0;
  send(a, 'join-queue');
  assert.equal((await waitFor(a, (message) => message.type === 'queue-status')).queued, false);

  const [stationId, roster] = [...matches.entries()][0];
  const [red, blue] = roster.map((player) => byId[player.id]);
  const otherStationId = [...matches.keys()][1];
  const [otherRed, otherBlue] = matches.get(otherStationId).map((player) => byId[player.id]);

  send(red, 'match-event', { event: { type: 'submit-pattern', pattern: Array(9).fill('grin') } });
  send(blue, 'match-event', { event: { type: 'submit-pattern', pattern: Array(9).fill('gem') } });
  send(otherRed, 'match-event', { event: { type: 'submit-pattern', pattern: Array(9).fill('sunny') } });
  send(otherBlue, 'match-event', { event: { type: 'submit-pattern', pattern: Array(9).fill('winky') } });
  const [start, watchedStart] = await Promise.all([
    waitFor(red, isGameEvent('game-start', stationId)),
    waitFor(watcher, isGameEvent('game-start', stationId))
  ]);
  await waitFor(otherRed, isGameEvent('game-start', otherStationId));
  assert.equal(watchedStart.event.turnPlayerId, start.event.turnPlayerId);

  // The guessing player cracks the opponent's first slot; a lobby spectator receives the result.
  const guesser = start.event.turnPlayerId === roster[0].id ? red : blue;
  const answer = guesser === red ? 'gem' : 'grin';
  const wrong = guesser === red ? 'grin' : 'gem';
  send(guesser, 'match-event', { event: { type: 'guess', tokenId: answer } });
  const seen = await waitFor(watcher, isGameEvent('guess-result', stationId));
  assert.equal(seen.event.correct, true);
  assert.equal(seen.event.tokenId, answer);
  assert.equal(seen.event.index, 0);
  send(guesser, 'match-event', { event: { type: 'guess', tokenId: wrong } });
  const miss = await waitFor(watcher, (message) => isGameEvent('guess-result', stationId)(message) && message.event.index === 1);
  assert.equal(miss.event.correct, false);

  // A late spectator gets the revealed slots in the snapshot, never the hidden patterns.
  const late = await connect(port, 'player-late');
  clients.push(late);
  const snapshot = await waitFor(late, (message) => message.type === 'snapshot');
  const booth = snapshot.stations.find((station) => station.stationId === stationId);
  const guesserColor = guesser === red ? 'red' : 'blue';
  assert.equal(booth.inProgress, true);
  assert.deepEqual(booth.revealed[guesserColor], [answer]);
  assert.equal(booth.names.red, 'Player');
  assert.equal(JSON.stringify(snapshot).includes('"target'), false);
  for (const client of clients) {
    assert.equal(client.messages.some(isGameEvent('submit-pattern')), false);
  }

  // When a player leaves, everyone hears that the match ended.
  send(otherRed, 'leave-station');
  await waitFor(watcher, isGameEvent('match-ended', otherStationId));
  await waitFor(otherBlue, isGameEvent('match-ended', otherStationId));
});

test('keeps a lone queued player waiting and gives solo bot practice its own booth', { timeout: 20000 }, async (t) => {
  const { child, port } = await startServer();
  const clients = [];
  t.after(() => {
    for (const client of clients) client.socket.close();
    child.kill('SIGKILL');
  });
  const solo = await connect(port, 'solo');
  const other = await connect(port, 'other');
  clients.push(solo, other);

  send(solo, 'join-queue');
  assert.deepEqual(await waitFor(solo, (message) => message.type === 'queue-status'), { type: 'queue-status', queued: true, size: 1 });
  await delay(300);
  assert.equal(solo.messages.some((message) => message.type === 'match-ready'), false);

  send(solo, 'play-bot');
  const seat = await waitFor(solo, (message) => message.type === 'bot-station');
  assert.equal((await waitFor(solo, (message) => message.type === 'queue-status' && !message.queued)).size, 0);
  const update = await waitFor(other, (message) => message.type === 'station-updated' && message.stationId === seat.stationId);
  assert.equal(update.bot, true);
  assert.equal(update.inProgress, true);

  send(solo, 'leave-station');
  const freed = await waitFor(other, (message) => message.type === 'station-updated' && message.stationId === seat.stationId && !message.inProgress);
  assert.deepEqual(freed.players, {});
});

test('booth pads seat players directly and queued players fill a waiting booth', { timeout: 20000 }, async (t) => {
  const { child, port } = await startServer();
  const clients = [];
  t.after(() => {
    for (const client of clients) client.socket.close();
    child.kill('SIGKILL');
  });
  for (const id of ['pad-a', 'pad-b', 'pad-c', 'queued', 'solo']) clients.push(await connect(port, id));
  const [padA, padB, padC, queued, solo] = clients;
  const seatedAt = (stationId) => (message) => message.type === 'station-updated' && message.stationId === stationId;

  // pad join: sits down and waits at that booth
  send(padA, 'join-station', { stationId: 'L1', color: 'red' });
  const waiting = await waitFor(solo, (message) => seatedAt('L1')(message) && message.waiting);
  assert.deepEqual(waiting.players, { red: 'pad-a' });
  assert.equal(waiting.inProgress, false);

  // the taken side is refused; the free side starts the match at once
  send(padC, 'join-station', { stationId: 'L1', color: 'red' });
  assert.equal((await waitFor(padC, (message) => message.type === 'join-denied')).stationId, 'L1');
  send(padB, 'join-station', { stationId: 'L1', color: 'blue' });
  const ready = await waitFor(padA, (message) => message.type === 'match-ready');
  assert.equal(ready.stationId, 'L1');
  assert.deepEqual(ready.players.map((player) => player.id), ['pad-a', 'pad-b']);
  await waitFor(padB, (message) => message.type === 'match-ready');

  // a busy booth refuses pad joins
  padC.messages.length = 0;
  send(padC, 'join-station', { stationId: 'L1', color: 'blue' });
  assert.equal((await waitFor(padC, (message) => message.type === 'join-denied')).message, 'That booth is busy.');

  // someone waiting on a pad is matched with the next queued player right away
  send(padC, 'join-station', { stationId: 'R3', color: 'blue' });
  await waitFor(solo, (message) => seatedAt('R3')(message) && message.waiting);
  send(queued, 'join-queue');
  const filled = await waitFor(queued, (message) => message.type === 'match-ready');
  assert.equal(filled.stationId, 'R3');
  assert.deepEqual(filled.players.map((player) => `${player.id}:${player.color}`), ['queued:red', 'pad-c:blue']);

  // a lone pad player can switch that booth to bot practice
  send(solo, 'join-station', { stationId: 'R5', color: 'red' });
  await waitFor(padA, (message) => seatedAt('R5')(message) && message.waiting);
  send(solo, 'play-bot');
  assert.equal((await waitFor(solo, (message) => message.type === 'bot-station')).stationId, 'R5');
  const practice = await waitFor(padA, (message) => seatedAt('R5')(message) && message.bot);
  assert.equal(practice.waiting, false);
});

// Stands in for api.bloxity.io: "token-<id>" is a valid Boxity token for account <id>.
async function startFakeBoxityApi() {
  const api = createServer((req, res) => {
    const token = (req.headers.authorization || '').replace(/^Bearer /, '');
    if (req.url !== '/v1/auth/me' || !token.startsWith('token-')) {
      res.writeHead(401).end();
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ user: { _id: token.slice('token-'.length), username: 'buyer' } }));
  });
  api.listen(0, '127.0.0.1');
  await once(api, 'listening');
  return { api, url: `http://127.0.0.1:${api.address().port}` };
}

const postWebhook = (port, body, headers = {}) => fetch(`http://127.0.0.1:${port}/api/legion-webhook`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json', ...headers },
  body: JSON.stringify(body)
});

test('verifies Boxity accounts, relays emotes and keeps the avatar a client describes', { timeout: 20000 }, async (t) => {
  const { api, url } = await startFakeBoxityApi();
  const { child, port } = await startServer({ BLOXITY_API_URL: url });
  const clients = [];
  t.after(() => {
    for (const client of clients) client.socket.close();
    child.kill('SIGKILL');
    api.close();
  });
  const me = await connect(port, 'me');
  const other = await connect(port, 'other');
  clients.push(me, other);

  send(me, 'identify', {
    token: 'token-acc1',
    player: {
      name: 'Me',
      skinUrl: 'https://api.bloxity.io/v1/avatar/skin-texture/s3_sh12_fc4.png',
      equipped: { hatId: '7', maskId: '9', shoesId: '2', bogus: 'x' },
      proportions: { height: 9, headScale: 0.1, shoulderWidth: 1.2 }
    }
  });
  const verified = await waitFor(other, (message) => message.type === 'player-updated' && message.player.userId === 'acc1');
  assert.equal(verified.player.skinUrl, 'https://api.bloxity.io/v1/avatar/skin-texture/s3_sh12_fc4.png');
  assert.deepEqual(verified.player.equipped, { hatId: '7', maskId: '9', shoesId: '2' });
  assert.deepEqual(verified.player.proportions, { height: 1.6, shoulderWidth: 1.2, headScale: 0.3 });

  // a token Boxity rejects never yields an account id
  send(other, 'identify', { token: 'forged', player: { name: 'Other' } });
  await waitFor(me, (message) => message.type === 'player-updated' && message.player.name === 'Other');
  await delay(200);
  assert.equal(me.messages.some((message) => message.type === 'player-updated' && message.player.id === 'other' && message.player.userId), false);

  send(me, 'emote', { emoteId: '0123456789abcdef01234567' });
  send(me, 'emote', { emoteId: 'not-an-emote' });
  send(me, 'emote', { emoteId: null });
  const emotes = [
    await waitFor(other, (message) => message.type === 'player-emote' && message.emoteId),
    await waitFor(other, (message) => message.type === 'player-emote' && message.emoteId === null)
  ];
  assert.deepEqual(emotes.map((message) => message.id), ['me', 'me']);
  assert.equal(other.messages.filter((message) => message.type === 'player-emote').length, 2);
});

test('records Gems webhooks once and hands the grant to the verified buyer', { timeout: 20000 }, async (t) => {
  const { api, url } = await startFakeBoxityApi();
  const { child, port } = await startServer({ BLOXITY_API_URL: url, LEGION_WEBHOOK_SECRET: 's3cret' });
  const clients = [];
  t.after(() => {
    for (const client of clients) client.socket.close();
    child.kill('SIGKILL');
    api.close();
  });
  const grant = { transactionId: 'tx-1', userId: 'acc9', username: 'buyer', gameSlug: 'pattern-rush', sku: 'coins_500', productName: '500 coins', productPrice: 50, metadata: { a: 1 }, timestamp: Date.now() };

  assert.equal((await postWebhook(port, grant)).status, 401);
  assert.equal((await postWebhook(port, grant, { 'x-legion-webhook-secret': 'wrong' })).status, 401);
  assert.equal((await postWebhook(port, { sku: 'x' }, { 'x-legion-webhook-secret': 's3cret' })).status, 400);
  const ok = await postWebhook(port, grant, { 'x-legion-webhook-secret': 's3cret' });
  assert.equal(ok.status, 200);
  assert.deepEqual(await ok.json(), { success: true, transactionId: 'tx-1' });
  // Boxity retries are acknowledged without granting twice
  assert.equal((await postWebhook(port, grant, { 'x-legion-webhook-secret': 's3cret' })).status, 200);

  const buyer = await connect(port, 'buyer');
  const bystander = await connect(port, 'bystander');
  clients.push(buyer, bystander);
  send(bystander, 'identify', { token: 'token-acc2', player: { name: 'Bystander' } });
  send(buyer, 'identify', { token: 'token-acc9', player: { name: 'Buyer' } });
  const delivered = await waitFor(buyer, (message) => message.type === 'gems-grant');
  assert.deepEqual(delivered.grant, { transactionId: 'tx-1', sku: 'coins_500', productName: '500 coins', metadata: { a: 1 } });
  await delay(300);
  assert.equal(buyer.messages.filter((message) => message.type === 'gems-grant').length, 1);
  assert.equal(bystander.messages.some((message) => message.type === 'gems-grant'), false);
});

test('refuses new players once the pod reaches its seat cap', { timeout: 20000 }, async (t) => {
  const { child, port } = await startServer({ SEAT_CAP: '2' });
  const clients = [];
  t.after(() => {
    for (const client of clients) client.socket.close();
    child.kill('SIGKILL');
  });
  clients.push(await connect(port, 'one'), await connect(port, 'two'));
  await assert.rejects(connect(port, 'three'), /503/);
  // a seated player reconnecting keeps their place
  const again = await connect(port, 'two');
  clients.push(again);
  await waitFor(again, (message) => message.type === 'snapshot');
});

// SIGTERM cannot be delivered to a child process on Windows; this runs in CI (Linux) before deploy.
test('a draining pod moves idle players off and only lets players in a running match back in', { timeout: 20000, skip: process.platform === 'win32' }, async (t) => {
  const { child, port } = await startServer({ DRAIN_TIMEOUT_MS: '15000' });
  const clients = [];
  t.after(() => {
    for (const client of clients) client.socket.close();
    if (child.exitCode === null) child.kill('SIGKILL');
  });
  const idle = await connect(port, 'idle');
  const red = await connect(port, 'red');
  const blue = await connect(port, 'blue');
  clients.push(idle, red, blue);
  send(red, 'join-station', { stationId: 'L2', color: 'red' });
  send(blue, 'join-station', { stationId: 'L2', color: 'blue' });
  await waitFor(red, (message) => message.type === 'match-ready');

  const [idleClosed] = await Promise.all([once(idle.socket, 'close'), child.kill('SIGTERM')]);
  assert.equal(idleClosed[0], 1012);
  // the hopped player and newcomers are refused, so their clients go back to the matchmaker
  await assert.rejects(connect(port, 'idle'), /503/);
  await assert.rejects(connect(port, 'newcomer'), /503/);
  // a player in the running match can reconnect to finish it
  red.socket.close();
  const back = await connect(port, 'red');
  clients.push(back);
  await waitFor(back, (message) => message.type === 'snapshot');
  assert.equal(blue.socket.readyState, WebSocket.OPEN);

  // once the match ends, the pod closes everyone and exits cleanly
  send(back, 'leave-station');
  const [code] = await once(child, 'exit');
  assert.equal(code, 0);
});
