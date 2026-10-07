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

async function startServer() {
  const port = await unusedPort();
  const child = spawn(process.execPath, ['src/index.js'], {
    cwd: serverDirectory,
    env: { ...process.env, PORT: String(port), CORS_ORIGIN: '*', QUEUE_DELAY_MS: '100' },
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
  child.kill();
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
    child.kill();
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
    child.kill();
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
    child.kill();
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
