import { createHash, timingSafeEqual } from 'node:crypto';

// POST /api/legion-webhook: Boxity's Gems purchase fulfillment. Answering 2xx confirms the grant;
// anything else makes Boxity refund the buyer, so we only answer 200 once the grant is recorded.
const MAX_BODY_BYTES = 16 * 1024;
const digest = (value) => createHash('sha256').update(value).digest();

function json(res, status, body) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(body));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new Error('Body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

const shortString = (value, max = 128) => typeof value === 'string' && value.length > 0 && value.length <= max;

export function createWebhookHandler({ store, secret = process.env.LEGION_WEBHOOK_SECRET, onGrant }) {
  return async function handleWebhook(req, res) {
    if (secret) {
      const given = req.headers['x-legion-webhook-secret'];
      if (typeof given !== 'string' || !timingSafeEqual(digest(given), digest(secret))) {
        json(res, 401, { success: false, error: 'Invalid webhook secret' });
        return;
      }
    }
    let grant;
    try {
      grant = JSON.parse(await readBody(req));
    } catch {
      json(res, 400, { success: false, error: 'Invalid JSON body' });
      return;
    }
    if (!grant || typeof grant !== 'object' || !shortString(grant.transactionId) || !shortString(grant.userId) || !shortString(grant.sku)) {
      json(res, 400, { success: false, error: 'transactionId, userId and sku are required' });
      return;
    }
    try {
      await store.record(grant);
    } catch (error) {
      console.error('[gems] could not record grant', grant.transactionId, error.message);
      json(res, 503, { success: false, error: 'Grant could not be recorded' });
      return;
    }
    console.log(`[gems] recorded ${grant.transactionId}: ${grant.sku} for ${grant.userId}`);
    json(res, 200, { success: true, transactionId: grant.transactionId });
    onGrant?.(grant.userId);
  };
}
