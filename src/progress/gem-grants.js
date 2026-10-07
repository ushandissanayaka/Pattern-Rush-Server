import { MongoClient } from 'mongodb';

// Gems purchases are fulfilled by Boxity's server-to-server webhook. On Legion that webhook can
// land on any of the game's pods, not the buyer's, so grants are recorded in the game's managed
// Mongo (MONGODB_URI) keyed by transactionId, and every pod applies the unapplied grants of the
// players it holds. Without MONGODB_URI (local runs) grants stay in this process's memory.
export function createGrantStore(uri = process.env.MONGODB_URI) {
  return uri ? mongoStore(uri) : memoryStore();
}

function grantFields(grant) {
  return {
    userId: grant.userId,
    username: grant.username,
    gameSlug: grant.gameSlug,
    sku: grant.sku,
    productName: grant.productName,
    productPrice: grant.productPrice,
    metadata: grant.metadata,
    timestamp: grant.timestamp
  };
}

function memoryStore() {
  const grants = new Map();
  return {
    async record(grant) {
      if (!grants.has(grant.transactionId)) {
        grants.set(grant.transactionId, { _id: grant.transactionId, ...grantFields(grant), applied: false, receivedAt: new Date() });
      }
    },
    async claimFor(userIds) {
      const wanted = new Set(userIds);
      const claimed = [];
      for (const grant of grants.values()) {
        if (grant.applied || !wanted.has(grant.userId)) continue;
        grant.applied = true;
        grant.appliedAt = new Date();
        claimed.push(grant);
      }
      return claimed;
    },
    async close() {}
  };
}

function mongoStore(uri) {
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
  const grants = client.db().collection('gem_grants');
  grants.createIndex({ userId: 1, applied: 1 }).catch((error) => console.warn('[gems] index setup failed:', error.message));
  return {
    // Idempotent: Boxity may retry a webhook, and a transactionId is only ever recorded once.
    async record(grant) {
      await grants.updateOne(
        { _id: grant.transactionId },
        { $setOnInsert: { ...grantFields(grant), applied: false, receivedAt: new Date() } },
        { upsert: true }
      );
    },
    // Each grant is claimed with a conditional update, so two pods never apply the same one.
    async claimFor(userIds) {
      if (!userIds.length) return [];
      const pending = await grants.find({ userId: { $in: userIds }, applied: false }, { projection: { _id: 1 } }).limit(100).toArray();
      const claimed = [];
      for (const { _id } of pending) {
        const grant = await grants.findOneAndUpdate(
          { _id, applied: false },
          { $set: { applied: true, appliedAt: new Date(), appliedBy: process.env.POD_NAME || null } },
          { returnDocument: 'after' }
        );
        if (grant) claimed.push(grant);
      }
      return claimed;
    },
    close: () => client.close()
  };
}
