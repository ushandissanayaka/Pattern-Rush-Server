// Player identity: the avatar a client describes, and the Boxity account behind it.

const API_URL = (process.env.BLOXITY_API_URL || 'https://api.bloxity.io').replace(/\/$/, '');

const EQUIPPED_SLOTS = [
  'hatId', 'backId', 'skinId', 'headId', 'armLId', 'armRId', 'legLId', 'legRId', 'torsoId', 'hairId',
  'maskId', 'neckId', 'chestId', 'waistId', 'handId', 'shoesId', 'faceId', 'pantsId', 'shirtId'
];

// Same ranges Legion.SDK.avatar.setProportions clamps to.
const PROPORTION_RANGES = {
  height: [0.5, 1.6],
  shoulderWidth: [0.5, 1.5],
  armLength: [0.05, 3],
  legOffsetX: [-0.7, 5],
  torsoScaleX: [0.3, 2],
  neckHeight: [0.94, 1.2],
  headScale: [0.3, 2.6]
};

// Plain skins come from the avatar CDN; skins with a face, shirt or pants come from getSkinTextureUrl().
const SKIN_URL = /^https:\/\/(static\.bloxity\.io\/avatars\/|api\.bloxity\.io\/v1\/avatar\/skin-texture\/)/i;

export function cleanIdentity(player, identity) {
  if (!identity || typeof identity !== 'object') return;
  if (typeof identity.name === 'string') player.name = identity.name.trim().slice(0, 24) || 'Player';
  if (typeof identity.skinUrl === 'string' && SKIN_URL.test(identity.skinUrl)) {
    player.skinUrl = identity.skinUrl.slice(0, 512);
  }
  if (identity.equipped && typeof identity.equipped === 'object' && !Array.isArray(identity.equipped)) {
    const equipped = {};
    for (const key of EQUIPPED_SLOTS) {
      const value = identity.equipped[key];
      if (typeof value === 'string' || typeof value === 'number') equipped[key] = String(value).slice(0, 40);
    }
    player.equipped = equipped;
  }
  if (identity.proportions && typeof identity.proportions === 'object') {
    const proportions = {};
    for (const [key, [min, max]] of Object.entries(PROPORTION_RANGES)) {
      const value = Number(identity.proportions[key]);
      if (Number.isFinite(value)) proportions[key] = Math.min(max, Math.max(min, value));
    }
    player.proportions = proportions;
  }
}

// Resolves a Boxity token (Legion.SDK.auth.getToken()) to its account id, or null if Boxity rejects it.
export async function verifyBoxityToken(token) {
  if (typeof token !== 'string' || !token || token.length > 4096) return null;
  try {
    const response = await fetch(`${API_URL}/v1/auth/me`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(8000)
    });
    if (!response.ok) return null;
    const body = await response.json();
    const user = body?.user ?? body;
    return typeof user?._id === 'string' && user._id ? { userId: user._id, username: String(user.username || '') } : null;
  } catch (error) {
    console.warn('[identity] could not verify Boxity token:', error.message);
    return null;
  }
}
