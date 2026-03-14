import 'server-only';
import { ensureRedis } from '@/lib/redis';

declare global {
  var __matchIndexMemoryCache: Map<string, { expiresAt: number; value: unknown }> | undefined;
}

export type CacheTier = 'master' | 'season-current' | 'season-finished' | 'matchday-hot' | 'matchday-warm';
export type RefreshScope = 'master' | 'season_current' | 'season_finished' | 'matchday_hot' | 'matchday_warm';
export type RefreshPolicySlug =
  | 'master.competitions'
  | 'master.teams'
  | 'master.countries'
  | 'season.current.standings'
  | 'season.finished.read_model'
  | 'match.read_model'
  | 'search.read_model'
  | 'match.live.detail'
  | 'match.upcoming.detail';

interface CachePolicy {
  refreshScope: RefreshScope;
  ttlSeconds: number;
}

export interface CacheKeyInput {
  namespace: string;
  locale?: string;
  season?: string | number;
  id?: string | number;
  params?: Record<string, boolean | number | string | undefined>;
}

interface ReadThroughCacheOptions<T> {
  key: string;
  tier: CacheTier;
  policySlug?: RefreshPolicySlug;
  loader: () => Promise<T>;
}

const CACHE_POLICIES: Record<CacheTier, CachePolicy> = {
  // SQL seed policies split master data by entity, so runtime uses the stricter team TTL.
  master: {
    refreshScope: 'master',
    ttlSeconds: 60 * 60 * 12,
  },
  'season-current': {
    refreshScope: 'season_current',
    ttlSeconds: 60 * 2,
  },
  'season-finished': {
    refreshScope: 'season_finished',
    ttlSeconds: 60 * 60 * 24,
  },
  'matchday-hot': {
    refreshScope: 'matchday_hot',
    ttlSeconds: 15,
  },
  'matchday-warm': {
    refreshScope: 'matchday_warm',
    ttlSeconds: 60 * 5,
  },
};

const CACHE_TIERS_BY_REFRESH_SCOPE: Record<RefreshScope, CacheTier> = {
  master: 'master',
  season_current: 'season-current',
  season_finished: 'season-finished',
  matchday_hot: 'matchday-hot',
  matchday_warm: 'matchday-warm',
};

const REFRESH_SCOPES_BY_POLICY_SLUG: Record<RefreshPolicySlug, RefreshScope> = {
  'master.competitions': 'master',
  'master.teams': 'master',
  'master.countries': 'master',
  'season.current.standings': 'season_current',
  'season.finished.read_model': 'season_finished',
  'match.read_model': 'matchday_warm',
  'search.read_model': 'matchday_warm',
  'match.live.detail': 'matchday_hot',
  'match.upcoming.detail': 'matchday_warm',
};

function serializeParams(params?: Record<string, boolean | number | string | undefined>) {
  if (!params) {
    return '';
  }

  const entries = Object.entries(params)
    .filter(([, value]) => value !== undefined)
    .sort(([left], [right]) => left.localeCompare(right));

  return entries.map(([key, value]) => `${key}=${String(value)}`).join('&');
}

function getMemoryCacheStore() {
  if (!globalThis.__matchIndexMemoryCache) {
    globalThis.__matchIndexMemoryCache = new Map();
  }

  return globalThis.__matchIndexMemoryCache;
}

function readMemoryCache<T>(key: string) {
  const store = getMemoryCacheStore();
  const cached = store.get(key);

  if (!cached) {
    return null;
  }

  if (cached.expiresAt <= Date.now()) {
    store.delete(key);
    return null;
  }

  return cached.value as T;
}

function writeMemoryCache<T>(key: string, tier: CacheTier, value: T) {
  const store = getMemoryCacheStore();
  store.set(key, {
    value,
    expiresAt: Date.now() + getCacheTtl(tier) * 1000,
  });
}

export function buildCacheKey({ namespace, locale, season, id, params }: CacheKeyInput) {
  const keyParts = [namespace];

  if (locale) {
    keyParts.push(`locale:${locale}`);
  }

  if (season !== undefined) {
    keyParts.push(`season:${String(season)}`);
  }

  if (id !== undefined) {
    keyParts.push(`id:${String(id)}`);
  }

  const serializedParams = serializeParams(params);

  if (serializedParams) {
    keyParts.push(serializedParams);
  }

  return keyParts.join(':');
}

export function getCacheTtl(tier: CacheTier) {
  return CACHE_POLICIES[tier].ttlSeconds;
}

export function getRefreshScopeForCacheTier(tier: CacheTier) {
  return CACHE_POLICIES[tier].refreshScope;
}

export function getCacheTierForRefreshScope(scope: RefreshScope) {
  return CACHE_TIERS_BY_REFRESH_SCOPE[scope];
}

export function getRefreshScopeForPolicySlug(policySlug: RefreshPolicySlug) {
  return REFRESH_SCOPES_BY_POLICY_SLUG[policySlug];
}

function validateCachePolicyAlignment(tier: CacheTier, policySlug?: RefreshPolicySlug) {
  if (!policySlug) {
    return;
  }

  const tierScope = getRefreshScopeForCacheTier(tier);
  const policyScope = getRefreshScopeForPolicySlug(policySlug);

  if (tierScope !== policyScope) {
    throw new Error(`Cache tier ${tier} does not match refresh policy ${policySlug}`);
  }
}

export async function readThroughCache<T>({ key, tier, policySlug, loader }: ReadThroughCacheOptions<T>) {
  const effectivePolicySlug = policySlug ?? (tier === 'season-finished' ? 'season.finished.read_model' : undefined);

  validateCachePolicyAlignment(tier, effectivePolicySlug);

  let redis = null;

  try {
    redis = await ensureRedis();
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[cache:connect]', key, error);
    }
  }

  if (!redis) {
    const memoryValue = readMemoryCache<T>(key);
    if (memoryValue !== null) {
      return memoryValue;
    }

    const freshValue = await loader();
    writeMemoryCache(key, tier, freshValue);
    return freshValue;
  }

  try {
    const cachedValue = await redis.get(key);

    if (cachedValue) {
      const parsed = JSON.parse(cachedValue) as T;
      writeMemoryCache(key, tier, parsed);
      return parsed;
    }
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[cache:read]', key, error);
    }

    const memoryValue = readMemoryCache<T>(key);
    if (memoryValue !== null) {
      return memoryValue;
    }
  }

  const freshValue = await loader();
  writeMemoryCache(key, tier, freshValue);

  try {
    await redis.set(key, JSON.stringify(freshValue), 'EX', getCacheTtl(tier));
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[cache:write]', key, error);
    }
  }

  return freshValue;
}

export async function deleteCacheKey(key: string) {
  getMemoryCacheStore().delete(key);

  let redis = null;

  try {
    redis = await ensureRedis();
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[cache:delete:connect]', key, error);
    }
  }

  if (!redis) {
    return 0;
  }

  try {
    return await redis.del(key);
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[cache:delete]', key, error);
    }

    return 0;
  }
}
