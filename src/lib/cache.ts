import 'server-only';
import { ensureRedis } from '@/lib/redis';

export type CacheTier = 'master' | 'season-current' | 'season-finished' | 'matchday-hot' | 'matchday-warm';

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
  loader: () => Promise<T>;
}

const CACHE_TTL_SECONDS: Record<CacheTier, number> = {
  master: 60 * 60 * 24,
  'season-current': 60 * 2,
  'season-finished': 60 * 60 * 24,
  'matchday-hot': 15,
  'matchday-warm': 60 * 5,
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
  return CACHE_TTL_SECONDS[tier];
}

export async function readThroughCache<T>({ key, tier, loader }: ReadThroughCacheOptions<T>) {
  let redis = null;

  try {
    redis = await ensureRedis();
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[cache:connect]', key, error);
    }
  }

  if (!redis) {
    return loader();
  }

  try {
    const cachedValue = await redis.get(key);

    if (cachedValue) {
      return JSON.parse(cachedValue) as T;
    }
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[cache:read]', key, error);
    }
  }

  const freshValue = await loader();

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
