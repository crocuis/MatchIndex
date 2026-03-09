import 'server-only';
import Redis from 'ioredis';

declare global {
  var __matchIndexRedis: Redis | null | undefined;
}

function isCacheEnabled() {
  const raw = process.env.CACHE_ENABLED?.trim().toLowerCase();

  if (!raw) {
    return true;
  }

  return raw !== 'false' && raw !== '0' && raw !== 'off';
}

export function getRedis() {
  if (!isCacheEnabled()) {
    return null;
  }

  const redisUrl = process.env.REDIS_URL;

  if (!redisUrl) {
    return null;
  }

  if (globalThis.__matchIndexRedis === undefined) {
    const client = new Redis(redisUrl, {
      lazyConnect: true,
      maxRetriesPerRequest: 1,
      enableOfflineQueue: false,
    });

    client.on('error', (error) => {
      if (process.env.NODE_ENV !== 'production') {
        console.warn('[redis]', error.message);
      }
    });

    globalThis.__matchIndexRedis = client;
  }

  return globalThis.__matchIndexRedis;
}

export async function ensureRedis() {
  const client = getRedis();

  if (!client) {
    return null;
  }

  try {
    if (client.status === 'wait') {
      await client.connect();
    }
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[redis:connect]', error);
    }

    globalThis.__matchIndexRedis = null;
    return null;
  }

  if (client.status !== 'ready') {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[redis:unavailable]', client.status);
    }

    globalThis.__matchIndexRedis = null;
    return null;
  }

  return client;
}
