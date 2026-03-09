import 'server-only';
import postgres, { type Sql } from 'postgres';

declare global {
  var __matchIndexSqlClients: Map<string, Sql> | undefined;
}

interface DbClientConfig {
  max: number;
  idle_timeout: number;
  prepare: boolean;
}

function readPoolSize(envKey: string, fallback: number) {
  const rawValue = process.env[envKey];
  if (!rawValue) {
    return fallback;
  }

  const parsed = Number.parseInt(rawValue, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function getReadDbConfig(): DbClientConfig {
  return {
    max: readPoolSize('DB_POOL_MAX_READ', 4),
    idle_timeout: 20,
    prepare: false,
  };
}

function getSingleConnectionDbConfig(envKey: string): DbClientConfig {
  return {
    max: readPoolSize(envKey, 1),
    idle_timeout: 20,
    prepare: false,
  };
}

const SINGLE_CONNECTION_SCOPE_ENV_KEYS: Record<string, string> = {
  'statsbomb-ingest': 'DB_POOL_MAX_INGEST',
  'statsbomb-materialize': 'DB_POOL_MAX_MATERIALIZE',
  'statsbomb-details': 'DB_POOL_MAX_DETAILS',
  'photo-ingest': 'DB_POOL_MAX_PHOTO_INGEST',
  'photo-seed': 'DB_POOL_MAX_PHOTO_SEED',
};

const DEFAULT_SINGLE_CONNECTION_DB_CONFIG: DbClientConfig = {
  idle_timeout: 20,
  prepare: false,
  max: 1,
};

function getDbClientStore() {
  if (!globalThis.__matchIndexSqlClients) {
    globalThis.__matchIndexSqlClients = new Map<string, Sql>();
  }

  return globalThis.__matchIndexSqlClients;
}

function getConnectionString() {
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return connectionString;
}

function getSharedDbClient(scope: string, config: DbClientConfig) {
  const store = getDbClientStore();
  const existing = store.get(scope);

  if (existing) {
    return existing;
  }

  const client = postgres(getConnectionString(), config);
  store.set(scope, client);
  return client;
}

export function getDb() {
  return getSharedDbClient('app-read', getReadDbConfig());
}

export function getSingleConnectionDb(scope: string) {
  const envKey = SINGLE_CONNECTION_SCOPE_ENV_KEYS[scope];
  const config = envKey ? getSingleConnectionDbConfig(envKey) : DEFAULT_SINGLE_CONNECTION_DB_CONFIG;
  return getSharedDbClient(scope, config);
}

export async function resetDbClient(scope: string) {
  const store = getDbClientStore();
  const client = store.get(scope);

  if (!client) {
    return;
  }

  try {
    await client.end({ timeout: 1 });
  } finally {
    store.delete(scope);
  }
}
