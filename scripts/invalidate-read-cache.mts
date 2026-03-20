import Redis from 'ioredis';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  help: boolean;
}

const CACHE_NAMESPACES = [
  'club-by-id-v5',
  'club-overview-stats',
  'club-season-history',
  'club-season-meta',
  'clubs-by-league-season-v3',
  'clubs-by-league-v3',
  'clubs-paginated-v4',
  'clubs-v4',
  'dashboard-tournament-summary-v5',
  'finished-matches-paginated',
  'league-by-id',
  'league-count',
  'league-filter-options',
  'leagues',
  'leagues-by-ids',
  'leagues-paginated',
  'match-analysis',
  'match-analysis-artifact-v1',
  'match-freeze-frames-artifact-v1',
  'match-lineups-v2',
  'match-stats',
  'match-timeline',
  'match-visible-areas-artifact-v1',
  'matches-by-club-season-v4',
  'matches-by-club-v3',
  'matches-by-league-season-v4',
  'matches-by-league-v4',
  'matches-by-nation',
  'matches-v2',
  'nation-by-id-men',
  'nation-by-id-women',
  'nations',
  'nations-paginated',
  'nations-women',
  'nations-women-paginated',
  'player-by-id-v8',
  'player-count-by-nation',
  'player-links-by-ids-v1',
  'players',
  'players-by-club',
  'players-by-club-season',
  'players-by-nation',
  'players-paginated-v3',
  'recent-finished-matches-by-club-season-v3',
  'recent-finished-matches-by-club-v2',
  'recent-finished-matches-by-league-ids-v3',
  'search-v4',
  'seasons-by-league',
  'standings-by-league-ids-v4',
  'standings-by-season-v4',
  'standings-v5',
  'team-translation-snapshot-v1',
  'top-scorer-rows-by-season-v2',
  'top-scorer-rows-v2',
  'top-scorers',
  'top-scorers-by-season',
  'upcoming-scheduled-matches-by-club-season-v3',
  'upcoming-scheduled-matches-by-league-ids-v4',
] as const;

function parseArgs(argv: string[]): CliOptions {
  return {
    help: argv.includes('--help') || argv.includes('-h'),
  };
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/invalidate-read-cache.mts

현재 앱의 readThroughCache Redis 키를 namespace 기반으로 SCAN + DEL 합니다.
REDIS_URL 또는 CACHE_ENABLED 설정이 없으면 no-op로 종료합니다.
`);
}

function isCacheEnabled() {
  const raw = process.env.CACHE_ENABLED?.trim().toLowerCase();

  if (!raw) {
    return true;
  }

  return raw !== 'false' && raw !== '0' && raw !== 'off';
}

async function scanKeys(client: Redis, pattern: string) {
  const matched = new Set<string>();
  let cursor = '0';

  do {
    const [nextCursor, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', 500);
    cursor = nextCursor;
    for (const key of keys) {
      matched.add(key);
    }
  } while (cursor !== '0');

  return [...matched];
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  if (!isCacheEnabled()) {
    console.log(JSON.stringify({ skipped: true, reason: 'CACHE_ENABLED disabled' }, null, 2));
    return;
  }

  const redisUrl = process.env.REDIS_URL;
  if (!redisUrl) {
    console.log(JSON.stringify({ skipped: true, reason: 'REDIS_URL missing' }, null, 2));
    return;
  }

  const client = new Redis(redisUrl, {
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
  });

  try {
    await client.connect();

    const keys = new Set<string>();
    for (const namespace of CACHE_NAMESPACES) {
      const matched = await scanKeys(client, `${namespace}*`);
      for (const key of matched) {
        keys.add(key);
      }
    }

    const allKeys = [...keys];
    if (allKeys.length === 0) {
      console.log(JSON.stringify({ skipped: false, deleted: 0 }, null, 2));
      return;
    }

    let deleted = 0;
    for (let index = 0; index < allKeys.length; index += 500) {
      const chunk = allKeys.slice(index, index + 500);
      deleted += await client.del(...chunk);
    }

    console.log(JSON.stringify({ skipped: false, deleted, scannedNamespaces: CACHE_NAMESPACES.length }, null, 2));
  } finally {
    client.disconnect();
  }
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
