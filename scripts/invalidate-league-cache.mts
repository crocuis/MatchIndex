import { deleteCacheKey, buildCacheKey } from '../src/lib/cache.ts';

async function main() {
  const argv = process.argv.slice(2);
  const getOption = (name: string) => argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  const leagueId = getOption('league');
  const season = getOption('season');

  if (!leagueId || !season) {
    console.log('Usage: node --experimental-strip-types scripts/invalidate-league-cache.mts --league=<slug> --season=<season>');
    return;
  }

  const locales = ['en', 'ko'];
  const keys: string[] = [];

  for (const locale of locales) {
    keys.push(buildCacheKey({ namespace: 'league-by-id', locale, id: leagueId }));
    keys.push(buildCacheKey({ namespace: 'standings-by-season-v5', locale, id: leagueId, params: { season, includeForm: true } }));
    keys.push(buildCacheKey({ namespace: 'standings-by-season-v5', locale, id: leagueId, params: { season, includeForm: false } }));
    keys.push(buildCacheKey({ namespace: 'matches-by-league-season-v5', locale, id: leagueId, params: { season } }));
    keys.push(buildCacheKey({ namespace: 'clubs-by-league-season-v3', locale, id: leagueId, params: { season } }));
    keys.push(buildCacheKey({ namespace: 'top-scorers-by-season', id: leagueId, params: { season, limit: 10 } }));
    keys.push(buildCacheKey({ namespace: 'top-scorer-rows-by-season-v3', locale, id: leagueId, params: { season, limit: 10 } }));
  }

  await Promise.all(keys.map((key) => deleteCacheKey(key)));
  console.log(JSON.stringify({ invalidated: keys.length, keys }, null, 2));
}

await main();
