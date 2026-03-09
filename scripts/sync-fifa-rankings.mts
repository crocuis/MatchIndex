import Redis from 'ioredis';
import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { NATION_CODE_SKIP, resolveNationCodeAlias } from './nation-code-aliases.mts';
import { getNationFlagUrl } from '../src/data/nationVisuals.ts';

interface CliOptions {
  countryCodes?: string[];
  dryRun: boolean;
  force: boolean;
  gender: 'men' | 'women';
  help: boolean;
  limit?: number;
  onlyMissing: boolean;
}

interface CountryRow {
  code_alpha3: string;
  fifa_ranking: number | null;
}

interface RankingResult {
  code: string;
  currentRank: number;
  lastUpdateDate: string;
}

interface RankingFailure {
  code: string;
  reason: string;
}

interface OverviewDateEntry {
  id?: string;
}

interface RankingOverviewEntry {
  code: string;
  confederation?: string;
  flagUrl?: string;
  lastUpdateDate: string;
  name: string;
  rank: number;
}

interface EnsureTopCountriesResult {
  clearedCodes: string[];
  insertedCodes: string[];
  preview: Array<{ code: string; name: string; rank: number }>;
  touchedCount: number;
}

const OFFICIAL_SOURCE = 'fifa_official';
const FIFA_OVERVIEW_BASE_URL = 'https://inside.fifa.com/fifa-world-ranking';
const FIFA_OVERVIEW_API_URL = 'https://inside.fifa.com/api/ranking-overview';
const TOP_RANK_LIMIT = 100;

function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: false,
    force: false,
    gender: 'men',
    help: false,
    onlyMissing: false,
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--force') {
      options.force = true;
      continue;
    }

    if (arg.startsWith('--gender=')) {
      const value = arg.slice('--gender='.length);
      if (value === 'men' || value === 'women') {
        options.gender = value;
      }
      continue;
    }

    if (arg === '--only-missing') {
      options.onlyMissing = true;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
      continue;
    }

    if (arg.startsWith('--country=')) {
      const values = arg.slice('--country='.length)
        .split(',')
        .map((value) => value.trim().toUpperCase())
        .filter((value) => /^[A-Z]{3}$/.test(value));

      if (values.length > 0) {
        options.countryCodes = values;
      }
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-fifa-rankings.mts [options]

Options:
  --dry-run            Preview updates without writing to the database
  --force              Write even when this official update date was already synced
  --gender=men|women   Sync men's or women's official FIFA rankings
  --only-missing       Sync only countries with missing FIFA ranking values
  --limit=<n>          Limit number of countries fetched
  --country=<A,B,C>    Sync only selected FIFA country codes
  --help, -h           Show this help message

Environment:
  DATABASE_URL         PostgreSQL connection string
  REDIS_URL            Optional Redis URL for nation cache invalidation
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    max: 1,
    idle_timeout: 5,
    prepare: false,
  });
}

function getRedis() {
  const redisUrl = process.env.REDIS_URL?.trim();
  if (!redisUrl) {
    return null;
  }

  return new Redis(redisUrl, {
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
  });
}

function extractNextData(html: string) {
  const match = html.match(/<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/);

  if (!match) {
    throw new Error('Unable to find __NEXT_DATA__ payload');
  }

  return JSON.parse(match[1]) as {
    props?: {
      pageProps?: {
        pageData?: {
          historyRanking?: Record<string, {
            statistic?: {
              highlightsItems?: Array<{ label?: string; value?: string }>;
            };
          }>;
          ranking?: {
            rankings?: {
              menRanking?: {
                lastUpdateDate?: string;
                rows?: Array<{ countryCode?: string; lastUpdateDate?: string; rank?: number; active?: boolean }>;
              };
              womenRanking?: {
                lastUpdateDate?: string;
                rows?: Array<{ countryCode?: string; lastUpdateDate?: string; rank?: number; active?: boolean }>;
              };
            };
          };
        };
      };
    };
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function findAvailableDates(value: unknown): OverviewDateEntry[] | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const maybeDates = value.allAvailableDates;
  if (Array.isArray(maybeDates)) {
    return maybeDates as OverviewDateEntry[];
  }

  for (const child of Object.values(value)) {
    const dates = findAvailableDates(child);
    if (dates) {
      return dates;
    }
  }

  return undefined;
}

function normalizeConfederation(value: string | undefined) {
  const normalized = value?.trim().toUpperCase();
  return normalized && /^[A-Z]+$/.test(normalized) ? normalized : undefined;
}

async function fetchLatestOverviewDateId(gender: 'men' | 'women') {
  const response = await fetch(`${FIFA_OVERVIEW_BASE_URL}/${gender}`, {
    headers: { Accept: 'text/html' },
    redirect: 'follow',
  });

  if (!response.ok) {
    throw new Error(`Unable to load FIFA ${gender} overview page: HTTP ${response.status}`);
  }

  const nextData = extractNextData(await response.text()) as unknown;
  const allAvailableDates = findAvailableDates(nextData);
  const latestDateId = allAvailableDates?.find((entry) => typeof entry?.id === 'string')?.id;

  if (!latestDateId) {
    throw new Error(`Unable to resolve latest FIFA ${gender} overview date id`);
  }

  return latestDateId;
}

async function fetchOverviewTopRankings(gender: 'men' | 'women', limit: number): Promise<RankingOverviewEntry[]> {
  const latestDateId = await fetchLatestOverviewDateId(gender);
  const url = new URL(FIFA_OVERVIEW_API_URL);
  url.searchParams.set('locale', 'en');
  url.searchParams.set('dateId', latestDateId);
  url.searchParams.set('rankingType', 'football');

  const response = await fetch(url, {
    headers: { Accept: 'application/json' },
    redirect: 'follow',
  });

  if (!response.ok) {
    throw new Error(`Unable to load FIFA ${gender} overview API: HTTP ${response.status}`);
  }

  const payload = await response.json() as {
    rankings?: Array<{
      lastUpdateDate?: string;
      rankingItem?: {
        countryCode?: string;
        flag?: { src?: string };
        name?: string;
        rank?: number;
      };
      tag?: {
        id?: string;
        text?: string;
      };
    }>;
  };

  const rankings = (payload.rankings ?? [])
    .map((entry): RankingOverviewEntry | null => {
      const code = entry.rankingItem?.countryCode?.trim().toUpperCase();
      const name = entry.rankingItem?.name?.trim();
      const rank = entry.rankingItem?.rank;
      const lastUpdateDate = entry.lastUpdateDate?.trim();

      if (!code || !name || !rank || !lastUpdateDate) {
        return null;
      }

      return {
        code,
        confederation: normalizeConfederation(entry.tag?.id) ?? normalizeConfederation(entry.tag?.text),
        flagUrl: entry.rankingItem?.flag?.src?.trim() || undefined,
        lastUpdateDate,
        name,
        rank,
      } satisfies RankingOverviewEntry;
    })
    .filter((entry): entry is RankingOverviewEntry => entry !== null)
    .filter((entry) => entry.rank > 0 && entry.rank <= limit);

  return rankings.toSorted((left, right) => left.rank - right.rank);
}

async function ensureTopCountries(
  sql: postgres.Sql,
  gender: 'men' | 'women',
  dryRun: boolean,
): Promise<EnsureTopCountriesResult> {
  const rankingColumn = gender === 'women' ? sql`fifa_ranking_women` : sql`fifa_ranking`;
  const overviewEntries = await fetchOverviewTopRankings(gender, TOP_RANK_LIMIT);
  const canonicalOverviewCodes = Array.from(new Set(
    overviewEntries
      .map((entry) => resolveNationCodeAlias(entry.code))
      .filter((code) => /^[A-Z]{3}$/.test(code) && !NATION_CODE_SKIP.has(code))
  ));
  const existingRows = await sql<Array<{ code_alpha3: string }>>`
    SELECT code_alpha3
    FROM countries
    WHERE code_alpha3 = ANY(${canonicalOverviewCodes})
  `;
  const existingCodes = new Set(existingRows.map((row) => row.code_alpha3));
  const clearedCodes: string[] = [];
  const insertedCodes: string[] = [];
  const preview: Array<{ code: string; name: string; rank: number }> = [];
  let touchedCount = 0;

  const staleTopRows = await sql<Array<{ code_alpha3: string }>>`
    SELECT code_alpha3
    FROM countries
    WHERE is_active = TRUE
      AND ${rankingColumn} IS NOT NULL
      AND ${rankingColumn} > 0
      AND ${rankingColumn} <= ${TOP_RANK_LIMIT}
      AND NOT (code_alpha3 = ANY(${canonicalOverviewCodes}))
    ORDER BY ${rankingColumn} ASC, code_alpha3 ASC
  `;

  clearedCodes.push(...staleTopRows.map((row) => row.code_alpha3));

  if (!dryRun && staleTopRows.length > 0) {
    await sql`
      UPDATE countries
      SET ${rankingColumn} = NULL, updated_at = NOW()
      WHERE code_alpha3 = ANY(${staleTopRows.map((row) => row.code_alpha3)})
    `;
  }

  for (const entry of overviewEntries) {
    const canonicalCode = resolveNationCodeAlias(entry.code);

    if (!/^[A-Z]{3}$/.test(canonicalCode) || NATION_CODE_SKIP.has(canonicalCode)) {
      continue;
    }

    if (!existingCodes.has(canonicalCode)) {
      insertedCodes.push(canonicalCode);
      preview.push({ code: canonicalCode, name: entry.name, rank: entry.rank });
    }

    if (dryRun) {
      continue;
    }

    const flagUrl = getNationFlagUrl(canonicalCode, entry.flagUrl) ?? entry.flagUrl ?? null;

    await sql`
      INSERT INTO countries (
        code_alpha3,
        confederation,
        ${rankingColumn},
        flag_url,
        is_active,
        updated_at
      )
      VALUES (
        ${canonicalCode},
        ${entry.confederation ?? null},
        ${entry.rank},
        ${flagUrl},
        TRUE,
        NOW()
      )
      ON CONFLICT (code_alpha3)
      DO UPDATE SET
        confederation = COALESCE(EXCLUDED.confederation, countries.confederation),
        ${rankingColumn} = EXCLUDED.${rankingColumn},
        flag_url = COALESCE(EXCLUDED.flag_url, countries.flag_url),
        is_active = TRUE,
        updated_at = NOW()
    `;

    await sql`
      INSERT INTO country_translations (country_id, locale, name)
      VALUES ((SELECT id FROM countries WHERE code_alpha3 = ${canonicalCode}), 'en', ${entry.name})
      ON CONFLICT (country_id, locale)
      DO UPDATE SET name = EXCLUDED.name
    `;

    touchedCount += 1;
  }

  return {
    clearedCodes,
    insertedCodes,
    preview,
    touchedCount: touchedCount + clearedCodes.length,
  };
}

function parseRank(value: string | undefined) {
  const match = value?.match(/\d+/);
  if (!match) {
    return undefined;
  }

  const parsed = Number.parseInt(match[0], 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseRanking(html: string, code: string, gender: 'men' | 'women'): RankingResult {
  const nextData = extractNextData(html);
  const pageData = nextData.props?.pageProps?.pageData;
  const rankingCollection = gender === 'women'
    ? pageData?.ranking?.rankings?.womenRanking
    : pageData?.ranking?.rankings?.menRanking;
  const rankingRow = rankingCollection?.rows?.find((row) => row.countryCode === code || row.active);
  const highlights = pageData?.historyRanking?.['1']?.statistic?.highlightsItems ?? [];
  const currentRank = rankingRow?.rank ?? parseRank(highlights.find((item) => item.label === 'Current rank')?.value);
  const lastUpdateDate = rankingCollection?.lastUpdateDate
    ?? rankingRow?.lastUpdateDate;

  if (!currentRank) {
    throw new Error(`Unable to parse current rank for ${code}`);
  }

  if (!lastUpdateDate) {
    throw new Error(`Unable to parse last update date for ${code}`);
  }

  return {
    code,
    currentRank,
    lastUpdateDate,
  };
}

async function fetchRanking(sourceCode: string, targetCode: string, gender: 'men' | 'women') {
  const response = await fetch(`https://inside.fifa.com/fifa-world-ranking/${sourceCode}?gender=${gender}`, {
    headers: { Accept: 'text/html' },
    redirect: 'follow',
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  return parseRanking(await response.text(), targetCode, gender);
}

async function safeFetchRanking(code: string, gender: 'men' | 'women'): Promise<RankingResult | RankingFailure> {
  try {
    const sourceCode = resolveNationCodeAlias(code);

    if (NATION_CODE_SKIP.has(sourceCode)) {
      return {
        code,
        reason: 'skipped unsupported pseudo code',
      };
    }

    return await fetchRanking(sourceCode, code, gender);
  } catch (error) {
    return {
      code,
      reason: error instanceof Error ? error.message : String(error),
    };
  }
}

async function invalidateNationCache() {
  const redis = getRedis();

  if (!redis) {
    return 0;
  }

  try {
    await redis.connect();
    return redis.del('nations:locale:en', 'nations:locale:ko', 'nations-women:locale:en', 'nations-women:locale:ko');
  } catch {
    return 0;
  } finally {
    redis.disconnect();
  }
}

function toRankingDate(value: string) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid FIFA update date: ${value}`);
  }

  return date.toISOString().slice(0, 10);
}

function printSummary(summary: Record<string, unknown>) {
  console.log(JSON.stringify(summary, null, 2));
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

    const sql = getSql();
    const rankingColumn = options.gender === 'women' ? sql`fifa_ranking_women` : sql`fifa_ranking`;
    const rankingSource = `${OFFICIAL_SOURCE}_${options.gender}`;

    try {
      const ensuredTopCountries = options.countryCodes
        ? { clearedCodes: [], insertedCodes: [], preview: [], touchedCount: 0 }
        : await ensureTopCountries(sql, options.gender, options.dryRun);
      const countryRows = await sql<CountryRow[]>`
      SELECT code_alpha3, ${rankingColumn} AS fifa_ranking
      FROM countries
      WHERE is_active = TRUE
      ORDER BY code_alpha3 ASC
    `;

    const selectedCodes = options.countryCodes ? new Set(options.countryCodes) : null;
    const countries = countryRows
      .filter((country) => {
        const sourceCode = resolveNationCodeAlias(country.code_alpha3);
        return /^[A-Z]{3}$/.test(sourceCode) && !NATION_CODE_SKIP.has(sourceCode);
      })
      .filter((country) => !options.onlyMissing || !country.fifa_ranking || country.fifa_ranking <= 0)
      .filter((country) => !selectedCodes || selectedCodes.has(country.code_alpha3))
      .slice(0, options.limit ?? countryRows.length);

    if (countries.length === 0) {
      throw new Error('No countries found to sync');
    }

    const previousRankByCode = new Map(countries.map((country) => [country.code_alpha3, country.fifa_ranking]));
    const results: RankingResult[] = [];
    const failures: RankingFailure[] = [];

    for (const country of countries) {
      const result = await safeFetchRanking(country.code_alpha3, options.gender);
      if ('currentRank' in result) {
        results.push(result);
      } else {
        failures.push(result);
      }
    }

    if (results.length === 0) {
      const invalidatedCacheKeys = !options.dryRun && ensuredTopCountries.touchedCount > 0
        ? await invalidateNationCache()
        : 0;

      printSummary({
        dryRun: options.dryRun,
        gender: options.gender,
        skipped: true,
        skippedReason: 'no successful ranking fetches',
        countryCount: countries.length,
        ensuredTop100Count: ensuredTopCountries.touchedCount,
        ensuredTop100ClearedCount: ensuredTopCountries.clearedCodes.length,
        ensuredTop100ClearedCodes: ensuredTopCountries.clearedCodes,
        ensuredTop100InsertedCount: ensuredTopCountries.insertedCodes.length,
        ensuredTop100Preview: ensuredTopCountries.preview.slice(0, 20),
        failureCount: failures.length,
        failures: failures.slice(0, 20),
        invalidatedCacheKeys,
      });
      return;
    }

    const updateDates = Array.from(new Set(results.map((result) => result.lastUpdateDate)));
    const rankingDate = toRankingDate(updateDates.toSorted().at(-1) ?? results[0].lastUpdateDate);

    const [existingDate] = await sql<Array<{ already_synced: boolean }>>`
      SELECT EXISTS (
        SELECT 1
        FROM ranking_history
        WHERE ranking_date = ${rankingDate}
          AND source = ${rankingSource}
          AND ranking_category = ${options.gender}
      ) AS already_synced
    `;

    const changes = results.filter((result) => previousRankByCode.get(result.code) !== result.currentRank);

    if (options.dryRun || (existingDate?.already_synced && !options.force)) {
      const invalidatedCacheKeys = !options.dryRun && ensuredTopCountries.touchedCount > 0
        ? await invalidateNationCache()
        : 0;

      printSummary({
        dryRun: options.dryRun,
        gender: options.gender,
        skipped: existingDate?.already_synced && !options.force,
        skippedReason: existingDate?.already_synced && !options.force ? 'ranking date already synced' : undefined,
        countryCount: countries.length,
        rankingDate,
        updateDates,
        changedCount: changes.length,
        ensuredTop100Count: ensuredTopCountries.touchedCount,
        ensuredTop100ClearedCount: ensuredTopCountries.clearedCodes.length,
        ensuredTop100ClearedCodes: ensuredTopCountries.clearedCodes,
        ensuredTop100InsertedCount: ensuredTopCountries.insertedCodes.length,
        ensuredTop100Preview: ensuredTopCountries.preview.slice(0, 20),
        failureCount: failures.length,
        failures: failures.slice(0, 20),
        invalidatedCacheKeys,
        preview: changes.slice(0, 10).map((result) => ({
          code: result.code,
          previousRank: previousRankByCode.get(result.code),
          currentRank: result.currentRank,
        })),
      });
      return;
    }

    for (const result of results) {
      const previousRank = previousRankByCode.get(result.code);

      if (previousRank && previousRank > 0) {
        await sql`
          INSERT INTO ranking_history (country_id, ranking_date, ranking_category, fifa_ranking, source)
          VALUES (
            (SELECT id FROM countries WHERE code_alpha3 = ${result.code}),
            ${rankingDate},
            ${options.gender},
              ${previousRank},
              ${rankingSource}
            )
          ON CONFLICT (country_id, ranking_date, ranking_category)
          DO NOTHING
        `;
      }

      if (options.gender === 'women') {
        await sql`
          UPDATE countries
          SET fifa_ranking_women = ${result.currentRank}, updated_at = NOW()
          WHERE code_alpha3 = ${result.code}
        `;
      } else {
        await sql`
          UPDATE countries
          SET fifa_ranking = ${result.currentRank}, updated_at = NOW()
          WHERE code_alpha3 = ${result.code}
        `;
      }
    }

    const invalidatedCacheKeys = await invalidateNationCache();

    printSummary({
      dryRun: false,
      gender: options.gender,
      countryCount: countries.length,
      rankingDate,
      updateDates,
      changedCount: changes.length,
      ensuredTop100Count: ensuredTopCountries.touchedCount,
      ensuredTop100ClearedCount: ensuredTopCountries.clearedCodes.length,
      ensuredTop100ClearedCodes: ensuredTopCountries.clearedCodes,
      ensuredTop100InsertedCount: ensuredTopCountries.insertedCodes.length,
      ensuredTop100Preview: ensuredTopCountries.preview.slice(0, 20),
      failureCount: failures.length,
      failures: failures.slice(0, 20),
      invalidatedCacheKeys,
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
