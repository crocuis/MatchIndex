import Redis from 'ioredis';
import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { loadCountryCodeResolver } from '../src/data/countryCodeResolver.ts';
import { COMPETITION_NAMES_KO, COUNTRY_TRANSLATIONS, TEAM_NAMES_KO } from './ko-localization-data.mts';

interface CliOptions {
  dryRun: boolean;
  help: boolean;
}

interface CompetitionGapRow {
  en_name: string;
  en_short_name: string;
  ko_name: string | null;
  ko_short_name: string | null;
  slug: string;
}

interface TeamGapRow {
  en_name: string;
  en_short_name: string;
  ko_name: string | null;
  ko_short_name: string | null;
  slug: string;
}

interface CountryGapRow {
  code_alpha3: string;
  en_name: string;
  ko_name: string | null;
}

interface SummaryEntry {
  id: string;
  before?: string | null;
  after: string;
}

const GOOGLE_TRANSLATE_URL = 'https://translate.googleapis.com/translate_a/single';
const TARGET_LOCALE = 'ko';

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: false,
    help: false,
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/repair-tab-localizations.mts [options]

Options:
  --dry-run   Detect and preview localization repairs without writing
  --help, -h  Show this help message
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

function containsLatin(value: string | null | undefined) {
  return /[A-Za-z]/.test(value ?? '');
}

function hasLocalizedGap(localized: string | null | undefined, english: string) {
  const trimmed = localized?.trim();
  if (!trimmed) {
    return true;
  }

  if (/^[A-Z0-9.&/+' -]{1,8}$/.test(trimmed) && trimmed === english.trim()) {
    return false;
  }

  return containsLatin(trimmed) && trimmed.toLowerCase() === english.trim().toLowerCase();
}

function shouldTranslateShortName(value: string | null | undefined) {
  const trimmed = value?.trim();
  if (!trimmed) {
    return false;
  }

  return !/^[A-Z0-9.&/+' -]{1,6}$/.test(trimmed);
}

function normalizeTranslatedText(value: string) {
  return value
    .replace(/\s+/g, ' ')
    .replace(/\s+([&/+-])/g, '$1')
    .replace(/([&/+-])\s+/g, '$1')
    .trim();
}

function parseTranslatedText(payload: unknown) {
  if (!Array.isArray(payload) || !Array.isArray(payload[0])) {
    return null;
  }

  const parts = payload[0]
    .filter((entry): entry is unknown[] => Array.isArray(entry))
    .map((entry) => typeof entry[0] === 'string' ? entry[0] : '')
    .join('');

  const translated = normalizeTranslatedText(parts);
  return translated || null;
}

async function translateText(text: string, cache: Map<string, string>) {
  const normalized = text.trim();
  if (!normalized) {
    return normalized;
  }

  const cached = cache.get(normalized);
  if (cached) {
    return cached;
  }

  const url = new URL(GOOGLE_TRANSLATE_URL);
  url.searchParams.set('client', 'gtx');
  url.searchParams.set('sl', 'auto');
  url.searchParams.set('tl', TARGET_LOCALE);
  url.searchParams.set('dt', 't');
  url.searchParams.set('q', normalized);

  const response = await fetch(url, {
    headers: { Accept: 'application/json' },
    redirect: 'follow',
  });

  if (!response.ok) {
    throw new Error(`translate failed for "${normalized}": HTTP ${response.status}`);
  }

  const translated = parseTranslatedText(await response.json()) ?? normalized;
  cache.set(normalized, translated);
  return translated;
}

async function mapWithConcurrency<T, R>(items: T[], limit: number, mapper: (item: T) => Promise<R>) {
  const results: R[] = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await mapper(items[currentIndex]);
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, () => worker()));
  return results;
}

async function getCompetitionGaps(sql: postgres.Sql<Record<string, never>>) {
  return sql<CompetitionGapRow[]>`
    WITH latest_competition_seasons AS (
      SELECT DISTINCT ON (cs.competition_id)
        cs.competition_id
      FROM competition_seasons cs
      JOIN seasons s ON s.id = cs.season_id
      ORDER BY cs.competition_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
    )
    SELECT
      c.slug,
      COALESCE(
        (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
        c.slug
      ) AS en_name,
      COALESCE(
        (SELECT ct.short_name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
        (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
        c.slug
      ) AS en_short_name,
      (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = ${TARGET_LOCALE}) AS ko_name,
      (SELECT ct.short_name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = ${TARGET_LOCALE}) AS ko_short_name
    FROM competitions c
    JOIN latest_competition_seasons lcs ON lcs.competition_id = c.id
    ORDER BY c.slug ASC
  `;
}

async function getTeamGaps(sql: postgres.Sql<Record<string, never>>) {
  return sql<TeamGapRow[]>`
    WITH latest_team_seasons AS (
      SELECT DISTINCT ON (ts.team_id)
        ts.team_id
      FROM team_seasons ts
      JOIN competition_seasons cs ON cs.id = ts.competition_season_id
      JOIN seasons s ON s.id = cs.season_id
      ORDER BY ts.team_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, cs.id DESC
    )
    SELECT
      t.slug,
      COALESCE(
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
        t.slug
      ) AS en_name,
      COALESCE(
        (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
        t.slug
      ) AS en_short_name,
      (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${TARGET_LOCALE}) AS ko_name,
      (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${TARGET_LOCALE}) AS ko_short_name
    FROM teams t
    JOIN latest_team_seasons lts ON lts.team_id = t.id
    ORDER BY t.slug ASC
  `;
}

async function getCountryGaps(sql: postgres.Sql<Record<string, never>>) {
  return sql<CountryGapRow[]>`
    SELECT
      c.code_alpha3,
      COALESCE(
        (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'en'),
        c.code_alpha3
      ) AS en_name,
      (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = ${TARGET_LOCALE}) AS ko_name
    FROM countries c
    WHERE c.is_active = TRUE
      AND (
        COALESCE(c.confederation, '') <> ''
        OR (c.fifa_ranking IS NOT NULL AND c.fifa_ranking > 0)
        OR (c.fifa_ranking_women IS NOT NULL AND c.fifa_ranking_women > 0)
        OR c.flag_url IS NOT NULL
        OR c.crest_url IS NOT NULL
      )
    ORDER BY c.code_alpha3 ASC
  `;
}

async function invalidateCaches() {
  const redis = getRedis();
  if (!redis) {
    return 0;
  }

  try {
    await redis.connect();
    return redis.del(
      'leagues:locale:ko',
      'clubs:locale:ko',
      'nations:locale:ko',
      'nations-women:locale:ko',
    );
  } catch {
    return 0;
  } finally {
    redis.disconnect();
  }
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

    const sql = getSql();
    const countryCodeResolver = await loadCountryCodeResolver(sql);
    const translationCache = new Map<string, string>();

  try {
    const [competitionRows, teamRows, countryRows] = await Promise.all([
      getCompetitionGaps(sql),
      getTeamGaps(sql),
      getCountryGaps(sql),
    ]);

    const competitionTargets = competitionRows.filter((row) => hasLocalizedGap(row.ko_name, row.en_name) || !row.ko_short_name?.trim());
    const teamTargets = teamRows.filter((row) => hasLocalizedGap(row.ko_name, row.en_name) || !row.ko_short_name?.trim());
    const countryTargets = countryRows.filter((row) => hasLocalizedGap(row.ko_name, row.en_name));

    const competitionRepairs = await mapWithConcurrency(competitionTargets, 4, async (row) => {
      const manual = COMPETITION_NAMES_KO[row.slug];
      const name = manual?.name ?? await translateText(row.en_name, translationCache);
      const shortName = manual?.shortName
        ?? (shouldTranslateShortName(row.en_short_name)
          ? await translateText(row.en_short_name, translationCache)
          : row.en_short_name);

      return { row, name, shortName };
    });

    const teamRepairs = await mapWithConcurrency(teamTargets, 6, async (row) => {
      const manual = TEAM_NAMES_KO[row.slug];
      const name = manual?.name ?? await translateText(row.en_name, translationCache);
      const shortName = manual?.shortName
        ?? (shouldTranslateShortName(row.en_short_name)
          ? await translateText(row.en_short_name, translationCache)
          : row.en_short_name);

      return { row, name, shortName };
    });

    const countryRepairs = await mapWithConcurrency(countryTargets, 6, async (row) => {
      const canonicalCode = countryCodeResolver.resolve(row.code_alpha3) ?? row.code_alpha3;
      const manual = COUNTRY_TRANSLATIONS[canonicalCode] ?? COUNTRY_TRANSLATIONS[row.code_alpha3];
      const name = manual?.ko ?? await translateText(row.en_name, translationCache);
      return { row, name };
    });

    if (!options.dryRun) {
      for (const repair of competitionRepairs) {
        await sql`
          INSERT INTO competition_translations (competition_id, locale, name, short_name)
          VALUES ((SELECT id FROM competitions WHERE slug = ${repair.row.slug}), ${TARGET_LOCALE}, ${repair.name}, ${repair.shortName})
          ON CONFLICT (competition_id, locale)
          DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
        `;
      }

      for (const repair of teamRepairs) {
        await sql`
          INSERT INTO team_translations (team_id, locale, name, short_name)
          VALUES ((SELECT id FROM teams WHERE slug = ${repair.row.slug}), ${TARGET_LOCALE}, ${repair.name}, ${repair.shortName})
          ON CONFLICT (team_id, locale)
          DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
        `;
      }

      for (const repair of countryRepairs) {
        await sql`
          INSERT INTO country_translations (country_id, locale, name)
          VALUES ((SELECT id FROM countries WHERE code_alpha3 = ${repair.row.code_alpha3}), ${TARGET_LOCALE}, ${repair.name})
          ON CONFLICT (country_id, locale)
          DO UPDATE SET name = EXCLUDED.name
        `;
      }
    }

    const invalidatedCacheKeys = !options.dryRun && (competitionRepairs.length > 0 || teamRepairs.length > 0 || countryRepairs.length > 0)
      ? await invalidateCaches()
      : 0;

    console.log(JSON.stringify({
      dryRun: options.dryRun,
      invalidatedCacheKeys,
      repaired: {
        competitions: competitionRepairs.length,
        teams: teamRepairs.length,
        countries: countryRepairs.length,
      },
      preview: {
        competitions: competitionRepairs.slice(0, 10).map<SummaryEntry>((repair) => ({
          id: repair.row.slug,
          before: repair.row.ko_name,
          after: repair.name,
        })),
        teams: teamRepairs.slice(0, 10).map<SummaryEntry>((repair) => ({
          id: repair.row.slug,
          before: repair.row.ko_name,
          after: repair.name,
        })),
        countries: countryRepairs.slice(0, 10).map<SummaryEntry>((repair) => ({
          id: repair.row.code_alpha3,
          before: repair.row.ko_name,
          after: repair.name,
        })),
      },
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
