import { loadProjectEnv } from "./load-project-env.mts";
loadProjectEnv();

import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import postgres from 'postgres';

interface CliOptions {
  dryRun: boolean;
  help: boolean;
}

interface LogoIndexEntry {
  categoryId: string;
  categoryName: string;
  id: string;
  name: string;
  altNames?: string[];
  h: string;
  png?: Array<{
    dimension: number;
    sizeBytes: number;
  }>;
}

interface MatchableEntity {
  id: string;
  name: string;
  country?: string;
  shortName?: string;
  code?: string;
  logo?: string;
  flag?: string;
}

const INDEX_URL = 'https://football-logos.cc/ac.json';
const OUTPUT_PATH = resolve(process.cwd(), 'src/data/entityImages.generated.ts');
const PNG_DIMENSION = 700;
const HASH_START_BY_DIMENSION: Record<number, number> = {
  3000: 0,
  1500: 8,
  700: 16,
  512: 24,
  256: 32,
  128: 40,
  64: 48,
};

const CLUB_ID_OVERRIDES: Record<string, string> = {
  bayern: 'bayern-munchen',
  'bayern-ucl': 'bayern-munchen',
  mancity: 'manchester-city',
  manutd: 'manchester-united',
  monaco: 'as-monaco',
  psg: 'paris-saint-germain',
};

const LEAGUE_ID_OVERRIDES: Record<string, string> = {
  pl: 'english-premier-league',
  laliga: 'la-liga',
  championship: 'efl-championship',
  ucl: 'uefa-champions-league',
  uel: 'uefa-europa-league',
  cwc: 'fifa-club-world-cup',
};

const NATION_FLAG_CODE_MAP: Record<string, string> = {
  ENG: 'gb-eng',
  ESP: 'es',
  FRA: 'fr',
  BRA: 'br',
  GER: 'de',
  ITA: 'it',
  NED: 'nl',
  POR: 'pt',
  KSA: 'sa',
};

function parseArgs(argv: string[]): CliOptions {
  return {
    dryRun: argv.includes('--dry-run'),
    help: argv.includes('--help') || argv.includes('-h'),
  };
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-entity-images.mts [options]

Options:
  --dry-run   Preview generated output without writing the file
  --help, -h  Show this help message
`);
}

function normalizeText(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\b(fc|cf|afc|ac|sc|club|football|the)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function escapeString(value: string) {
  return value.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function getAssetUrl(entry: LogoIndexEntry) {
  const hasTargetPng = entry.png?.some((variant) => variant.dimension === PNG_DIMENSION) ?? false;
  if (!hasTargetPng) {
    return null;
  }

  const hashStart = HASH_START_BY_DIMENSION[PNG_DIMENSION];
  if (hashStart === undefined) {
    return null;
  }

  return `https://assets.football-logos.cc/logos/${entry.categoryId}/${PNG_DIMENSION}x${PNG_DIMENSION}/${entry.id}.${entry.h.slice(hashStart, hashStart + 8)}.png`;
}

function getEntityKeys(entity: MatchableEntity) {
  const keys = [entity.name, entity.shortName, entity.code]
    .filter((value): value is string => Boolean(value))
    .map((value) => normalizeText(value));

  return Array.from(new Set(keys.filter(Boolean)));
}

function getEntryKeys(entry: LogoIndexEntry) {
  const values = [entry.id, entry.name, ...(entry.altNames ?? [])];
  return new Set(values.map((value) => normalizeText(value)).filter(Boolean));
}

function scoreEntry(entity: MatchableEntity, entry: LogoIndexEntry, expectedCategory?: string) {
  const entityKeys = getEntityKeys(entity);
  const entryKeys = getEntryKeys(entry);
  let score = 0;

  for (const key of entityKeys) {
    if (entryKeys.has(key)) {
      score += key === normalizeText(entity.name) ? 6 : 4;
    }
  }

  if (score === 0) {
    return 0;
  }

  if (expectedCategory && normalizeText(entry.categoryName) === normalizeText(expectedCategory)) {
    score += 3;
  }

  if (normalizeText(entry.id) === normalizeText(entity.name)) {
    score += 2;
  }

  return score;
}

function findBestEntry(entries: LogoIndexEntry[], entity: MatchableEntity, expectedCategory?: string, overrideId?: string) {
  if (overrideId) {
    return entries.find((entry) => entry.id === overrideId) ?? null;
  }

  const scoredEntries = entries
    .map((entry) => ({ entry, score: scoreEntry(entity, entry, expectedCategory) }))
    .filter((candidate) => candidate.score > 0)
    .sort((a, b) => b.score - a.score || a.entry.name.localeCompare(b.entry.name));

  return scoredEntries[0]?.entry ?? null;
}

async function fetchLogoIndex() {
  const response = await fetch(INDEX_URL, {
    headers: { Accept: 'application/json' },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch logo index: HTTP ${response.status}`);
  }

  return response.json() as Promise<LogoIndexEntry[]>;
}

async function loadBaseData() {
  const [{ baseClubs }, { baseLeagues }, { baseNations }] = await Promise.all([
    import(new URL('../src/data/clubs.ts', import.meta.url).href),
    import(new URL('../src/data/leagues.ts', import.meta.url).href),
    import(new URL('../src/data/nations.ts', import.meta.url).href),
  ]);

  return {
    baseClubs: baseClubs as MatchableEntity[],
    baseLeagues: baseLeagues as MatchableEntity[],
    baseNations: baseNations as MatchableEntity[],
  };
}

async function loadDatabaseData() {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    return null;
  }

  const sql = postgres(databaseUrl, { prepare: false, max: 1, idle_timeout: 5 });

  try {
    const [clubs, leagues, nations] = await Promise.all([
      sql<MatchableEntity[]>`
        SELECT
          t.slug AS id,
          COALESCE(tt.name, t.slug) AS name,
          COALESCE(tt.short_name, tt.name, t.slug) AS "shortName",
          COALESCE(ctr.name, country.code_alpha3) AS country,
          t.crest_url AS logo
        FROM teams t
        JOIN countries country ON country.id = t.country_id
        LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
        LEFT JOIN country_translations ctr ON ctr.country_id = country.id AND ctr.locale = 'en'
        WHERE t.is_national = FALSE
        ORDER BY t.slug ASC
      `,
      sql<MatchableEntity[]>`
        SELECT
          c.slug AS id,
          COALESCE(ct.name, c.slug) AS name,
          COALESCE(ct.short_name, ct.name, c.slug) AS "shortName",
          COALESCE(ctr.name, country.code_alpha3) AS country,
          c.emblem_url AS logo
        FROM competitions c
        LEFT JOIN countries country ON country.id = c.country_id
        LEFT JOIN competition_translations ct ON ct.competition_id = c.id AND ct.locale = 'en'
        LEFT JOIN country_translations ctr ON ctr.country_id = country.id AND ctr.locale = 'en'
        ORDER BY c.slug ASC
      `,
      sql<MatchableEntity[]>`
        SELECT
          LOWER(country.code_alpha3) AS id,
          COALESCE(ctr.name, country.code_alpha3) AS name,
          country.code_alpha3 AS code,
          country.flag_url AS flag
        FROM countries country
        LEFT JOIN country_translations ctr ON ctr.country_id = country.id AND ctr.locale = 'en'
        ORDER BY country.code_alpha3 ASC
      `,
    ]);

    return { clubs, leagues, nations };
  } finally {
    await sql.end({ timeout: 1 });
  }
}

function mergeEntities(primary: MatchableEntity[], secondary: MatchableEntity[]) {
  const merged = new Map<string, MatchableEntity>();

  for (const entity of [...secondary, ...primary]) {
    const existing = merged.get(entity.id);
    merged.set(entity.id, {
      ...existing,
      ...entity,
    });
  }

  return Array.from(merged.values());
}

function buildClubLogoMap(entries: LogoIndexEntry[], baseClubs: MatchableEntity[]) {
  const map: Record<string, string> = {};

  for (const club of baseClubs) {
    const entry = findBestEntry(entries, club, club.country, CLUB_ID_OVERRIDES[club.id]);
    const assetUrl = entry ? getAssetUrl(entry) : null;

    if (assetUrl) {
      map[club.id] = assetUrl;
      continue;
    }

    console.warn(`[entity-images] no club logo match for ${club.id} (${club.name})`);
  }

  return map;
}

function buildLeagueLogoMap(entries: LogoIndexEntry[], baseLeagues: MatchableEntity[]) {
  const map: Record<string, string> = {};

  for (const league of baseLeagues) {
    const entry = findBestEntry(entries, league, league.country, LEAGUE_ID_OVERRIDES[league.id]);
    const assetUrl = entry ? getAssetUrl(entry) : null;

    if (assetUrl) {
      map[league.id] = assetUrl;
      continue;
    }

    console.warn(`[entity-images] no league logo match for ${league.id} (${league.name})`);
  }

  return map;
}

function buildNationFlagMap(baseNations: MatchableEntity[]) {
  const map: Record<string, string> = {};

  for (const nation of baseNations) {
    if (nation.flag) {
      continue;
    }

    if (!nation.code) {
      console.warn(`[entity-images] no nation code for ${nation.id} (${nation.name})`);
      continue;
    }

    const code = NATION_FLAG_CODE_MAP[nation.code];
    if (!code) {
      console.warn(`[entity-images] no flag code mapping for ${nation.id} (${nation.code})`);
      continue;
    }

    map[nation.id] = `https://flagcdn.com/${code}.svg`;
  }

  return map;
}

function formatMap(name: string, values: Record<string, string>) {
  const lines = Object.entries(values)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `  '${escapeString(key)}': '${escapeString(value)}',`);

  return `export const ${name}: Record<string, string> = {\n${lines.join('\n')}\n};`;
}

function buildOutputFile(clubLogoMap: Record<string, string>, leagueLogoMap: Record<string, string>, nationFlagMap: Record<string, string>) {
  return [
    '// This file is generated by scripts/sync-entity-images.mts.',
    '// Do not edit manually.',
    '',
    formatMap('clubLogoMap', clubLogoMap),
    '',
    formatMap('leagueLogoMap', leagueLogoMap),
    '',
    formatMap('nationFlagMap', nationFlagMap),
    '',
  ].join('\n');
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const databaseData = await loadDatabaseData();
  const baseData = databaseData ? null : await loadBaseData();
  const clubs = databaseData
    ? databaseData.clubs
    : (baseData?.baseClubs ?? []);
  const leagues = databaseData
    ? databaseData.leagues
    : (baseData?.baseLeagues ?? []);
  const nations = databaseData
    ? databaseData.nations
    : (baseData?.baseNations ?? []);
  const entries = await fetchLogoIndex();
  const clubLogoMap = buildClubLogoMap(entries, clubs);
  const leagueLogoMap = buildLeagueLogoMap(entries, leagues);
  const nationFlagMap = buildNationFlagMap(nations);
  const output = buildOutputFile(clubLogoMap, leagueLogoMap, nationFlagMap);

  console.log(
    `[entity-images] clubs=${Object.keys(clubLogoMap).length} leagues=${Object.keys(leagueLogoMap).length} nations=${Object.keys(nationFlagMap).length}`
  );

  if (options.dryRun) {
    console.log(output);
    return;
  }

  await writeFile(OUTPUT_PATH, output, 'utf8');
  console.log(`[entity-images] wrote ${OUTPUT_PATH}`);
}

await main();
