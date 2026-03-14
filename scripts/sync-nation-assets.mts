import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { loadCountryCodeResolver } from '../src/data/countryCodeResolver.ts';
import { getNationBadgeUrl, getNationFlagUrl } from '../src/data/nationVisuals.ts';

interface CountryRow {
  code_alpha3: string;
  name: string;
}

interface CliOptions {
  dryRun: boolean;
  help: boolean;
  limit?: number;
  onlyMissing: boolean;
}

interface LogoIndexEntry {
  categoryId: string;
  categoryName: string;
  id: string;
  name: string;
  altNames?: string[];
  h: string;
  png?: Array<{ dimension: number; sizeBytes: number }>;
}

const MIN_LOGO_SCORE = 6;

const INDEX_URL = 'https://football-logos.cc/ac.json';
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

const NATION_LOGO_ID_OVERRIDES: Record<string, string> = {
  CIV: 'ivory-coast-national-team',
  CPV: 'cape-verde-national-team',
  KOR: 'south-korea-national-team',
  USA: 'usa-national-team',
};

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
    help: false,
    onlyMissing: false,
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg === '--only-missing') {
      options.onlyMissing = true;
      continue;
    }

    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-nation-assets.mts [options]

Options:
  --dry-run   Preview nation asset updates without writing to the database
  --only-missing Update only countries with missing crest_url
  --limit=<n> Limit number of countries to process
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
    .replace(/\b(national|team|football|association|federation|republic|kingdom|the)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
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

function getEntryKeys(entry: LogoIndexEntry) {
  const values = [entry.categoryId, entry.id, entry.name, ...(entry.altNames ?? [])];
  return new Set(values.map((value) => normalizeText(value)).filter(Boolean));
}

function scoreEntry(code: string, name: string, entry: LogoIndexEntry) {
  const targetKeys = [normalizeText(name), normalizeText(`${name} national team`)];
  const entryKeys = getEntryKeys(entry);
  let score = 0;

  for (const key of targetKeys) {
    if (entryKeys.has(key)) {
      score += key === normalizeText(name) ? 6 : 4;
    }
  }

  const normalizedCategory = normalizeText(entry.categoryName);
  if (normalizedCategory === normalizeText(name)) {
    score += 4;
  }

  if (entry.id.endsWith('-national-team')) {
    score += 1;
  }

  if (code === 'ENG' && entry.id === 'england-national-team') score += 10;
  if (code === 'SCO' && entry.id === 'scotland-national-team') score += 10;
  if (code === 'RSA' && entry.id === 'south-africa-national-team') score += 8;
  if (code === 'NZL' && entry.id === 'new-zealand-national-team') score += 8;

  return score;
}

function findBestEntry(entries: LogoIndexEntry[], code: string, name: string) {
  const overrideId = NATION_LOGO_ID_OVERRIDES[code];
  if (overrideId) {
    const exact = entries.find((entry) => entry.id === overrideId);
    if (exact) {
      return exact;
    }
  }

  const candidates = entries
    .filter((entry) => entry.id.endsWith('-national-team'))
    .map((entry) => ({ entry, score: scoreEntry(code, name, entry) }))
    .filter((candidate) => candidate.score >= MIN_LOGO_SCORE)
    .sort((left, right) => right.score - left.score || left.entry.name.localeCompare(right.entry.name));

  return candidates[0]?.entry ?? null;
}

async function fetchLogoIndex() {
  const response = await fetch(INDEX_URL, { headers: { Accept: 'application/json' } });

  if (!response.ok) {
    throw new Error(`Failed to fetch logo index: HTTP ${response.status}`);
  }

  return response.json() as Promise<LogoIndexEntry[]>;
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

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const [entries, sql] = await Promise.all([fetchLogoIndex(), Promise.resolve(getSql())]);

  try {
    const countryCodeResolver = await loadCountryCodeResolver(sql);
    const rows = await sql<CountryRow[]>`
      SELECT
        c.code_alpha3,
        COALESCE(
          (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'en'),
          c.code_alpha3
        ) AS name
      FROM countries c
      WHERE c.is_active = TRUE
      ORDER BY c.code_alpha3 ASC
    `;

    const countries = rows
      .filter((row) => /^[A-Z]{3}$/.test(row.code_alpha3))
      .slice(0, options.limit ?? rows.length);
    const preview = [] as Array<Record<string, string | null>>;

    for (const country of countries) {
      const sourceCode = countryCodeResolver.resolve(country.code_alpha3) ?? country.code_alpha3;
      const skipCrestSync = countryCodeResolver.isSkipped(sourceCode);

      if (options.onlyMissing) {
        const [current] = await sql<Array<{ crest_url: string | null }>>`
          SELECT crest_url
          FROM countries
          WHERE code_alpha3 = ${country.code_alpha3}
        `;

        if (current?.crest_url) {
          continue;
        }
      }

      const flagUrl = getNationFlagUrl(sourceCode) ?? null;
      const logoEntry = findBestEntry(entries, sourceCode, country.name);
      const crestUrl = skipCrestSync
        ? flagUrl
        : (logoEntry ? getAssetUrl(logoEntry) : null) ?? getNationBadgeUrl(sourceCode) ?? null;

      preview.push({
        code: country.code_alpha3,
        name: country.name,
        flagUrl,
        crestUrl,
      });

      if (options.dryRun) {
        continue;
      }

      await sql`
        UPDATE countries
        SET flag_url = ${flagUrl}, crest_url = ${crestUrl}, updated_at = NOW()
        WHERE code_alpha3 = ${country.code_alpha3}
      `;
    }

    console.log(JSON.stringify({
      dryRun: options.dryRun,
      countriesProcessed: countries.length,
      preview: preview.slice(0, 12),
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
