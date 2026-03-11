import { readFile } from 'node:fs/promises';
import path from 'node:path';
import postgres, { type Sql } from 'postgres';
import { resolveNationCodeAlias } from '../src/data/nationCodeAliases.ts';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  dryRun: boolean;
  help: boolean;
  inputPath?: string;
  limit?: number;
  playerSlug?: string;
}

interface NationalitySyncPayload {
  provider: string;
  fetchedAt?: string;
  rows: NationalitySyncRow[];
}

interface NationalitySyncRow {
  nationalities?: string[] | null;
  playerName?: string | null;
  playerSlug?: string | null;
  raw?: unknown;
  sourceUrl?: string | null;
}

interface SourceRow {
  id: number;
}

interface SyncRunRow {
  id: number;
}

interface CountryLookupRow {
  id: number;
  code_alpha2: string | null;
  code_alpha3: string;
  translation_name: string | null;
}

interface CountrySeedDefinition {
  codeAlpha2: string;
  codeAlpha3: string;
  name: string;
}

const COUNTRY_NAME_ALIASES: Record<string, string> = {
  'bosnia herzegovina': 'BIH',
  'cape verde': 'CPV',
  'congo dr': 'COD',
  'cote d ivoire': 'CIV',
  'curacao': 'CUW',
  'dr congo': 'COD',
  'england': 'ENG',
  'ivory coast': 'CIV',
  'korea republic': 'KOR',
  'korea south': 'KOR',
  'north ireland': 'NIR',
  'north macedonia': 'MKD',
  'republic of ireland': 'IRL',
  'scotland': 'SCO',
  'south korea': 'KOR',
  'syria': 'SYR',
  'the gambia': 'GAM',
  'trinidad tobago': 'TRI',
  'u s a': 'USA',
  'united states': 'USA',
  'united states of america': 'USA',
  'wales': 'WAL',
};

const COUNTRY_SEED_DEFINITIONS: Record<string, CountrySeedDefinition> = {
  armenia: { codeAlpha2: 'AM', codeAlpha3: 'ARM', name: 'Armenia' },
  bhutan: { codeAlpha2: 'BT', codeAlpha3: 'BTN', name: 'Bhutan' },
  chad: { codeAlpha2: 'TD', codeAlpha3: 'TCD', name: 'Chad' },
  comoros: { codeAlpha2: 'KM', codeAlpha3: 'COM', name: 'Comoros' },
  congo: { codeAlpha2: 'CG', codeAlpha3: 'COG', name: 'Congo' },
  'central african republic': { codeAlpha2: 'CF', codeAlpha3: 'CAF', name: 'Central African Republic' },
  'dominican republic': { codeAlpha2: 'DO', codeAlpha3: 'DOM', name: 'Dominican Republic' },
  eritrea: { codeAlpha2: 'ER', codeAlpha3: 'ERI', name: 'Eritrea' },
  estonia: { codeAlpha2: 'EE', codeAlpha3: 'EST', name: 'Estonia' },
  'french guiana': { codeAlpha2: 'GF', codeAlpha3: 'GUF', name: 'French Guiana' },
  gibraltar: { codeAlpha2: 'GI', codeAlpha3: 'GIB', name: 'Gibraltar' },
  kazakhstan: { codeAlpha2: 'KZ', codeAlpha3: 'KAZ', name: 'Kazakhstan' },
  kenya: { codeAlpha2: 'KE', codeAlpha3: 'KEN', name: 'Kenya' },
  kyrgyzstan: { codeAlpha2: 'KG', codeAlpha3: 'KGZ', name: 'Kyrgyzstan' },
  laos: { codeAlpha2: 'LA', codeAlpha3: 'LAO', name: 'Laos' },
  latvia: { codeAlpha2: 'LV', codeAlpha3: 'LVA', name: 'Latvia' },
  liechtenstein: { codeAlpha2: 'LI', codeAlpha3: 'LIE', name: 'Liechtenstein' },
  lithuania: { codeAlpha2: 'LT', codeAlpha3: 'LTU', name: 'Lithuania' },
  luxembourg: { codeAlpha2: 'LU', codeAlpha3: 'LUX', name: 'Luxembourg' },
  madagascar: { codeAlpha2: 'MG', codeAlpha3: 'MDG', name: 'Madagascar' },
  moldova: { codeAlpha2: 'MD', codeAlpha3: 'MDA', name: 'Moldova' },
  'san marino': { codeAlpha2: 'SM', codeAlpha3: 'SMR', name: 'San Marino' },
  'sierra leone': { codeAlpha2: 'SL', codeAlpha3: 'SLE', name: 'Sierra Leone' },
  suriname: { codeAlpha2: 'SR', codeAlpha3: 'SUR', name: 'Suriname' },
  togo: { codeAlpha2: 'TG', codeAlpha3: 'TGO', name: 'Togo' },
  zimbabwe: { codeAlpha2: 'ZW', codeAlpha3: 'ZWE', name: 'Zimbabwe' },
};

function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { dryRun: false, help: false };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--input=')) {
      options.inputPath = arg.slice('--input='.length).trim();
      continue;
    }

    if (arg.startsWith('--player=')) {
      options.playerSlug = arg.slice('--player='.length).trim();
      continue;
    }

    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-player-nationalities.mts --input=<path> [options]

Options:
  --input=<path>        JSON file produced by fetch-player-contracts-transfermarkt.py
  --player=<slug>       Restrict sync to one player slug
  --limit=<n>           Limit rows read from the input payload
  --dry-run             Preview updates without writing to the database
  --help, -h            Show this help message

Environment:
  DATABASE_URL
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

function resolvePathLike(filePath: string) {
  return path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
}

async function readPayload(inputPath: string) {
  return JSON.parse(await readFile(resolvePathLike(inputPath), 'utf8')) as NationalitySyncPayload;
}

function normalizeKey(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

async function ensureDataSource(sql: Sql, provider: string) {
  const sourceName = provider === 'transfermarkt' ? 'Transfermarkt Nationality Backfill' : `${provider} Nationality Backfill`;
  const baseUrl = provider === 'transfermarkt' ? 'https://www.transfermarkt.com' : null;
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
    VALUES (${`${provider}_nationality_backfill`}, ${sourceName}, ${baseUrl}, 'scraper', 'nationality_sync', 3)
    ON CONFLICT (slug)
    DO UPDATE SET
      name = EXCLUDED.name,
      base_url = EXCLUDED.base_url,
      source_kind = EXCLUDED.source_kind,
      upstream_ref = EXCLUDED.upstream_ref,
      priority = EXCLUDED.priority
    RETURNING id
  `;
  return rows[0].id;
}

async function createSyncRun(sql: Sql, sourceId: number, metadata: Record<string, unknown>) {
  const rows = await sql<SyncRunRow[]>`
    INSERT INTO source_sync_runs (source_id, upstream_ref, status, metadata)
    VALUES (${sourceId}, 'player_nationality', 'running', ${JSON.stringify(metadata)}::jsonb)
    RETURNING id
  `;
  return rows[0].id;
}

async function finishSyncRun(sql: Sql, syncRunId: number, status: 'completed' | 'failed', summary: Record<string, unknown>) {
  await sql`
    UPDATE source_sync_runs
    SET
      status = ${status},
      fetched_files = 1,
      changed_files = ${Number(summary.updated ?? 0)},
      completed_at = NOW(),
      metadata = COALESCE(metadata, '{}'::jsonb) || ${JSON.stringify(summary)}::jsonb
    WHERE id = ${syncRunId}
  `;
}

async function insertRawPayload(
  sql: Sql,
  params: {
    payload: unknown;
    playerSlug: string;
    sourceId: number;
    syncRunId: number;
  }
) {
  await sql`
    INSERT INTO raw_payloads (source_id, sync_run_id, endpoint, entity_type, external_id, http_status, payload)
    VALUES (${params.sourceId}, ${params.syncRunId}, 'player_nationality', 'player', ${params.playerSlug}, 200, ${JSON.stringify(params.payload)}::jsonb)
  `;
}

async function loadCountryRows(sql: Sql) {
  return sql<CountryLookupRow[]>`
    SELECT
      c.id,
      c.code_alpha2,
      c.code_alpha3,
      ct.name AS translation_name
    FROM countries c
    LEFT JOIN country_translations ct ON ct.country_id = c.id
  `;
}

async function ensureCountrySeed(sql: Sql, definition: CountrySeedDefinition) {
  const rows = await sql<Array<{ id: number }>>`
    INSERT INTO countries (code_alpha2, code_alpha3, is_active, updated_at)
    VALUES (${definition.codeAlpha2}, ${definition.codeAlpha3}, TRUE, NOW())
    ON CONFLICT (code_alpha3)
    DO UPDATE SET
      code_alpha2 = COALESCE(countries.code_alpha2, EXCLUDED.code_alpha2),
      updated_at = NOW()
    RETURNING id
  `;

  await sql`
    INSERT INTO country_translations (country_id, locale, name)
    VALUES (${rows[0].id}, 'en', ${definition.name})
    ON CONFLICT (country_id, locale)
    DO UPDATE SET name = COALESCE(country_translations.name, EXCLUDED.name)
  `;

  return rows[0].id;
}

function buildCountryResolver(rows: CountryLookupRow[]) {
  const countryIdByKey = new Map<string, number>();
  const countryIdByCode = new Map<string, number>();
  const regionNames = new Intl.DisplayNames(['en'], { type: 'region' });

  for (const row of rows) {
    countryIdByCode.set(resolveNationCodeAlias(row.code_alpha3), row.id);

    const displayName = row.code_alpha2 ? regionNames.of(row.code_alpha2.toUpperCase()) : undefined;

    for (const candidate of [row.code_alpha2, row.code_alpha3, row.translation_name, displayName]) {
      if (!candidate) {
        continue;
      }
      countryIdByKey.set(normalizeKey(candidate), row.id);
    }
  }

  for (const [alias, code] of Object.entries(COUNTRY_NAME_ALIASES)) {
    const resolvedId = countryIdByCode.get(resolveNationCodeAlias(code));
    if (resolvedId) {
      countryIdByKey.set(alias, resolvedId);
    }
  }

  return {
    register(row: CountryLookupRow) {
      countryIdByCode.set(resolveNationCodeAlias(row.code_alpha3), row.id);

      const displayName = row.code_alpha2 ? regionNames.of(row.code_alpha2.toUpperCase()) : undefined;
      for (const candidate of [row.code_alpha2, row.code_alpha3, row.translation_name, displayName]) {
        if (!candidate) {
          continue;
        }
        countryIdByKey.set(normalizeKey(candidate), row.id);
      }
    },
    resolve(nationalities: string[]) {
      for (const nationality of nationalities) {
        const normalized = normalizeKey(nationality);
        if (!normalized) {
          continue;
        }

        const directMatch = countryIdByKey.get(normalized);
        if (directMatch) {
          return directMatch;
        }

        const aliasCode = COUNTRY_NAME_ALIASES[normalized];
        if (aliasCode) {
          const aliasMatch = countryIdByCode.get(resolveNationCodeAlias(aliasCode));
          if (aliasMatch) {
            return aliasMatch;
          }
        }
      }

      return undefined;
    },
  };
}

async function ensureCountryIdForNationalities(
  sql: Sql,
  resolver: ReturnType<typeof buildCountryResolver>,
  nationalities: string[],
) {
  const existing = resolver.resolve(nationalities);
  if (existing) {
    return existing;
  }

  for (const nationality of nationalities) {
    const definition = COUNTRY_SEED_DEFINITIONS[normalizeKey(nationality)];
    if (!definition) {
      continue;
    }

    const id = await ensureCountrySeed(sql, definition);
    resolver.register({
      id,
      code_alpha2: definition.codeAlpha2,
      code_alpha3: definition.codeAlpha3,
      translation_name: definition.name,
    });
    return id;
  }

  return undefined;
}

async function updatePlayerCountry(sql: Sql, playerSlug: string, countryId: number) {
  const rows = await sql<Array<{ updated: boolean }>>`
    UPDATE players p
    SET
      country_id = COALESCE(p.country_id, ${countryId}),
      updated_at = CASE WHEN p.country_id IS NULL THEN NOW() ELSE p.updated_at END
    WHERE p.slug = ${playerSlug}
    RETURNING p.country_id IS NULL AS updated
  `;

  return rows[0]?.updated ?? false;
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }
  if (!options.inputPath) {
    throw new Error('--input is required');
  }

  const payload = await readPayload(options.inputPath);
  const rows = (options.limit ? payload.rows.slice(0, options.limit) : payload.rows)
    .filter((row) => !options.playerSlug || row.playerSlug === options.playerSlug);
  const sql = getSql();

  let syncRunId: number | null = null;
  try {
    const resolver = buildCountryResolver(await loadCountryRows(sql));
    const sourceId = options.dryRun ? null : await ensureDataSource(sql, payload.provider.trim().toLowerCase());
    if (!options.dryRun && sourceId !== null) {
      syncRunId = await createSyncRun(sql, sourceId, {
        fetchedAt: payload.fetchedAt ?? null,
        inputPath: resolvePathLike(options.inputPath),
        provider: payload.provider,
      });
    }

    let updated = 0;
    let skipped = 0;
    let unresolved = 0;

    for (const row of rows) {
      const playerSlug = row.playerSlug?.trim();
      const nationalities = row.nationalities?.map((value) => value.trim()).filter(Boolean) ?? [];
      if (!playerSlug || nationalities.length === 0) {
        skipped += 1;
        continue;
      }

      const countryId = await ensureCountryIdForNationalities(sql, resolver, nationalities);
      if (!countryId) {
        unresolved += 1;
        continue;
      }

      if (!options.dryRun) {
        const changed = await updatePlayerCountry(sql, playerSlug, countryId);
        if (changed && sourceId !== null && syncRunId !== null) {
          await insertRawPayload(sql, {
            payload: row.raw ?? row,
            playerSlug,
            sourceId,
            syncRunId,
          });
        }
      }

      updated += 1;
    }

    const summary = { provider: payload.provider, rows: rows.length, skipped, unresolved, updated };
    if (!options.dryRun && syncRunId !== null) {
      await finishSyncRun(sql, syncRunId, 'completed', summary);
    }
    console.log(JSON.stringify(summary, null, 2));
  } catch (error) {
    if (syncRunId !== null) {
      await finishSyncRun(sql, syncRunId, 'failed', { error: error instanceof Error ? error.message : String(error) });
    }
    throw error;
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
