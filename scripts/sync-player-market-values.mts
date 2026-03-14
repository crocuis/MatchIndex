import { readFile } from 'node:fs/promises';
import path from 'node:path';
import postgres, { type Sql } from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  dryRun: boolean;
  help: boolean;
  inputPath?: string;
  playerSlug?: string;
}

interface MarketValuePayload {
  playerExternalId?: string | null;
  provider: string;
  rows: Array<{
    age?: number | null;
    clubExternalId?: string | null;
    clubName?: string | null;
    currencyCode?: string | null;
    marketValue?: number | null;
    observedAt?: string | null;
    playerExternalId?: string | null;
    raw?: unknown;
    sourceUrl?: string | null;
  }>;
}

interface PlayerRow { id: number; }
interface SourceRow { id: number; }
interface SeasonRow { id: number; slug: string; }
interface TeamMappingRow { entity_id: number; external_id: string; name: string | null; short_name: string | null; slug: string; }
interface TeamLookupRow { id: number; name: string | null; short_name: string | null; slug: string; }

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
    }
  }
  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-player-market-values.mts --input=<path> --player=<slug> [options]

Options:
  --input=<path>     JSON payload produced by fetch-player-market-values-transfermarkt.mts
  --player=<slug>    Internal player slug
  --dry-run          Preview matched updates without writing
  --help, -h         Show this help message
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });
}

function resolvePath(filePath: string) {
  return path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
}

async function readPayload(filePath: string) {
  return JSON.parse(await readFile(resolvePath(filePath), 'utf8')) as MarketValuePayload;
}

function normalizeText(value: string | null | undefined) {
  return value
    ?.normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .toLowerCase() ?? '';
}

function buildTeamKeys(value: string | null | undefined) {
  const normalized = normalizeText(value);
  if (!normalized) {
    return [] as string[];
  }

  const compact = normalized
    .replace(/\b(fc|cf|afc|cfc|sc|ac|club|football|futbol|clube|club de futebol)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  return [...new Set([normalized, compact].filter(Boolean))];
}

async function ensureSource(sql: Sql, provider: string) {
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
    VALUES (${provider}, 'Transfermarkt Player Profiles', 'https://www.transfermarkt.com', 'scraper', 'tmapi', 3)
    ON CONFLICT (slug)
    DO UPDATE SET name = EXCLUDED.name, base_url = EXCLUDED.base_url, source_kind = EXCLUDED.source_kind, upstream_ref = EXCLUDED.upstream_ref, priority = EXCLUDED.priority
    RETURNING id
  `;
  return rows[0].id;
}

async function getPlayerId(sql: Sql, playerSlug: string) {
  const rows = await sql<PlayerRow[]>`SELECT id FROM players WHERE slug = ${playerSlug} LIMIT 1`;
  return rows[0]?.id;
}

async function loadSeasonRows(sql: Sql) {
  return sql<SeasonRow[]>`SELECT id, slug FROM seasons ORDER BY start_date ASC`;
}

async function loadTeamMappings(sql: Sql, sourceId: number) {
  return sql<TeamMappingRow[]>`
    SELECT
      sem.entity_id,
      sem.external_id,
      t.slug,
      tt.name,
      tt.short_name
    FROM source_entity_mapping sem
    JOIN teams t ON t.id = sem.entity_id
    LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
    WHERE sem.entity_type = 'team'
      AND sem.source_id = ${sourceId}
  `;
}

async function loadTeams(sql: Sql) {
  return sql<TeamLookupRow[]>`
    SELECT
      t.id,
      t.slug,
      tt.name,
      tt.short_name
    FROM teams t
    LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
  `;
}

function resolveSeasonId(rows: SeasonRow[], observedAt: string | null | undefined) {
  if (!observedAt) {
    return { seasonId: undefined, seasonLabel: undefined };
  }

  const observed = new Date(`${observedAt}T00:00:00Z`).getTime();
  for (const row of rows) {
    const slugMatch = row.slug.match(/^(\d{4})\/(\d{2})$/);
    if (!slugMatch) {
      continue;
    }
    const start = Date.UTC(Number.parseInt(slugMatch[1], 10), 6, 1);
    const end = Date.UTC(Number.parseInt(slugMatch[1], 10) + 1, 6, 1);
    if (observed >= start && observed < end) {
      return { seasonId: row.id, seasonLabel: row.slug };
    }
  }

  const year = Number.parseInt(observedAt.slice(0, 4), 10);
  if (Number.isFinite(year)) {
    const label = `${year}/${String((year + 1) % 100).padStart(2, '0')}`;
    const matched = rows.find((row) => row.slug === label);
    return { seasonId: matched?.id, seasonLabel: matched?.slug ?? label };
  }

  return { seasonId: undefined, seasonLabel: undefined };
}

function buildTeamResolver(rows: TeamMappingRow[], teams: TeamLookupRow[]) {
  const byExternalId = new Map(rows.map((row) => [row.external_id, row.entity_id]));
  const byName = new Map<string, number>();
  for (const row of [...rows, ...teams.map((team) => ({ entity_id: team.id, external_id: '', name: team.name, short_name: team.short_name, slug: team.slug }))]) {
    for (const candidate of [row.name, row.short_name, row.slug]) {
      for (const key of buildTeamKeys(candidate)) {
        if (!byName.has(key)) {
          byName.set(key, row.entity_id);
        }
      }
    }
  }

  return {
    resolve(clubExternalId?: string | null, clubName?: string | null) {
      if (clubExternalId && byExternalId.has(clubExternalId)) {
        return byExternalId.get(clubExternalId);
      }
      for (const key of buildTeamKeys(clubName)) {
        const match = byName.get(key);
        if (match) {
          return match;
        }
      }

      return undefined;
    },
  };
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }
  if (!options.inputPath || !options.playerSlug) {
    throw new Error('--input and --player are required');
  }

  const payload = await readPayload(options.inputPath);
  const provider = payload.provider.trim().toLowerCase();
  const sql = getSql();

  try {
    const playerId = await getPlayerId(sql, options.playerSlug);
    if (!playerId) {
      throw new Error(`Player not found: ${options.playerSlug}`);
    }

    const sourceId = await ensureSource(sql, provider);
    const seasonRows = await loadSeasonRows(sql);
    const teamResolver = buildTeamResolver(await loadTeamMappings(sql, sourceId), await loadTeams(sql));

    let updated = 0;
    for (const row of payload.rows) {
      if (!row.observedAt || row.marketValue === null || row.marketValue === undefined) {
        continue;
      }

      const { seasonId, seasonLabel } = resolveSeasonId(seasonRows, row.observedAt);
      const clubId = teamResolver.resolve(row.clubExternalId, row.clubName);
      updated += 1;

      if (options.dryRun) {
        continue;
      }

      await sql`
        INSERT INTO player_market_values (
          player_id, source_id, season_id, season_label, club_id, club_name,
          external_player_id, external_club_id, observed_at, age, market_value_eur,
          currency_code, source_url, raw_payload, updated_at
        )
        VALUES (
          ${playerId}, ${sourceId}, ${seasonId ?? null}, ${seasonLabel ?? null}, ${clubId ?? null}, ${row.clubName ?? null},
          ${row.playerExternalId ?? payload.playerExternalId ?? null}, ${row.clubExternalId ?? null}, ${row.observedAt}, ${row.age ?? null}, ${row.marketValue},
          ${row.currencyCode ?? 'EUR'}, ${row.sourceUrl ?? null}, ${JSON.stringify(row.raw ?? row)}::jsonb, NOW()
        )
        ON CONFLICT (player_id, source_id, observed_at)
        DO UPDATE SET
          season_id = COALESCE(EXCLUDED.season_id, player_market_values.season_id),
          season_label = COALESCE(EXCLUDED.season_label, player_market_values.season_label),
          club_id = COALESCE(EXCLUDED.club_id, player_market_values.club_id),
          club_name = COALESCE(EXCLUDED.club_name, player_market_values.club_name),
          external_player_id = COALESCE(EXCLUDED.external_player_id, player_market_values.external_player_id),
          external_club_id = COALESCE(EXCLUDED.external_club_id, player_market_values.external_club_id),
          age = COALESCE(EXCLUDED.age, player_market_values.age),
          market_value_eur = EXCLUDED.market_value_eur,
          currency_code = EXCLUDED.currency_code,
          source_url = COALESCE(EXCLUDED.source_url, player_market_values.source_url),
          raw_payload = EXCLUDED.raw_payload,
          updated_at = NOW()
      `;

      if (row.playerExternalId ?? payload.playerExternalId) {
        await sql`
          INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata)
          VALUES ('player', ${playerId}, ${sourceId}, ${row.playerExternalId ?? payload.playerExternalId!}, ${JSON.stringify({ sourceUrl: row.sourceUrl ?? null })}::jsonb)
          ON CONFLICT (entity_type, source_id, external_id)
          DO UPDATE SET metadata = COALESCE(source_entity_mapping.metadata, '{}'::jsonb) || EXCLUDED.metadata, updated_at = NOW()
        `;
      }

      if (clubId && row.clubExternalId) {
        await sql`
          INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata)
          VALUES ('team', ${clubId}, ${sourceId}, ${row.clubExternalId}, ${JSON.stringify({ clubName: row.clubName ?? null })}::jsonb)
          ON CONFLICT (entity_type, source_id, external_id)
          DO UPDATE SET metadata = COALESCE(source_entity_mapping.metadata, '{}'::jsonb) || EXCLUDED.metadata, updated_at = NOW()
        `;
      }
    }

    console.log(JSON.stringify({ player: options.playerSlug, provider, updated, dryRun: options.dryRun }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
