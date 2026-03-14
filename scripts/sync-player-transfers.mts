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

interface TransferPayload {
  playerExternalId?: string | null;
  provider: string;
  rows: Array<{
    age?: number | null;
    contractUntilDate?: string | null;
    currencyCode?: string | null;
    externalTransferId: string;
    fee?: number | null;
    feeDisplay?: string | null;
    fromClubExternalId?: string | null;
    fromClubName?: string | null;
    isPending?: boolean;
    marketValue?: number | null;
    movedAt?: string | null;
    raw?: unknown;
    seasonLabel?: string | null;
    sourceUrl?: string | null;
    toClubExternalId?: string | null;
    toClubName?: string | null;
    transferType?: string | null;
    transferTypeLabel?: string | null;
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
  console.log(`Usage: node --experimental-strip-types scripts/sync-player-transfers.mts --input=<path> --player=<slug> [options]

Options:
  --input=<path>     JSON payload produced by fetch-player-transfers-transfermarkt.mts
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
  return JSON.parse(await readFile(resolvePath(filePath), 'utf8')) as TransferPayload;
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

function resolveSeasonId(rows: SeasonRow[], movedAt?: string | null, seasonLabel?: string | null) {
  if (seasonLabel) {
    const matched = rows.find((row) => row.slug === seasonLabel);
    if (matched) {
      return matched.id;
    }
  }

  if (!movedAt) {
    return undefined;
  }

  const year = Number.parseInt(movedAt.slice(0, 4), 10);
  const month = Number.parseInt(movedAt.slice(5, 7), 10);
  if (!Number.isFinite(year) || !Number.isFinite(month)) {
    return undefined;
  }

  const startYear = month >= 7 ? year : year - 1;
  const label = `${startYear}/${String((startYear + 1) % 100).padStart(2, '0')}`;
  return rows.find((row) => row.slug === label)?.id;
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
    const seasons = await loadSeasonRows(sql);
    const teamResolver = buildTeamResolver(await loadTeamMappings(sql, sourceId), await loadTeams(sql));

    let updated = 0;
    for (const row of payload.rows) {
      if (!row.externalTransferId) {
        continue;
      }
      const fromTeamId = teamResolver.resolve(row.fromClubExternalId, row.fromClubName);
      const toTeamId = teamResolver.resolve(row.toClubExternalId, row.toClubName);
      const seasonId = resolveSeasonId(seasons, row.movedAt, row.seasonLabel);
      updated += 1;

      if (options.dryRun) {
        continue;
      }

      await sql`
        INSERT INTO player_transfers (
          player_id, source_id, season_id, season_label, external_transfer_id, moved_at, age,
          from_team_id, from_team_name, from_team_external_id,
          to_team_id, to_team_name, to_team_external_id,
          market_value_eur, fee_eur, currency_code, fee_display,
          transfer_type, transfer_type_label, is_pending, contract_until_date,
          source_url, raw_payload, updated_at
        )
        VALUES (
          ${playerId}, ${sourceId}, ${seasonId ?? null}, ${row.seasonLabel ?? null}, ${row.externalTransferId}, ${row.movedAt ?? null}, ${row.age ?? null},
          ${fromTeamId ?? null}, ${row.fromClubName ?? null}, ${row.fromClubExternalId ?? null},
          ${toTeamId ?? null}, ${row.toClubName ?? null}, ${row.toClubExternalId ?? null},
          ${row.marketValue ?? null}, ${row.fee ?? null}, ${row.currencyCode ?? 'EUR'}, ${row.feeDisplay ?? null},
          ${row.transferType ?? null}, ${row.transferTypeLabel ?? null}, ${row.isPending ?? false}, ${row.contractUntilDate ?? null},
          ${row.sourceUrl ?? null}, ${JSON.stringify(row.raw ?? row)}::jsonb, NOW()
        )
        ON CONFLICT (player_id, source_id, external_transfer_id)
        DO UPDATE SET
          season_id = COALESCE(EXCLUDED.season_id, player_transfers.season_id),
          season_label = COALESCE(EXCLUDED.season_label, player_transfers.season_label),
          moved_at = COALESCE(EXCLUDED.moved_at, player_transfers.moved_at),
          age = COALESCE(EXCLUDED.age, player_transfers.age),
          from_team_id = COALESCE(EXCLUDED.from_team_id, player_transfers.from_team_id),
          from_team_name = COALESCE(EXCLUDED.from_team_name, player_transfers.from_team_name),
          from_team_external_id = COALESCE(EXCLUDED.from_team_external_id, player_transfers.from_team_external_id),
          to_team_id = COALESCE(EXCLUDED.to_team_id, player_transfers.to_team_id),
          to_team_name = COALESCE(EXCLUDED.to_team_name, player_transfers.to_team_name),
          to_team_external_id = COALESCE(EXCLUDED.to_team_external_id, player_transfers.to_team_external_id),
          market_value_eur = COALESCE(EXCLUDED.market_value_eur, player_transfers.market_value_eur),
          fee_eur = COALESCE(EXCLUDED.fee_eur, player_transfers.fee_eur),
          currency_code = COALESCE(EXCLUDED.currency_code, player_transfers.currency_code),
          fee_display = COALESCE(EXCLUDED.fee_display, player_transfers.fee_display),
          transfer_type = COALESCE(EXCLUDED.transfer_type, player_transfers.transfer_type),
          transfer_type_label = COALESCE(EXCLUDED.transfer_type_label, player_transfers.transfer_type_label),
          is_pending = EXCLUDED.is_pending,
          contract_until_date = COALESCE(EXCLUDED.contract_until_date, player_transfers.contract_until_date),
          source_url = COALESCE(EXCLUDED.source_url, player_transfers.source_url),
          raw_payload = EXCLUDED.raw_payload,
          updated_at = NOW()
      `;

      if (payload.playerExternalId) {
        await sql`
          INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata)
          VALUES ('player', ${playerId}, ${sourceId}, ${payload.playerExternalId}, ${JSON.stringify({ sourceUrl: row.sourceUrl ?? null })}::jsonb)
          ON CONFLICT (entity_type, source_id, external_id)
          DO UPDATE SET metadata = COALESCE(source_entity_mapping.metadata, '{}'::jsonb) || EXCLUDED.metadata, updated_at = NOW()
        `;
      }

      for (const [entityId, externalId, name] of [
        [fromTeamId, row.fromClubExternalId, row.fromClubName],
        [toTeamId, row.toClubExternalId, row.toClubName],
      ] as const) {
        if (!entityId || !externalId) {
          continue;
        }

        await sql`
          INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata)
          VALUES ('team', ${entityId}, ${sourceId}, ${externalId}, ${JSON.stringify({ clubName: name ?? null })}::jsonb)
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
