import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  competitionSlug?: string;
  help: boolean;
  limit?: number;
  outputPath?: string;
  playerSlug?: string;
  seasonSlug?: string;
  teamSlug?: string;
}

interface TargetRow {
  competition_slug: string;
  competition_season_id: number;
  contract_id: number;
  full_name: string | null;
  known_as: string;
  player_slug: string;
  short_name: string | null;
  team_name: string;
  team_slug: string;
}

interface ExportTarget {
  competitionSeasonId: number;
  contractId: number;
  fbrefUrl?: string;
  playerName: string;
  playerNames: string[];
  playerSlug: string;
  sourceUrl?: string;
  teamName: string;
  teamNames: string[];
  teamSlug: string;
}

interface TransfermarktMappingEntry {
  playerSlug: string;
  sourceUrl: string;
}

interface FbrefMappingEntry {
  playerSlug: string;
  sourceUrl: string;
}

interface StoredPlayerMappingRow {
  external_id: string;
  player_slug: string;
  source_url: string | null;
}

function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { help: false };

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--competition=')) {
      options.competitionSlug = arg.slice('--competition='.length).trim();
      continue;
    }

    if (arg.startsWith('--season=')) {
      options.seasonSlug = arg.slice('--season='.length).trim();
      continue;
    }

    if (arg.startsWith('--player=')) {
      options.playerSlug = arg.slice('--player='.length).trim();
      continue;
    }

    if (arg.startsWith('--team=')) {
      options.teamSlug = arg.slice('--team='.length).trim();
      continue;
    }

    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
      continue;
    }

    if (arg.startsWith('--output=')) {
      options.outputPath = arg.slice('--output='.length).trim();
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/export-player-contract-targets.mts --competition=<slug> --season=<slug> [options]

Options:
  --competition=<slug>  Internal competition slug (e.g. premier-league)
  --season=<slug>       Internal season slug (e.g. 2025-2026)
  --team=<slug>         Restrict export to one internal team slug
  --player=<slug>       Restrict export to one player slug
  --limit=<n>           Limit exported targets
  --output=<path>       Write JSON to a file instead of stdout
  --help, -h            Show this help message

Environment:
  DATABASE_URL          PostgreSQL connection string
  TRANSFERMARKT_PLAYER_MAPPINGS_FILE
                        Optional JSON file with [{"playerSlug":"...","sourceUrl":"..."}]
  FBREF_PLAYER_MAPPINGS_FILE
                        Optional JSON file with [{"playerSlug":"...","sourceUrl":"..."}]
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

function resolveOutputPath(outputPath: string) {
  return path.isAbsolute(outputPath) ? outputPath : path.join(process.cwd(), outputPath);
}

function resolveMappingsPath() {
  const configuredPath = process.env.TRANSFERMARKT_PLAYER_MAPPINGS_FILE?.trim();
  if (!configuredPath) {
    return path.join(process.cwd(), 'data', 'transfermarkt-player-mappings.json');
  }

  return path.isAbsolute(configuredPath) ? configuredPath : path.join(process.cwd(), configuredPath);
}

function resolveFbrefMappingsPath() {
  const configuredPath = process.env.FBREF_PLAYER_MAPPINGS_FILE?.trim();
  if (!configuredPath) {
    return path.join(process.cwd(), 'data', 'fbref-player-mappings.json');
  }

  return path.isAbsolute(configuredPath) ? configuredPath : path.join(process.cwd(), configuredPath);
}

function collectNames(values: Array<string | null | undefined>) {
  return Array.from(new Set(values.map((value) => value?.trim()).filter((value): value is string => Boolean(value))));
}

async function loadTransfermarktMappings(sql: ReturnType<typeof postgres>) {
  const rows = await sql<StoredPlayerMappingRow[]>`
    SELECT DISTINCT ON (p.slug)
      sem.external_id,
      p.slug AS player_slug,
      sem.metadata->>'sourceUrl' AS source_url
    FROM source_entity_mapping sem
    JOIN data_sources ds ON ds.id = sem.source_id
    JOIN players p ON p.id = sem.entity_id
    WHERE sem.entity_type = 'player'
      AND ds.slug = 'transfermarkt'
    ORDER BY p.slug, sem.updated_at DESC NULLS LAST, sem.id DESC
  `;

  const mapping = new Map<string, string>();
  for (const row of rows) {
    const sourceUrl = row.source_url?.trim() || (row.external_id ? `https://www.transfermarkt.com/-/profil/spieler/${row.external_id}` : null);
    if (row.player_slug?.trim() && sourceUrl) {
      mapping.set(row.player_slug.trim(), sourceUrl);
    }
  }

  try {
    const raw = await readFile(resolveMappingsPath(), 'utf8');
    const payload = JSON.parse(raw) as TransfermarktMappingEntry[];
    for (const entry of payload) {
      if (entry.playerSlug?.trim() && entry.sourceUrl?.trim() && !mapping.has(entry.playerSlug.trim())) {
        mapping.set(entry.playerSlug.trim(), entry.sourceUrl.trim());
      }
    }
  } catch {
    return mapping;
  }

  return mapping;
}

async function loadFbrefMappings() {
  try {
    const raw = await readFile(resolveFbrefMappingsPath(), 'utf8');
    const payload = JSON.parse(raw) as FbrefMappingEntry[];
    return new Map(
      payload
        .filter((entry) => entry.playerSlug?.trim() && entry.sourceUrl?.trim())
        .map((entry) => [entry.playerSlug.trim(), entry.sourceUrl.trim()])
    );
  } catch {
    return new Map<string, string>();
  }
}

async function loadTargets(sql: ReturnType<typeof postgres>, options: Required<Pick<CliOptions, 'competitionSlug' | 'seasonSlug'>> & Pick<CliOptions, 'limit' | 'playerSlug' | 'teamSlug'>) {
  const rows = await sql<TargetRow[]>`
    SELECT
      pc.id AS contract_id,
      pc.competition_season_id,
      c.slug AS competition_slug,
      p.slug AS player_slug,
      pt.known_as,
      CONCAT_WS(' ', pt.first_name, pt.last_name) AS full_name,
      t.slug AS team_slug,
      tt.name AS team_name,
      tt.short_name
    FROM player_contracts pc
    JOIN players p ON p.id = pc.player_id
    JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
    JOIN teams t ON t.id = pc.team_id
    JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
    JOIN competition_seasons cs ON cs.id = pc.competition_season_id
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    WHERE c.slug = ${options.competitionSlug}
      AND s.slug = ${options.seasonSlug}
      AND pc.left_date IS NULL
      AND (${options.teamSlug ?? null}::text IS NULL OR t.slug = ${options.teamSlug ?? null})
      AND (${options.playerSlug ?? null}::text IS NULL OR p.slug = ${options.playerSlug ?? null})
    ORDER BY pt.known_as ASC
    LIMIT ${options.limit ?? 10000}
  `;

  const fallbackRows = rows.length > 0
    ? rows
    : await sql<TargetRow[]>`
        WITH target_season AS (
          SELECT start_date, end_date
          FROM seasons
          WHERE slug = ${options.seasonSlug}
          LIMIT 1
        ), latest_team_contracts AS (
          SELECT DISTINCT ON (pc.player_id)
            pc.id AS contract_id,
            pc.competition_season_id,
            c.slug AS competition_slug,
            p.slug AS player_slug,
            pt.known_as,
            CONCAT_WS(' ', pt.first_name, pt.last_name) AS full_name,
            t.slug AS team_slug,
            tt.name AS team_name,
            tt.short_name,
            COALESCE(pc.left_date, pc.contract_end_date, pc.joined_date, s.end_date, s.start_date) AS recency_date
          FROM player_contracts pc
          CROSS JOIN target_season target
          JOIN players p ON p.id = pc.player_id
          JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
          JOIN teams t ON t.id = pc.team_id
          JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
          JOIN competition_seasons cs ON cs.id = pc.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          JOIN seasons s ON s.id = cs.season_id
          WHERE pc.left_date IS NULL
            AND (${options.teamSlug ?? null}::text IS NULL OR t.slug = ${options.teamSlug ?? null})
            AND (${options.playerSlug ?? null}::text IS NULL OR p.slug = ${options.playerSlug ?? null})
            AND COALESCE(pc.left_date, pc.contract_end_date, pc.joined_date, s.end_date, s.start_date) IS NOT NULL
            AND COALESCE(pc.left_date, pc.contract_end_date, pc.joined_date, s.end_date, s.start_date) >= target.start_date - INTERVAL '18 months'
          ORDER BY pc.player_id, recency_date DESC NULLS LAST, pc.joined_date DESC NULLS LAST, pc.id DESC
        )
        SELECT
          contract_id,
          competition_season_id,
          competition_slug,
          player_slug,
          known_as,
          full_name,
          team_slug,
          team_name,
          short_name
        FROM latest_team_contracts
        ORDER BY known_as ASC
        LIMIT ${options.limit ?? 10000}
      `;

  const sourceUrlByPlayerSlug = await loadTransfermarktMappings(sql);
  const fbrefUrlByPlayerSlug = await loadFbrefMappings();

  return fallbackRows.map<ExportTarget>((row) => ({
    competitionSeasonId: row.competition_season_id,
    contractId: row.contract_id,
    fbrefUrl: fbrefUrlByPlayerSlug.get(row.player_slug),
    playerName: row.known_as,
    playerNames: collectNames([row.known_as, row.full_name]),
    playerSlug: row.player_slug,
    sourceUrl: sourceUrlByPlayerSlug.get(row.player_slug),
    teamName: row.team_name,
    teamNames: collectNames([row.team_name, row.short_name, row.team_slug]),
    teamSlug: row.team_slug,
  }));
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  if (!options.competitionSlug || !options.seasonSlug) {
    throw new Error('--competition and --season are required');
  }

  const sql = getSql();
  try {
    const targets = await loadTargets(sql, {
      competitionSlug: options.competitionSlug,
        limit: options.limit,
        playerSlug: options.playerSlug,
        teamSlug: options.teamSlug,
        seasonSlug: options.seasonSlug,
      });

    const payload = {
      competitionSlug: options.competitionSlug,
      exportedAt: new Date().toISOString(),
      seasonSlug: options.seasonSlug,
      teamSlug: options.teamSlug ?? null,
      targets,
    };

    const serialized = JSON.stringify(payload, null, 2);
    if (options.outputPath) {
      await writeFile(resolveOutputPath(options.outputPath), `${serialized}\n`, 'utf8');
    } else {
      console.log(serialized);
    }
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
