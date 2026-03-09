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

async function loadTransfermarktMappings() {
  try {
    const raw = await readFile(resolveMappingsPath(), 'utf8');
    const payload = JSON.parse(raw) as TransfermarktMappingEntry[];
    return new Map(
      payload
        .filter((entry) => entry.playerSlug?.trim() && entry.sourceUrl?.trim())
        .map((entry) => [entry.playerSlug.trim(), entry.sourceUrl.trim()])
    );
  } catch {
    return new Map<string, string>();
  }
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

async function loadTargets(sql: ReturnType<typeof postgres>, options: Required<Pick<CliOptions, 'competitionSlug' | 'seasonSlug'>> & Pick<CliOptions, 'limit' | 'playerSlug'>) {
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
      AND (${options.playerSlug ?? null}::text IS NULL OR p.slug = ${options.playerSlug ?? null})
    ORDER BY pt.known_as ASC
    LIMIT ${options.limit ?? 10000}
  `;

  const sourceUrlByPlayerSlug = await loadTransfermarktMappings();
  const fbrefUrlByPlayerSlug = await loadFbrefMappings();

  return rows.map<ExportTarget>((row) => ({
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
      seasonSlug: options.seasonSlug,
    });

    const payload = {
      competitionSlug: options.competitionSlug,
      exportedAt: new Date().toISOString(),
      seasonSlug: options.seasonSlug,
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
