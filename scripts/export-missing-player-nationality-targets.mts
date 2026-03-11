import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  help: boolean;
  limit?: number;
  outputPath?: string;
  playerSlug?: string;
}

interface MappingEntry {
  playerSlug: string;
  sourceUrl: string;
}

interface TargetRow {
  full_name: string | null;
  short_name: string | null;
  player_slug: string;
  known_as: string;
  team_name: string | null;
  team_slug: string | null;
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

    if (arg.startsWith('--output=')) {
      options.outputPath = arg.slice('--output='.length).trim();
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
  console.log(`Usage: node --experimental-strip-types scripts/export-missing-player-nationality-targets.mts [options]

Options:
  --output=<path>       Write JSON to a file instead of stdout
  --player=<slug>       Restrict export to one player slug
  --limit=<n>           Limit exported players
  --help, -h            Show this help message

Environment:
  DATABASE_URL
  TRANSFERMARKT_PLAYER_MAPPINGS_FILE
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

function resolveMappingsPath() {
  const configuredPath = process.env.TRANSFERMARKT_PLAYER_MAPPINGS_FILE?.trim();
  return resolvePathLike(configuredPath || path.join('data', 'transfermarkt-player-mappings.json'));
}

async function loadMappings() {
  try {
    const raw = await readFile(resolveMappingsPath(), 'utf8');
    const payload = JSON.parse(raw) as MappingEntry[];
    return new Map(
      payload
        .filter((entry) => entry.playerSlug?.trim() && entry.sourceUrl?.trim())
        .map((entry) => [entry.playerSlug.trim(), entry.sourceUrl.trim()])
    );
  } catch {
    return new Map<string, string>();
  }
}

async function loadTargets(sql: ReturnType<typeof getSql>, options: Pick<CliOptions, 'limit' | 'playerSlug'>) {
  return sql<TargetRow[]>`
    WITH latest_player_contracts AS (
      SELECT DISTINCT ON (pc.player_id)
        pc.player_id,
        pc.team_id,
        pc.competition_season_id
      FROM player_contracts pc
      JOIN competition_seasons cs ON cs.id = pc.competition_season_id
      JOIN seasons s ON s.id = cs.season_id
      ORDER BY
        pc.player_id,
        COALESCE(pc.left_date, s.end_date, s.start_date) DESC NULLS LAST,
        s.start_date DESC NULLS LAST,
        pc.competition_season_id DESC
    )
    SELECT
      p.slug AS player_slug,
      pt.known_as,
      CONCAT_WS(' ', pt.first_name, pt.last_name) AS full_name,
      tt.name AS team_name,
      tt.short_name,
      t.slug AS team_slug
    FROM players p
    JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
    LEFT JOIN latest_player_contracts lpc ON lpc.player_id = p.id
    LEFT JOIN teams t ON t.id = lpc.team_id
    LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
    WHERE p.country_id IS NULL
      AND (${options.playerSlug ?? null}::text IS NULL OR p.slug = ${options.playerSlug ?? null})
    ORDER BY pt.known_as ASC
    LIMIT ${options.limit ?? 10000}
  `;
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const sql = getSql();
  try {
    const [rows, mappings] = await Promise.all([
      loadTargets(sql, options),
      loadMappings(),
    ]);

    const unmappedPlayerSlugs: string[] = [];
    const targets = rows.map((row) => {
      const sourceUrl = mappings.get(row.player_slug);
      if (!sourceUrl) {
        unmappedPlayerSlugs.push(row.player_slug);
      }

      return {
        playerSlug: row.player_slug,
        playerName: row.known_as,
        playerNames: [row.known_as, row.full_name].filter(Boolean),
        sourceUrl,
        teamName: row.team_name ?? row.team_slug ?? '',
        teamNames: [row.team_name, row.short_name, row.team_slug].filter(Boolean),
      };
    });

    const payload = {
      exportedAt: new Date().toISOString(),
      provider: 'transfermarkt',
      targets,
      totalMissingPlayers: rows.length,
      mappedTargets: targets.filter((target) => target.sourceUrl?.trim()).length,
      unmappedPlayerSlugs,
    };

    const serialized = JSON.stringify(payload, null, 2);
    if (options.outputPath) {
      await writeFile(resolvePathLike(options.outputPath), `${serialized}\n`, 'utf8');
    } else {
      console.log(serialized);
    }
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
