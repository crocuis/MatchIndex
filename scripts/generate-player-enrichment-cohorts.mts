import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  competitions: string[];
  help: boolean;
  outputPath?: string;
  seasonsPerLeague: number;
}

interface CohortRow {
  competition: string;
  season_slug: string;
  start_date: string;
  end_date: string;
  contracts: number;
}

interface BatchCohort {
  capologyLeague?: string;
  capologySeason?: string;
  competition: string;
  contracts: number;
  season: string;
}

const DEFAULT_COMPETITIONS = ['1-bundesliga', 'premier-league', 'la-liga', 'serie-a', 'ligue-1'];

const CAPOLOGY_LEAGUE_BY_COMPETITION: Record<string, string> = {
  '1-bundesliga': 'Germany Bundesliga',
  'premier-league': 'England Premier League',
  'la-liga': 'Spain La Liga',
  'serie-a': 'Italy Serie A',
  'ligue-1': 'France Ligue 1',
};

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

function parsePositiveInt(value: string | undefined, fallback: number) {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    competitions: DEFAULT_COMPETITIONS,
    help: false,
    seasonsPerLeague: 10,
  };

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--competitions=')) {
      options.competitions = arg.slice('--competitions='.length).split(',').map((value) => value.trim()).filter(Boolean);
      continue;
    }
    if (arg.startsWith('--seasons-per-league=')) {
      options.seasonsPerLeague = parsePositiveInt(arg.slice('--seasons-per-league='.length), 10);
      continue;
    }
    if (arg.startsWith('--output=')) {
      options.outputPath = arg.slice('--output='.length).trim();
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/generate-player-enrichment-cohorts.mts [options]

Options:
  --competitions=<slug,...>  Competition slugs to include
  --seasons-per-league=<n>   Max seasons per competition (default: 10)
  --output=<path>            Output JSON path
  --help, -h                 Show this help message
`);
}

function resolveOutputPath(outputPath?: string) {
  const targetPath = outputPath?.trim() || path.join('data', 'player-enrichment-five-leagues-10y.json');
  return path.isAbsolute(targetPath) ? targetPath : path.join(process.cwd(), targetPath);
}

function formatCapologySeason(startDate: string, endDate: string) {
  const startYear = new Date(startDate).getUTCFullYear();
  const endYear = new Date(endDate).getUTCFullYear();
  const endYearShort = String(endYear).slice(-2);
  return `${startYear}-${endYearShort}`;
}

async function loadCohorts(sql: ReturnType<typeof postgres>, competitions: string[], seasonsPerLeague: number) {
  const rows = await sql<CohortRow[]>`
    WITH season_candidates AS (
      SELECT
        c.slug AS competition,
        s.slug AS season_slug,
        s.start_date,
        s.end_date,
        COUNT(*)::int AS contracts,
        ROW_NUMBER() OVER (
          PARTITION BY c.slug, EXTRACT(YEAR FROM s.start_date)
          ORDER BY COUNT(*) DESC, s.start_date DESC, s.slug DESC
        ) AS canonical_rank
      FROM player_contracts pc
      JOIN competition_seasons cs ON cs.id = pc.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      JOIN seasons s ON s.id = cs.season_id
      WHERE pc.left_date IS NULL
        AND c.slug = ANY(${sql.array(competitions)})
        AND s.start_date IS NOT NULL
        AND s.end_date IS NOT NULL
        AND s.end_date >= s.start_date + INTERVAL '200 days'
      GROUP BY c.slug, s.slug, s.start_date, s.end_date
    ), ranked AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY competition ORDER BY start_date DESC, contracts DESC, season_slug DESC) AS league_rank
      FROM season_candidates
      WHERE canonical_rank = 1
    )
    SELECT competition, season_slug, start_date::text, end_date::text, contracts
    FROM ranked
    WHERE league_rank <= ${seasonsPerLeague}
    ORDER BY competition ASC, start_date DESC, season_slug DESC
  `;

  return rows.map<BatchCohort>((row) => ({
    capologyLeague: CAPOLOGY_LEAGUE_BY_COMPETITION[row.competition],
    capologySeason: CAPOLOGY_LEAGUE_BY_COMPETITION[row.competition]
      ? formatCapologySeason(row.start_date, row.end_date)
      : undefined,
    competition: row.competition,
    contracts: row.contracts,
    season: row.season_slug,
  }));
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
    const cohorts = await loadCohorts(sql, options.competitions, options.seasonsPerLeague);
    const outputPath = resolveOutputPath(options.outputPath);
    await writeFile(outputPath, `${JSON.stringify(cohorts, null, 2)}\n`, 'utf8');
    console.log(JSON.stringify({ outputPath, competitions: options.competitions, seasonsPerLeague: options.seasonsPerLeague, generated: cohorts.length }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
