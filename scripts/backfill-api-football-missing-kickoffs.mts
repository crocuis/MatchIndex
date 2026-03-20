import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  dryRun: boolean;
  help: boolean;
  limit?: number;
  timeZone: string;
}

interface MissingKickoffRow {
  match_date: string;
}

function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseTimeZone(value: string | undefined) {
  const normalized = value?.trim() || 'UTC';

  try {
    new Intl.DateTimeFormat('en-US', { timeZone: normalized }).format(new Date());
  } catch {
    throw new Error(`Invalid timezone value: ${normalized}`);
  }

  return normalized;
}

function addDays(dateStr: string, days: number) {
  const date = new Date(`${dateStr}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: true,
    help: false,
    timeZone: 'UTC',
  };

  for (const arg of argv) {
    if (arg === '--write') {
      options.dryRun = false;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
      continue;
    }

    if (arg.startsWith('--timezone=')) {
      options.timeZone = parseTimeZone(arg.slice('--timezone='.length));
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/backfill-api-football-missing-kickoffs.mts [options]

Options:
  --write                Persist kickoff_at backfill updates
  --limit=<n>            Only inspect the first N missing matches
  --timezone=Area/City   API-Football fixture query timezone (default: UTC)
  --help, -h             Show this help message

Notes:
  - Default mode is dry-run.
  - The script targets supported API-Football competitions only.
  - It queries each missing match date and the previous UTC date to catch late local kickoffs.
`);
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is not set');
  }

  const apiFootballModuleUrl = new URL('../src/data/apiFootball.ts', import.meta.url);
  const syncModuleUrl = new URL('../src/data/apiFootballDailyFixturesSync.ts', import.meta.url);
  const { getDefaultApiFootballDataCompetitionTargets } = await import(apiFootballModuleUrl.href);
  const { refreshApiFootballDerivedViews, syncApiFootballDailyFixtures } = await import(syncModuleUrl.href);

  const competitionSlugs = getDefaultApiFootballDataCompetitionTargets().map((target: { competitionSlug: string }) => target.competitionSlug);
  const sql = postgres(process.env.DATABASE_URL, {
    max: 1,
    idle_timeout: 20,
    prepare: false,
  });

  try {
    const rows = await sql<MissingKickoffRow[]>`
      SELECT DISTINCT m.match_date::TEXT AS match_date
      FROM matches m
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      WHERE c.slug = ANY(${competitionSlugs})
        AND m.status IN ('scheduled', 'timed')
        AND m.kickoff_at IS NULL
      ORDER BY 1 ASC
    `;

    const targetDates = rows
      .slice(0, options.limit)
      .flatMap((row) => [addDays(row.match_date, -1), row.match_date]);
    const localDates = [...new Set(targetDates)].sort();

    const summaries = [] as Array<{
      localDate: string;
      matchesMatched: number;
      matchesUpdated: number;
      fetchedFiles: number;
      changedFiles: number;
      endpoints: string[];
    }>;
    let refreshDeferred = false;

    for (const localDate of localDates) {
      const summary = await syncApiFootballDailyFixtures({
        candidateMode: 'missing-kickoff',
        dryRun: options.dryRun,
        localDate,
        refreshDerivedViews: false,
        timeZone: options.timeZone,
      });

      summaries.push(summary);
      refreshDeferred = refreshDeferred || summary.matchesUpdated > 0;
    }

    if (!options.dryRun && refreshDeferred) {
      await refreshApiFootballDerivedViews();
    }

    const remainingRows = await sql<{ missing_count: number }[]>`
      SELECT COUNT(*)::INT AS missing_count
      FROM matches m
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      WHERE c.slug = ANY(${competitionSlugs})
        AND m.status IN ('scheduled', 'timed')
        AND m.kickoff_at IS NULL
    `;

    console.log(JSON.stringify({
      dryRun: options.dryRun,
      datesQueried: localDates,
      missingDatesConsidered: rows.slice(0, options.limit).map((row) => row.match_date),
      remainingMissingKickoffs: remainingRows[0]?.missing_count ?? 0,
      timeZone: options.timeZone,
      totalMatchesMatched: summaries.reduce((sum, item) => sum + item.matchesMatched, 0),
      totalMatchesUpdated: summaries.reduce((sum, item) => sum + item.matchesUpdated, 0),
      summaries,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}

await main();
