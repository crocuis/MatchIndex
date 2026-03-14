import { spawn } from 'node:child_process';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  dryRun: boolean;
  help: boolean;
  localDate?: string;
  seasons?: number[];
  timeZone?: string;
  today?: boolean;
}

const FIXTURE_SYNC_COMPETITION_CODES = ['PL', 'PD', 'SA', 'BL1', 'FL1', 'CL', 'EL'] as const;
const DEFAULT_SEASON_ROLLOVER_MONTH = 7;

function getCurrentSeasonStartYear(referenceDate: Date, rolloverMonth: number) {
  const year = referenceDate.getUTCFullYear();
  const month = referenceDate.getUTCMonth() + 1;
  return month >= rolloverMonth ? year : year - 1;
}

function parsePositiveInt(value: string) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function parseIsoDate(value: string, label: string) {
  const normalized = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    throw new Error(`Invalid ${label}: ${value}`);
  }

  return normalized;
}

function parseTimeZone(value: string | undefined) {
  const normalized = value?.trim() || process.env.TZ?.trim() || 'Asia/Seoul';

  try {
    new Intl.DateTimeFormat('en-CA', { timeZone: normalized }).format(new Date());
  } catch {
    throw new Error(`Invalid timezone value: ${normalized}`);
  }

  return normalized;
}

function formatDateInTimeZone(date: Date, timeZone: string) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  const parts = formatter.formatToParts(date);
  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;

  if (!year || !month || !day) {
    throw new Error(`Failed to format date in timezone: ${timeZone}`);
  }

  return `${year}-${month}-${day}`;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: false,
    help: false,
    timeZone: parseTimeZone(undefined),
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

    if (arg.startsWith('--seasons=')) {
      const seasons = arg
        .slice('--seasons='.length)
        .split(',')
        .map((value) => parsePositiveInt(value.trim()))
        .filter((value): value is number => value !== null);

      if (seasons.length > 0) {
        options.seasons = [...new Set(seasons)].sort((left, right) => left - right);
      }

      continue;
    }

    if (arg.startsWith('--local-date=')) {
      options.localDate = parseIsoDate(arg.slice('--local-date='.length), 'local-date');
      continue;
    }

    if (arg.startsWith('--timezone=')) {
      options.timeZone = parseTimeZone(arg.slice('--timezone='.length));
      continue;
    }

    if (arg === '--today') {
      options.today = true;
    }
  }

  if (options.today && options.localDate) {
    throw new Error('--today cannot be combined with --local-date');
  }

  return options;
}

function readSeasonRolloverMonth() {
  const parsed = parsePositiveInt(process.env.API_FOOTBALL_FIXTURE_SYNC_ROLLOVER_MONTH?.trim() ?? '');
  if (!parsed) {
    return DEFAULT_SEASON_ROLLOVER_MONTH;
  }

  return Math.min(12, Math.max(1, parsed));
}

function readMaxSeasonYear(currentSeasonStartYear: number) {
  const parsed = parsePositiveInt(process.env.API_FOOTBALL_MAX_SEASON_YEAR?.trim() ?? '');
  if (!parsed) {
    return currentSeasonStartYear - 1;
  }

  return Math.min(parsed, currentSeasonStartYear - 1);
}

function getDefaultSeasonYears(referenceDate: Date = new Date()) {
  const rolloverMonth = readSeasonRolloverMonth();
  const currentSeasonStartYear = getCurrentSeasonStartYear(referenceDate, rolloverMonth);
  const maxSeasonYear = readMaxSeasonYear(currentSeasonStartYear);

  return [Math.max(1, maxSeasonYear - 1), maxSeasonYear];
}

function buildCommand(scriptName: string, seasons: number[], dryRun: boolean) {
  const args = [
    process.execPath,
    '--experimental-strip-types',
    `scripts/${scriptName}`,
    ...seasons.map(String),
    `--competitions=${FIXTURE_SYNC_COMPETITION_CODES.join(',')}`,
  ];

  if (!dryRun) {
    args.push('--write');
  }

  return args;
}

function buildCommands(seasons: number[], dryRun: boolean) {
  return [
    {
      description: 'API-Football 경기 일정 원본 적재',
      args: buildCommand('api-football-ingest-competitions.mts', seasons, dryRun),
    },
    {
      description: 'API-Football 경기 일정 정규화 적재',
      args: buildCommand('api-football-materialize-competitions.mts', seasons, dryRun),
    },
  ];
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-api-football-fixtures.mts [options]

Options:
  --seasons=2025,2026   Override season start years to sync
  --local-date=YYYY-MM-DD Run timezone-aware daily fixture sync for one local date
  --today               Run timezone-aware daily fixture sync for the current local date
  --timezone=Area/City  Timezone for --today or --local-date (default: Asia/Seoul or TZ)
  --dry-run             Print the execution plan without writing data
  --help, -h            Show this help message

Default competitions:
  ${FIXTURE_SYNC_COMPETITION_CODES.join(', ')}

Default seasons:
  Previous + current European season window based on UTC month

Daily mode:
  Use --today or --local-date to update only that local day's match info via API-Football date+timezone fixtures

Required environment for write mode:
  DATABASE_URL
  API_FOOTBALL_KEY

Optional environment:
  API_FOOTBALL_FIXTURE_SYNC_ROLLOVER_MONTH  Defaults to ${DEFAULT_SEASON_ROLLOVER_MONTH}
  API_FOOTBALL_MAX_SEASON_YEAR              Caps automatic season selection for plan-restricted keys
  API_FOOTBALL_REQUEST_DELAY_MS             Defaults to 6500
  API_FOOTBALL_RATE_LIMIT_RETRY_MS          Defaults to 65000
`);
}

async function runCommand(args: string[], description: string) {
  return new Promise<void>((resolve, reject) => {
    const child = spawn(args[0], args.slice(1), {
      cwd: process.cwd(),
      env: process.env,
      stdio: 'inherit',
    });

    child.on('exit', (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(`${description} failed with exit code ${code ?? 'unknown'}`));
    });

    child.on('error', reject);
  });
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  if (options.today || options.localDate) {
    const localDate = options.localDate ?? formatDateInTimeZone(new Date(), options.timeZone ?? 'Asia/Seoul');
    const moduleUrl = new URL('../src/data/apiFootballDailyFixturesSync.ts', import.meta.url);
    const { syncApiFootballDailyFixtures } = await import(moduleUrl.href);
    const summary = await syncApiFootballDailyFixtures({
      dryRun: options.dryRun,
      localDate,
      timeZone: options.timeZone ?? 'Asia/Seoul',
    });

    console.log(JSON.stringify(summary, null, 2));
    return;
  }

  const seasons = options.seasons ?? getDefaultSeasonYears();
  const commands = buildCommands(seasons, options.dryRun);

  if (options.dryRun) {
    console.log(JSON.stringify({
      dryRun: true,
      competitions: FIXTURE_SYNC_COMPETITION_CODES,
      seasons,
      commands,
    }, null, 2));
    return;
  }

  for (const command of commands) {
    await runCommand(command.args, command.description);
  }

  console.log(JSON.stringify({
    ok: true,
    competitions: FIXTURE_SYNC_COMPETITION_CODES,
    seasons,
    commandCount: commands.length,
  }, null, 2));
}

await main();
