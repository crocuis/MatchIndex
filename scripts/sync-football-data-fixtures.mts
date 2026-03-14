import { spawn } from 'node:child_process';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  competitionCodes?: string[];
  dateFrom?: string;
  dateTo?: string;
  dryRun: boolean;
  help: boolean;
  localDate?: string;
  seasons?: number[];
  status?: string;
  timeZone?: string;
  today?: boolean;
}

interface FootballDataHelpers {
  fetchFootballDataJson: <T>(path: string) => Promise<T>;
}

const FIXTURE_SYNC_COMPETITION_CODES = ['PL', 'PD', 'SA', 'BL1', 'FL1', 'CL', 'EL'] as const;
const DEFAULT_SEASON_ROLLOVER_MONTH = 7;

function parsePositiveInt(value: string) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function parseDateFilter(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    throw new Error(`Invalid date filter: ${trimmed}`);
  }

  return trimmed;
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

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: false,
    help: false,
    timeZone: parseTimeZone(undefined),
  };
  const hasToday = argv.includes('--today');
  const hasExplicitDateFrom = argv.some((arg) => arg.startsWith('--date-from='));
  const hasExplicitDateTo = argv.some((arg) => arg.startsWith('--date-to='));
  const hasLocalDate = argv.some((arg) => arg.startsWith('--local-date='));

  if (hasToday && (hasExplicitDateFrom || hasExplicitDateTo || hasLocalDate)) {
    throw new Error('--today cannot be combined with --date-from, --date-to, or --local-date');
  }

  if (hasLocalDate && (hasExplicitDateFrom || hasExplicitDateTo)) {
    throw new Error('--local-date cannot be combined with --date-from or --date-to');
  }

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

    if (arg.startsWith('--competitions=')) {
      const codes = arg
        .slice('--competitions='.length)
        .split(',')
        .map((value) => value.trim().toUpperCase())
        .filter(Boolean);

      if (codes.length > 0) {
        options.competitionCodes = [...new Set(codes)];
      }

      continue;
    }

    if (arg.startsWith('--status=')) {
      const value = arg.slice('--status='.length).trim().toUpperCase();
      if (value) {
        options.status = value;
      }

      continue;
    }

    if (arg.startsWith('--date-from=')) {
      options.dateFrom = parseDateFilter(arg.slice('--date-from='.length));
      continue;
    }

    if (arg.startsWith('--date-to=')) {
      options.dateTo = parseDateFilter(arg.slice('--date-to='.length));
      continue;
    }

    if (arg.startsWith('--local-date=')) {
      options.localDate = parseDateFilter(arg.slice('--local-date='.length));
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

  if (options.dateFrom && options.dateTo && options.dateFrom > options.dateTo) {
    throw new Error('--date-from must be earlier than or equal to --date-to');
  }

  return options;
}

function readSeasonRolloverMonth() {
  const parsed = parsePositiveInt(process.env.FOOTBALL_DATA_FIXTURE_SYNC_ROLLOVER_MONTH?.trim() ?? '');
  if (!parsed) {
    return DEFAULT_SEASON_ROLLOVER_MONTH;
  }

  return Math.min(12, Math.max(1, parsed));
}

function getDefaultSeasonYears(referenceDate: Date = new Date()) {
  const rolloverMonth = readSeasonRolloverMonth();
  const year = referenceDate.getUTCFullYear();
  const month = referenceDate.getUTCMonth() + 1;
  const currentSeasonStartYear = month >= rolloverMonth ? year : year - 1;

  return [currentSeasonStartYear - 1, currentSeasonStartYear];
}

function buildCommandForCompetitions(
  scriptName: string,
  seasons: number[],
  competitionCodes: string[],
  options: Pick<CliOptions, 'dateFrom' | 'dateTo' | 'dryRun' | 'localDate' | 'status' | 'timeZone' | 'today'>,
) {
  const args = [
    process.execPath,
    '--experimental-strip-types',
    `scripts/${scriptName}`,
    ...seasons.map(String),
    `--competitions=${competitionCodes.join(',')}`,
  ];

  if (options.status) {
    args.push(`--status=${options.status}`);
  } else {
    args.push('--all-statuses');
  }

  if (options.dateFrom) {
    args.push(`--date-from=${options.dateFrom}`);
  }

  if (options.dateTo) {
    args.push(`--date-to=${options.dateTo}`);
  }

  if (options.localDate) {
    args.push(`--local-date=${options.localDate}`);
  }

  if (options.today) {
    args.push('--today');
  }

  if ((options.localDate || options.today) && options.timeZone) {
    args.push(`--timezone=${options.timeZone}`);
  }

  if (!options.dryRun) {
    args.push('--write');
  }

  return args;
}

function buildCommandsForCompetitions(seasons: number[], competitionCodes: string[], options: CliOptions) {
  return [
    {
      description: 'football-data.org 경기 일정 원본 적재',
      args: buildCommandForCompetitions('football-data-ingest-manifests.mts', seasons, competitionCodes, options),
    },
    {
      description: 'football-data.org 경기 일정 정규화 적재',
      args: buildCommandForCompetitions('football-data-materialize-core.mts', seasons, competitionCodes, options),
    },
  ];
}

async function resolveAccessibleCompetitionCodes(competitionCodes: string[]) {
  const moduleUrl = new URL('../src/data/footballDataOrg.ts', import.meta.url);
  const { fetchFootballDataJson } = await import(moduleUrl.href) as FootballDataHelpers;

  const allowed: string[] = [];
  const skipped: Array<{ code: string; reason: string }> = [];

  for (const code of competitionCodes) {
    try {
      await fetchFootballDataJson(`/competitions/${code}`);
      allowed.push(code);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.includes('request failed: 403')) {
        skipped.push({ code, reason: 'subscription_forbidden' });
        continue;
      }

      throw error;
    }
  }

  if (allowed.length === 0) {
    throw new Error('현재 football-data.org 구독으로 접근 가능한 대상 대회가 없습니다');
  }

  return { allowed, skipped };
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-football-data-fixtures.mts [options]

Options:
  --seasons=2025,2026   Override season start years to sync
  --competitions=CL,EL   Restrict to selected competition codes
  --status=FINISHED      Restrict to one football-data.org match status
  --date-from=YYYY-MM-DD Restrict matches from this date
  --date-to=YYYY-MM-DD   Restrict matches until this date
  --local-date=YYYY-MM-DD Restrict matches to one day in the selected timezone
  --today                Restrict matches to the current day in the selected timezone
  --timezone=Area/City   Timezone for --today or --local-date (default: Asia/Seoul or TZ)
  --dry-run             Print the execution plan without writing data
  --help, -h            Show this help message

Default competitions:
  ${FIXTURE_SYNC_COMPETITION_CODES.join(', ')}

Default payload mode:
  Fetch all match statuses for the selected seasons so scheduled fixtures are included

Date filters:
  Use --today or --local-date for timezone-aware one-day syncs, or combine --date-from / --date-to with --status=FINISHED for raw UTC-date syncs

Required environment for write mode:
  DATABASE_URL
  FOOTBALL_DATA_API_KEY

Optional environment:
  FOOTBALL_DATA_BASE_URL=https://api.football-data.org/v4
  FOOTBALL_DATA_FIXTURE_SYNC_ROLLOVER_MONTH=${DEFAULT_SEASON_ROLLOVER_MONTH}
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

  const seasons = options.seasons ?? getDefaultSeasonYears();
  const requestedCompetitionCodes = options.competitionCodes ?? [...FIXTURE_SYNC_COMPETITION_CODES];
  const { allowed, skipped } = await resolveAccessibleCompetitionCodes(requestedCompetitionCodes);
  const commands = buildCommandsForCompetitions(seasons, allowed, options);

  if (options.dryRun) {
    console.log(JSON.stringify({
      dryRun: true,
      requestedCompetitions: requestedCompetitionCodes,
      competitions: allowed,
      skippedCompetitions: skipped,
      filters: {
        dateFrom: options.dateFrom ?? null,
        dateTo: options.dateTo ?? null,
        localDate: options.localDate ?? null,
        status: options.status ?? null,
        timeZone: options.timeZone ?? null,
        today: options.today ?? false,
      },
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
      requestedCompetitions: requestedCompetitionCodes,
      competitions: allowed,
      skippedCompetitions: skipped,
      filters: {
        dateFrom: options.dateFrom ?? null,
        dateTo: options.dateTo ?? null,
        localDate: options.localDate ?? null,
        status: options.status ?? null,
        timeZone: options.timeZone ?? null,
        today: options.today ?? false,
      },
      seasons,
      commandCount: commands.length,
    }, null, 2));
}

await main();
