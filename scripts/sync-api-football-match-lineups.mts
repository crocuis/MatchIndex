interface CliOptions {
  dryRun: boolean;
  help: boolean;
  localDate?: string;
  profile: 'daily' | 'hourly';
  timeZone: string;
}

const PROFILES: Record<CliOptions['profile'], { daysBack: number; description: string }> = {
  hourly: {
    daysBack: 0,
    description: '당일 API-Football 라인업 증분 수집',
  },
  daily: {
    daysBack: 1,
    description: '전일+당일 API-Football 라인업 재동기화',
  },
};

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

function shiftIsoDate(isoDate: string, days: number) {
  const shifted = new Date(`${isoDate}T00:00:00Z`);
  shifted.setUTCDate(shifted.getUTCDate() + days);
  return shifted.toISOString().slice(0, 10);
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: false,
    help: false,
    profile: 'hourly',
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

    if (arg.startsWith('--profile=')) {
      const profile = arg.slice('--profile='.length).trim();
      if (profile === 'daily' || profile === 'hourly') {
        options.profile = profile;
      }
      continue;
    }

    if (arg.startsWith('--local-date=')) {
      options.localDate = parseIsoDate(arg.slice('--local-date='.length), 'local-date');
      continue;
    }

    if (arg.startsWith('--timezone=')) {
      options.timeZone = parseTimeZone(arg.slice('--timezone='.length));
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-api-football-match-lineups.mts [options]

Options:
  --profile=hourly|daily   Select sync profile (default: hourly)
  --local-date=YYYY-MM-DD  End date in the selected timezone (default: current local date)
  --timezone=Area/City     Local timezone for match selection (default: Asia/Seoul or TZ)
  --dry-run                Print the execution plan without writing data
  --help, -h               Show this help message

Profiles:
  hourly  Same-day API-Football lineup sync
  daily   Re-sync yesterday and today for API-Football lineups

Required environment for write mode:
  DATABASE_URL
  API_FOOTBALL_KEY
`);
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const profile = PROFILES[options.profile];
  const endLocalDate = options.localDate ?? formatDateInTimeZone(new Date(), options.timeZone);
  const startLocalDate = shiftIsoDate(endLocalDate, -profile.daysBack);

  const moduleUrl = new URL('../src/data/apiFootballMatchLineupsSync.ts', import.meta.url);
  const { syncApiFootballMatchLineups } = await import(moduleUrl.href);
  const summary = await syncApiFootballMatchLineups({
    dryRun: options.dryRun,
    endLocalDate,
    startLocalDate,
    timeZone: options.timeZone,
  });

  console.log(JSON.stringify({
    profile: options.profile,
    profileDescription: profile.description,
    ...summary,
  }, null, 2));
}

await main();
