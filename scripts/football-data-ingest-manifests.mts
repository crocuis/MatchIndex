function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseCompetitionCodes(argv: string[]) {
  const raw = argv.find((arg) => arg.startsWith('--competitions='));
  if (!raw) {
    return undefined;
  }

  return raw
    .slice('--competitions='.length)
    .split(',')
    .map((code) => code.trim().toUpperCase())
    .filter(Boolean);
}

function parseStatus(argv: string[]) {
  if (argv.includes('--all-statuses')) {
    return undefined;
  }

  const raw = argv.find((arg) => arg.startsWith('--status='));
  if (!raw) {
    return undefined;
  }

  const value = raw.slice('--status='.length).trim().toUpperCase();
  return value || undefined;
}

function parseDateFilter(argv: string[], optionName: '--date-from=' | '--date-to=') {
  const raw = argv.find((arg) => arg.startsWith(optionName));
  if (!raw) {
    return undefined;
  }

  const value = raw.slice(optionName.length).trim();
  if (!value) {
    return undefined;
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`Invalid ${optionName.slice(2, -1)} value: ${value}`);
  }

  return value;
}

function parseLocalDate(argv: string[]) {
  const raw = argv.find((arg) => arg.startsWith('--local-date='));
  if (!raw) {
    return undefined;
  }

  const value = raw.slice('--local-date='.length).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`Invalid local-date value: ${value}`);
  }

  return value;
}

function parseTimeZone(argv: string[]) {
  const raw = argv.find((arg) => arg.startsWith('--timezone='));
  const value = raw?.slice('--timezone='.length).trim() || process.env.TZ?.trim() || 'Asia/Seoul';

  try {
    new Intl.DateTimeFormat('en-CA', { timeZone: value }).format(new Date());
  } catch {
    throw new Error(`Invalid timezone value: ${value}`);
  }

  return value;
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

function parseDateRange(argv: string[]) {
  const hasToday = argv.includes('--today');
  const dateFrom = parseDateFilter(argv, '--date-from=');
  const dateTo = parseDateFilter(argv, '--date-to=');
  const localDate = parseLocalDate(argv);
  const timeZone = parseTimeZone(argv);

  if (hasToday && (dateFrom || dateTo || localDate)) {
    throw new Error('--today cannot be combined with --date-from, --date-to, or --local-date');
  }

  if (localDate && (dateFrom || dateTo)) {
    throw new Error('--local-date cannot be combined with --date-from or --date-to');
  }

  if (hasToday) {
    return {
      dateFrom: undefined,
      dateTo: undefined,
      localDate: formatDateInTimeZone(new Date(), timeZone),
      timeZone,
    };
  }

  if (dateFrom && dateTo && dateFrom > dateTo) {
    throw new Error('--date-from must be earlier than or equal to --date-to');
  }

  return { dateFrom, dateTo, localDate, timeZone: localDate ? timeZone : undefined };
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const moduleUrl = new URL('../src/data/footballDataOrgIngest.ts', import.meta.url);
  const { ingestFootballDataManifests } = await import(moduleUrl.href);
  const argv = process.argv.slice(2);
  const args = new Set(argv);
  const orderedArgs = argv.filter((arg) => !arg.startsWith('--'));
  const seasons = orderedArgs.map((value) => parsePositiveInt(value)).filter((value): value is number => value !== undefined);
  const { dateFrom, dateTo, localDate, timeZone } = parseDateRange(argv);

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/football-data-ingest-manifests.mts [season ...] [options]

Options:
  --competitions=PL,PD,BL1,SA,FL1,CL,EL
                         Restrict to supported competition codes
  --status=FINISHED      Restrict to one football-data.org match status
  --date-from=YYYY-MM-DD Restrict matches from this UTC date
  --date-to=YYYY-MM-DD   Restrict matches until this UTC date
  --local-date=YYYY-MM-DD Restrict matches to one calendar day in the selected timezone
  --today                Restrict matches to the current calendar day in the selected timezone
  --timezone=Area/City   Timezone for --today or --local-date (default: Asia/Seoul or TZ)
  --write               Persist raw payloads and manifests (default: dry-run)
  --help, -h            Show this help message
`);
    return;
  }

  const summary = await ingestFootballDataManifests({
    dryRun: args.has('--write') ? false : true,
    competitionCodes: parseCompetitionCodes(argv),
    dateFrom,
    dateTo,
    localDate,
    seasons,
    status: parseStatus(argv),
    timeZone,
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
