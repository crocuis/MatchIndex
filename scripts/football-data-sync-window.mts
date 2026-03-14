interface CliOptions {
  competitionCodes?: string[];
  backfillYearOffset: number;
  help: boolean;
  rolloverMonth: number;
  windowYears: number;
  write: boolean;
}

interface CompetitionAccessProbe {
  accessibleCodes: string[];
  restrictedCodes: string[];
}

function isFootballDataRateLimitError(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return message.includes('football-data.org request failed: 429');
}

function parsePositiveInt(value: string | undefined, fallback?: number) {
  if (!value) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
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

function parseArgs(argv: string[]): CliOptions {
  return {
    competitionCodes: parseCompetitionCodes(argv),
    backfillYearOffset: parsePositiveInt(argv.find((arg) => arg.startsWith('--backfill-year-offset='))?.split('=').at(1), 0) ?? 0,
    help: argv.includes('--help') || argv.includes('-h'),
    rolloverMonth: parsePositiveInt(argv.find((arg) => arg.startsWith('--rollover-month='))?.split('=').at(1), 7) ?? 7,
    windowYears: parsePositiveInt(argv.find((arg) => arg.startsWith('--window-years='))?.split('=').at(1), 1) ?? 1,
    write: argv.includes('--write'),
  };
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/football-data-sync-window.mts [options]

Options:
  --window-years=<n>          Rolling window size in years (default: 1)
  --backfill-year-offset=<n>  Shift the window into the past by N years (default: 0)
  --rollover-month=<1-12>     Season rollover month for deriving season start years (default: 7)
  --competitions=PL,PD,...    Limit to selected football-data.org competition codes
  --write                     Execute ingest + materialize (default: print plan only)
  --help, -h                  Show this help message

Examples:
  node --experimental-strip-types scripts/football-data-sync-window.mts
  node --experimental-strip-types scripts/football-data-sync-window.mts --write
  node --experimental-strip-types scripts/football-data-sync-window.mts --backfill-year-offset=1 --write
  node --experimental-strip-types scripts/football-data-sync-window.mts --competitions=PL,PD,BL1,SA,FL1,CL,EL --write
`);
}

function shiftUtcYears(date: Date, years: number) {
  const next = new Date(date);
  next.setUTCFullYear(next.getUTCFullYear() + years);
  return next;
}

function getSeasonStartYear(date: Date, rolloverMonth: number) {
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth() + 1;
  return month >= rolloverMonth ? year : year - 1;
}

function buildSeasonYears(windowStart: Date, windowEnd: Date, rolloverMonth: number) {
  const startSeasonYear = getSeasonStartYear(windowStart, rolloverMonth);
  const endSeasonYear = getSeasonStartYear(windowEnd, rolloverMonth);
  const seasons: number[] = [];

  for (let year = startSeasonYear; year <= endSeasonYear; year += 1) {
    seasons.push(year);
  }

  return seasons;
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

  const windowEnd = shiftUtcYears(new Date(), -options.backfillYearOffset);
  const windowStart = shiftUtcYears(windowEnd, -options.windowYears);
  const seasons = buildSeasonYears(windowStart, windowEnd, options.rolloverMonth);
  const plan = {
    write: options.write,
    windowYears: options.windowYears,
    backfillYearOffset: options.backfillYearOffset,
    rolloverMonth: options.rolloverMonth,
    competitions: options.competitionCodes ?? 'default',
    windowStart: windowStart.toISOString().slice(0, 10),
    windowEnd: windowEnd.toISOString().slice(0, 10),
    seasons,
  };

  if (!options.write) {
    console.log(JSON.stringify({ dryRun: true, ...plan }, null, 2));
    return;
  }

  const ingestModuleUrl = new URL('../src/data/footballDataOrgIngest.ts', import.meta.url);
  const materializeModuleUrl = new URL('../src/data/footballDataOrgMaterialize.ts', import.meta.url);
  const sourceModuleUrl = new URL('../src/data/footballDataOrg.ts', import.meta.url);
  const { ingestFootballDataManifests } = await import(ingestModuleUrl.href);
  const { materializeFootballDataOrgCore } = await import(materializeModuleUrl.href);
  const {
    buildFootballDataCompetitionMatchesPath,
    fetchFootballDataJson,
    getDefaultFootballDataCompetitionTargets,
  } = await import(sourceModuleUrl.href);

  const requestedCompetitionCodes = options.competitionCodes ?? getDefaultFootballDataCompetitionTargets().map((target: { code: string }) => target.code);
  const probeSeason = Math.max(...seasons);
  const access: CompetitionAccessProbe = { accessibleCodes: [], restrictedCodes: [] };

  try {
    for (const code of requestedCompetitionCodes) {
      try {
        await fetchFootballDataJson(buildFootballDataCompetitionMatchesPath(code, probeSeason));
        access.accessibleCodes.push(code);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (message.includes('403')) {
          access.restrictedCodes.push(code);
          continue;
        }

        throw error;
      }
    }

    if (access.accessibleCodes.length === 0) {
      throw new Error('No accessible football-data.org competitions are available for the current API key');
    }

    const ingestSummary = await ingestFootballDataManifests({
      dryRun: false,
      competitionCodes: access.accessibleCodes,
      seasons,
    });

    const materializeSummary = await materializeFootballDataOrgCore({
      dryRun: false,
      competitionCodes: access.accessibleCodes,
      seasons,
    });

    console.log(JSON.stringify({
      ok: true,
      ...plan,
      requestedCompetitionCodes,
      accessibleCompetitionCodes: access.accessibleCodes,
      restrictedCompetitionCodes: access.restrictedCodes,
      ingestSummary,
      materializeSummary,
    }, null, 2));
  } catch (error) {
    if (!isFootballDataRateLimitError(error)) {
      throw error;
    }

    console.log(JSON.stringify({
      ok: true,
      skipped: true,
      skippedReason: 'football-data.org rate limited',
      ...plan,
      requestedCompetitionCodes,
      accessibleCompetitionCodes: access.accessibleCodes,
      restrictedCompetitionCodes: access.restrictedCodes,
    }, null, 2));
  }
}

await main();
