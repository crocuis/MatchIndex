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

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const moduleUrl = new URL('../src/data/apiFootballCompetitionIngest.ts', import.meta.url);
  const { ingestApiFootballCompetitions } = await import(moduleUrl.href);
  const argv = process.argv.slice(2);
  const args = new Set(argv);
  const orderedArgs = argv.filter((arg) => !arg.startsWith('--'));
  const seasons = orderedArgs.map((value) => parsePositiveInt(value)).filter((value): value is number => value !== undefined);

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/api-football-ingest-competitions.mts [season ...] [options]

Options:
  --competitions=PL,FAC,PD,CDR,BL1,DFP,FL1,CDF,SA,CI,CL,EL
                        Restrict to supported competition codes
  --write               Persist raw payloads and manifests (default: dry-run)
  --help, -h            Show this help message

Default seasons:
  Most recent 2 API-Football season years (for example 2024 2025)

Required environment for --write:
  DATABASE_URL
  API_FOOTBALL_KEY

Optional environment:
  API_FOOTBALL_REQUEST_DELAY_MS      Defaults to 6500
  API_FOOTBALL_RATE_LIMIT_RETRY_MS   Defaults to 65000
`);
    return;
  }

  const summary = await ingestApiFootballCompetitions({
    dryRun: args.has('--write') ? false : true,
    competitionCodes: parseCompetitionCodes(argv),
    seasons,
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
