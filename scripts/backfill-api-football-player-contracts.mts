function parsePositiveInt(value: string | undefined) {
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseCompetitionCodes(argv: string[]) {
  const raw = argv.find((arg) => arg.startsWith('--competitions='));
  if (!raw) return undefined;
  return raw.slice('--competitions='.length).split(',').map((code) => code.trim().toUpperCase()).filter(Boolean);
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const moduleUrl = new URL('../src/data/apiFootballPlayerContractsBackfill.ts', import.meta.url);
  const { backfillApiFootballPlayerContracts } = await import(moduleUrl.href);
  const argv = process.argv.slice(2);
  const args = new Set(argv);
  const orderedArgs = argv.filter((arg) => !arg.startsWith('--'));
  const seasons = orderedArgs.map((value) => parsePositiveInt(value)).filter((value): value is number => value !== undefined);

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/backfill-api-football-player-contracts.mts [season ...] [options]

Options:
  --competitions=PL,PD   Restrict to supported competition codes
  --write                Persist player_contracts/team_seasons
  --help, -h             Show this help message
`);
    return;
  }

  const summary = await backfillApiFootballPlayerContracts({
    dryRun: args.has('--write') ? false : true,
    competitionCodes: parseCompetitionCodes(argv),
    seasons,
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
