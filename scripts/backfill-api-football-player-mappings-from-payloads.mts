function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const moduleUrl = new URL('../src/data/apiFootballPlayerMappingBackfill.ts', import.meta.url);
  const { backfillApiFootballPlayerMappings } = await import(moduleUrl.href);
  const argv = process.argv.slice(2);
  const args = new Set(argv);
  const orderedArgs = argv.filter((arg) => !arg.startsWith('--'));
  const seasons = orderedArgs.map((value) => parsePositiveInt(value)).filter((value): value is number => value !== undefined);

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/backfill-api-football-player-mappings-from-payloads.mts [season ...] [options]

Options:
  season ...   Optional start years to process (default: 2015 through 2026)
  --write       Persist exact name matches into source_entity_mapping
  --help, -h    Show this help message

Required environment:
  DATABASE_URL
`);
    return;
  }

  const summary = await backfillApiFootballPlayerMappings({
    dryRun: args.has('--write') ? false : true,
    seasons,
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
