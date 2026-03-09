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

  const moduleUrl = new URL('../src/data/apiFootballPlayerMappingSync.ts', import.meta.url);
  const { syncApiFootballPlayerMappings } = await import(moduleUrl.href);
  const argv = process.argv.slice(2);
  const args = new Set(argv);
  const orderedArgs = argv.filter((arg) => !arg.startsWith('--'));
  const playerArg = argv.find((arg) => arg.startsWith('--player='));

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/sync-api-football-player-mappings.mts [limit] [options]

Options:
  --write          Persist mappings into source_entity_mapping (default: dry-run)
  --player=<slug>  Sync mapping for a single player
  --help, -h       Show this help message
`);
    return;
  }

  const summary = await syncApiFootballPlayerMappings({
    dryRun: args.has('--write') ? false : true,
    limit: parsePositiveInt(orderedArgs[0]),
    playerId: playerArg?.slice('--player='.length),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
