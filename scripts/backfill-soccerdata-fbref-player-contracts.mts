async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const argv = process.argv.slice(2);
  const args = new Set(argv);

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/backfill-soccerdata-fbref-player-contracts.mts [options]

Options:
  --competition=PL   Restrict to one competition code
  --season=2024-2025 Restrict to one season label
  --write            Persist player_contracts/team_seasons
  --help, -h         Show this help message
`);
    return;
  }

  const option = (name: string) => argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);

  const moduleUrl = new URL('../src/data/soccerdataFbrefContractsBackfill.ts', import.meta.url);
  const { backfillSoccerdataFbrefContracts } = await import(moduleUrl.href);
  const summary = await backfillSoccerdataFbrefContracts({
    competitionCode: option('competition'),
    dryRun: args.has('--write') ? false : true,
    season: option('season'),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
