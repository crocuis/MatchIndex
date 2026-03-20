function parseOption(argv: string[], name: string) {
  const raw = argv.find((arg) => arg.startsWith(`--${name}=`));
  return raw ? raw.slice(name.length + 3) : undefined;
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const argv = process.argv.slice(2);
  const args = new Set(argv);

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/soccerdata-materialize-sofascore-details.mts [options]

Options:
  --competition=PL           Competition code (PL, PD, BL1, FL1, SA, UCL, UEL; default: UEL)
  --season=2025-2026        Season label
  --source=soccerdata_sofascore  Source slug (default: soccerdata_sofascore)
  --write                   Persist canonical detail rows and player_contracts
  --help, -h                Show this help message
`);
    return;
  }

  const moduleUrl = new URL('../src/data/sofascoreDetailsMaterialize.ts', import.meta.url);
  const { materializeSofascoreDetails } = await import(moduleUrl.href);
  const summary = await materializeSofascoreDetails({
    competitionCodes: [parseOption(argv, 'competition') ?? 'UEL'],
    dryRun: args.has('--write') ? false : true,
    seasonLabel: parseOption(argv, 'season') ?? '2025-2026',
    sourceSlug: parseOption(argv, 'source') ?? 'soccerdata_sofascore',
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
