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
    console.log(`Usage: node --experimental-strip-types scripts/soccerdata-materialize-fbref.mts [options]

Options:
  --source=soccerdata_fbref  Source slug (default: soccerdata_fbref)
  --competition=PL           Competition code
  --season=2023             Completed season label or start year
  --write                   Reserved for future canonical materialize mode (default: dry-run)
  --help, -h                Show this help message
`);
    return;
  }

  const moduleUrl = new URL('../src/data/soccerdataFbrefMaterialize.ts', import.meta.url);
  const { materializeSoccerdataFbref } = await import(moduleUrl.href);
  const summary = await materializeSoccerdataFbref({
    competitionCode: parseOption(argv, 'competition'),
    dryRun: args.has('--write') ? false : true,
    season: parseOption(argv, 'season'),
    sourceSlug: parseOption(argv, 'source'),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
