function parseOption(argv: string[], name: string) {
  const raw = argv.find((arg) => arg.startsWith(`--${name}=`));
  return raw ? raw.slice(name.length + 3) : undefined;
}

function parseCategories(argv: string[]) {
  const raw = parseOption(argv, 'categories');

  if (!raw) {
    return undefined;
  }

  return raw
    .split(',')
    .map((category) => category.trim().toLowerCase())
    .filter(Boolean);
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const argv = process.argv.slice(2);
  const args = new Set(argv);

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/fbref-ingest-player-stats.mts [options]

Options:
  --source=fbref_scrape         Source slug (default: fbref_scrape)
  --competition=PL              Competition code
  --season=2024-2025            Season label
  --categories=standard,passing Comma-separated FBref stat categories
  --write                       Register source + sync run scaffold (default: dry-run)
  --help, -h                    Show this help message

Required environment for --write:
  DATABASE_URL
`);
    return;
  }

  const moduleUrl = new URL('../src/data/fbrefPlayerStatsIngest.ts', import.meta.url);
  const { ingestFbrefPlayerStats } = await import(moduleUrl.href);
  const summary = await ingestFbrefPlayerStats({
    competitionCode: parseOption(argv, 'competition'),
    dryRun: args.has('--write') ? false : true,
    season: parseOption(argv, 'season'),
    sourceSlug: parseOption(argv, 'source'),
    statCategories: parseCategories(argv),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
