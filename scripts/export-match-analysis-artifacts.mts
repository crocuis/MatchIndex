interface CliOptions {
  dryRun: boolean;
  force: boolean;
  help: boolean;
  limit?: number;
  matchId?: string;
}

function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: !argv.includes('--write'),
    force: argv.includes('--force'),
    help: argv.includes('--help'),
  };

  for (const arg of argv) {
    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
      continue;
    }

    if (arg.startsWith('--match-id=')) {
      const value = arg.slice('--match-id='.length).trim();
      options.matchId = value || undefined;
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/export-match-analysis-artifacts.mts [options]

Options:
  --write              Write JSON.gz files and upsert DB metadata
  --force              Rebuild artifacts even if metadata already exists
  --match-id=<id>      Export a single match
  --limit=<n>          Limit number of target matches
  --help               Show this help message`);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const moduleUrl = new URL('./export-match-analysis-artifacts-lib.mts', import.meta.url);
  const { exportMatchAnalysisArtifacts } = await import(moduleUrl.href);
  const summary = await exportMatchAnalysisArtifacts(options);
  console.log(JSON.stringify(summary, null, 2));
}

await main();
