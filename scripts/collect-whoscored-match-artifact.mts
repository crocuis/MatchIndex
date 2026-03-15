async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const argv = process.argv.slice(2);
  const args = new Set(argv);
  const getOption = (name: string) => argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);

  const matchId = getOption('match-id');
  const whoscoredUrl = getOption('whoscored-url');
  if (!matchId || !whoscoredUrl || args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/collect-whoscored-match-artifact.mts --match-id=<id> --whoscored-url=<url> [--write]`);
    return;
  }

  const moduleUrl = new URL('../src/data/whoscoredMatchArtifacts.ts', import.meta.url);
  const { collectWhoScoredMatchArtifacts } = await import(moduleUrl.href);
  const summary = await collectWhoScoredMatchArtifacts({
    dryRun: args.has('--write') ? false : true,
    matchId,
    whoscoredUrl,
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
