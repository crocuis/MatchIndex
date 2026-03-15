async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const argv = process.argv.slice(2);
  const args = new Set(argv);
  const getOption = (name: string) => argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);

  const matchId = getOption('match-id');
  if (!matchId || args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/collect-sofascore-match-artifact.mts --match-id=<id> [--write]`);
    return;
  }

  const moduleUrl = new URL('../src/data/sofascoreMatchArtifacts.ts', import.meta.url);
  const { collectSofascoreMatchArtifacts } = await import(moduleUrl.href);
  const summary = await collectSofascoreMatchArtifacts({
    dryRun: args.has('--write') ? false : true,
    matchId,
    sourceSlug: getOption('source'),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
