function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseNamedInt(args: string[], name: string) {
  const index = args.indexOf(name);
  if (index === -1) {
    return undefined;
  }

  return parsePositiveInt(args[index + 1]);
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();
  const moduleUrl = new URL('../src/data/statsbombMaterializeDetails.ts', import.meta.url);
  const { materializeStatsBombDetails } = await import(moduleUrl.href);
  const args = new Set(process.argv.slice(2));
  const orderedArgs = process.argv.slice(2).filter((arg) => !arg.startsWith('--'));

  const summary = await materializeStatsBombDetails({
    dryRun: !args.has('--write'),
    competitionLimit: parsePositiveInt(orderedArgs[0]),
    matchesPerSeasonLimit: parsePositiveInt(orderedArgs[1]),
    competitionOffset: parseNamedInt(process.argv.slice(2), '--competition-offset'),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
