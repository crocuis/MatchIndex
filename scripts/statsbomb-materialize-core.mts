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
  const moduleUrl = new URL('../src/data/statsbombMaterialize.ts', import.meta.url);
  const { materializeStatsBombCore } = await import(moduleUrl.href);
  const args = new Set(process.argv.slice(2));
  const orderedArgs = process.argv.slice(2).filter((arg) => !arg.startsWith('--'));

  const summary = await materializeStatsBombCore({
    dryRun: !args.has('--write'),
    competitionLimit: parsePositiveInt(orderedArgs[0]),
    matchesPerSeasonLimit: parsePositiveInt(orderedArgs[1]),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
