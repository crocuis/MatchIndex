async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const argv = process.argv.slice(2);
  const args = new Set(argv);

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/sync-fbref-player-mappings.mts [options]

Options:
  --file=data/fbref-player-mappings.json  Mapping file path
  --source=soccerdata_fbref               Target source slug
  --write                                 Persist source_entity_mapping rows
`);
    return;
  }

  const getOption = (name: string) => argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  const moduleUrl = new URL('../src/data/fbrefPlayerMappingSync.ts', import.meta.url);
  const { syncFbrefPlayerMappings } = await import(moduleUrl.href);
  const summary = await syncFbrefPlayerMappings({
    dryRun: args.has('--write') ? false : true,
    filePath: getOption('file'),
    sourceSlug: getOption('source'),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
