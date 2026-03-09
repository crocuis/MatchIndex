function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

async function main() {
  const moduleUrl = new URL('../src/data/playerPhotoIngest.ts', import.meta.url);
  const { backfillApiFootballPlayerPhotoSourceIds } = await import(moduleUrl.href);
  const args = new Set(process.argv.slice(2));
  const orderedArgs = process.argv.slice(2).filter((arg) => !arg.startsWith('--'));
  const dryRun = args.has('--write') ? false : true;
  const playerArg = process.argv.slice(2).find((arg) => arg.startsWith('--player='));

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/backfill-player-photo-source-ids.mts [limit] [options]

Options:
  --write          Persist backfill results (default: dry-run)
  --player=<slug>  Backfill a single player
  --help, -h       Show this help message`);
    return;
  }

  const summary = await backfillApiFootballPlayerPhotoSourceIds({
    dryRun,
    limit: parsePositiveInt(orderedArgs[0]),
    playerId: playerArg?.slice('--player='.length),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
