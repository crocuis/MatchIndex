function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

async function main() {
  const moduleUrl = new URL('../src/data/playerPhotoSeed.ts', import.meta.url);
  const { seedPlayerPhotoFixtures } = await import(moduleUrl.href);
  const args = new Set(process.argv.slice(2));
  const orderedArgs = process.argv.slice(2).filter((arg) => !arg.startsWith('--'));
  const dryRun = args.has('--write') ? false : true;
  const playerArg = process.argv.slice(2).find((arg) => arg.startsWith('--player='));

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/seed-player-photo-fixtures.mts [limit] [options]

Options:
  --write          Persist fixture rows (default: dry-run)
  --player=<slug>  Seed a single player fixture
  --help, -h       Show this help message`);
    return;
  }

  const summary = await seedPlayerPhotoFixtures({
    dryRun,
    limit: parsePositiveInt(orderedArgs[0]),
    playerId: playerArg?.slice('--player='.length),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
