function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

async function main() {
  const moduleUrl = new URL('../src/data/playerPhotoMappings.ts', import.meta.url);
  const { generateApiFootballPlayerMappings } = await import(moduleUrl.href);
  const args = new Set(process.argv.slice(2));
  const orderedArgs = process.argv.slice(2).filter((arg) => !arg.startsWith('--'));
  const dryRun = args.has('--write') ? false : true;
  const playerArg = process.argv.slice(2).find((arg) => arg.startsWith('--player='));

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/generate-api-football-player-mappings.mts [limit] [options]

Options:
  --write          Persist mappings JSON (default: dry-run)
  --player=<slug>  Generate mapping for a single player
  --help, -h       Show this help message

Required environment:
  API_FOOTBALL_KEY                    Direct API-Sports key

Optional environment:
  API_FOOTBALL_BASE_URL               Defaults to https://v3.football.api-sports.io
  API_FOOTBALL_PLAYER_SEARCH_PATH     Defaults to /players?search={search}
  API_FOOTBALL_PLAYER_MAPPINGS_FILE   Defaults to data/api-football-player-mappings.json
  API_FOOTBALL_REQUEST_DELAY_MS       Defaults to 250
`);
    return;
  }

  const summary = await generateApiFootballPlayerMappings({
    dryRun,
    limit: parsePositiveInt(orderedArgs[0]),
    playerId: playerArg?.slice('--player='.length),
  });

  console.log(JSON.stringify(summary, null, 2));
}

await main();
