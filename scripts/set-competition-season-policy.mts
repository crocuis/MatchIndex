import postgres from 'postgres';

function parseYear(season: string) {
  const match = season.match(/(\d{4})/);
  if (!match) {
    throw new Error(`Unable to parse season year from '${season}'`);
  }
  return Number.parseInt(match[1], 10);
}

function parseList(value?: string) {
  return value ? value.split(',').map((item) => item.trim().toLowerCase()).filter(Boolean) : undefined;
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const argv = process.argv.slice(2);
  const args = new Set(argv);
  const getOption = (name: string) => argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  const competition = getOption('competition');
  const season = getOption('season');

  if (!competition || !season || argv.includes('--help') || argv.includes('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/set-competition-season-policy.mts --competition=PL --season=2024-2025 [options]

Options:
  --player-season-stats-owner=api_football
  --player-contracts-owner=sofascore
  --match-stats-owner=sofascore
  --match-artifacts-owner=understat+sofascore
  --preferred-artifact-sources=understat,whoscored,sofascore
  --backfill-allowed-sources=fbref
  --frozen-at=2026-06-01T00:00:00Z
  --freeze-reason=season-finalized
  --write
`);
    return;
  }

  const moduleUrl = new URL('../src/data/sourceOwnership.ts', import.meta.url);
  const { updateCompetitionSeasonPolicy } = await import(moduleUrl.href);
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }
  const db = postgres(connectionString, { max: 1, idle_timeout: 20, prepare: false });

  try {
    const result = await updateCompetitionSeasonPolicy(db, competition!, parseYear(season), {
      backfillAllowedSources: parseList(getOption('backfill-allowed-sources')),
      freezeReason: getOption('freeze-reason') ?? undefined,
      frozenAt: getOption('frozen-at') ?? undefined,
      owners: {
        matchArtifacts: getOption('match-artifacts-owner') ?? undefined,
        matchStats: getOption('match-stats-owner') ?? undefined,
        playerContracts: getOption('player-contracts-owner') ?? undefined,
        playerSeasonStats: getOption('player-season-stats-owner') ?? undefined,
      },
      preferredArtifactSources: parseList(getOption('preferred-artifact-sources')),
    }, args.has('--write') ? false : true);

    console.log(JSON.stringify(result, null, 2));
  } finally {
    await db.end({ timeout: 1 }).catch(() => undefined);
  }
}

await main();
