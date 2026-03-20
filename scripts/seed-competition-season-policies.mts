import postgres from 'postgres';

const CURRENT_SEASON_COMPETITIONS = ['PL', 'BL1', 'PD', 'SA', 'FL1'] as const;
const CURRENT_SEASON = '2024-2025';
const HISTORICAL_COMPETITIONS = ['PL', 'BL1', 'PD', 'SA', 'FL1'] as const;
const FROZEN_AT = '2026-03-15T00:00:00Z';

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const argv = process.argv.slice(2);
  const args = new Set(argv);

  if (args.has('--help') || args.has('-h')) {
    console.log(`Usage: node --experimental-strip-types scripts/seed-competition-season-policies.mts [--write]`);
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
    const results = [];

    for (const competition of CURRENT_SEASON_COMPETITIONS) {
      results.push(await updateCompetitionSeasonPolicy(db, competition, 2024, {
        backfillAllowedSources: ['fbref'],
        owners: {
          matchArtifacts: 'understat+sofascore',
          matchStats: 'sofascore',
          playerContracts: 'sofascore',
          playerSeasonStats: competition === 'PL' ? 'api_football' : 'fbref',
        },
        preferredArtifactSources: ['understat', 'whoscored', 'sofascore'],
      }, args.has('--write') ? false : true));
    }

    for (const competition of HISTORICAL_COMPETITIONS) {
      const historicalRows = await db<{ start_year: number }[]>`
        SELECT DISTINCT EXTRACT(YEAR FROM s.start_date)::INT AS start_year
        FROM competition_seasons cs
        JOIN competitions c ON c.id = cs.competition_id
        JOIN seasons s ON s.id = cs.season_id
        WHERE LOWER(c.code) = LOWER(${competition})
          AND EXTRACT(YEAR FROM s.start_date)::INT <= 2023
        ORDER BY start_year DESC
      `;

      for (const row of historicalRows) {
        results.push(await updateCompetitionSeasonPolicy(db, competition, row.start_year, {
          backfillAllowedSources: ['fbref'],
          freezeReason: 'historical-season',
          frozenAt: FROZEN_AT,
          owners: {
            matchArtifacts: 'statsbomb',
            matchStats: 'sofascore',
            playerContracts: 'fbref',
            playerSeasonStats: 'fbref',
          },
          preferredArtifactSources: ['statsbomb', 'whoscored', 'sofascore'],
        }, args.has('--write') ? false : true));
      }
    }

    console.log(JSON.stringify({ season: CURRENT_SEASON, results }, null, 2));
  } finally {
    await db.end({ timeout: 1 }).catch(() => undefined);
  }
}

await main();
