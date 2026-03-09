function parsePositiveInt(value: string | undefined, fallbackValue: number) {
  if (!value) {
    return fallbackValue;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallbackValue;
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();
  const statsbombModuleUrl = new URL('../src/data/statsbomb.ts', import.meta.url);
  const { listCompetitionSeasons, listMatches } = await import(statsbombModuleUrl.href);
  const command = process.argv[2] ?? 'competitions';

  if (command === 'competitions') {
    const limit = parsePositiveInt(process.argv[3], 5);
    const competitions = await listCompetitionSeasons();
    console.log(JSON.stringify(competitions.slice(0, limit), null, 2));
    return;
  }

  if (command === 'matches') {
    const competitionId = process.argv[3];
    const seasonId = process.argv[4];
    const limit = parsePositiveInt(process.argv[5], 5);

    if (!competitionId || !seasonId) {
      throw new Error('Usage: statsbomb:preview matches <competitionId> <seasonId> [limit]');
    }

    const matches = await listMatches(competitionId, seasonId);
    console.log(JSON.stringify(matches.slice(0, limit), null, 2));
    return;
  }

  throw new Error('Usage: statsbomb:preview [competitions [limit] | matches <competitionId> <seasonId> [limit]]');
}

await main();
