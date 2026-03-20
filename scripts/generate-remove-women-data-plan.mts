import { writeFileSync } from 'node:fs';

import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  competitionSlugs: string[];
  help: boolean;
  outputPath: string;
}

interface IdRow {
  id: number | string;
}

interface CompetitionRow {
  id: number | string;
  slug: string;
  name: string;
}

interface RemovalPlan {
  competitionIds: number[];
  targetCompetitionSeasonIds: number[];
  targetMatchIds: number[];
  targetTeamIds: number[];
  protectedTeamIds: number[];
  deleteTeamIds: number[];
  targetPlayerIds: number[];
  protectedPlayerIds: number[];
  deletePlayerIds: number[];
}

const DEFAULT_OUTPUT_PATH = '/tmp/remove-women-data-plan.json';

function parseArgs(argv: string[]): CliOptions {
  const competitionSlugs = new Set<string>();
  let help = false;
  let outputPath = DEFAULT_OUTPUT_PATH;

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      help = true;
      continue;
    }

    if (arg.startsWith('--competition=')) {
      const rawValue = arg.slice('--competition='.length).trim();
      for (const slug of rawValue.split(',')) {
        const normalizedSlug = slug.trim();
        if (normalizedSlug) {
          competitionSlugs.add(normalizedSlug);
        }
      }
      continue;
    }

    if (arg.startsWith('--output=')) {
      outputPath = arg.slice('--output='.length).trim() || DEFAULT_OUTPUT_PATH;
    }
  }

  return {
    competitionSlugs: [...competitionSlugs],
    help,
    outputPath,
  };
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/generate-remove-women-data-plan.mts [options]

Options:
  --competition=<slug[,slug]>  특정 대회 slug만 대상으로 제한
  --output=<path>              플랜 JSON 출력 경로 (기본값: ${DEFAULT_OUTPUT_PATH})
  --help, -h                   도움말 출력
`);
}

function getConnectionString() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return connectionString;
}

async function queryUnsafe<T>(query: string) {
  const sql = postgres(getConnectionString(), { max: 1, idle_timeout: 5, prepare: false });

  try {
    return await sql.unsafe<T[]>(query);
  } finally {
    await sql.end({ timeout: 5 });
  }
}

function normalizeIds(rows: IdRow[]) {
  return [...new Set(rows.map((row) => Number(row.id)).filter((id) => Number.isInteger(id) && id > 0))].sort((a, b) => a - b);
}

function normalizeCompetitions(rows: CompetitionRow[]) {
  return rows.map((row) => {
    const id = Number(row.id);
    if (!Number.isInteger(id) || id <= 0) {
      throw new Error(`Invalid competition identifier: ${String(row.id)}`);
    }

    return {
      id,
      slug: row.slug,
      name: row.name,
    };
  });
}

function formatIdList(values: number[]) {
  if (values.length === 0) {
    return '';
  }

  return [...new Set(values)].sort((a, b) => a - b).join(', ');
}

function formatTextList(values: string[]) {
  return values.map((value) => `'${value.replaceAll("'", "''")}'`).join(', ');
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const targetCompetitions = normalizeCompetitions(await queryUnsafe<CompetitionRow>(
    options.competitionSlugs.length > 0
      ? `
        SELECT c.id, c.slug, COALESCE(ct_en.name, c.slug) AS name
        FROM competitions c
        LEFT JOIN competition_translations ct_en
          ON ct_en.competition_id = c.id
         AND ct_en.locale = 'en'
        WHERE c.slug IN (${formatTextList(options.competitionSlugs)})
        ORDER BY c.slug ASC
      `
      : `
        SELECT c.id, c.slug, COALESCE(ct_en.name, c.slug) AS name
        FROM competitions c
        LEFT JOIN competition_translations ct_en
          ON ct_en.competition_id = c.id
         AND ct_en.locale = 'en'
        WHERE c.gender = 'female'
        ORDER BY c.slug ASC
      `
  ));

  if (targetCompetitions.length === 0) {
    throw new Error('No target competitions found');
  }

  const competitionIds = targetCompetitions.map((competition) => competition.id);
  const competitionIdList = formatIdList(competitionIds);

  const targetCompetitionSeasonIds = normalizeIds(await queryUnsafe<IdRow>(`
    SELECT id
    FROM competition_seasons
    WHERE competition_id IN (${competitionIdList})
  `));
  const competitionSeasonIdList = formatIdList(targetCompetitionSeasonIds);

  const targetMatchIds = competitionSeasonIdList
    ? normalizeIds(await queryUnsafe<IdRow>(`
        SELECT id
        FROM matches
        WHERE competition_season_id IN (${competitionSeasonIdList})
      `))
    : [];
  const matchIdList = formatIdList(targetMatchIds);

  const targetTeamIds = competitionSeasonIdList
    ? normalizeIds(await queryUnsafe<IdRow>(`
        SELECT DISTINCT team_id AS id
        FROM (
          SELECT ts.team_id
          FROM team_seasons ts
          WHERE ts.competition_season_id IN (${competitionSeasonIdList})
          UNION
          SELECT pc.team_id
          FROM player_contracts pc
          WHERE pc.competition_season_id IN (${competitionSeasonIdList})
          UNION
          SELECT m.home_team_id AS team_id
          FROM matches m
          WHERE m.competition_season_id IN (${competitionSeasonIdList})
          UNION
          SELECT m.away_team_id AS team_id
          FROM matches m
          WHERE m.competition_season_id IN (${competitionSeasonIdList})
          UNION
          SELECT cs.winner_team_id AS team_id
          FROM competition_seasons cs
          WHERE cs.id IN (${competitionSeasonIdList})
        ) team_refs
        WHERE team_id IS NOT NULL
      `))
    : [];

  const deleteTeamIds = competitionSeasonIdList
    ? normalizeIds(await queryUnsafe<IdRow>(`
        WITH target_teams AS (
          SELECT DISTINCT team_id AS id
          FROM (
            SELECT ts.team_id
            FROM team_seasons ts
            WHERE ts.competition_season_id IN (${competitionSeasonIdList})
            UNION
            SELECT pc.team_id
            FROM player_contracts pc
            WHERE pc.competition_season_id IN (${competitionSeasonIdList})
            UNION
            SELECT m.home_team_id AS team_id
            FROM matches m
            WHERE m.competition_season_id IN (${competitionSeasonIdList})
            UNION
            SELECT m.away_team_id AS team_id
            FROM matches m
            WHERE m.competition_season_id IN (${competitionSeasonIdList})
            UNION
            SELECT cs.winner_team_id AS team_id
            FROM competition_seasons cs
            WHERE cs.id IN (${competitionSeasonIdList})
          ) refs
          WHERE team_id IS NOT NULL
        )
        SELECT tt.id
        FROM target_teams tt
        WHERE NOT EXISTS (
            SELECT 1
            FROM team_seasons ts
            WHERE ts.team_id = tt.id
              AND ts.competition_season_id NOT IN (${competitionSeasonIdList})
          )
          AND NOT EXISTS (
            SELECT 1
            FROM player_contracts pc
            WHERE pc.team_id = tt.id
              AND pc.competition_season_id NOT IN (${competitionSeasonIdList})
          )
          AND NOT EXISTS (
            SELECT 1
            FROM matches m
            WHERE (m.home_team_id = tt.id OR m.away_team_id = tt.id)
              AND m.competition_season_id NOT IN (${competitionSeasonIdList})
          )
          AND NOT EXISTS (
            SELECT 1
            FROM competition_seasons cs
            WHERE cs.winner_team_id = tt.id
              AND cs.id NOT IN (${competitionSeasonIdList})
          )
      `))
    : [];
  const protectedTeamIds = targetTeamIds.filter((id) => !new Set(deleteTeamIds).has(id));
  const targetTeamIdList = formatIdList(targetTeamIds);

  const targetPlayerIds = normalizeIds(await queryUnsafe<IdRow>(`
    SELECT DISTINCT id
    FROM (
      ${competitionSeasonIdList
        ? `SELECT pc.player_id AS id FROM player_contracts pc WHERE pc.competition_season_id IN (${competitionSeasonIdList})`
        : 'SELECT NULL::bigint AS id WHERE FALSE'}
      UNION
      ${matchIdList
        ? `SELECT ml.player_id AS id FROM match_lineups ml WHERE ml.match_id IN (${matchIdList})`
        : 'SELECT NULL::bigint AS id WHERE FALSE'}
      UNION
      ${targetTeamIdList
        ? `SELECT pmv.player_id AS id FROM player_market_values pmv WHERE pmv.club_id IN (${targetTeamIdList})`
        : 'SELECT NULL::bigint AS id WHERE FALSE'}
      UNION
      ${targetTeamIdList
        ? `SELECT pt.player_id AS id FROM player_transfers pt WHERE pt.from_team_id IN (${targetTeamIdList}) OR pt.to_team_id IN (${targetTeamIdList})`
        : 'SELECT NULL::bigint AS id WHERE FALSE'}
    ) player_refs
    WHERE id IS NOT NULL
  `));

  const deletePlayerIds = competitionSeasonIdList
    ? normalizeIds(await queryUnsafe<IdRow>(`
        WITH target_players AS (
          SELECT DISTINCT id
          FROM (
            SELECT pc.player_id AS id
            FROM player_contracts pc
            WHERE pc.competition_season_id IN (${competitionSeasonIdList})
            UNION
            SELECT ml.player_id AS id
            FROM match_lineups ml
            JOIN matches m ON m.id = ml.match_id AND m.match_date = ml.match_date
            WHERE m.competition_season_id IN (${competitionSeasonIdList})
            UNION
            ${targetTeamIdList
              ? `SELECT pmv.player_id AS id FROM player_market_values pmv WHERE pmv.club_id IN (${targetTeamIdList})`
              : 'SELECT NULL::bigint AS id WHERE FALSE'}
            UNION
            ${targetTeamIdList
              ? `SELECT pt.player_id AS id FROM player_transfers pt WHERE pt.from_team_id IN (${targetTeamIdList}) OR pt.to_team_id IN (${targetTeamIdList})`
              : 'SELECT NULL::bigint AS id WHERE FALSE'}
          ) refs
          WHERE id IS NOT NULL
        )
        SELECT tp.id
        FROM target_players tp
        WHERE NOT EXISTS (
            SELECT 1
            FROM player_contracts pc
            WHERE pc.player_id = tp.id
              AND pc.competition_season_id NOT IN (${competitionSeasonIdList})
          )
          AND NOT EXISTS (
            SELECT 1
            FROM match_lineups ml
            JOIN matches m ON m.id = ml.match_id AND m.match_date = ml.match_date
            WHERE ml.player_id = tp.id
              AND m.competition_season_id NOT IN (${competitionSeasonIdList})
          )
      `))
    : [];

  const protectedPlayerIds = targetPlayerIds.filter((id) => !new Set(deletePlayerIds).has(id));

  const plan: RemovalPlan = {
    competitionIds,
    targetCompetitionSeasonIds,
    targetMatchIds,
    targetTeamIds,
    protectedTeamIds,
    deleteTeamIds,
    targetPlayerIds,
    protectedPlayerIds,
    deletePlayerIds,
  };

  writeFileSync(options.outputPath, JSON.stringify(plan, null, 2));
  console.log(JSON.stringify({ outputPath: options.outputPath, plan }, null, 2));
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
