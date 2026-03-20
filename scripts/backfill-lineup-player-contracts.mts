import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

const BATCH_SIZE = 500;

interface CliOptions {
  competitionSlug?: string;
  dryRun: boolean;
  help: boolean;
  seasonSlug?: string;
  teamSlug?: string;
}

interface ContractDraftRow {
  competition_season_id: number;
  player_id: number;
  shirt_number: number | null;
  team_id: number;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { dryRun: true, help: false };
  for (const arg of argv) {
    if (arg === '--write') {
      options.dryRun = false;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--competition=')) {
      options.competitionSlug = arg.slice('--competition='.length).trim();
      continue;
    }
    if (arg.startsWith('--season=')) {
      options.seasonSlug = arg.slice('--season='.length).trim();
      continue;
    }
    if (arg.startsWith('--team=')) {
      options.teamSlug = arg.slice('--team='.length).trim();
    }
  }
  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/backfill-lineup-player-contracts.mts --competition=<slug> --season=<slug> --team=<slug> [options]

Options:
  --competition=<slug>  Internal competition slug
  --season=<slug>       Internal season slug
  --team=<slug>         Internal team slug
  --write               Persist player_contracts/team_seasons
  --help, -h            Show this help message
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    max: 1,
    idle_timeout: 5,
    prepare: false,
  });
}

async function loadDrafts(
  sql: ReturnType<typeof postgres>,
  options: Required<Pick<CliOptions, 'competitionSlug' | 'seasonSlug' | 'teamSlug'>>,
) {
  return sql<ContractDraftRow[]>`
    WITH target_context AS (
      SELECT
        cs.id AS competition_season_id,
        team.id AS team_id
      FROM teams team
      JOIN matches m ON m.home_team_id = team.id OR m.away_team_id = team.id
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      JOIN seasons s ON s.id = cs.season_id
      WHERE team.slug = ${options.teamSlug}
        AND c.slug = ${options.competitionSlug}
        AND s.slug = ${options.seasonSlug}
      GROUP BY cs.id, team.id
      ORDER BY cs.id DESC
      LIMIT 1
    )
    SELECT DISTINCT ON (ml.player_id)
      tc.competition_season_id,
      ml.player_id,
      ml.shirt_number,
      tc.team_id
    FROM target_context tc
    JOIN matches m ON m.competition_season_id = tc.competition_season_id
    JOIN match_lineups ml ON ml.match_id = m.id AND ml.team_id = tc.team_id
    WHERE m.status <> 'scheduled'
    ORDER BY ml.player_id, m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
  `;
}

async function upsertDraft(sql: ReturnType<typeof postgres>, draft: ContractDraftRow) {
  await sql`
    INSERT INTO player_contracts (player_id, team_id, competition_season_id, shirt_number, is_on_loan, left_date, updated_at)
    VALUES (${draft.player_id}, ${draft.team_id}, ${draft.competition_season_id}, ${draft.shirt_number}, FALSE, NULL, NOW())
    ON CONFLICT (player_id, competition_season_id)
    DO UPDATE SET
      team_id = EXCLUDED.team_id,
      shirt_number = COALESCE(EXCLUDED.shirt_number, player_contracts.shirt_number),
      is_on_loan = EXCLUDED.is_on_loan,
      left_date = EXCLUDED.left_date,
      updated_at = NOW()
  `;

  await sql`
    INSERT INTO team_seasons (team_id, competition_season_id, updated_at)
    VALUES (${draft.team_id}, ${draft.competition_season_id}, NOW())
    ON CONFLICT (team_id, competition_season_id)
    DO UPDATE SET updated_at = NOW()
  `;
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  if (!options.competitionSlug || !options.seasonSlug || !options.teamSlug) {
    throw new Error('--competition, --season, and --team are required');
  }

  const sql = getSql();
  try {
    const drafts = await loadDrafts(sql, {
      competitionSlug: options.competitionSlug,
      seasonSlug: options.seasonSlug,
      teamSlug: options.teamSlug,
    });

    if (!options.dryRun) {
      await sql`BEGIN`;
      try {
        for (let i = 0; i < drafts.length; i += BATCH_SIZE) {
          const chunk = drafts.slice(i, i + BATCH_SIZE);
          await sql`
            INSERT INTO player_contracts (player_id, team_id, competition_season_id, shirt_number, is_on_loan, left_date, updated_at)
            SELECT t.player_id, t.team_id, t.competition_season_id, t.shirt_number, FALSE, NULL, NOW()
            FROM UNNEST(
              ${sql.array(chunk.map(d => d.player_id))}::int[],
              ${sql.array(chunk.map(d => d.team_id))}::int[],
              ${sql.array(chunk.map(d => d.competition_season_id))}::int[],
              ${sql.array(chunk.map(d => d.shirt_number))}::int[]
            ) AS t(player_id, team_id, competition_season_id, shirt_number)
            ON CONFLICT (player_id, competition_season_id)
            DO UPDATE SET
              team_id = EXCLUDED.team_id,
              shirt_number = COALESCE(EXCLUDED.shirt_number, player_contracts.shirt_number),
              is_on_loan = EXCLUDED.is_on_loan,
              left_date = EXCLUDED.left_date,
              updated_at = NOW()
          `;
          const uniqueTeamSeasons = [
            ...new Map(chunk.map(d => [`${d.team_id}:${d.competition_season_id}`, d])).values(),
          ];
          await sql`
            INSERT INTO team_seasons (team_id, competition_season_id, updated_at)
            SELECT t.team_id, t.competition_season_id, NOW()
            FROM UNNEST(
              ${sql.array(uniqueTeamSeasons.map(d => d.team_id))}::int[],
              ${sql.array(uniqueTeamSeasons.map(d => d.competition_season_id))}::int[]
            ) AS t(team_id, competition_season_id)
            ON CONFLICT (team_id, competition_season_id)
            DO UPDATE SET updated_at = NOW()
          `;
        }
        await sql`COMMIT`;
      } catch (error) {
        await sql`ROLLBACK`;
        throw error;
      }
    }

    console.log(JSON.stringify({
      dryRun: options.dryRun,
      competitionSlug: options.competitionSlug,
      seasonSlug: options.seasonSlug,
      teamSlug: options.teamSlug,
      contractRowsPlanned: drafts.length,
      contractRowsWritten: options.dryRun ? 0 : drafts.length,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
