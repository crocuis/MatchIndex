import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface ProfileResult {
  name: string;
  elapsedMs: number;
  executionMs: number | null;
  planningMs: number | null;
  totalCost: number | null;
  nodeType: string | null;
}

function getPlanMetric(value: unknown) {
  return typeof value === 'number' ? value : null;
}

async function main() {
  loadProjectEnv();

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(connectionString, { max: 1, prepare: false });

  const leagueIds = "'la-liga','premier-league','1-bundesliga','serie-a','ligue-1','champions-league','europa-league'";
  const queries = [
    {
      name: 'getLeaguesDb core',
      sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        WITH latest_competition_seasons AS (
          SELECT DISTINCT ON (cs.competition_id)
            cs.id,
            cs.competition_id,
            cs.season_id,
            COUNT(DISTINCT ts.team_id)::INT AS participant_count,
            COUNT(DISTINCT m.id)::INT AS match_count
          FROM competition_seasons cs
          JOIN seasons s ON s.id = cs.season_id
          LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
          LEFT JOIN matches m ON m.competition_season_id = cs.id
          GROUP BY cs.id, cs.competition_id, cs.season_id, s.end_date, s.start_date, s.id
          ORDER BY cs.competition_id,
            CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
            s.end_date DESC NULLS LAST,
            s.start_date DESC NULLS LAST,
            s.id DESC
        )
        SELECT c.slug AS id
        FROM competitions c
        LEFT JOIN countries country ON country.id = c.country_id
        JOIN latest_competition_seasons lcs ON lcs.competition_id = c.id
        JOIN seasons s ON s.id = lcs.season_id
        GROUP BY c.id, country.id, s.id, lcs.participant_count
        ORDER BY c.slug ASC`,
    },
    {
      name: 'getRecentFinishedMatchesByLeagueIdsDb core',
      sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT m.id
        FROM matches m
        JOIN teams home ON home.id = m.home_team_id
        JOIN teams away ON away.id = m.away_team_id
        JOIN competition_seasons cs ON cs.id = m.competition_season_id
        JOIN competitions c ON c.id = cs.competition_id
        LEFT JOIN venues v ON v.id = m.venue_id
        WHERE c.slug = ANY(ARRAY[${leagueIds}])
          AND m.status IN ('finished', 'finished_aet', 'finished_pen')
        ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
        LIMIT 6`,
    },
    {
      name: 'getUpcomingScheduledMatchesByLeagueIdsDb core',
      sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT m.id
        FROM matches m
        JOIN teams home ON home.id = m.home_team_id
        JOIN teams away ON away.id = m.away_team_id
        JOIN competition_seasons cs ON cs.id = m.competition_season_id
        JOIN competitions c ON c.id = cs.competition_id
        LEFT JOIN venues v ON v.id = m.venue_id
        WHERE c.slug = ANY(ARRAY[${leagueIds}])
          AND m.status IN ('scheduled', 'timed')
        ORDER BY m.match_date ASC, m.kickoff_at ASC NULLS LAST, m.id ASC
        LIMIT 72`,
    },
    {
      name: 'getStandingsByLeagueDb core',
      sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        WITH latest_competition_season AS (
          SELECT cs.id
          FROM competition_seasons cs
          JOIN competitions c ON c.id = cs.competition_id
          JOIN seasons s ON s.id = cs.season_id
          LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
          LEFT JOIN matches m ON m.competition_season_id = cs.id
          WHERE c.slug = 'la-liga'
          GROUP BY cs.id, s.end_date, s.start_date, s.id
          ORDER BY CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
            s.end_date DESC NULLS LAST,
            s.start_date DESC NULLS LAST,
            s.id DESC
          LIMIT 1
        )
        SELECT standings.position
        FROM mv_standings standings
        JOIN latest_competition_season lcs ON lcs.id = standings.competition_season_id
        JOIN teams team ON team.id = standings.team_id
        LEFT JOIN mv_team_form form
          ON form.competition_season_id = standings.competition_season_id
          AND form.team_id = standings.team_id
        ORDER BY standings.position ASC`,
    },
    {
      name: 'getTopScorerRowsDb core',
      sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        WITH latest_competition_season AS (
          SELECT cs.id
          FROM competition_seasons cs
          JOIN competitions c ON c.id = cs.competition_id
          JOIN seasons s ON s.id = cs.season_id
          LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
          LEFT JOIN matches m ON m.competition_season_id = cs.id
          WHERE c.slug = 'la-liga'
          GROUP BY cs.id, s.end_date, s.start_date, s.id
          ORDER BY CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
            s.end_date DESC NULLS LAST,
            s.start_date DESC NULLS LAST,
            s.id DESC
          LIMIT 1
        )
        SELECT player.slug AS player_id
        FROM mv_top_scorers scorers
        JOIN latest_competition_season lcs ON lcs.id = scorers.competition_season_id
        JOIN players player ON player.id = scorers.player_id
        JOIN teams team ON team.id = scorers.team_id
        ORDER BY scorers.rank ASC
        LIMIT 5`,
    },
    {
      name: 'getDashboardTournamentSummaryDb core',
      sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        WITH latest_competition_season AS (
          SELECT cs.id
          FROM competition_seasons cs
          JOIN competitions c ON c.id = cs.competition_id
          JOIN seasons s ON s.id = cs.season_id
          LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
          LEFT JOIN matches m ON m.competition_season_id = cs.id
          WHERE c.slug = 'champions-league'
          GROUP BY cs.id, s.end_date, s.start_date, s.id
          ORDER BY CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
            s.end_date DESC NULLS LAST,
            s.start_date DESC NULLS LAST,
            s.id DESC
          LIMIT 1
        )
        SELECT m.id
        FROM latest_competition_season lcs
        JOIN matches m ON m.competition_season_id = lcs.id
        WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')
        ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
        LIMIT 2`,
    },
  ];

  const results: ProfileResult[] = [];

  try {
    for (const query of queries) {
      const started = performance.now();
      const rows = await sql.unsafe(query.sql);
      const elapsedMs = Math.round(performance.now() - started);
      const explain = rows[0]?.['QUERY PLAN']?.[0] as Record<string, unknown> | undefined;
      const plan = explain?.Plan as Record<string, unknown> | undefined;

      results.push({
        name: query.name,
        elapsedMs,
        executionMs: getPlanMetric(explain?.['Execution Time']),
        planningMs: getPlanMetric(explain?.['Planning Time']),
        totalCost: getPlanMetric(plan?.['Total Cost']),
        nodeType: typeof plan?.['Node Type'] === 'string' ? plan['Node Type'] : null,
      });
    }

    console.log(JSON.stringify(results, null, 2));
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
