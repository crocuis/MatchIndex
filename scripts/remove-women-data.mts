import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  competitionSlugs: string[];
  help: boolean;
  planOut: string | null;
  write: boolean;
}

interface CompetitionRow {
  id: number | string;
  slug: string;
  name: string;
}

interface CountRow {
  count: number | string;
}

interface IdRow {
  id: number | string;
}

const ID_BATCH_SIZE = 50;
const PLAYER_DELETE_BATCH_SIZE = 10;

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

function parseArgs(argv: string[]): CliOptions {
  const competitionSlugs = new Set<string>();
  let help = false;
  let planOut: string | null = null;
  let write = false;

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      help = true;
      continue;
    }

    if (arg === '--write') {
      write = true;
      continue;
    }

    if (arg.startsWith('--plan-out=')) {
      const filePath = arg.slice('--plan-out='.length).trim();
      planOut = filePath || null;
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
    }
  }

  return {
    competitionSlugs: [...competitionSlugs],
    help,
    planOut,
    write,
  };
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/remove-women-data.mts [options]

기본 동작:
  - 모든 여자 대회(competitions.gender = 'female')를 제거 대상으로 잡는다.
  - 제거 대상 대회에만 연결된 팀/선수만 삭제한다.
  - 다른 비대상 대회와 연결된 팀/선수는 보존한다.

Options:
  --competition=<slug[,slug]>  특정 대회 slug만 제거 대상으로 제한
  --plan-out=<path>            계산된 삭제 ID 플랜을 JSON 파일로 저장
  --write                      실제 DB 반영 (기본값은 dry-run)
  --help, -h                   도움말 출력

Examples:
  node --experimental-strip-types scripts/remove-women-data.mts
  node --experimental-strip-types scripts/remove-women-data.mts --plan-out=/tmp/remove-women-data-plan.json
  node --experimental-strip-types scripts/remove-women-data.mts --competition=fa-women-s-super-league-women,nwsl-women
  node --experimental-strip-types scripts/remove-women-data.mts --write
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });
}

function uniqueNumbers(values: number[]) {
  return [...new Set(values)].sort((left, right) => left - right);
}

function formatIdList(values: number[]) {
  const unique = uniqueNumbers(values);
  if (unique.length === 0) {
    return '';
  }

  for (const value of unique) {
    if (!Number.isInteger(value) || value <= 0) {
      throw new Error(`Invalid numeric identifier: ${String(value)}`);
    }
  }

  return unique.join(', ');
}

function chunkNumbers(values: number[], batchSize: number) {
  const unique = uniqueNumbers(values);
  const chunks: number[][] = [];

  for (let index = 0; index < unique.length; index += batchSize) {
    chunks.push(unique.slice(index, index + batchSize));
  }

  return chunks;
}

async function runBatchedUnsafe(
  sql: ReturnType<typeof getSql>,
  label: string,
  ids: number[],
  buildQuery: (idList: string) => string,
  batchSize: number = ID_BATCH_SIZE
) {
  const batches = chunkNumbers(ids, batchSize);

  for (const [batchIndex, batch] of batches.entries()) {
    const idList = formatIdList(batch);
    if (!idList) {
      continue;
    }

    if (batches.length > 1) {
      console.log(`[remove-women-data] ${label} ${batchIndex + 1}/${batches.length}`);
    }

    await sql.unsafe(buildQuery(idList));
  }
}

async function loadTargetCompetitions(sql: ReturnType<typeof getSql>, options: CliOptions) {
  if (options.competitionSlugs.length > 0) {
    return sql<CompetitionRow[]>`
      SELECT
        c.id,
        c.slug,
        COALESCE(ct_en.name, c.slug) AS name
      FROM competitions c
      LEFT JOIN competition_translations ct_en
        ON ct_en.competition_id = c.id
       AND ct_en.locale = 'en'
      WHERE c.slug IN ${sql(options.competitionSlugs)}
      ORDER BY c.slug ASC
    `;
  }

  return sql<CompetitionRow[]>`
    SELECT
      c.id,
      c.slug,
      COALESCE(ct_en.name, c.slug) AS name
    FROM competitions c
    LEFT JOIN competition_translations ct_en
      ON ct_en.competition_id = c.id
     AND ct_en.locale = 'en'
    WHERE c.gender = 'female'
    ORDER BY c.slug ASC
  `;
}

async function loadIdRows(sql: ReturnType<typeof getSql>, query: string) {
  if (!query) {
    return [] as IdRow[];
  }

  return sql.unsafe<IdRow[]>(query);
}

async function loadCount(sql: ReturnType<typeof getSql>, query: string) {
  if (!query) {
    return 0;
  }

  const rows = await sql.unsafe<CountRow[]>(query);
  const rawCount = rows[0]?.count ?? 0;
  const count = Number(rawCount);
  if (!Number.isFinite(count)) {
    throw new Error(`Invalid count value: ${String(rawCount)}`);
  }

  return count;
}

function normalizeIds(rows: IdRow[]) {
  return uniqueNumbers(rows.map((row) => {
    const id = Number(row.id);
    if (!Number.isInteger(id) || id <= 0) {
      throw new Error(`Invalid numeric identifier: ${String(row.id)}`);
    }

    return id;
  }));
}

function normalizeCompetitions(rows: CompetitionRow[]) {
  return rows.map((row) => {
    const id = Number(row.id);
    if (!Number.isInteger(id) || id <= 0) {
      throw new Error(`Invalid competition identifier: ${String(row.id)}`);
    }

    return {
      ...row,
      id,
    };
  });
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const sql = getSql();

  try {
    const targetCompetitions = normalizeCompetitions(await loadTargetCompetitions(sql, options));
    const resolvedCompetitionSlugs = targetCompetitions.map((competition) => competition.slug);
    const requestedButMissing = options.competitionSlugs.filter((slug) => !resolvedCompetitionSlugs.includes(slug));

    if (targetCompetitions.length === 0) {
      throw new Error(
        options.competitionSlugs.length > 0
          ? `No competitions matched the requested slugs: ${options.competitionSlugs.join(', ')}`
          : 'No female competitions found to remove'
      );
    }

    const competitionIdList = formatIdList(targetCompetitions.map((competition) => competition.id));
    const targetCompetitionSeasonIds = normalizeIds(await loadIdRows(
      sql,
      `SELECT id FROM competition_seasons WHERE competition_id IN (${competitionIdList})`
    ));
    const competitionSeasonIdList = formatIdList(targetCompetitionSeasonIds);

    const targetMatchIds = competitionSeasonIdList
      ? normalizeIds(await loadIdRows(
        sql,
        `SELECT id FROM matches WHERE competition_season_id IN (${competitionSeasonIdList})`
      ))
      : [];
    const matchIdList = formatIdList(targetMatchIds);

    const targetTeamIds = competitionSeasonIdList
      ? normalizeIds(await loadIdRows(
        sql,
        `
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
        `
      ))
      : [];
    const targetTeamIdList = formatIdList(targetTeamIds);

    const protectedTeamIds = targetTeamIdList
      ? normalizeIds(await loadIdRows(
        sql,
        `
          SELECT DISTINCT id
          FROM (
            SELECT ts.team_id AS id
            FROM team_seasons ts
            WHERE ts.team_id IN (${targetTeamIdList})
              AND ts.competition_season_id NOT IN (${competitionSeasonIdList || 'NULL'})
            UNION
            SELECT pc.team_id AS id
            FROM player_contracts pc
            WHERE pc.team_id IN (${targetTeamIdList})
              AND pc.competition_season_id NOT IN (${competitionSeasonIdList || 'NULL'})
            UNION
            SELECT m.home_team_id AS id
            FROM matches m
            WHERE m.home_team_id IN (${targetTeamIdList})
              AND m.competition_season_id NOT IN (${competitionSeasonIdList || 'NULL'})
            UNION
            SELECT m.away_team_id AS id
            FROM matches m
            WHERE m.away_team_id IN (${targetTeamIdList})
              AND m.competition_season_id NOT IN (${competitionSeasonIdList || 'NULL'})
            UNION
            SELECT cs.winner_team_id AS id
            FROM competition_seasons cs
            WHERE cs.winner_team_id IN (${targetTeamIdList})
              AND cs.id NOT IN (${competitionSeasonIdList || 'NULL'})
          ) protected_refs
          WHERE id IS NOT NULL
        `
      ))
      : [];
    const protectedTeamIdSet = new Set(protectedTeamIds);
    const deleteTeamIds = targetTeamIds.filter((teamId) => !protectedTeamIdSet.has(teamId));
    const deleteTeamIdList = formatIdList(deleteTeamIds);

    const targetPlayerIds = normalizeIds(await loadIdRows(
      sql,
      `
        SELECT DISTINCT id
        FROM (
          ${competitionSeasonIdList
            ? `SELECT pc.player_id AS id FROM player_contracts pc WHERE pc.competition_season_id IN (${competitionSeasonIdList})`
            : 'SELECT NULL::bigint AS id WHERE FALSE'}
          UNION
          ${competitionSeasonIdList
            ? `SELECT pss.player_id AS id FROM player_season_stats pss WHERE pss.competition_season_id IN (${competitionSeasonIdList})`
            : 'SELECT NULL::bigint AS id WHERE FALSE'}
          UNION
          ${matchIdList
            ? `SELECT ml.player_id AS id FROM match_lineups ml WHERE ml.match_id IN (${matchIdList})`
            : 'SELECT NULL::bigint AS id WHERE FALSE'}
          UNION
          ${matchIdList
            ? `SELECT me.player_id AS id FROM match_events me WHERE me.match_id IN (${matchIdList})`
            : 'SELECT NULL::bigint AS id WHERE FALSE'}
          UNION
          ${matchIdList
            ? `SELECT me.secondary_player_id AS id FROM match_events me WHERE me.match_id IN (${matchIdList})`
            : 'SELECT NULL::bigint AS id WHERE FALSE'}
          UNION
          ${matchIdList
            ? `SELECT meff.player_id AS id FROM match_event_freeze_frames meff JOIN match_events me ON me.id = meff.event_id WHERE me.match_id IN (${matchIdList})`
            : 'SELECT NULL::bigint AS id WHERE FALSE'}
        ) player_refs
        WHERE id IS NOT NULL
      `
    ));
    const targetPlayerIdList = formatIdList(targetPlayerIds);

    let protectedPlayerIds = !options.write && targetPlayerIds.length > 0
      ? normalizeIds(await loadIdRows(
        sql,
        `
          SELECT DISTINCT id
          FROM (
            ${competitionSeasonIdList
              ? `
                SELECT pc.player_id AS id
                FROM player_contracts pc
                WHERE pc.player_id IN (${targetPlayerIdList})
                  AND pc.competition_season_id NOT IN (${competitionSeasonIdList})
                UNION
                SELECT pss.player_id AS id
                FROM player_season_stats pss
                WHERE pss.player_id IN (${targetPlayerIdList})
                  AND pss.competition_season_id NOT IN (${competitionSeasonIdList})
                UNION
                SELECT ml.player_id AS id
                FROM match_lineups ml
                JOIN matches m ON m.id = ml.match_id AND m.match_date = ml.match_date
                WHERE ml.player_id IN (${targetPlayerIdList})
                  AND m.competition_season_id NOT IN (${competitionSeasonIdList})
                UNION
                SELECT me.player_id AS id
                FROM match_events me
                JOIN matches m ON m.id = me.match_id AND m.match_date = me.match_date
                WHERE me.player_id IN (${targetPlayerIdList})
                  AND m.competition_season_id NOT IN (${competitionSeasonIdList})
                UNION
                SELECT me.secondary_player_id AS id
                FROM match_events me
                JOIN matches m ON m.id = me.match_id AND m.match_date = me.match_date
                WHERE me.secondary_player_id IN (${targetPlayerIdList})
                  AND m.competition_season_id NOT IN (${competitionSeasonIdList})
                UNION
                SELECT meff.player_id AS id
                FROM match_event_freeze_frames meff
                JOIN match_events me ON me.id = meff.event_id
                JOIN matches m ON m.id = me.match_id AND m.match_date = me.match_date
                WHERE meff.player_id IN (${targetPlayerIdList})
                  AND m.competition_season_id NOT IN (${competitionSeasonIdList})
              `
              : 'SELECT NULL::bigint AS id WHERE FALSE'}
            UNION
            ${targetTeamIdList
              ? `
                SELECT pmv.player_id AS id
                FROM player_market_values pmv
                WHERE pmv.player_id IN (${targetPlayerIdList})
                  AND pmv.club_id IS NOT NULL
                  AND pmv.club_id NOT IN (${targetTeamIdList})
                UNION
                SELECT pt.player_id AS id
                FROM player_transfers pt
                WHERE pt.player_id IN (${targetPlayerIdList})
                  AND (
                    (pt.from_team_id IS NOT NULL AND pt.from_team_id NOT IN (${targetTeamIdList}))
                    OR (pt.to_team_id IS NOT NULL AND pt.to_team_id NOT IN (${targetTeamIdList}))
                  )
              `
              : 'SELECT NULL::bigint AS id WHERE FALSE'}
          ) protected_refs
          WHERE id IS NOT NULL
        `
      ))
      : [];
    let deletePlayerIds = !options.write
      ? targetPlayerIds.filter((playerId) => !new Set(protectedPlayerIds).has(playerId))
      : [];
    let deletePlayerIdList = formatIdList(deletePlayerIds);

    const teamSeasonsCount = competitionSeasonIdList
      ? await loadCount(sql, `SELECT COUNT(*)::int AS count FROM team_seasons WHERE competition_season_id IN (${competitionSeasonIdList})`)
      : 0;
    const playerContractsCount = competitionSeasonIdList
      ? await loadCount(sql, `SELECT COUNT(*)::int AS count FROM player_contracts WHERE competition_season_id IN (${competitionSeasonIdList})`)
      : 0;
    const playerSeasonStatsCount = competitionSeasonIdList
      ? await loadCount(sql, `SELECT COUNT(*)::int AS count FROM player_season_stats WHERE competition_season_id IN (${competitionSeasonIdList})`)
      : 0;
    const matchLineupsCount = matchIdList
      ? await loadCount(sql, `SELECT COUNT(*)::int AS count FROM match_lineups WHERE match_id IN (${matchIdList})`)
      : 0;
    const matchStatsCount = matchIdList
      ? await loadCount(sql, `SELECT COUNT(*)::int AS count FROM match_stats WHERE match_id IN (${matchIdList})`)
      : 0;

    function buildSummary() {
      return {
        dryRun: !options.write,
        scope: options.competitionSlugs.length > 0 ? 'selected-competitions' : 'all-female-competitions',
        requestedCompetitionSlugs: options.competitionSlugs,
        missingCompetitionSlugs: requestedButMissing,
        competitions: targetCompetitions,
        counts: {
          competitions: targetCompetitions.length,
          competitionSeasons: targetCompetitionSeasonIds.length,
          matches: targetMatchIds.length,
          targetTeams: targetTeamIds.length,
          preservedTeams: protectedTeamIds.length,
          deletedTeams: deleteTeamIds.length,
          targetPlayers: targetPlayerIds.length,
          preservedPlayers: protectedPlayerIds.length,
          deletedPlayers: deletePlayerIds.length,
          teamSeasons: teamSeasonsCount,
          playerContracts: playerContractsCount,
          playerSeasonStats: playerSeasonStatsCount,
          matchLineups: matchLineupsCount,
          matchStats: matchStatsCount,
        },
      };
    }

    const plan: RemovalPlan = {
      competitionIds: targetCompetitions.map((competition) => competition.id),
      targetCompetitionSeasonIds,
      targetMatchIds,
      targetTeamIds,
      protectedTeamIds,
      deleteTeamIds,
      targetPlayerIds,
      protectedPlayerIds,
      deletePlayerIds,
    };

    if (options.planOut) {
      const { writeFileSync } = await import('node:fs');
      writeFileSync(options.planOut, JSON.stringify(plan, null, 2));
    }

    if (!options.write) {
      console.log(JSON.stringify(buildSummary(), null, 2));
      return;
    }

    await sql.begin(async (tx) => {
      const transactionSql = tx as unknown as ReturnType<typeof getSql>;

      if (competitionSeasonIdList) {
        await transactionSql.unsafe(`
          DELETE FROM data_freshness
          WHERE competition_season_id IN (${competitionSeasonIdList})
        `);
      }

      if (targetMatchIds.length > 0) {
        await runBatchedUnsafe(transactionSql, 'delete data_freshness by match', targetMatchIds, (idList) => `
          DELETE FROM data_freshness
          WHERE match_id IN (${idList})
        `);
        await runBatchedUnsafe(transactionSql, 'delete match_stats', targetMatchIds, (idList) => `DELETE FROM match_stats WHERE match_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete match_lineups by match', targetMatchIds, (idList) => `DELETE FROM match_lineups WHERE match_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete match_events by match', targetMatchIds, (idList) => `DELETE FROM match_events WHERE match_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete matches', targetMatchIds, (idList) => `DELETE FROM matches WHERE id IN (${idList})`);
      }

      if (competitionIdList) {
        await transactionSql.unsafe(`
          DELETE FROM data_freshness
          WHERE entity_type = 'competition'
            AND entity_id IN (${competitionIdList})
        `);
      }

      if (deleteTeamIdList) {
        await transactionSql.unsafe(`
          DELETE FROM data_freshness
          WHERE entity_type = 'team'
            AND entity_id IN (${deleteTeamIdList})
        `);
      }

      if (competitionSeasonIdList) {
        await transactionSql.unsafe(`DELETE FROM player_season_stats WHERE competition_season_id IN (${competitionSeasonIdList})`);
        await transactionSql.unsafe(`DELETE FROM player_contracts WHERE competition_season_id IN (${competitionSeasonIdList})`);
        await transactionSql.unsafe(`DELETE FROM team_seasons WHERE competition_season_id IN (${competitionSeasonIdList})`);
        await transactionSql.unsafe(`DELETE FROM competition_seasons WHERE id IN (${competitionSeasonIdList})`);
      }

      if (competitionIdList) {
        await transactionSql.unsafe(`DELETE FROM source_entity_mapping WHERE entity_type = 'competition' AND entity_id IN (${competitionIdList})`);
        await transactionSql.unsafe(`DELETE FROM entity_aliases WHERE entity_type = 'competition' AND entity_id IN (${competitionIdList})`);
        await transactionSql.unsafe(`DELETE FROM competitions WHERE id IN (${competitionIdList})`);
      }

      if (targetPlayerIds.length > 0) {
        protectedPlayerIds = normalizeIds(await loadIdRows(
          transactionSql,
          `
            SELECT DISTINCT id
            FROM (
              SELECT pc.player_id AS id
              FROM player_contracts pc
              WHERE pc.player_id IN (${targetPlayerIdList})
              UNION
              SELECT pss.player_id AS id
              FROM player_season_stats pss
              WHERE pss.player_id IN (${targetPlayerIdList})
              UNION
              SELECT ml.player_id AS id
              FROM match_lineups ml
              WHERE ml.player_id IN (${targetPlayerIdList})
              UNION
              SELECT me.player_id AS id
              FROM match_events me
              WHERE me.player_id IN (${targetPlayerIdList})
              UNION
              SELECT me.secondary_player_id AS id
              FROM match_events me
              WHERE me.secondary_player_id IN (${targetPlayerIdList})
              UNION
              SELECT meff.player_id AS id
              FROM match_event_freeze_frames meff
              WHERE meff.player_id IN (${targetPlayerIdList})
              UNION
              ${deleteTeamIdList
                ? `
                  SELECT pmv.player_id AS id
                  FROM player_market_values pmv
                  WHERE pmv.player_id IN (${targetPlayerIdList})
                    AND pmv.club_id IS NOT NULL
                    AND pmv.club_id NOT IN (${deleteTeamIdList})
                  UNION
                  SELECT pt.player_id AS id
                  FROM player_transfers pt
                  WHERE pt.player_id IN (${targetPlayerIdList})
                    AND (
                      (pt.from_team_id IS NOT NULL AND pt.from_team_id NOT IN (${deleteTeamIdList}))
                      OR (pt.to_team_id IS NOT NULL AND pt.to_team_id NOT IN (${deleteTeamIdList}))
                    )
                `
                : `
                  SELECT pmv.player_id AS id
                  FROM player_market_values pmv
                  WHERE pmv.player_id IN (${targetPlayerIdList})
                    AND pmv.club_id IS NOT NULL
                  UNION
                  SELECT pt.player_id AS id
                  FROM player_transfers pt
                  WHERE pt.player_id IN (${targetPlayerIdList})
                    AND (pt.from_team_id IS NOT NULL OR pt.to_team_id IS NOT NULL)
                `}
            ) protected_refs
            WHERE id IS NOT NULL
          `
        ));
        deletePlayerIds = targetPlayerIds.filter((playerId) => !new Set(protectedPlayerIds).has(playerId));
        deletePlayerIdList = formatIdList(deletePlayerIds);
      }

      if (deletePlayerIdList) {
        await transactionSql.unsafe(`
          DELETE FROM data_freshness
          WHERE entity_type = 'player'
            AND entity_id IN (${deletePlayerIdList})
        `);
      }

      if (deletePlayerIds.length > 0) {
        await runBatchedUnsafe(transactionSql, 'delete freeze frames by player', deletePlayerIds, (idList) => `DELETE FROM match_event_freeze_frames WHERE player_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete match_lineups by player', deletePlayerIds, (idList) => `DELETE FROM match_lineups WHERE player_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete match_events by player', deletePlayerIds, (idList) => `DELETE FROM match_events WHERE player_id IN (${idList}) OR secondary_player_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete player_season_stats by player', deletePlayerIds, (idList) => `DELETE FROM player_season_stats WHERE player_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete player_contracts by player', deletePlayerIds, (idList) => `DELETE FROM player_contracts WHERE player_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete player_photo_sources', deletePlayerIds, (idList) => `DELETE FROM player_photo_sources WHERE player_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete player_market_values', deletePlayerIds, (idList) => `DELETE FROM player_market_values WHERE player_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete player_transfers', deletePlayerIds, (idList) => `DELETE FROM player_transfers WHERE player_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete player_translations', deletePlayerIds, (idList) => `DELETE FROM player_translations WHERE player_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete player source mapping', deletePlayerIds, (idList) => `DELETE FROM source_entity_mapping WHERE entity_type = 'player' AND entity_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete player aliases', deletePlayerIds, (idList) => `DELETE FROM entity_aliases WHERE entity_type = 'player' AND entity_id IN (${idList})`);
        await runBatchedUnsafe(transactionSql, 'delete players', deletePlayerIds, (idList) => `DELETE FROM players WHERE id IN (${idList})`, PLAYER_DELETE_BATCH_SIZE);
      }

      if (deleteTeamIdList) {
        await transactionSql.unsafe(`
          UPDATE competition_seasons
          SET winner_team_id = NULL,
              updated_at = NOW()
          WHERE winner_team_id IN (${deleteTeamIdList})
        `);
        await transactionSql.unsafe(`
          UPDATE player_market_values
          SET club_id = NULL,
              updated_at = NOW()
          WHERE club_id IN (${deleteTeamIdList})
        `);
        await transactionSql.unsafe(`
          UPDATE player_transfers
          SET from_team_id = NULL,
              updated_at = NOW()
          WHERE from_team_id IN (${deleteTeamIdList})
        `);
        await transactionSql.unsafe(`
          UPDATE player_transfers
          SET to_team_id = NULL,
              updated_at = NOW()
          WHERE to_team_id IN (${deleteTeamIdList})
        `);
        await transactionSql.unsafe(`DELETE FROM source_entity_mapping WHERE entity_type = 'team' AND entity_id IN (${deleteTeamIdList})`);
        await transactionSql.unsafe(`DELETE FROM entity_aliases WHERE entity_type = 'team' AND entity_id IN (${deleteTeamIdList})`);
        await transactionSql.unsafe(`DELETE FROM teams WHERE id IN (${deleteTeamIdList})`);
      }

    });

    console.log('[remove-women-data] refresh mv_team_form');
    await sql`REFRESH MATERIALIZED VIEW mv_team_form`;
    console.log('[remove-women-data] refresh mv_standings');
    await sql`REFRESH MATERIALIZED VIEW mv_standings`;
    console.log('[remove-women-data] refresh mv_top_scorers');
    await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;

    console.log(JSON.stringify({ ...buildSummary(), dryRun: false, applied: true }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
