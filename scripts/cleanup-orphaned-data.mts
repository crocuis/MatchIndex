import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  help: boolean;
  write: boolean;
}

interface CountRow {
  count: number | string;
}

interface CleanupOperation {
  key: string;
  countQuery: string;
  deleteQuery: string;
}

const CLEANUP_OPERATIONS: CleanupOperation[] = [
  {
    key: 'entityAliasesCompetition',
    countQuery: `SELECT COUNT(*)::int AS count FROM entity_aliases ea WHERE ea.entity_type = 'competition' AND NOT EXISTS (SELECT 1 FROM competitions c WHERE c.id = ea.entity_id)`,
    deleteQuery: `DELETE FROM entity_aliases ea WHERE ea.entity_type = 'competition' AND NOT EXISTS (SELECT 1 FROM competitions c WHERE c.id = ea.entity_id)`,
  },
  {
    key: 'entityAliasesTeam',
    countQuery: `SELECT COUNT(*)::int AS count FROM entity_aliases ea WHERE ea.entity_type = 'team' AND NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = ea.entity_id)`,
    deleteQuery: `DELETE FROM entity_aliases ea WHERE ea.entity_type = 'team' AND NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = ea.entity_id)`,
  },
  {
    key: 'entityAliasesPlayer',
    countQuery: `SELECT COUNT(*)::int AS count FROM entity_aliases ea WHERE ea.entity_type = 'player' AND NOT EXISTS (SELECT 1 FROM players p WHERE p.id = ea.entity_id)`,
    deleteQuery: `DELETE FROM entity_aliases ea WHERE ea.entity_type = 'player' AND NOT EXISTS (SELECT 1 FROM players p WHERE p.id = ea.entity_id)`,
  },
  {
    key: 'entityAliasesMatch',
    countQuery: `SELECT COUNT(*)::int AS count FROM entity_aliases ea WHERE ea.entity_type = 'match' AND NOT EXISTS (SELECT 1 FROM matches m WHERE m.id = ea.entity_id)`,
    deleteQuery: `DELETE FROM entity_aliases ea WHERE ea.entity_type = 'match' AND NOT EXISTS (SELECT 1 FROM matches m WHERE m.id = ea.entity_id)`,
  },
  {
    key: 'sourceEntityMappingCompetition',
    countQuery: `SELECT COUNT(*)::int AS count FROM source_entity_mapping sem WHERE sem.entity_type = 'competition' AND NOT EXISTS (SELECT 1 FROM competitions c WHERE c.id = sem.entity_id)`,
    deleteQuery: `DELETE FROM source_entity_mapping sem WHERE sem.entity_type = 'competition' AND NOT EXISTS (SELECT 1 FROM competitions c WHERE c.id = sem.entity_id)`,
  },
  {
    key: 'sourceEntityMappingTeam',
    countQuery: `SELECT COUNT(*)::int AS count FROM source_entity_mapping sem WHERE sem.entity_type = 'team' AND NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = sem.entity_id)`,
    deleteQuery: `DELETE FROM source_entity_mapping sem WHERE sem.entity_type = 'team' AND NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = sem.entity_id)`,
  },
  {
    key: 'sourceEntityMappingPlayer',
    countQuery: `SELECT COUNT(*)::int AS count FROM source_entity_mapping sem WHERE sem.entity_type = 'player' AND NOT EXISTS (SELECT 1 FROM players p WHERE p.id = sem.entity_id)`,
    deleteQuery: `DELETE FROM source_entity_mapping sem WHERE sem.entity_type = 'player' AND NOT EXISTS (SELECT 1 FROM players p WHERE p.id = sem.entity_id)`,
  },
  {
    key: 'sourceEntityMappingMatch',
    countQuery: `SELECT COUNT(*)::int AS count FROM source_entity_mapping sem WHERE sem.entity_type = 'match' AND NOT EXISTS (SELECT 1 FROM matches m WHERE m.id = sem.entity_id)`,
    deleteQuery: `DELETE FROM source_entity_mapping sem WHERE sem.entity_type = 'match' AND NOT EXISTS (SELECT 1 FROM matches m WHERE m.id = sem.entity_id)`,
  },
  {
    key: 'dataFreshnessCompetitionEntity',
    countQuery: `SELECT COUNT(*)::int AS count FROM data_freshness df WHERE df.entity_type = 'competition' AND df.entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM competitions c WHERE c.id = df.entity_id)`,
    deleteQuery: `DELETE FROM data_freshness df WHERE df.entity_type = 'competition' AND df.entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM competitions c WHERE c.id = df.entity_id)`,
  },
  {
    key: 'dataFreshnessTeamEntity',
    countQuery: `SELECT COUNT(*)::int AS count FROM data_freshness df WHERE df.entity_type = 'team' AND df.entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = df.entity_id)`,
    deleteQuery: `DELETE FROM data_freshness df WHERE df.entity_type = 'team' AND df.entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = df.entity_id)`,
  },
  {
    key: 'dataFreshnessPlayerEntity',
    countQuery: `SELECT COUNT(*)::int AS count FROM data_freshness df WHERE df.entity_type = 'player' AND df.entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM players p WHERE p.id = df.entity_id)`,
    deleteQuery: `DELETE FROM data_freshness df WHERE df.entity_type = 'player' AND df.entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM players p WHERE p.id = df.entity_id)`,
  },
  {
    key: 'dataFreshnessMatchEntity',
    countQuery: `SELECT COUNT(*)::int AS count FROM data_freshness df WHERE df.entity_type = 'match' AND df.entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM matches m WHERE m.id = df.entity_id)`,
    deleteQuery: `DELETE FROM data_freshness df WHERE df.entity_type = 'match' AND df.entity_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM matches m WHERE m.id = df.entity_id)`,
  },
  {
    key: 'matchStatsMissingMatch',
    countQuery: `SELECT COUNT(*)::int AS count FROM match_stats ms WHERE NOT EXISTS (SELECT 1 FROM matches m WHERE m.id = ms.match_id AND m.match_date = ms.match_date)`,
    deleteQuery: `DELETE FROM match_stats ms WHERE NOT EXISTS (SELECT 1 FROM matches m WHERE m.id = ms.match_id AND m.match_date = ms.match_date)`,
  },
  {
    key: 'matchStatsMissingTeam',
    countQuery: `SELECT COUNT(*)::int AS count FROM match_stats ms WHERE NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = ms.team_id)`,
    deleteQuery: `DELETE FROM match_stats ms WHERE NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = ms.team_id)`,
  },
  {
    key: 'matchLineupsMissingMatch',
    countQuery: `SELECT COUNT(*)::int AS count FROM match_lineups ml WHERE NOT EXISTS (SELECT 1 FROM matches m WHERE m.id = ml.match_id AND m.match_date = ml.match_date)`,
    deleteQuery: `DELETE FROM match_lineups ml WHERE NOT EXISTS (SELECT 1 FROM matches m WHERE m.id = ml.match_id AND m.match_date = ml.match_date)`,
  },
  {
    key: 'matchLineupsMissingTeam',
    countQuery: `SELECT COUNT(*)::int AS count FROM match_lineups ml WHERE NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = ml.team_id)`,
    deleteQuery: `DELETE FROM match_lineups ml WHERE NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = ml.team_id)`,
  },
  {
    key: 'matchLineupsMissingPlayer',
    countQuery: `SELECT COUNT(*)::int AS count FROM match_lineups ml WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.id = ml.player_id)`,
    deleteQuery: `DELETE FROM match_lineups ml WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.id = ml.player_id)`,
  },
  {
    key: 'teamSeasonsMissingTeam',
    countQuery: `SELECT COUNT(*)::int AS count FROM team_seasons ts WHERE NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = ts.team_id)`,
    deleteQuery: `DELETE FROM team_seasons ts WHERE NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = ts.team_id)`,
  },
  {
    key: 'teamSeasonsMissingCompetitionSeason',
    countQuery: `SELECT COUNT(*)::int AS count FROM team_seasons ts WHERE NOT EXISTS (SELECT 1 FROM competition_seasons cs WHERE cs.id = ts.competition_season_id)`,
    deleteQuery: `DELETE FROM team_seasons ts WHERE NOT EXISTS (SELECT 1 FROM competition_seasons cs WHERE cs.id = ts.competition_season_id)`,
  },
  {
    key: 'playerContractsMissingPlayer',
    countQuery: `SELECT COUNT(*)::int AS count FROM player_contracts pc WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.id = pc.player_id)`,
    deleteQuery: `DELETE FROM player_contracts pc WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.id = pc.player_id)`,
  },
  {
    key: 'playerContractsMissingTeam',
    countQuery: `SELECT COUNT(*)::int AS count FROM player_contracts pc WHERE NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = pc.team_id)`,
    deleteQuery: `DELETE FROM player_contracts pc WHERE NOT EXISTS (SELECT 1 FROM teams t WHERE t.id = pc.team_id)`,
  },
  {
    key: 'playerContractsMissingCompetitionSeason',
    countQuery: `SELECT COUNT(*)::int AS count FROM player_contracts pc WHERE NOT EXISTS (SELECT 1 FROM competition_seasons cs WHERE cs.id = pc.competition_season_id)`,
    deleteQuery: `DELETE FROM player_contracts pc WHERE NOT EXISTS (SELECT 1 FROM competition_seasons cs WHERE cs.id = pc.competition_season_id)`,
  },
  {
    key: 'playerSeasonStatsMissingPlayer',
    countQuery: `SELECT COUNT(*)::int AS count FROM player_season_stats pss WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.id = pss.player_id)`,
    deleteQuery: `DELETE FROM player_season_stats pss WHERE NOT EXISTS (SELECT 1 FROM players p WHERE p.id = pss.player_id)`,
  },
  {
    key: 'playerSeasonStatsMissingCompetitionSeason',
    countQuery: `SELECT COUNT(*)::int AS count FROM player_season_stats pss WHERE NOT EXISTS (SELECT 1 FROM competition_seasons cs WHERE cs.id = pss.competition_season_id)`,
    deleteQuery: `DELETE FROM player_season_stats pss WHERE NOT EXISTS (SELECT 1 FROM competition_seasons cs WHERE cs.id = pss.competition_season_id)`,
  },
];

function parseArgs(argv: string[]): CliOptions {
  let help = false;
  let write = false;

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      help = true;
      continue;
    }

    if (arg === '--write') {
      write = true;
    }
  }

  return { help, write };
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/cleanup-orphaned-data.mts [options]

기본 동작:
  - orphan 가능성이 있는 파생/제네릭 테이블의 남은 레코드를 집계한다.
- FK cascade가 없는 느슨한 참조 테이블도 함께 검사한다.

Options:
  --write      실제 삭제 반영 (기본값은 dry-run)
  --help, -h   도움말 출력

Examples:
  node --experimental-strip-types scripts/cleanup-orphaned-data.mts
  node --experimental-strip-types scripts/cleanup-orphaned-data.mts --write
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });
}

async function loadCount(sql: ReturnType<typeof getSql>, query: string) {
  const rows = await sql.unsafe<CountRow[]>(query);
  const rawCount = rows[0]?.count ?? 0;
  const count = Number(rawCount);

  if (!Number.isFinite(count)) {
    throw new Error(`Invalid count value: ${String(rawCount)}`);
  }

  return count;
}

async function collectCounts(sql: ReturnType<typeof getSql>) {
  const counts: Record<string, number> = {};

  for (const operation of CLEANUP_OPERATIONS) {
    counts[operation.key] = await loadCount(sql, operation.countQuery);
  }

  return counts;
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
    const counts = await collectCounts(sql);
    const total = Object.values(counts).reduce((sum, count) => sum + count, 0);

    if (!options.write) {
      console.log(JSON.stringify({ dryRun: true, total, counts }, null, 2));
      return;
    }

    await sql.begin(async (tx) => {
      const transactionSql = tx as unknown as ReturnType<typeof getSql>;

      for (const operation of CLEANUP_OPERATIONS) {
        if ((counts[operation.key] ?? 0) === 0) {
          continue;
        }

        console.log(`[cleanup-orphaned-data] ${operation.key} ${counts[operation.key]}`);
        await transactionSql.unsafe(operation.deleteQuery);
      }
    });

    if (total > 0) {
      console.log('[cleanup-orphaned-data] refresh mv_team_form');
      await sql`REFRESH MATERIALIZED VIEW mv_team_form`;
      console.log('[cleanup-orphaned-data] refresh mv_standings');
      await sql`REFRESH MATERIALIZED VIEW mv_standings`;
      console.log('[cleanup-orphaned-data] refresh mv_top_scorers');
      await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;
    }

    console.log(JSON.stringify({ dryRun: false, total, counts, applied: true }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
