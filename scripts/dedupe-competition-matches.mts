import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  competition: string;
  dryRun: boolean;
  help: boolean;
  season: string;
}

interface DuplicateGroupRow {
  match_date: string;
  kickoff_at: string | null;
  stage: string | null;
  group_name: string | null;
  home_slug: string;
  away_slug: string;
  match_ids: number[];
}

interface MatchCandidateRow {
  id: number;
  source_metadata: Record<string, unknown> | string | null;
  events: number;
  lineups: number;
  stats: number;
  kickoff_at: string | null;
  home_score: number | null;
  away_score: number | null;
  status: string;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    competition: 'champions-league',
    dryRun: false,
    help: false,
    season: '2024/25',
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--competition=')) {
      options.competition = arg.slice('--competition='.length).trim() || options.competition;
      continue;
    }

    if (arg.startsWith('--season=')) {
      options.season = arg.slice('--season='.length).trim() || options.season;
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/dedupe-competition-matches.mts [options]

Options:
  --competition=<slug>  Competition slug (default: champions-league)
  --season=<slug>       Season slug (default: 2024/25)
  --dry-run             Print duplicate merge plan without writing
  --help, -h            Show this help message
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });
}

function parseSourceMetadata(metadata: MatchCandidateRow['source_metadata']) {
  if (!metadata) {
    return null;
  }

  if (typeof metadata === 'string') {
    return JSON.parse(metadata) as Record<string, unknown>;
  }

  return metadata;
}

function getSourceRank(source: string | null) {
  switch (source) {
    case 'api_football':
      return 3;
    case 'football_data_org':
      return 2;
    case 'sofascore':
      return 1;
    default:
      return 1;
  }
}

function normalizeSlugForMatchGrouping(column: string) {
  return `TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(${column}, '[0-9]+', ' ', 'g'), '(fc|cf|afc|ac|as|fk|sk|ssc|club|de|del|futbol|football|balompie|town|eindhoven|rc|racing)', ' ', 'g'), '[-_]+', ' ', 'g'))`;
}

function chooseCanonical(rows: MatchCandidateRow[]) {
  return rows
    .map((row) => {
      const metadata = parseSourceMetadata(row.source_metadata);
      const source = typeof metadata?.source === 'string' ? metadata.source : null;
      return {
        row,
        source,
        sourceRank: getSourceRank(source),
        payloadRank: row.events + row.lineups + row.stats,
        kickoffRank: row.kickoff_at ? 1 : 0,
      };
    })
    .sort((left, right) => {
      if (right.payloadRank !== left.payloadRank) {
        return right.payloadRank - left.payloadRank;
      }

       if (right.kickoffRank !== left.kickoffRank) {
        return right.kickoffRank - left.kickoffRank;
      }

      if (right.sourceRank !== left.sourceRank) {
        return right.sourceRank - left.sourceRank;
      }

      return right.row.id - left.row.id;
    })[0]!.row;
}

async function loadDuplicateGroups(sql: ReturnType<typeof getSql>, competition: string, season: string) {
  const homeSlugKey = normalizeSlugForMatchGrouping('home.slug');
  const awaySlugKey = normalizeSlugForMatchGrouping('away.slug');

  return sql.unsafe<DuplicateGroupRow[]>(`
    SELECT
      MIN(m.match_date)::text AS match_date,
      MIN(m.kickoff_at::text) AS kickoff_at,
      MIN(m.stage) AS stage,
      MIN(m.group_name) AS group_name,
      MIN(home.slug) AS home_slug,
      MIN(away.slug) AS away_slug,
      ARRAY_AGG(m.id ORDER BY m.id) AS match_ids
    FROM matches m
    JOIN competition_seasons cs ON cs.id = m.competition_season_id
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    JOIN teams home ON home.id = m.home_team_id
    JOIN teams away ON away.id = m.away_team_id
    WHERE c.slug = '${competition.replace(/'/g, "''")}'
      AND s.slug = '${season.replace(/'/g, "''")}'
    GROUP BY
      COALESCE(m.matchday::text, m.match_date::text),
      ${homeSlugKey},
      ${awaySlugKey}
    HAVING COUNT(*) > 1
    ORDER BY MIN(m.match_date), MIN(home.slug), MIN(away.slug)
  `);
}

async function loadCandidates(sql: ReturnType<typeof getSql>, matchIds: number[]) {
  const idList = matchIds.map((matchId) => Number(matchId)).filter((matchId) => Number.isFinite(matchId)).join(', ');
  if (!idList) {
    return [] as MatchCandidateRow[];
  }

  return sql.unsafe<MatchCandidateRow[]>(`
    SELECT
      m.id,
      m.source_metadata,
      0::int AS events,
      (SELECT COUNT(*)::int FROM match_lineups ml WHERE ml.match_id = m.id AND ml.match_date = m.match_date) AS lineups,
      (SELECT COUNT(*)::int FROM match_stats ms WHERE ms.match_id = m.id AND ms.match_date = m.match_date) AS stats,
      m.kickoff_at::text AS kickoff_at,
      m.home_score,
      m.away_score,
      m.status::text AS status
    FROM matches m
    WHERE m.id IN (${idList})
    ORDER BY m.id
  `);
}

async function mergeAliasMatch(tx: Awaited<ReturnType<ReturnType<typeof getSql>['reserve']>>, canonicalId: number, aliasId: number, matchDate: string) {
  await tx`
    INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata)
    SELECT entity_type, ${canonicalId}, source_id, external_id, external_code, season_context, metadata
    FROM source_entity_mapping
    WHERE entity_type = 'match'
      AND entity_id = ${aliasId}
    ON CONFLICT (entity_type, source_id, external_id)
    DO UPDATE SET
      entity_id = EXCLUDED.entity_id,
      external_code = COALESCE(source_entity_mapping.external_code, EXCLUDED.external_code),
      season_context = COALESCE(source_entity_mapping.season_context, EXCLUDED.season_context),
      metadata = COALESCE(source_entity_mapping.metadata, EXCLUDED.metadata),
      updated_at = NOW()
  `;

  await tx`DELETE FROM source_entity_mapping WHERE entity_type = 'match' AND entity_id = ${aliasId}`;

  await tx`
    UPDATE data_freshness
    SET match_id = ${canonicalId}, updated_at = NOW()
    WHERE match_id = ${aliasId}
      AND match_date = ${matchDate}
  `;

  await tx`
    DELETE FROM match_stats alias_ms
    USING match_stats canonical_ms
    WHERE alias_ms.match_id = ${aliasId}
      AND alias_ms.match_date = ${matchDate}
      AND canonical_ms.match_id = ${canonicalId}
      AND canonical_ms.match_date = ${matchDate}
      AND canonical_ms.team_id = alias_ms.team_id
  `;

  await tx`
    UPDATE match_stats
    SET match_id = ${canonicalId}
    WHERE match_id = ${aliasId}
      AND match_date = ${matchDate}
  `;

  await tx`
    DELETE FROM match_lineups alias_ml
    USING match_lineups canonical_ml
    WHERE alias_ml.match_id = ${aliasId}
      AND alias_ml.match_date = ${matchDate}
      AND canonical_ml.match_id = ${canonicalId}
      AND canonical_ml.match_date = ${matchDate}
      AND canonical_ml.team_id = alias_ml.team_id
      AND canonical_ml.player_id = alias_ml.player_id
  `;

  await tx`
    UPDATE match_lineups
    SET match_id = ${canonicalId}
    WHERE match_id = ${aliasId}
      AND match_date = ${matchDate}
  `;

  await tx`DELETE FROM matches WHERE id = ${aliasId} AND match_date = ${matchDate}`;
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
    const groups = await loadDuplicateGroups(sql, options.competition, options.season);
    const plan = [] as Array<{ matchDate: string; homeSlug: string; awaySlug: string; canonicalId: number; aliasIds: number[] }>;

    for (const group of groups) {
      const candidates = await loadCandidates(sql, group.match_ids);
      const canonical = chooseCanonical(candidates);
      const aliasIds = candidates.map((candidate) => candidate.id).filter((id) => id !== canonical.id);
      if (aliasIds.length === 0) {
        continue;
      }

      plan.push({
        matchDate: group.match_date,
        homeSlug: group.home_slug,
        awaySlug: group.away_slug,
        canonicalId: canonical.id,
        aliasIds,
      });
    }

    if (options.dryRun) {
      console.log(JSON.stringify({
        dryRun: true,
        competition: options.competition,
        season: options.season,
        duplicateGroupCount: plan.length,
        plan,
      }, null, 2));
      return;
    }

    const tx = await sql.reserve();
    try {
      await tx`BEGIN`;
      try {
        for (const entry of plan) {
          for (const aliasId of entry.aliasIds) {
            await mergeAliasMatch(tx, entry.canonicalId, aliasId, entry.matchDate);
          }
        }

        await tx`REFRESH MATERIALIZED VIEW mv_team_form`;
        await tx`REFRESH MATERIALIZED VIEW mv_standings`;
        await tx`REFRESH MATERIALIZED VIEW mv_top_scorers`;
        await tx`COMMIT`;
      } catch (error) {
        await tx`ROLLBACK`;
        throw error;
      }
    } finally {
      tx.release();
    }

    console.log(JSON.stringify({
      ok: true,
      competition: options.competition,
      season: options.season,
      duplicateGroupCount: plan.length,
      mergedAliasCount: plan.reduce((sum, entry) => sum + entry.aliasIds.length, 0),
    }, null, 2));
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
