import postgres, { type Sql } from 'postgres';
import {
  buildApiFootballFixtureLineupsPath,
  buildApiFootballFixtureStatisticsPath,
  fetchApiFootballJson,
  getApiFootballRecentSeasonYears,
  getApiFootballSourceConfig,
  parseApiFootballDataCompetitionTargets,
  type ApiFootballCompetitionTarget,
  type ApiFootballEnvelope,
  type ApiFootballFixtureLineupResponseItem,
  type ApiFootballFixtureStatisticsResponseItem,
} from './apiFootball.ts';

const BATCH_SIZE = 500;
const FETCH_CONCURRENCY = 4;

interface SourceRow {
  id: number;
}

interface MappingRow {
  entity_id: number;
  external_id: string;
}

interface TargetMatchRow {
  match_id: number;
  match_date: string;
  home_team_id: number;
  away_team_id: number;
  competition_slug: string;
  season_start_year: number;
  external_fixture_id: string;
  status: string;
  referee: string | null;
  home_formation: string | null;
  away_formation: string | null;
  stat_rows: number;
  home_total_passes: number | null;
  away_total_passes: number | null;
  needs_lineups: boolean;
  needs_stats: boolean;
}

interface MatchUpdateDraft {
  matchId: number;
  matchDate: string;
  referee: string | null;
  homeFormation: string | null;
  awayFormation: string | null;
}

interface MatchStatsDraft {
  matchId: number;
  matchDate: string;
  teamId: number;
  isHome: boolean;
  possession: number | null;
  totalPasses: number | null;
  accuratePasses: number | null;
  passAccuracy: number | null;
  totalShots: number | null;
  shotsOnTarget: number | null;
  shotsOffTarget: number | null;
  blockedShots: number | null;
  cornerKicks: number | null;
  freeKicks: number | null;
  throwIns: number | null;
  fouls: number | null;
  offsides: number | null;
  gkSaves: number | null;
  expectedGoals: number | null;
}

interface TargetMatchFetchResult {
  matchDraft: MatchUpdateDraft;
  statsDrafts: MatchStatsDraft[];
  lineupsFetched: number;
  statsFetched: number;
}

export interface BackfillApiFootballMatchDataOptions {
  dryRun?: boolean;
  competitionCodes?: string[];
  seasons?: number[];
  limit?: number;
}

export interface BackfillApiFootballMatchDataSummary {
  dryRun: boolean;
  targetMatches: number;
  lineupsFetched: number;
  statsFetched: number;
  matchRowsPlanned: number;
  matchRowsWritten: number;
  statsRowsPlanned: number;
  statsRowsWritten: number;
  seasons: number[];
  competitions: string[];
}

function getDb() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, idle_timeout: 20, prepare: false });
}

function normalizeSeasons(input?: number[]) {
  if (input && input.length > 0) {
    return [...new Set(input)].sort((a, b) => a - b);
  }

  return getApiFootballRecentSeasonYears(2);
}

function parsePositiveInt(value: string | number | null | undefined) {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? Math.trunc(value) : null;
  }

  if (typeof value !== 'string') {
    return null;
  }

  const normalized = value.trim();
  if (!normalized) {
    return null;
  }

  const numeric = Number.parseFloat(normalized.replace(/%/g, '').replace(/,/g, ''));
  return Number.isFinite(numeric) ? Math.trunc(numeric) : null;
}

function parseDecimal(value: string | number | null | undefined) {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value !== 'string') {
    return null;
  }

  const normalized = value.trim();
  if (!normalized) {
    return null;
  }

  const numeric = Number.parseFloat(normalized.replace(/,/g, ''));
  return Number.isFinite(numeric) ? numeric : null;
}

function normalizeStatType(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function roundExpectedGoals(value: number | null) {
  if (value === null) {
    return null;
  }

  return Number(value.toFixed(2));
}

function getApiFootballErrorMessage(payload: { errors?: Record<string, string> }) {
  if (!payload.errors) {
    return null;
  }

  const values = Object.values(payload.errors).filter((value): value is string => Boolean(value));
  return values.length > 0 ? values.join('; ') : null;
}

function buildMatchStatsDraft(
  target: TargetMatchRow,
  teamId: number,
  statistics: ApiFootballFixtureStatisticsResponseItem['statistics'],
): MatchStatsDraft {
  const statMap = new Map<string, string | number | null>();

  for (const statistic of statistics ?? []) {
    if (!statistic?.type) {
      continue;
    }

    statMap.set(normalizeStatType(statistic.type), statistic.value ?? null);
  }

  const totalPasses = parsePositiveInt(
    statMap.get('total passes')
    ?? statMap.get('passes total')
  );
  const accuratePasses = parsePositiveInt(
    statMap.get('passes accurate')
    ?? statMap.get('accurate passes')
  );
  const passAccuracy = parsePositiveInt(
    statMap.get('passes')
    ?? statMap.get('passes %')
    ?? statMap.get('pass accuracy')
  ) ?? (totalPasses && accuratePasses ? Math.round((accuratePasses / totalPasses) * 100) : null);

  return {
    matchId: target.match_id,
    matchDate: target.match_date,
    teamId,
    isHome: teamId === target.home_team_id,
    possession: parsePositiveInt(statMap.get('ball possession') ?? statMap.get('possession')),
    totalPasses,
    accuratePasses,
    passAccuracy,
    totalShots: parsePositiveInt(statMap.get('goal attempts') ?? statMap.get('total shots')),
    shotsOnTarget: parsePositiveInt(statMap.get('shots on goal') ?? statMap.get('shots on target')),
    shotsOffTarget: parsePositiveInt(statMap.get('shots off goal')),
    blockedShots: parsePositiveInt(statMap.get('blocked shots')),
    cornerKicks: parsePositiveInt(statMap.get('corner kicks')),
    freeKicks: parsePositiveInt(statMap.get('free kicks')),
    throwIns: parsePositiveInt(statMap.get('throw ins') ?? statMap.get('throwins')),
    fouls: parsePositiveInt(statMap.get('fouls')),
    offsides: parsePositiveInt(statMap.get('offsides')),
    gkSaves: parsePositiveInt(statMap.get('goalkeeper saves') ?? statMap.get('saves')),
    expectedGoals: roundExpectedGoals(parseDecimal(statMap.get('expected goals') ?? statMap.get('xg'))),
  };
}

async function ensureSource(sql: Sql) {
  const config = getApiFootballSourceConfig();
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, priority)
    VALUES (${config.slug}, ${config.name}, ${config.baseUrl}, 'api', 2)
    ON CONFLICT (slug)
    DO UPDATE SET
      name = EXCLUDED.name,
      base_url = EXCLUDED.base_url,
      source_kind = EXCLUDED.source_kind,
      priority = EXCLUDED.priority
    RETURNING id
  `;

  return rows[0].id;
}

async function loadTeamMappings(sql: Sql, sourceId: number) {
  const rows = await sql<MappingRow[]>`
    SELECT entity_id, external_id
    FROM source_entity_mapping
    WHERE entity_type = 'team' AND source_id = ${sourceId}
  `;

  return new Map(rows.map((row) => [row.external_id, Number(row.entity_id)]));
}

async function loadTargetMatches(
  sql: Sql,
  sourceId: number,
  targets: ApiFootballCompetitionTarget[],
  seasons: number[],
  limit?: number,
) {
  const rows = await sql<TargetMatchRow[]>`
    SELECT
      m.id AS match_id,
      m.match_date::TEXT AS match_date,
      m.home_team_id,
      m.away_team_id,
      c.slug AS competition_slug,
      EXTRACT(YEAR FROM s.start_date)::INT AS season_start_year,
      sem.external_id AS external_fixture_id,
      m.status::TEXT AS status,
      m.referee,
      m.home_formation,
      m.away_formation,
      COUNT(ms.id)::INT AS stat_rows,
      MAX(CASE WHEN ms.team_id = m.home_team_id THEN ms.total_passes END) AS home_total_passes,
      MAX(CASE WHEN ms.team_id = m.away_team_id THEN ms.total_passes END) AS away_total_passes,
      (m.home_formation IS NULL OR m.away_formation IS NULL) AS needs_lineups,
      (
        m.status IN ('finished', 'finished_aet', 'finished_pen', 'live_1h', 'live_ht', 'live_2h', 'live_et', 'live_pen')
        AND (
          COUNT(ms.id) < 2
          OR MAX(CASE WHEN ms.team_id = m.home_team_id THEN ms.total_passes END) IS NULL
          OR MAX(CASE WHEN ms.team_id = m.away_team_id THEN ms.total_passes END) IS NULL
        )
      ) AS needs_stats
    FROM matches m
    JOIN competition_seasons cs ON cs.id = m.competition_season_id
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    JOIN source_entity_mapping sem ON sem.entity_type = 'match'
      AND sem.entity_id = m.id
      AND sem.source_id = ${sourceId}
    LEFT JOIN match_stats ms ON ms.match_id = m.id AND ms.match_date = m.match_date
    WHERE c.slug = ANY(${targets.map((target) => target.competitionSlug)})
      AND EXTRACT(YEAR FROM s.start_date)::INT = ANY(${seasons})
    GROUP BY m.id, m.match_date, m.home_team_id, m.away_team_id, c.slug, s.start_date, sem.external_id, m.status, m.referee, m.home_formation, m.away_formation
    HAVING m.referee IS NULL
      OR m.home_formation IS NULL
      OR m.away_formation IS NULL
      OR (
        m.status IN ('finished', 'finished_aet', 'finished_pen', 'live_1h', 'live_ht', 'live_2h', 'live_et', 'live_pen')
        AND (
          COUNT(ms.id) < 2
          OR MAX(CASE WHEN ms.team_id = m.home_team_id THEN ms.total_passes END) IS NULL
          OR MAX(CASE WHEN ms.team_id = m.away_team_id THEN ms.total_passes END) IS NULL
        )
      )
    ORDER BY m.match_date DESC, m.id DESC
    ${limit && limit > 0 ? sql`LIMIT ${limit}` : sql``}
  `;

  return rows;
}

async function updateRefereesFromStoredMetadata(sql: Sql) {
  await sql`
    UPDATE matches
    SET
      referee = source_metadata->'api_football'->>'referee',
      updated_at = NOW()
    WHERE referee IS NULL
      AND source_metadata->'api_football'->>'referee' IS NOT NULL
  `;
}

async function updateFormationsFromStoredLineups(sql: Sql) {
  await sql`
    WITH formations AS (
      SELECT
        ml.match_id,
        ml.match_date,
        ml.team_id,
        MAX(ml.source_details->>'formation') AS formation
      FROM match_lineups ml
      WHERE COALESCE(ml.source_details->>'source', '') = 'api_football'
        AND ml.source_details->>'formation' IS NOT NULL
      GROUP BY ml.match_id, ml.match_date, ml.team_id
    )
    UPDATE matches m
    SET
      home_formation = COALESCE(m.home_formation, home_formations.formation),
      away_formation = COALESCE(m.away_formation, away_formations.formation),
      updated_at = NOW()
    FROM formations home_formations
    JOIN formations away_formations
      ON away_formations.match_id = home_formations.match_id
      AND away_formations.match_date = home_formations.match_date
    WHERE m.id = home_formations.match_id
      AND m.match_date = home_formations.match_date
      AND home_formations.team_id = m.home_team_id
      AND away_formations.team_id = m.away_team_id
      AND (
        (m.home_formation IS NULL AND home_formations.formation IS NOT NULL)
        OR (m.away_formation IS NULL AND away_formations.formation IS NOT NULL)
      )
  `;
}

async function upsertMatch(sql: Sql, draft: MatchUpdateDraft) {
  await sql`
    UPDATE matches
    SET
      referee = COALESCE(${draft.referee}, referee),
      home_formation = COALESCE(${draft.homeFormation}, home_formation),
      away_formation = COALESCE(${draft.awayFormation}, away_formation),
      updated_at = NOW()
    WHERE id = ${draft.matchId}
      AND match_date = ${draft.matchDate}
  `;
}

async function upsertMatchStats(sql: Sql, draft: MatchStatsDraft) {
  await sql`
    INSERT INTO match_stats (
      match_id,
      match_date,
      team_id,
      is_home,
      possession,
      total_passes,
      accurate_passes,
      pass_accuracy,
      total_shots,
      shots_on_target,
      shots_off_target,
      blocked_shots,
      corner_kicks,
      free_kicks,
      throw_ins,
      fouls,
      offsides,
      gk_saves,
      expected_goals
    )
    VALUES (
      ${draft.matchId},
      ${draft.matchDate},
      ${draft.teamId},
      ${draft.isHome},
      ${draft.possession},
      ${draft.totalPasses},
      ${draft.accuratePasses},
      ${draft.passAccuracy},
      ${draft.totalShots},
      ${draft.shotsOnTarget},
      ${draft.shotsOffTarget},
      ${draft.blockedShots},
      ${draft.cornerKicks},
      ${draft.freeKicks},
      ${draft.throwIns},
      ${draft.fouls},
      ${draft.offsides},
      ${draft.gkSaves},
      ${draft.expectedGoals}
    )
    ON CONFLICT (match_id, match_date, team_id)
    DO UPDATE SET
      is_home = EXCLUDED.is_home,
      possession = COALESCE(EXCLUDED.possession, match_stats.possession),
      total_passes = COALESCE(EXCLUDED.total_passes, match_stats.total_passes),
      accurate_passes = COALESCE(EXCLUDED.accurate_passes, match_stats.accurate_passes),
      pass_accuracy = COALESCE(EXCLUDED.pass_accuracy, match_stats.pass_accuracy),
      total_shots = COALESCE(EXCLUDED.total_shots, match_stats.total_shots),
      shots_on_target = COALESCE(EXCLUDED.shots_on_target, match_stats.shots_on_target),
      shots_off_target = COALESCE(EXCLUDED.shots_off_target, match_stats.shots_off_target),
      blocked_shots = COALESCE(EXCLUDED.blocked_shots, match_stats.blocked_shots),
      corner_kicks = COALESCE(EXCLUDED.corner_kicks, match_stats.corner_kicks),
      free_kicks = COALESCE(EXCLUDED.free_kicks, match_stats.free_kicks),
      throw_ins = COALESCE(EXCLUDED.throw_ins, match_stats.throw_ins),
      fouls = COALESCE(EXCLUDED.fouls, match_stats.fouls),
      offsides = COALESCE(EXCLUDED.offsides, match_stats.offsides),
      gk_saves = COALESCE(EXCLUDED.gk_saves, match_stats.gk_saves),
      expected_goals = COALESCE(EXCLUDED.expected_goals, match_stats.expected_goals)
  `;
}

async function mapWithConcurrency<T, R>(items: T[], concurrency: number, worker: (item: T) => Promise<R>) {
  if (items.length === 0) {
    return [] as R[];
  }

  const results = new Array<R>(items.length);
  let nextIndex = 0;

  const runWorker = async () => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      if (currentIndex >= items.length) {
        return;
      }

      results[currentIndex] = await worker(items[currentIndex]!);
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, () => runWorker()),
  );

  return results;
}

async function fetchTargetMatchData(
  target: TargetMatchRow,
  teamMappings: Map<string, number>,
): Promise<TargetMatchFetchResult> {
  const matchDraft: MatchUpdateDraft = {
    matchId: target.match_id,
    matchDate: target.match_date,
    referee: target.referee,
    homeFormation: target.home_formation,
    awayFormation: target.away_formation,
  };
  const statsDrafts: MatchStatsDraft[] = [];

  const [lineupPayload, statsPayload] = await Promise.all([
    target.needs_lineups
      ? fetchApiFootballJson<ApiFootballEnvelope<ApiFootballFixtureLineupResponseItem>>(
        buildApiFootballFixtureLineupsPath(target.external_fixture_id),
      )
      : Promise.resolve(null),
    target.needs_stats
      ? fetchApiFootballJson<ApiFootballEnvelope<ApiFootballFixtureStatisticsResponseItem>>(
        buildApiFootballFixtureStatisticsPath(target.external_fixture_id),
      )
      : Promise.resolve(null),
  ]);

  if (lineupPayload) {
    const lineupError = getApiFootballErrorMessage(lineupPayload);
    if (lineupError) {
      throw new Error(`API-Football lineups request failed for fixture ${target.external_fixture_id}: ${lineupError}`);
    }

    for (const teamLineup of lineupPayload.response ?? []) {
      const externalTeamId = teamLineup.team?.id ? String(teamLineup.team.id) : null;
      if (!externalTeamId) {
        continue;
      }

      const internalTeamId = teamMappings.get(externalTeamId);
      if (!internalTeamId) {
        continue;
      }

      if (internalTeamId === target.home_team_id) {
        matchDraft.homeFormation = teamLineup.formation ?? matchDraft.homeFormation;
      }

      if (internalTeamId === target.away_team_id) {
        matchDraft.awayFormation = teamLineup.formation ?? matchDraft.awayFormation;
      }
    }
  }

  if (statsPayload) {
    const statsError = getApiFootballErrorMessage(statsPayload);
    if (statsError) {
      throw new Error(`API-Football statistics request failed for fixture ${target.external_fixture_id}: ${statsError}`);
    }

    for (const teamStats of statsPayload.response ?? []) {
      const externalTeamId = teamStats.team?.id ? String(teamStats.team.id) : null;
      if (!externalTeamId) {
        continue;
      }

      const internalTeamId = teamMappings.get(externalTeamId);
      if (!internalTeamId) {
        continue;
      }

      statsDrafts.push(buildMatchStatsDraft(target, internalTeamId, teamStats.statistics));
    }
  }

  return {
    matchDraft,
    statsDrafts,
    lineupsFetched: lineupPayload ? 1 : 0,
    statsFetched: statsPayload ? 1 : 0,
  };
}

export async function backfillApiFootballMatchData(
  options: BackfillApiFootballMatchDataOptions = {},
): Promise<BackfillApiFootballMatchDataSummary> {
  const seasons = normalizeSeasons(options.seasons);
  const targets = parseApiFootballDataCompetitionTargets(options.competitionCodes);
  const sql = getDb();

  try {
    const sourceId = await ensureSource(sql);
    const teamMappings = await loadTeamMappings(sql, sourceId);

    if (!(options.dryRun ?? true)) {
      await updateFormationsFromStoredLineups(sql);
      await updateRefereesFromStoredMetadata(sql);
    }

    const targetMatches = await loadTargetMatches(sql, sourceId, targets, seasons, options.limit);

    if (targetMatches.length === 0) {
      return {
        dryRun: options.dryRun ?? true,
        targetMatches: 0,
        lineupsFetched: 0,
        statsFetched: 0,
        matchRowsPlanned: 0,
        matchRowsWritten: 0,
        statsRowsPlanned: 0,
        statsRowsWritten: 0,
        seasons,
        competitions: targets.map((target) => target.code),
      };
    }

    const matchDrafts = new Map<string, MatchUpdateDraft>();
    const statDrafts = new Map<string, MatchStatsDraft>();
    let lineupsFetched = 0;
    let statsFetched = 0;

    const fetchedMatches = await mapWithConcurrency(
      targetMatches,
      FETCH_CONCURRENCY,
      async (target) => fetchTargetMatchData(target, teamMappings),
    );

    for (const result of fetchedMatches) {
      lineupsFetched += result.lineupsFetched;
      statsFetched += result.statsFetched;
      matchDrafts.set(`${result.matchDraft.matchId}:${result.matchDraft.matchDate}`, result.matchDraft);

      for (const draft of result.statsDrafts) {
        statDrafts.set(`${draft.matchId}:${draft.matchDate}:${draft.teamId}`, draft);
      }
    }

    if (options.dryRun ?? true) {
      return {
        dryRun: true,
        targetMatches: targetMatches.length,
        lineupsFetched,
        statsFetched,
        matchRowsPlanned: matchDrafts.size,
        matchRowsWritten: 0,
        statsRowsPlanned: statDrafts.size,
        statsRowsWritten: 0,
        seasons,
        competitions: targets.map((target) => target.code),
      };
    }

    await sql`BEGIN`;
    try {
      const matchDraftList = Array.from(matchDrafts.values());
      for (let i = 0; i < matchDraftList.length; i += BATCH_SIZE) {
        const chunk = matchDraftList.slice(i, i + BATCH_SIZE);
        await sql`
          UPDATE matches m
          SET
            referee = COALESCE(t.referee, m.referee),
            home_formation = COALESCE(t.home_formation, m.home_formation),
            away_formation = COALESCE(t.away_formation, m.away_formation),
            updated_at = NOW()
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.matchId))}::int[],
            ${sql.array(chunk.map((r) => r.matchDate))}::text[],
            ${sql.array(chunk.map((r) => r.referee))}::text[],
            ${sql.array(chunk.map((r) => r.homeFormation))}::text[],
            ${sql.array(chunk.map((r) => r.awayFormation))}::text[]
          ) AS t(match_id, match_date, referee, home_formation, away_formation)
          WHERE m.id = t.match_id AND m.match_date = t.match_date
        `;
      }

      const statDraftList = Array.from(statDrafts.values());
      for (let i = 0; i < statDraftList.length; i += BATCH_SIZE) {
        const chunk = statDraftList.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO match_stats (
            match_id, match_date, team_id, is_home, possession,
            total_passes, accurate_passes, pass_accuracy,
            total_shots, shots_on_target, shots_off_target, blocked_shots,
            corner_kicks, free_kicks, throw_ins, fouls, offsides, gk_saves, expected_goals
          )
          SELECT
            t.match_id, t.match_date, t.team_id, t.is_home, t.possession,
            t.total_passes, t.accurate_passes, t.pass_accuracy,
            t.total_shots, t.shots_on_target, t.shots_off_target, t.blocked_shots,
            t.corner_kicks, t.free_kicks, t.throw_ins, t.fouls, t.offsides, t.gk_saves, t.expected_goals
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.matchId))}::int[],
            ${sql.array(chunk.map((r) => r.matchDate))}::text[],
            ${sql.array(chunk.map((r) => r.teamId))}::int[],
            ${sql.array(chunk.map((r) => r.isHome))}::bool[],
            ${sql.array(chunk.map((r) => r.possession))}::int[],
            ${sql.array(chunk.map((r) => r.totalPasses))}::int[],
            ${sql.array(chunk.map((r) => r.accuratePasses))}::int[],
            ${sql.array(chunk.map((r) => r.passAccuracy))}::int[],
            ${sql.array(chunk.map((r) => r.totalShots))}::int[],
            ${sql.array(chunk.map((r) => r.shotsOnTarget))}::int[],
            ${sql.array(chunk.map((r) => r.shotsOffTarget))}::int[],
            ${sql.array(chunk.map((r) => r.blockedShots))}::int[],
            ${sql.array(chunk.map((r) => r.cornerKicks))}::int[],
            ${sql.array(chunk.map((r) => r.freeKicks))}::int[],
            ${sql.array(chunk.map((r) => r.throwIns))}::int[],
            ${sql.array(chunk.map((r) => r.fouls))}::int[],
            ${sql.array(chunk.map((r) => r.offsides))}::int[],
            ${sql.array(chunk.map((r) => r.gkSaves))}::int[],
            ${sql.array(chunk.map((r) => r.expectedGoals))}::numeric[]
          ) AS t(match_id, match_date, team_id, is_home, possession,
                 total_passes, accurate_passes, pass_accuracy,
                 total_shots, shots_on_target, shots_off_target, blocked_shots,
                 corner_kicks, free_kicks, throw_ins, fouls, offsides, gk_saves, expected_goals)
          ON CONFLICT (match_id, match_date, team_id)
          DO UPDATE SET
            is_home = EXCLUDED.is_home,
            possession = COALESCE(EXCLUDED.possession, match_stats.possession),
            total_passes = COALESCE(EXCLUDED.total_passes, match_stats.total_passes),
            accurate_passes = COALESCE(EXCLUDED.accurate_passes, match_stats.accurate_passes),
            pass_accuracy = COALESCE(EXCLUDED.pass_accuracy, match_stats.pass_accuracy),
            total_shots = COALESCE(EXCLUDED.total_shots, match_stats.total_shots),
            shots_on_target = COALESCE(EXCLUDED.shots_on_target, match_stats.shots_on_target),
            shots_off_target = COALESCE(EXCLUDED.shots_off_target, match_stats.shots_off_target),
            blocked_shots = COALESCE(EXCLUDED.blocked_shots, match_stats.blocked_shots),
            corner_kicks = COALESCE(EXCLUDED.corner_kicks, match_stats.corner_kicks),
            free_kicks = COALESCE(EXCLUDED.free_kicks, match_stats.free_kicks),
            throw_ins = COALESCE(EXCLUDED.throw_ins, match_stats.throw_ins),
            fouls = COALESCE(EXCLUDED.fouls, match_stats.fouls),
            offsides = COALESCE(EXCLUDED.offsides, match_stats.offsides),
            gk_saves = COALESCE(EXCLUDED.gk_saves, match_stats.gk_saves),
            expected_goals = COALESCE(EXCLUDED.expected_goals, match_stats.expected_goals)
        `;
      }

      await sql`COMMIT`;
    } catch (error) {
      await sql`ROLLBACK`;
      throw error;
    }

    return {
      dryRun: false,
      targetMatches: targetMatches.length,
      lineupsFetched,
      statsFetched,
      matchRowsPlanned: matchDrafts.size,
      matchRowsWritten: matchDrafts.size,
      statsRowsPlanned: statDrafts.size,
      statsRowsWritten: statDrafts.size,
      seasons,
      competitions: targets.map((target) => target.code),
    };
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
