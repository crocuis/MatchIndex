import postgres, { type Sql } from 'postgres';
import { getApiFootballSourceConfig, parseApiFootballCompetitionTargets, type ApiFootballCompetitionTarget } from './apiFootball.ts';
import { backfillApiFootballPlayerContracts } from './apiFootballPlayerContractsBackfill.ts';
import { normalizePlayerSeasonYears } from './playerSeasonWindow.ts';
import { isCompetitionSeasonWriteAllowed, loadCompetitionSeasonPolicies } from './sourceOwnership.ts';

const BATCH_SIZE = 500;

interface SourceRow {
  id: number;
}

interface RawPayloadRow {
  payload: unknown;
  season_context: string | null;
  endpoint: string;
}

interface MaterializeTargetRow {
  competition_season_id: number;
  competition_slug: string;
  season_start_year: number;
}

interface PlayerMappingRow {
  entity_id: number;
  external_id: string;
}

interface AggregatedPlayerSeasonStats {
  playerId: number;
  competitionSeasonId: number;
  appearances: number;
  starts: number;
  minutesPlayed: number;
  goals: number;
  assists: number;
  penaltyGoals: number;
  ownGoals: number;
  yellowCards: number;
  redCards: number;
  yellowRedCards: number;
  cleanSheets: number;
  goalsConceded: number;
  saves: number;
  avgRating: number | null;
  ratingCount: number;
}

interface ApiFootballPlayerResponseItem {
  player?: {
    id?: number | string;
  };
  statistics?: ApiFootballPlayerStatistic[];
}

interface ApiFootballPlayerStatistic {
  league?: {
    id?: number | string;
    season?: number | string;
  };
  games?: {
    appearences?: number | null;
    lineups?: number | null;
    minutes?: number | null;
    rating?: string | number | null;
  };
  goals?: {
    total?: number | null;
    assists?: number | null;
    conceded?: number | null;
    saves?: number | null;
  };
  penalty?: {
    scored?: number | null;
    missed?: number | null;
    saved?: number | null;
  };
  cards?: {
    yellow?: number | null;
    red?: number | null;
    yellowred?: number | null;
  };
  substitutes?: {
    in?: number | null;
  };
  clean_sheet?: number | null;
}

export interface MaterializeApiFootballPlayerStatsOptions {
  dryRun?: boolean;
  competitionCodes?: string[];
  seasons?: number[];
  includeContractBackfill?: boolean;
}

export interface MaterializeApiFootballPlayerStatsSummary {
  dryRun: boolean;
  competitionCodes: string[];
  seasons: number[];
  rawPayloadsRead: number;
  playerMappingsFound: number;
  rowsPlanned: number;
  rowsWritten: number;
  unmatchedExternalPlayerIds: string[];
  contractBackfill: {
    enabled: boolean;
    teamMatchesFound: number;
    contractRowsPlanned: number;
    contractRowsWritten: number;
    unresolvedTeamNames: string[];
  };
}

function getMaterializeDb() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    max: 1,
    idle_timeout: 20,
    prepare: false,
  });
}

function toNumber(value: unknown, fallback: number = 0) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number.parseFloat(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return fallback;
}

function toRoundedRating(value: string | number | null | undefined) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const parsed = toNumber(value, Number.NaN);
  if (!Number.isFinite(parsed)) {
    return null;
  }

  return Math.round(parsed * 10) / 10;
}

async function ensureApiFootballSource(sql: Sql) {
  const config = getApiFootballSourceConfig();
  const rows = await sql<SourceRow[]>`
    SELECT id
    FROM data_sources
    WHERE slug = ${config.slug}
    LIMIT 1
  `;

  if (!rows[0]) {
    throw new Error('API-Football data source is not initialized');
  }

  return rows[0].id;
}

async function loadCompetitionSeasonTargets(
  sql: Sql,
  targets: ApiFootballCompetitionTarget[],
  seasons: number[],
) {
  const rows = await sql<MaterializeTargetRow[]>`
    SELECT
      cs.id AS competition_season_id,
      c.slug AS competition_slug,
      EXTRACT(YEAR FROM s.start_date)::INT AS season_start_year
    FROM competition_seasons cs
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    WHERE c.slug = ANY(${targets.map((target) => target.competitionSlug)})
      AND EXTRACT(YEAR FROM s.start_date)::INT = ANY(${seasons})
  `;

  const codeBySlug = new Map(targets.map((target) => [target.competitionSlug, target.code]));
  return new Map(
    rows.map((row) => [`${codeBySlug.get(row.competition_slug) ?? row.competition_slug.toUpperCase()}:${row.season_start_year}`, row])
  );
}

async function loadPlayerMappings(sql: Sql, sourceId: number) {
  const rows = await sql<PlayerMappingRow[]>`
    SELECT entity_id, external_id
    FROM source_entity_mapping
    WHERE entity_type = 'player'
      AND source_id = ${sourceId}
  `;

  return new Map(rows.map((row) => [row.external_id, row.entity_id]));
}

async function loadRawPayloads(
  sql: Sql,
  sourceId: number,
  seasons: number[],
) {
  return sql<RawPayloadRow[]>`
    SELECT DISTINCT ON (endpoint)
      payload,
      season_context,
      endpoint
    FROM raw_payloads
    WHERE source_id = ${sourceId}
      AND entity_type = 'player'
      AND season_context = ANY(${seasons.map(String)})
    ORDER BY endpoint, fetched_at DESC
  `;
}

function getPayloadItems(payload: unknown) {
  if (typeof payload === 'string') {
    try {
      return getPayloadItems(JSON.parse(payload));
    } catch {
      return [] as ApiFootballPlayerResponseItem[];
    }
  }

  if (!payload || typeof payload !== 'object') {
    return [] as ApiFootballPlayerResponseItem[];
  }

  const response = (payload as { response?: unknown[] }).response;
  return Array.isArray(response) ? response as ApiFootballPlayerResponseItem[] : [];
}

function buildAggregatedStats(
  rawPayloads: RawPayloadRow[],
  competitionSeasonByKey: Map<string, MaterializeTargetRow>,
  playerIdByExternalId: Map<string, number>,
  allowedLeagueIds: Set<string>,
) {
  const aggregated = new Map<string, AggregatedPlayerSeasonStats>();
  const unmatchedExternalPlayerIds = new Set<string>();

  for (const rawPayload of rawPayloads) {
    for (const item of getPayloadItems(rawPayload.payload)) {
      const externalPlayerId = item.player?.id ? String(item.player.id) : null;
      if (!externalPlayerId) {
        continue;
      }

      const playerId = playerIdByExternalId.get(externalPlayerId);
      if (!playerId) {
        unmatchedExternalPlayerIds.add(externalPlayerId);
        continue;
      }

      for (const statistic of item.statistics ?? []) {
        const leagueId = statistic.league?.id ? String(statistic.league.id) : null;
        const season = statistic.league?.season ? toNumber(statistic.league.season, Number.NaN) : Number.NaN;
        if (!leagueId || !allowedLeagueIds.has(leagueId) || !Number.isFinite(season)) {
          continue;
        }

        const competitionCode = parseApiFootballCompetitionTargets().find((target) => String(target.leagueId) === leagueId)?.code;
        if (!competitionCode) {
          continue;
        }

        const target = competitionSeasonByKey.get(`${competitionCode}:${season}`);
        if (!target) {
          continue;
        }

        const aggregateKey = `${playerId}:${target.competition_season_id}`;
        const current = aggregated.get(aggregateKey) ?? {
          playerId,
          competitionSeasonId: target.competition_season_id,
          appearances: 0,
          starts: 0,
          minutesPlayed: 0,
          goals: 0,
          assists: 0,
          penaltyGoals: 0,
          ownGoals: 0,
          yellowCards: 0,
          redCards: 0,
          yellowRedCards: 0,
          cleanSheets: 0,
          goalsConceded: 0,
          saves: 0,
          avgRating: null,
          ratingCount: 0,
        };

        current.appearances += toNumber(statistic.games?.appearences);
        current.starts += toNumber(statistic.games?.lineups);
        current.minutesPlayed += toNumber(statistic.games?.minutes);
        current.goals += toNumber(statistic.goals?.total);
        current.assists += toNumber(statistic.goals?.assists);
        current.penaltyGoals += toNumber(statistic.penalty?.scored);
        current.yellowCards += toNumber(statistic.cards?.yellow);
        current.redCards += toNumber(statistic.cards?.red);
        current.yellowRedCards += toNumber(statistic.cards?.yellowred);
        current.cleanSheets += toNumber(statistic.clean_sheet);
        current.goalsConceded += toNumber(statistic.goals?.conceded);
        current.saves += toNumber(statistic.goals?.saves) + toNumber(statistic.penalty?.saved);

        const rating = toRoundedRating(statistic.games?.rating);
        if (rating !== null) {
          const totalRating = (current.avgRating ?? 0) * current.ratingCount + rating;
          current.ratingCount += 1;
          current.avgRating = Math.round((totalRating / current.ratingCount) * 10) / 10;
        }

        aggregated.set(aggregateKey, current);
      }
    }
  }

  return {
    rows: Array.from(aggregated.values()),
    unmatchedExternalPlayerIds: Array.from(unmatchedExternalPlayerIds).sort(),
  };
}

async function upsertPlayerSeasonStats(sql: Sql, rows: AggregatedPlayerSeasonStats[]) {
  await sql`BEGIN`;
  try {
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const chunk = rows.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO player_season_stats (
          player_id, competition_season_id, appearances, starts, minutes_played,
          goals, assists, penalty_goals, own_goals, yellow_cards, red_cards,
          yellow_red_cards, clean_sheets, goals_conceded, saves, avg_rating, updated_at
        )
        SELECT
          t.player_id, t.competition_season_id, t.appearances, t.starts, t.minutes_played,
          t.goals, t.assists, t.penalty_goals, t.own_goals, t.yellow_cards, t.red_cards,
          t.yellow_red_cards, t.clean_sheets, t.goals_conceded, t.saves, t.avg_rating, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.playerId))}::int[],
          ${sql.array(chunk.map((r) => r.competitionSeasonId))}::int[],
          ${sql.array(chunk.map((r) => r.appearances))}::int[],
          ${sql.array(chunk.map((r) => r.starts))}::int[],
          ${sql.array(chunk.map((r) => r.minutesPlayed))}::int[],
          ${sql.array(chunk.map((r) => r.goals))}::int[],
          ${sql.array(chunk.map((r) => r.assists))}::int[],
          ${sql.array(chunk.map((r) => r.penaltyGoals))}::int[],
          ${sql.array(chunk.map((r) => r.ownGoals))}::int[],
          ${sql.array(chunk.map((r) => r.yellowCards))}::int[],
          ${sql.array(chunk.map((r) => r.redCards))}::int[],
          ${sql.array(chunk.map((r) => r.yellowRedCards))}::int[],
          ${sql.array(chunk.map((r) => r.cleanSheets))}::int[],
          ${sql.array(chunk.map((r) => r.goalsConceded))}::int[],
          ${sql.array(chunk.map((r) => r.saves))}::int[],
          ${sql.array(chunk.map((r) => r.avgRating))}::numeric[]
        ) AS t(player_id, competition_season_id, appearances, starts, minutes_played,
               goals, assists, penalty_goals, own_goals, yellow_cards, red_cards,
               yellow_red_cards, clean_sheets, goals_conceded, saves, avg_rating)
        ON CONFLICT (player_id, competition_season_id)
        DO UPDATE SET
          appearances = EXCLUDED.appearances,
          starts = EXCLUDED.starts,
          minutes_played = EXCLUDED.minutes_played,
          goals = EXCLUDED.goals,
          assists = EXCLUDED.assists,
          penalty_goals = EXCLUDED.penalty_goals,
          own_goals = EXCLUDED.own_goals,
          yellow_cards = EXCLUDED.yellow_cards,
          red_cards = EXCLUDED.red_cards,
          yellow_red_cards = EXCLUDED.yellow_red_cards,
          clean_sheets = EXCLUDED.clean_sheets,
          goals_conceded = EXCLUDED.goals_conceded,
          saves = EXCLUDED.saves,
          avg_rating = EXCLUDED.avg_rating,
          updated_at = NOW()
      `;
    }
    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }
}

async function refreshDerivedViews(sql: Sql) {
  await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;
}

export async function materializeApiFootballPlayerStats(
  options: MaterializeApiFootballPlayerStatsOptions = {},
): Promise<MaterializeApiFootballPlayerStatsSummary> {
  const targets = parseApiFootballCompetitionTargets(options.competitionCodes);
  const seasons = normalizePlayerSeasonYears(options.seasons);
  const includeContractBackfill = options.includeContractBackfill ?? true;
  const sql = getMaterializeDb();

  try {
    const sourceId = await ensureApiFootballSource(sql);
    const competitionSeasonByKey = await loadCompetitionSeasonTargets(sql, targets, seasons);
    const playerIdByExternalId = await loadPlayerMappings(sql, sourceId);
    const rawPayloads = await loadRawPayloads(sql, sourceId, seasons);
    const { rows, unmatchedExternalPlayerIds } = buildAggregatedStats(
      rawPayloads,
      competitionSeasonByKey,
      playerIdByExternalId,
      new Set(targets.map((target) => String(target.leagueId))),
    );
    const policies = await loadCompetitionSeasonPolicies(sql, Array.from(new Set(rows.map((row) => row.competitionSeasonId))));
    const allowedRows = rows.filter((row) => isCompetitionSeasonWriteAllowed(
      policies.get(row.competitionSeasonId),
      'playerSeasonStats',
      'api_football',
      'sync',
    ));
    const contractBackfillSummary = includeContractBackfill
      ? await backfillApiFootballPlayerContracts({
          dryRun: options.dryRun ?? true,
          competitionCodes: targets.map((target) => target.code),
          seasons,
        })
      : {
          dryRun: options.dryRun ?? true,
          rawPayloadsRead: 0,
          playerMappingsFound: 0,
          teamMatchesFound: 0,
          contractRowsPlanned: 0,
          contractRowsWritten: 0,
          unresolvedTeamNames: [],
        };

    if (options.dryRun ?? true) {
      return {
        dryRun: true,
        competitionCodes: targets.map((target) => target.code),
        seasons,
        rawPayloadsRead: rawPayloads.length,
        playerMappingsFound: playerIdByExternalId.size,
        rowsPlanned: allowedRows.length,
        rowsWritten: 0,
        unmatchedExternalPlayerIds,
        contractBackfill: {
          enabled: includeContractBackfill,
          teamMatchesFound: contractBackfillSummary.teamMatchesFound,
          contractRowsPlanned: contractBackfillSummary.contractRowsPlanned,
          contractRowsWritten: 0,
          unresolvedTeamNames: contractBackfillSummary.unresolvedTeamNames,
        },
      };
    }

    await upsertPlayerSeasonStats(sql, allowedRows);
    await refreshDerivedViews(sql);

    return {
      dryRun: false,
      competitionCodes: targets.map((target) => target.code),
      seasons,
      rawPayloadsRead: rawPayloads.length,
      playerMappingsFound: playerIdByExternalId.size,
      rowsPlanned: allowedRows.length,
      rowsWritten: allowedRows.length,
      unmatchedExternalPlayerIds,
      contractBackfill: {
        enabled: includeContractBackfill,
        teamMatchesFound: contractBackfillSummary.teamMatchesFound,
        contractRowsPlanned: contractBackfillSummary.contractRowsPlanned,
        contractRowsWritten: contractBackfillSummary.contractRowsWritten,
        unresolvedTeamNames: contractBackfillSummary.unresolvedTeamNames,
      },
    };
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
