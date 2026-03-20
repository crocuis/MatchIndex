import postgres, { type Sql } from 'postgres';
import { isCompetitionSeasonWriteAllowed, loadCompetitionSeasonPolicies } from './sourceOwnership.ts';

const BATCH_SIZE = 500;

interface SourceRow {
  id: number;
}

interface RawPayloadRow {
  endpoint: string;
  external_id: string | null;
  payload: unknown;
  season_context: string | null;
}

interface CompetitionSeasonRow {
  competition_season_id: number;
}

const LEAGUE_BY_COMPETITION_CODE: Record<string, string> = {
  BL1: 'GER-Bundesliga',
  FL1: 'FRA-Ligue 1',
  PD: 'ESP-La Liga',
  PL: 'ENG-Premier League',
  SA: 'ITA-Serie A',
};

interface PlayerMappingRow {
  entity_id: number;
  external_id: string;
}

interface PlayerRosterRow {
  player_id: number;
  player_name: string;
  team_id: number;
  team_name: string;
}

interface PlayerCanonicalRow {
  player_id: number;
  player_name: string;
  player_slug: string;
}

export interface FbrefUnresolvedPlayerCandidate {
  competitionCode: string;
  playerName: string;
  playerSlugCandidate: string;
  playerHref: string | null;
  season: string;
  teamName: string;
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
}

export interface MaterializeSoccerdataFbrefOptions {
  competitionCode?: string;
  dryRun?: boolean;
  season?: string;
  sourceSlug?: string;
}

export interface MaterializeSoccerdataFbrefSummary {
  competitionCode: string | null;
  dryRun: boolean;
  implemented: boolean;
  nextStep: string;
  playerMappingsFound: number;
  rawPayloadsRead: number;
  rowsPlanned: number;
  rowsWritten: number;
  season: string | null;
  sourceSlug: string;
  unmatchedExternalPlayerIds: string[];
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

function normalizeKey(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function normalizeName(value: string | null | undefined) {
  return (value ?? '').normalize('NFKD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-zA-Z0-9]+/g, ' ').trim().toLowerCase();
}

function normalizeTeamName(value: string | null | undefined) {
  return normalizeName(value)
    .replace(/\bfootball club\b/g, ' ')
    .replace(/\bafc\b/g, ' ')
    .replace(/\bfc\b/g, ' ')
    .replace(/\band\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function toObjectMap(payload: unknown) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return new Map<string, unknown>();
  }

  return new Map(
    Object.entries(payload as Record<string, unknown>).map(([key, value]) => [normalizeKey(key), value]),
  );
}

function toNumber(value: unknown) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const stripped = value.replace(/,/g, '').trim();
    if (!stripped) {
      return 0;
    }

    const parsed = Number.parseFloat(stripped);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  return 0;
}

function toRoundedRating(value: unknown) {
  const parsed = toNumber(value);
  return parsed > 0 ? Math.round(parsed * 10) / 10 : null;
}

function pickValue(values: Map<string, unknown>, keys: string[]) {
  for (const key of keys) {
    const value = values.get(normalizeKey(key));

    if (value !== undefined && value !== null && value !== '') {
      return value;
    }
  }

  return undefined;
}

function parseSeasonYear(season: string | null) {
  if (!season) {
    return null;
  }

  const match = season.match(/(\d{4})/);
  return match ? Number.parseInt(match[1]!, 10) : null;
}

async function ensureSoccerdataFbrefSource(sql: Sql, sourceSlug: string) {
  const rows = await sql<SourceRow[]>`
    SELECT id
    FROM data_sources
    WHERE slug = ${sourceSlug}
    LIMIT 1
  `;

  if (!rows[0]) {
    throw new Error(`Data source '${sourceSlug}' is not initialized`);
  }

  return rows[0].id;
}

async function loadCompetitionSeasonTarget(sql: Sql, competitionCode: string, season: string) {
  const seasonYear = parseSeasonYear(season);

  if (!seasonYear) {
    return null;
  }

  const rows = await sql<CompetitionSeasonRow[]>`
    SELECT cs.id AS competition_season_id
    FROM competition_seasons cs
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    WHERE LOWER(c.code) = LOWER(${competitionCode})
      AND EXTRACT(YEAR FROM s.start_date)::INT = ${seasonYear}
    LIMIT 1
  `;

  return rows[0] ?? null;
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

async function loadCompetitionRoster(sql: Sql, competitionSeasonId: number) {
  const rows = await sql<PlayerRosterRow[]>`
    SELECT DISTINCT
      pc.player_id,
      COALESCE(player_en.known_as, p.slug) AS player_name,
      pc.team_id,
      COALESCE(team_en.name, t.slug) AS team_name
    FROM player_contracts pc
    JOIN players p ON p.id = pc.player_id
    JOIN teams t ON t.id = pc.team_id
    LEFT JOIN player_translations player_en ON player_en.player_id = p.id AND player_en.locale = 'en'
    LEFT JOIN team_translations team_en ON team_en.team_id = t.id AND team_en.locale = 'en'
    WHERE pc.competition_season_id = ${competitionSeasonId}
  `;

  return rows;
}

async function loadCanonicalPlayers(sql: Sql) {
  return sql<PlayerCanonicalRow[]>`
    SELECT
      p.id AS player_id,
      COALESCE(player_en.known_as, p.slug) AS player_name,
      p.slug AS player_slug
    FROM players p
    LEFT JOIN player_translations player_en ON player_en.player_id = p.id AND player_en.locale = 'en'
  `;
}

async function loadRawPayloads(sql: Sql, sourceId: number, season: string, competitionCode: string) {
  const league = LEAGUE_BY_COMPETITION_CODE[competitionCode.toUpperCase()];

  if (!league) {
    return [];
  }

  return sql<RawPayloadRow[]>`
    SELECT DISTINCT ON (endpoint, external_id)
      endpoint,
      external_id,
      payload,
      season_context
    FROM raw_payloads
    WHERE source_id = ${sourceId}
      AND entity_type = 'player'
      AND season_context = ${season}
      AND endpoint LIKE ${'%player_season_stats_standard'}
      AND endpoint LIKE ${`fbref-%://${league}/${season}/%`}
    ORDER BY endpoint, external_id, fetched_at DESC
  `;
}

function extractFbrefPlayerSlug(payloadMap: Map<string, unknown>) {
  const href = String(pickValue(payloadMap, ['player_href']) ?? '').trim();

  if (!href) {
    return null;
  }

  const segments = href.split('/').filter(Boolean);
  const slug = segments[segments.length - 1];
  return slug ? slug.toLowerCase() : null;
}

function buildSlugCandidate(playerName: string) {
  return playerName
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .toLowerCase();
}

function extractFbrefPlayerHref(payloadMap: Map<string, unknown>) {
  const href = String(pickValue(payloadMap, ['player_href']) ?? '').trim();
  return href || null;
}

function extractFbrefExternalIds(payloadMap: Map<string, unknown>) {
  const href = String(pickValue(payloadMap, ['player_href']) ?? '').trim();

  if (!href) {
    return [] as string[];
  }

  return href.startsWith('http')
    ? Array.from(new Set([href, new URL(href).pathname]))
    : [href, `https://fbref.com${href}`];
}

function resolvePlayerId(
  rawPayload: RawPayloadRow,
  payloadMap: Map<string, unknown>,
  playerIdByExternalId: Map<string, number>,
  roster: PlayerRosterRow[],
  canonicalPlayers: PlayerCanonicalRow[],
) {
  if (rawPayload.external_id) {
    const mapped = playerIdByExternalId.get(rawPayload.external_id);
    if (mapped) {
      return mapped;
    }
  }

  for (const externalId of extractFbrefExternalIds(payloadMap)) {
    const mapped = playerIdByExternalId.get(externalId);
    if (mapped) {
      return mapped;
    }
  }

  const playerName = normalizeName(String(pickValue(payloadMap, ['player', 'playername', 'name']) ?? ''));
  const teamName = normalizeTeamName(String(pickValue(payloadMap, ['team', 'squad', 'teamname']) ?? ''));
  const fbrefPlayerSlug = extractFbrefPlayerSlug(payloadMap);

  if (!playerName || !teamName) {
    return null;
  }

  if (fbrefPlayerSlug) {
    const slugMatches = canonicalPlayers.filter((row) => row.player_slug === fbrefPlayerSlug);
    if (slugMatches.length === 1) {
      return slugMatches[0]!.player_id;
    }
  }

  const candidates = roster.filter((row) => normalizeName(row.player_name) === playerName && normalizeTeamName(row.team_name) === teamName);

  if (candidates.length === 1) {
    return candidates[0]!.player_id;
  }

  const playerOnlyCandidates = roster.filter((row) => normalizeName(row.player_name) === playerName);
  if (playerOnlyCandidates.length === 1) {
    return playerOnlyCandidates[0]!.player_id;
  }

  return candidates.length === 1 ? candidates[0]!.player_id : null;
}

function buildAggregatedStats(
  rawPayloads: RawPayloadRow[],
  competitionSeasonId: number,
  playerIdByExternalId: Map<string, number>,
  roster: PlayerRosterRow[],
  canonicalPlayers: PlayerCanonicalRow[],
) {
  const aggregated = new Map<string, AggregatedPlayerSeasonStats>();
  const unmatchedExternalPlayerIds = new Set<string>();

  for (const rawPayload of rawPayloads) {
    const values = toObjectMap(rawPayload.payload);
    const playerId = resolvePlayerId(rawPayload, values, playerIdByExternalId, roster, canonicalPlayers);

    if (!playerId) {
      unmatchedExternalPlayerIds.add(rawPayload.external_id ?? JSON.stringify(rawPayload.payload));
      continue;
    }

    const key = `${playerId}:${competitionSeasonId}`;
    const current = aggregated.get(key) ?? {
      playerId,
      competitionSeasonId,
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
    };

    current.appearances = toNumber(pickValue(values, ['playing time mp', 'playing_time_mp', 'mp', 'matches', 'apps']));
    current.starts = toNumber(pickValue(values, ['playing time starts', 'playing_time_starts', 'starts']));
    current.minutesPlayed = toNumber(pickValue(values, ['playing time min', 'playing_time_min', 'min', 'minutes']));
    current.goals = toNumber(pickValue(values, ['performance gls', 'gls', 'goals']));
    current.assists = toNumber(pickValue(values, ['performance ast', 'ast', 'assists']));
    current.penaltyGoals = toNumber(pickValue(values, ['performance pk', 'pk', 'penalty goals']));
    current.ownGoals = toNumber(pickValue(values, ['performance og', 'og', 'own goals']));
    current.yellowCards = toNumber(pickValue(values, ['performance crdy', 'crdy', 'yellow cards']));
    current.redCards = toNumber(pickValue(values, ['performance crdr', 'crdr', 'red cards']));
    current.yellowRedCards = toNumber(pickValue(values, ['yellowredcards', 'yellow red cards']));
    current.cleanSheets = toNumber(pickValue(values, ['performance cs', 'cs', 'clean sheets']));
    current.goalsConceded = toNumber(pickValue(values, ['performance ga', 'ga', 'goals against', 'goals conceded']));
    current.saves = toNumber(pickValue(values, ['performance saves', 'saves']));
    current.avgRating = toRoundedRating(pickValue(values, ['performance rating', 'rating', 'avg rating']));

    aggregated.set(key, current);
  }

  return {
    rows: Array.from(aggregated.values()),
    unmatchedExternalPlayerIds: Array.from(unmatchedExternalPlayerIds).sort(),
  };
}

async function upsertPlayerSeasonStats(sql: Sql, rows: AggregatedPlayerSeasonStats[]) {
  if (rows.length === 0) {
    return;
  }

  await sql`BEGIN`;

  try {
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const chunk = rows.slice(i, i + BATCH_SIZE);

      await sql`
        INSERT INTO player_season_stats (
          player_id, competition_season_id,
          appearances, starts, minutes_played,
          goals, assists, penalty_goals, own_goals,
          yellow_cards, red_cards, yellow_red_cards,
          clean_sheets, goals_conceded, saves, avg_rating,
          updated_at
        )
        SELECT *, NOW()
        FROM UNNEST(
          ${chunk.map((r) => r.playerId)}::int[],
          ${chunk.map((r) => r.competitionSeasonId)}::int[],
          ${chunk.map((r) => r.appearances)}::int[],
          ${chunk.map((r) => r.starts)}::int[],
          ${chunk.map((r) => r.minutesPlayed)}::int[],
          ${chunk.map((r) => r.goals)}::int[],
          ${chunk.map((r) => r.assists)}::int[],
          ${chunk.map((r) => r.penaltyGoals)}::int[],
          ${chunk.map((r) => r.ownGoals)}::int[],
          ${chunk.map((r) => r.yellowCards)}::int[],
          ${chunk.map((r) => r.redCards)}::int[],
          ${chunk.map((r) => r.yellowRedCards)}::int[],
          ${chunk.map((r) => r.cleanSheets)}::int[],
          ${chunk.map((r) => r.goalsConceded)}::int[],
          ${chunk.map((r) => r.saves)}::int[],
          ${chunk.map((r) => r.avgRating)}::numeric[]
        )
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
    await sql`ROLLBACK`.catch(() => undefined);
    throw error;
  }
}

async function refreshDerivedViews(sql: Sql) {
  await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;
}

export async function materializeSoccerdataFbref(
  options: MaterializeSoccerdataFbrefOptions = {},
): Promise<MaterializeSoccerdataFbrefSummary> {
  const competitionCode = options.competitionCode?.trim().toUpperCase() || null;
  const season = options.season?.trim() || null;
  const sourceSlug = options.sourceSlug?.trim() || 'soccerdata_fbref';
  const summary: MaterializeSoccerdataFbrefSummary = {
    competitionCode,
    dryRun: options.dryRun ?? true,
    implemented: true,
    nextStep: 'Review unmatchedExternalPlayerIds and extend source_entity_mapping or roster matching quality if needed.',
    playerMappingsFound: 0,
    rawPayloadsRead: 0,
    rowsPlanned: 0,
    rowsWritten: 0,
    season,
    sourceSlug,
    unmatchedExternalPlayerIds: [],
  };

  if (!competitionCode || !season) {
    return summary;
  }

  const sql = getMaterializeDb();

  try {
    const sourceId = await ensureSoccerdataFbrefSource(sql, sourceSlug);
    const competitionSeason = await loadCompetitionSeasonTarget(sql, competitionCode, season);

    if (!competitionSeason) {
      return summary;
    }

    const [playerIdByExternalId, roster, canonicalPlayers, rawPayloads] = await Promise.all([
      loadPlayerMappings(sql, sourceId),
      loadCompetitionRoster(sql, competitionSeason.competition_season_id),
      loadCanonicalPlayers(sql),
      loadRawPayloads(sql, sourceId, season, competitionCode),
    ]);
    const policies = await loadCompetitionSeasonPolicies(sql, [competitionSeason.competition_season_id]);
    const { rows, unmatchedExternalPlayerIds } = buildAggregatedStats(
      rawPayloads,
      competitionSeason.competition_season_id,
      playerIdByExternalId,
      roster,
      canonicalPlayers,
    );
    const allowedRows = rows.filter((row) => isCompetitionSeasonWriteAllowed(
      policies.get(row.competitionSeasonId),
      'playerSeasonStats',
      'fbref',
      'backfill',
    ));

    summary.playerMappingsFound = playerIdByExternalId.size;
    summary.rawPayloadsRead = rawPayloads.length;
    summary.rowsPlanned = allowedRows.length;
    summary.unmatchedExternalPlayerIds = unmatchedExternalPlayerIds;

    if (summary.dryRun) {
      return summary;
    }

    await upsertPlayerSeasonStats(sql, allowedRows);
    await refreshDerivedViews(sql);
    summary.rowsWritten = allowedRows.length;
    return summary;
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}

export async function listUnresolvedSoccerdataFbrefPlayers(
  options: Pick<MaterializeSoccerdataFbrefOptions, 'competitionCode' | 'season' | 'sourceSlug'> = {},
): Promise<FbrefUnresolvedPlayerCandidate[]> {
  const competitionCode = options.competitionCode?.trim().toUpperCase() || null;
  const season = options.season?.trim() || null;
  const sourceSlug = options.sourceSlug?.trim() || 'soccerdata_fbref';

  if (!competitionCode || !season) {
    return [];
  }

  const sql = getMaterializeDb();

  try {
    const sourceId = await ensureSoccerdataFbrefSource(sql, sourceSlug);
    const competitionSeason = await loadCompetitionSeasonTarget(sql, competitionCode, season);

    if (!competitionSeason) {
      return [];
    }

    const [playerIdByExternalId, roster, canonicalPlayers, rawPayloads] = await Promise.all([
      loadPlayerMappings(sql, sourceId),
      loadCompetitionRoster(sql, competitionSeason.competition_season_id),
      loadCanonicalPlayers(sql),
      loadRawPayloads(sql, sourceId, season, competitionCode),
    ]);

    const unresolved = new Map<string, FbrefUnresolvedPlayerCandidate>();

    for (const rawPayload of rawPayloads) {
      const payloadMap = toObjectMap(rawPayload.payload);
      const playerId = resolvePlayerId(rawPayload, payloadMap, playerIdByExternalId, roster, canonicalPlayers);

      if (playerId) {
        continue;
      }

      const playerName = String(pickValue(payloadMap, ['player', 'playername', 'name']) ?? '').trim();
      const teamName = String(pickValue(payloadMap, ['team', 'squad', 'teamname']) ?? '').trim();

      if (!playerName) {
        continue;
      }

      const key = rawPayload.external_id ?? `${playerName}:${teamName}:${season}`;
      unresolved.set(key, {
        competitionCode,
        playerName,
        playerSlugCandidate: buildSlugCandidate(playerName),
        playerHref: extractFbrefPlayerHref(payloadMap),
        season,
        teamName,
      });
    }

    return Array.from(unresolved.values()).sort((left, right) => left.playerName.localeCompare(right.playerName));
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
