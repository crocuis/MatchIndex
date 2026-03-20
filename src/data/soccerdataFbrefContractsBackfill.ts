import postgres, { type Sql } from 'postgres';
import { isCompetitionSeasonWriteAllowed, loadCompetitionSeasonPolicies } from './sourceOwnership.ts';

const BATCH_SIZE = 500;

interface SourceRow {
  id: number;
}

interface CompetitionSeasonRow {
  competition_season_id: number;
}

interface RawPayloadRow {
  endpoint: string;
  external_id: string | null;
  payload: unknown;
}

interface TeamRow {
  id: number;
  slug: string;
  team_name: string;
}

interface PlayerRow {
  id: number;
  player_name: string;
}

interface PlayerMappingRow {
  entity_id: number;
  external_id: string;
}

interface ContractDraft {
  competitionSeasonId: number;
  playerId: number;
  teamId: number;
}

export interface BackfillSoccerdataFbrefContractsOptions {
  competitionCode?: string;
  dryRun?: boolean;
  season?: string;
  sourceSlug?: string;
}

export interface BackfillSoccerdataFbrefContractsSummary {
  competitionCode: string | null;
  contractRowsPlanned: number;
  contractRowsWritten: number;
  dryRun: boolean;
  playerMappingsFound: number;
  rawPlayerRowsRead: number;
  rawTeamRowsRead: number;
  season: string | null;
  sourceSlug: string;
  unresolvedPlayerIds: string[];
  unresolvedTeamNames: string[];
}

const LEAGUE_BY_COMPETITION_CODE: Record<string, string> = {
  BL1: 'GER-Bundesliga',
  FL1: 'FRA-Ligue 1',
  PD: 'ESP-La Liga',
  PL: 'ENG-Premier League',
  SA: 'ITA-Serie A',
};

function getDb() {
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

function expandTeamAliases(value: string) {
  const normalized = normalizeTeamName(value);
  const aliases = new Set<string>([normalized]);

  if (normalized == 'bayern munich') aliases.add('bayern munchen');
  if (normalized == 'dortmund') aliases.add('borussia dortmund');
  if (normalized == 'gladbach') aliases.add('borussia monchengladbach');
  if (normalized == 'leverkusen') aliases.add('bayer leverkusen');
  if (normalized == 'mainz 05') aliases.add('mainz');
  if (normalized == 'stuttgart') aliases.add('vfb stuttgart');
  if (normalized == 'union berlin') aliases.add('1 fc union berlin');
  if (normalized == 'werder bremen') aliases.add('bremen');
  if (normalized == 'wolfsburg') aliases.add('vfl wolfsburg');
  if (normalized == 'alaves') aliases.add('deportivo alaves');
  if (normalized == 'athletic club') aliases.add('athletic bilbao');
  if (normalized == 'espanyol') aliases.add('espanyol barcelona');
  if (normalized == 'leganes') aliases.add('leganes madrid');
  if (normalized == 'mallorca') aliases.add('real mallorca');
  if (normalized == 'betis') aliases.add('real betis');
  if (normalized == 'sociedad') aliases.add('real sociedad');
  if (normalized == 'valladolid') aliases.add('real valladolid');
  if (normalized == 'las palmas') aliases.add('ud las palmas');
  if (normalized == 'monaco') aliases.add('as monaco');
  if (normalized == 'marseille') aliases.add('olympique marseille');
  if (normalized == 'lyon') aliases.add('olympique lyonnais');
  if (normalized == 'angers') aliases.add('angers sco');
  if (normalized == 'paris sg') aliases.add('paris saint germain');
  if (normalized == 'inter') aliases.add('internazionale');
  if (normalized == 'milan') aliases.add('ac milan');
  if (normalized == 'juventus') aliases.add('juve');
  if (normalized == 'roma') aliases.add('as roma');
  if (normalized == 'lazio') aliases.add('ss lazio');

  return Array.from(aliases);
}

function parseSeasonYear(season: string | null) {
  if (!season) {
    return null;
  }

  const match = season.match(/(\d{4})/);
  return match ? Number.parseInt(match[1]!, 10) : null;
}

function toObjectMap(payload: unknown) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return new Map<string, unknown>();
  }

  return new Map(
    Object.entries(payload as Record<string, unknown>).map(([key, value]) => [key.toLowerCase().replace(/[^a-z0-9]+/g, ''), value]),
  );
}

function pickValue(values: Map<string, unknown>, keys: string[]) {
  for (const key of keys) {
    const value = values.get(key.toLowerCase().replace(/[^a-z0-9]+/g, ''));
    if (value !== undefined && value !== null && value !== '') {
      return value;
    }
  }

  return undefined;
}

async function ensureSource(sql: Sql, sourceSlug: string) {
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

async function loadCompetitionSeason(sql: Sql, competitionCode: string, season: string) {
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
    WHERE source_id = ${sourceId}
      AND entity_type = 'player'
  `;

  return new Map(rows.map((row) => [row.external_id, row.entity_id]));
}

async function loadTeams(sql: Sql) {
  return sql<TeamRow[]>`
    SELECT t.id, t.slug, COALESCE(tt.name, t.slug) AS team_name
    FROM teams t
    LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
  `;
}

async function loadPlayers(sql: Sql) {
  return sql<PlayerRow[]>`
    SELECT p.id, COALESCE(pt.known_as, p.slug) AS player_name
    FROM players p
    LEFT JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
  `;
}

async function loadRawPayloads(sql: Sql, sourceId: number, season: string, competitionCode: string, entityType: 'player' | 'team', datasetSuffix: string) {
  const league = LEAGUE_BY_COMPETITION_CODE[competitionCode.toUpperCase()];

  if (!league) {
    return [] as RawPayloadRow[];
  }

  return sql<RawPayloadRow[]>`
    SELECT DISTINCT ON (endpoint, external_id)
      endpoint,
      external_id,
      payload
    FROM raw_payloads
    WHERE source_id = ${sourceId}
      AND entity_type = ${entityType}
      AND season_context = ${season}
      AND endpoint LIKE ${`fbref-%://${league}/${season}/${datasetSuffix}`}
    ORDER BY endpoint, external_id, fetched_at DESC
  `;
}

function buildTeamIndex(teams: TeamRow[]) {
  const index = new Map<string, TeamRow[]>();

  for (const team of teams) {
    for (const key of expandTeamAliases(team.team_name)) {
      const current = index.get(key) ?? [];
      current.push(team);
      index.set(key, current);
    }
  }

  return index;
}

function buildPlayerIndex(players: PlayerRow[]) {
  const index = new Map<string, PlayerRow[]>();

  for (const player of players) {
    const key = normalizeName(player.player_name);
    const current = index.get(key) ?? [];
    current.push(player);
    index.set(key, current);
  }

  return index;
}

function resolveTeamId(payload: unknown, teamIndex: Map<string, TeamRow[]>) {
  const values = toObjectMap(payload);
  const teamName = normalizeTeamName(String(pickValue(values, ['team', 'squad', 'teamname']) ?? ''));

  if (!teamName) {
    return null;
  }

  const candidates = expandTeamAliases(teamName).flatMap((alias) => teamIndex.get(alias) ?? []);
  const uniqueCandidates = Array.from(new Map(candidates.map((team) => [team.id, team])).values());
  return uniqueCandidates.length === 1 ? uniqueCandidates[0]!.id : null;
}

function resolvePlayerId(rawPayload: RawPayloadRow, payload: unknown, playerMappings: Map<string, number>, playerIndex: Map<string, PlayerRow[]>) {
  if (rawPayload.external_id) {
    const mapped = playerMappings.get(rawPayload.external_id);
    if (mapped) {
      return mapped;
    }
  }

  const values = toObjectMap(payload);
  const playerName = normalizeName(String(pickValue(values, ['player', 'playername', 'name']) ?? ''));
  if (!playerName) {
    return null;
  }

  const candidates = playerIndex.get(playerName) ?? [];
  return candidates.length === 1 ? candidates[0]!.id : null;
}

async function upsertContractsBatch(sql: Sql, drafts: ContractDraft[]) {
  const allDrafts = Array.from(drafts);

  if (allDrafts.length === 0) {
    return;
  }

  await sql`BEGIN`;

  try {
    for (let i = 0; i < allDrafts.length; i += BATCH_SIZE) {
      const chunk = allDrafts.slice(i, i + BATCH_SIZE);

      await sql`
        INSERT INTO player_contracts (
          player_id, team_id, competition_season_id,
          shirt_number, is_on_loan, left_date, updated_at
        )
        SELECT *, NULL::int, FALSE, NULL::date, NOW()
        FROM UNNEST(
          ${chunk.map((d) => d.playerId)}::int[],
          ${chunk.map((d) => d.teamId)}::int[],
          ${chunk.map((d) => d.competitionSeasonId)}::int[]
        )
        ON CONFLICT (player_id, competition_season_id)
        DO UPDATE SET
          team_id = EXCLUDED.team_id,
          updated_at = NOW()
      `;
    }

    const uniqueTeamSeasons = Array.from(
      new Map(allDrafts.map((d) => [`${d.teamId}:${d.competitionSeasonId}`, d])).values(),
    );

    for (let i = 0; i < uniqueTeamSeasons.length; i += BATCH_SIZE) {
      const chunk = uniqueTeamSeasons.slice(i, i + BATCH_SIZE);

      await sql`
        INSERT INTO team_seasons (team_id, competition_season_id, updated_at)
        SELECT *, NOW()
        FROM UNNEST(
          ${chunk.map((d) => d.teamId)}::int[],
          ${chunk.map((d) => d.competitionSeasonId)}::int[]
        )
        ON CONFLICT (team_id, competition_season_id)
        DO UPDATE SET updated_at = NOW()
      `;
    }

    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`.catch(() => undefined);
    throw error;
  }
}

export async function backfillSoccerdataFbrefContracts(
  options: BackfillSoccerdataFbrefContractsOptions = {},
): Promise<BackfillSoccerdataFbrefContractsSummary> {
  const competitionCode = options.competitionCode?.trim().toUpperCase() || null;
  const season = options.season?.trim() || null;
  const sourceSlug = options.sourceSlug?.trim() || 'soccerdata_fbref';
  const summary: BackfillSoccerdataFbrefContractsSummary = {
    competitionCode,
    contractRowsPlanned: 0,
    contractRowsWritten: 0,
    dryRun: options.dryRun ?? true,
    playerMappingsFound: 0,
    rawPlayerRowsRead: 0,
    rawTeamRowsRead: 0,
    season,
    sourceSlug,
    unresolvedPlayerIds: [],
    unresolvedTeamNames: [],
  };

  if (!competitionCode || !season) {
    return summary;
  }

  const sql = getDb();

  try {
    const sourceId = await ensureSource(sql, sourceSlug);
    const competitionSeason = await loadCompetitionSeason(sql, competitionCode, season);

    if (!competitionSeason) {
      return summary;
    }

    const policies = await loadCompetitionSeasonPolicies(sql, [competitionSeason.competition_season_id]);

    const [playerMappings, teams, players, rawTeamPayloads, rawPlayerPayloads] = await Promise.all([
      loadPlayerMappings(sql, sourceId),
      loadTeams(sql),
      loadPlayers(sql),
      loadRawPayloads(sql, sourceId, season, competitionCode, 'team', 'team_season_stats_standard'),
      loadRawPayloads(sql, sourceId, season, competitionCode, 'player', 'player_season_stats_standard'),
    ]);

    const teamIndex = buildTeamIndex(teams);
    const playerIndex = buildPlayerIndex(players);
    const teamIdByExternalId = new Map<string, number>();
    const unresolvedTeamNames = new Set<string>();

    for (const rawPayload of rawTeamPayloads) {
      const teamId = resolveTeamId(rawPayload.payload, teamIndex);

      if (!teamId) {
        unresolvedTeamNames.add(rawPayload.external_id ?? JSON.stringify(rawPayload.payload));
        continue;
      }

      if (rawPayload.external_id) {
        teamIdByExternalId.set(rawPayload.external_id, teamId);
      }
    }

    const unresolvedPlayerIds = new Set<string>();
    const drafts = new Map<string, ContractDraft>();

    for (const rawPayload of rawPlayerPayloads) {
      const payload = rawPayload.payload;
      const values = toObjectMap(payload);
      const teamName = String(pickValue(values, ['team', 'squad', 'teamname']) ?? '').trim();
      const playerId = resolvePlayerId(rawPayload, payload, playerMappings, playerIndex);
      const teamId = teamName ? resolveTeamId(payload, teamIndex) : null;

      if (!playerId) {
        unresolvedPlayerIds.add(rawPayload.external_id ?? JSON.stringify(payload));
        continue;
      }

      if (!teamId) {
        unresolvedTeamNames.add(teamName || rawPayload.external_id || JSON.stringify(payload));
        continue;
      }

      drafts.set(`${playerId}:${competitionSeason.competition_season_id}`, {
        competitionSeasonId: competitionSeason.competition_season_id,
        playerId,
        teamId,
      });
    }

    summary.playerMappingsFound = playerMappings.size;
    summary.rawPlayerRowsRead = rawPlayerPayloads.length;
    summary.rawTeamRowsRead = rawTeamPayloads.length;
    summary.contractRowsPlanned = drafts.size;
    summary.unresolvedPlayerIds = Array.from(unresolvedPlayerIds).sort();
    summary.unresolvedTeamNames = Array.from(unresolvedTeamNames).sort();

    if (summary.dryRun) {
      return summary;
    }

    if (!isCompetitionSeasonWriteAllowed(
      policies.get(competitionSeason.competition_season_id),
      'playerContracts',
      'fbref',
      'backfill',
    )) {
      summary.contractRowsPlanned = 0;
      return summary;
    }

    await upsertContractsBatch(sql, Array.from(drafts.values()));

    summary.contractRowsWritten = drafts.size;
    return summary;
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
