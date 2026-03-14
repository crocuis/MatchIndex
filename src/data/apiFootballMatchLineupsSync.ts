import { createHash } from 'node:crypto';
import postgres, { type JSONValue, type Sql } from 'postgres';
import {
  buildApiFootballFixtureLineupsPath,
  buildApiFootballFixturesByDatePath,
  fetchApiFootballJson,
  getDefaultApiFootballDataCompetitionTargets,
  getApiFootballSourceConfig,
  type ApiFootballEnvelope,
  type ApiFootballFixtureLineupPlayer,
  type ApiFootballFixtureLineupResponseItem,
  type ApiFootballFixtureResponseItem,
} from './apiFootball.ts';

interface SourceRow {
  id: number;
}

interface SyncRunRow {
  id: number;
}

interface TargetMatchRow {
  match_id: number;
  match_date: string;
  external_fixture_id: string;
  season_context: string | null;
  home_external_team_id: string;
  home_team_id: number;
  away_external_team_id: string;
  away_team_id: number;
  kickoff_at: string;
}

interface PlayerMappingRow {
  external_id: string;
  entity_id: number;
  slug: string;
}

interface TeamMappingRow {
  external_id: string;
  entity_id: number;
}

interface CandidateMatchRow {
  away_team_id: number;
  away_team_name: string;
  away_team_slug: string;
  home_team_id: number;
  home_team_name: string;
  home_team_slug: string;
  local_date: string;
  match_date: string;
  match_id: number;
}

interface ApiFootballPlayerDraft {
  externalId: string;
  firstName: string;
  knownAs: string;
  lastName: string;
  slug: string;
}

interface ApiFootballPreparedLineupDraft {
  endReason: string | null;
  fromMinute: number | null;
  gridPosition: string | null;
  isStarter: boolean;
  matchDate: string;
  matchId: number;
  minutesPlayed: number | null;
  playerExternalId: string;
  playerName: string;
  position: string | null;
  shirtNumber: number | null;
  sourceDetails: Record<string, unknown>;
  startReason: string | null;
  teamId: number;
  teamSlug: string;
  toMinute: number | null;
}

interface ApiFootballLineupInsertDraft {
  end_reason: string | null;
  from_minute: number | null;
  grid_position: string | null;
  is_starter: boolean;
  match_date: string;
  match_id: number;
  minutes_played: number | null;
  player_slug: string;
  position: string | null;
  shirt_number: number | null;
  source_details: Record<string, unknown>;
  start_reason: string | null;
  team_slug: string;
  to_minute: number | null;
}

interface RawPayloadTarget {
  endpoint: string;
  payload: ApiFootballEnvelope<ApiFootballFixtureLineupResponseItem>;
  target: TargetMatchRow;
}

export interface SyncApiFootballMatchLineupsOptions {
  dryRun?: boolean;
  endLocalDate: string;
  startLocalDate: string;
  timeZone: string;
}

export interface SyncApiFootballMatchLineupsSummary {
  changedFiles: number;
  dryRun: boolean;
  endLocalDate: string;
  endpoints: string[];
  fetchedFiles: number;
  lineupRowsPlanned: number;
  lineupRowsWritten: number;
  playerUpserts: number;
  startLocalDate: string;
  targetMatches: number;
  timeZone: string;
}

function getApiFootballLineupsDb() {
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

function buildPayloadHash(payload: unknown) {
  return createHash('sha256').update(JSON.stringify(payload)).digest('hex');
}

function slugify(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/-{2,}/g, '-');
}

function parseNameParts(name: string) {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  return {
    firstName: parts[0] ?? name,
    lastName: parts.slice(1).join(' '),
  };
}

function normalizeIsoDate(value: string, label: string) {
  const normalized = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    throw new Error(`Invalid ${label}: ${value}`);
  }

  return normalized;
}

function shiftIsoDate(isoDate: string, days: number) {
  const shifted = new Date(`${isoDate}T00:00:00Z`);
  shifted.setUTCDate(shifted.getUTCDate() + days);
  return shifted.toISOString().slice(0, 10);
}

function enumerateIsoDates(startIsoDate: string, endIsoDate: string) {
  const dates: string[] = [];
  let current = startIsoDate;

  while (current <= endIsoDate) {
    dates.push(current);
    current = shiftIsoDate(current, 1);
  }

  return dates;
}

function formatDateInTimeZone(date: Date, timeZone: string) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  const parts = formatter.formatToParts(date);
  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;

  if (!year || !month || !day) {
    throw new Error(`Failed to format date in timezone: ${timeZone}`);
  }

  return `${year}-${month}-${day}`;
}

function toNullableString(value: unknown) {
  if (value === null || value === undefined) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized.length > 0 ? normalized : null;
}

function toNullableNumber(value: unknown) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getApiFootballRequestDelayMs() {
  const raw = process.env.API_FOOTBALL_REQUEST_DELAY_MS?.trim();
  if (!raw) {
    return 6500;
  }

  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 6500;
}

function getApiFootballRateLimitRetryMs() {
  const raw = process.env.API_FOOTBALL_RATE_LIMIT_RETRY_MS?.trim();
  if (!raw) {
    return 65000;
  }

  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 65000;
}

function getApiFootballErrorMessage(payload: { errors?: Record<string, string> | string[] } | null | undefined) {
  if (Array.isArray(payload?.errors)) {
    const first = payload.errors.find((value) => typeof value === 'string' && value.trim().length > 0);
    return first ?? null;
  }

  const entries = Object.entries(payload?.errors ?? {}).filter(([, value]) => typeof value === 'string' && value.trim().length > 0);
  return entries.length > 0 ? `${entries[0][0]}: ${entries[0][1]}` : null;
}

function isSkippablePlanRestrictionError(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return /do not have access to this date/i.test(message);
}

async function fetchApiFootballEnvelope<T>(
  path: string,
  requestState: { lastRequestStartedAt: number },
  attempt: number = 1,
): Promise<ApiFootballEnvelope<T>> {
  const delayMs = getApiFootballRequestDelayMs();
  const elapsed = Date.now() - requestState.lastRequestStartedAt;
  if (requestState.lastRequestStartedAt > 0 && elapsed < delayMs) {
    await sleep(delayMs - elapsed);
  }

  requestState.lastRequestStartedAt = Date.now();
  const payload = await fetchApiFootballJson<ApiFootballEnvelope<T>>(path);
  const rateLimitMessage = Array.isArray(payload.errors)
    ? payload.errors.find((value) => typeof value === 'string' && value.toLowerCase().includes('rate'))
    : payload.errors?.rateLimit;
  if (rateLimitMessage) {
    if (attempt >= 3) {
      throw new Error(`API-Football rate limit persisted for ${path}: ${rateLimitMessage}`);
    }

    await sleep(getApiFootballRateLimitRetryMs());
    return fetchApiFootballEnvelope<T>(path, requestState, attempt + 1);
  }

  const errorMessage = getApiFootballErrorMessage(payload);
  if (errorMessage) {
    throw new Error(`API-Football request returned errors for ${path}: ${errorMessage}`);
  }

  return payload;
}

async function ensureApiFootballSource(sql: Sql) {
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

async function createSyncRun(
  sql: Sql,
  sourceId: number,
  summary: Pick<SyncApiFootballMatchLineupsSummary, 'endLocalDate' | 'startLocalDate' | 'timeZone'>,
) {
  const rows = await sql<SyncRunRow[]>`
    INSERT INTO source_sync_runs (
      source_id,
      upstream_ref,
      upstream_commit_sha,
      status,
      metadata
    )
    VALUES (
      ${sourceId},
      'v3',
      NULL,
      'running',
      ${JSON.stringify({
        mode: 'match_lineup_sync',
        source: 'api_football',
        startLocalDate: summary.startLocalDate,
        endLocalDate: summary.endLocalDate,
        timeZone: summary.timeZone,
      })}::jsonb
    )
    RETURNING id
  `;

  return rows[0].id;
}

async function updateSyncRun(
  sql: Sql,
  syncRunId: number,
  summary: Pick<SyncApiFootballMatchLineupsSummary, 'changedFiles' | 'endLocalDate' | 'fetchedFiles' | 'lineupRowsPlanned' | 'lineupRowsWritten' | 'playerUpserts' | 'startLocalDate' | 'targetMatches' | 'timeZone'>,
  status: 'completed' | 'failed',
) {
  await sql`
    UPDATE source_sync_runs
    SET
      status = ${status},
      fetched_files = ${summary.fetchedFiles},
      changed_files = ${summary.changedFiles},
      completed_at = NOW(),
      metadata = COALESCE(metadata, '{}'::jsonb) || ${JSON.stringify(summary)}::jsonb
    WHERE id = ${syncRunId}
  `;
}

async function upsertRawPayload(
  sql: Sql,
  params: {
    endpoint: string;
    externalId: string;
    payload: unknown;
    seasonContext: string | null;
    sourceId: number;
    syncRunId: number;
  },
) {
  const payloadHash = buildPayloadHash(params.payload);

  await sql`
    INSERT INTO raw_payloads (
      source_id,
      sync_run_id,
      endpoint,
      entity_type,
      external_id,
      season_context,
      http_status,
      payload,
      payload_hash
    )
    VALUES (
      ${params.sourceId},
      ${params.syncRunId},
      ${params.endpoint},
      'match',
      ${params.externalId},
      ${params.seasonContext},
      200,
      ${JSON.stringify(params.payload)}::jsonb,
      ${payloadHash}
    )
  `;
}

async function loadTeamMappings(sql: Sql, sourceId: number) {
  const rows = await sql<TeamMappingRow[]>`
    SELECT external_id, entity_id
    FROM source_entity_mapping
    WHERE entity_type = 'team'
      AND source_id = ${sourceId}
  `;

  return new Map(rows.map((row) => [row.external_id, row.entity_id]));
}

async function loadCandidateMatches(
  sql: Sql,
  startLocalDate: string,
  endLocalDate: string,
  timeZone: string,
) {
  return sql<CandidateMatchRow[]>`
    SELECT
      m.id AS match_id,
      m.match_date::TEXT AS match_date,
      TIMEZONE(${timeZone}, m.kickoff_at)::DATE::TEXT AS local_date,
      m.home_team_id,
      COALESCE(home_tt.name, home_team.slug) AS home_team_name,
      home_team.slug AS home_team_slug,
      m.away_team_id,
      COALESCE(away_tt.name, away_team.slug) AS away_team_name,
      away_team.slug AS away_team_slug
    FROM matches m
    JOIN teams home_team ON home_team.id = m.home_team_id
    LEFT JOIN team_translations home_tt ON home_tt.team_id = home_team.id AND home_tt.locale = 'en'
    JOIN teams away_team ON away_team.id = m.away_team_id
    LEFT JOIN team_translations away_tt ON away_tt.team_id = away_team.id AND away_tt.locale = 'en'
    WHERE m.kickoff_at IS NOT NULL
      AND TIMEZONE(${timeZone}, m.kickoff_at)::DATE BETWEEN ${startLocalDate}::DATE AND ${endLocalDate}::DATE
      AND m.status = ANY(${[
        'scheduled',
        'timed',
        'live_1h',
        'live_ht',
        'live_2h',
        'live_et',
        'live_pen',
        'finished',
        'finished_aet',
        'finished_pen',
        'awarded',
      ]})
  `;
}

function normalizeTeamNameForMatch(value: string) {
  const stopwords = new Set(['fc', 'cf', 'fk', 'sc', 'ac', 'cp', 'club', 'clube', 'de', 'the']);

  const tokens = value
    .replace(/ø/gi, 'o')
    .replace(/æ/gi, 'ae')
    .replace(/œ/gi, 'oe')
    .replace(/ß/gi, 'ss')
    .replace(/đ/gi, 'd')
    .replace(/ł/gi, 'l')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 0 && !stopwords.has(token) && !/^\d+$/.test(token));

  return tokens.join(' ');
}

function doesCandidateTeamMatch(candidateName: string, candidateSlug: string, fixtureName: string, mappedTeamId: number | undefined, candidateTeamId: number) {
  if (mappedTeamId && mappedTeamId === candidateTeamId) {
    return true;
  }

  const candidateKey = normalizeTeamNameForMatch(candidateName || candidateSlug);
  const fixtureKey = normalizeTeamNameForMatch(fixtureName);
  if (!candidateKey || !fixtureKey) {
    return false;
  }

  return candidateKey === fixtureKey || candidateKey.includes(fixtureKey) || fixtureKey.includes(candidateKey);
}

function resolveCandidateMatch(
  candidateMatches: CandidateMatchRow[],
  localDate: string,
  fixture: ApiFootballFixtureResponseItem,
  homeTeamId: number | undefined,
  awayTeamId: number | undefined,
) {
  const fixtureHomeName = fixture.teams?.home?.name ?? '';
  const fixtureAwayName = fixture.teams?.away?.name ?? '';

  return candidateMatches.find((candidate) => {
    if (candidate.local_date !== localDate) {
      return false;
    }

    return doesCandidateTeamMatch(candidate.home_team_name, candidate.home_team_slug, fixtureHomeName, homeTeamId, candidate.home_team_id)
      && doesCandidateTeamMatch(candidate.away_team_name, candidate.away_team_slug, fixtureAwayName, awayTeamId, candidate.away_team_id);
  }) ?? null;
}

function shouldSyncApiFootballFixture(
  fixture: ApiFootballFixtureResponseItem,
  targetLeagueIds: Set<number>,
  targetLocalDates: Set<string>,
  timeZone: string,
) {
  const leagueId = fixture.league?.id;
  const kickoffAt = fixture.fixture?.date;

  if (!leagueId || !targetLeagueIds.has(leagueId) || !kickoffAt) {
    return false;
  }

  return targetLocalDates.has(formatDateInTimeZone(new Date(kickoffAt), timeZone));
}

async function resolveTargetMatches(
  sql: Sql,
  sourceId: number,
  startLocalDate: string,
  endLocalDate: string,
  timeZone: string,
  requestState: { lastRequestStartedAt: number },
) {
  const targetLeagueIds = new Set(getDefaultApiFootballDataCompetitionTargets().map((target) => target.leagueId));
  const targetLocalDates = new Set(enumerateIsoDates(startLocalDate, endLocalDate));
  const relevantFetchDates = enumerateIsoDates(startLocalDate, endLocalDate);
  const teamIdByExternalId = await loadTeamMappings(sql, sourceId);
  const candidateMatches = await loadCandidateMatches(sql, startLocalDate, endLocalDate, timeZone);
  const targets = new Map<string, TargetMatchRow>();

  for (const fetchDate of relevantFetchDates) {
    const fixturesPath = buildApiFootballFixturesByDatePath(fetchDate, timeZone);
    let fixturesPayload: ApiFootballEnvelope<ApiFootballFixtureResponseItem>;
    try {
      fixturesPayload = await fetchApiFootballEnvelope<ApiFootballFixtureResponseItem>(fixturesPath, requestState);
    } catch (error) {
      if (isSkippablePlanRestrictionError(error) && fetchDate < endLocalDate) {
        continue;
      }

      throw error;
    }

    for (const fixture of fixturesPayload.response ?? []) {
      if (!shouldSyncApiFootballFixture(fixture, targetLeagueIds, targetLocalDates, timeZone)) {
        continue;
      }

      const fixtureId = toNullableString(fixture.fixture?.id);
      const kickoffAt = fixture.fixture?.date ?? null;
      const homeExternalTeamId = toNullableString(fixture.teams?.home?.id);
      const awayExternalTeamId = toNullableString(fixture.teams?.away?.id);
      if (!fixtureId || !kickoffAt || !homeExternalTeamId || !awayExternalTeamId) {
        continue;
      }

      const homeTeamId = teamIdByExternalId.get(homeExternalTeamId);
      const awayTeamId = teamIdByExternalId.get(awayExternalTeamId);
      const localDate = formatDateInTimeZone(new Date(kickoffAt), timeZone);
      const canonicalMatch = resolveCandidateMatch(candidateMatches, localDate, fixture, homeTeamId, awayTeamId);
      if (!canonicalMatch) {
        continue;
      }

      targets.set(fixtureId, {
        match_id: canonicalMatch.match_id,
        match_date: canonicalMatch.match_date,
        external_fixture_id: fixtureId,
        season_context: fixture.league?.season ? String(fixture.league.season) : null,
        home_external_team_id: homeExternalTeamId,
        home_team_id: canonicalMatch.home_team_id,
        away_external_team_id: awayExternalTeamId,
        away_team_id: canonicalMatch.away_team_id,
        kickoff_at: kickoffAt,
      });
    }
  }

  return [...targets.values()].sort((left, right) => {
    if (left.kickoff_at === right.kickoff_at) {
      return left.match_id - right.match_id;
    }

    return left.kickoff_at.localeCompare(right.kickoff_at);
  });
}

async function loadPlayerMappings(sql: Sql, sourceId: number) {
  const rows = await sql<PlayerMappingRow[]>`
    SELECT sem.external_id, sem.entity_id, p.slug
    FROM source_entity_mapping sem
    JOIN players p ON p.id = sem.entity_id
    WHERE sem.entity_type = 'player'
      AND sem.source_id = ${sourceId}
  `;

  return new Map(rows.map((row) => [row.external_id, row]));
}

async function upsertPlayerAlias(sql: Sql, playerSlug: string, alias: string) {
  await sql`
    INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
    VALUES ('player', (SELECT id FROM players WHERE slug = ${playerSlug}), ${alias}, 'en', 'common', TRUE, 'pending', 'imported', 'api_football')
    ON CONFLICT (entity_type, entity_id, alias_normalized)
    DO UPDATE SET
      locale = EXCLUDED.locale,
      alias_kind = EXCLUDED.alias_kind,
      is_primary = EXCLUDED.is_primary,
      status = EXCLUDED.status,
      source_type = EXCLUDED.source_type,
      source_ref = EXCLUDED.source_ref
  `;
}

async function upsertPlayer(sql: Sql, sourceId: number, draft: ApiFootballPlayerDraft) {
  await sql`
    INSERT INTO players (slug, country_id, position, is_active, updated_at)
    VALUES (${draft.slug}, NULL, NULL, TRUE, NOW())
    ON CONFLICT (slug)
    DO UPDATE SET
      is_active = TRUE,
      updated_at = NOW()
  `;

  await sql`
    INSERT INTO player_translations (player_id, locale, first_name, last_name, known_as)
    VALUES ((SELECT id FROM players WHERE slug = ${draft.slug}), 'en', ${draft.firstName}, ${draft.lastName}, ${draft.knownAs})
    ON CONFLICT (player_id, locale)
    DO UPDATE SET
      first_name = EXCLUDED.first_name,
      last_name = EXCLUDED.last_name,
      known_as = EXCLUDED.known_as
  `;

  await upsertPlayerAlias(sql, draft.slug, draft.knownAs);

  await sql`
    INSERT INTO source_entity_mapping (
      entity_type,
      entity_id,
      source_id,
      external_id,
      external_code,
      season_context,
      metadata,
      updated_at
    )
    VALUES (
      'player',
      (SELECT id FROM players WHERE slug = ${draft.slug}),
      ${sourceId},
      ${draft.externalId},
      NULL,
      NULL,
      ${JSON.stringify({ source: 'api_football' })}::jsonb,
      NOW()
    )
    ON CONFLICT (entity_type, source_id, external_id)
    DO UPDATE SET
      entity_id = EXCLUDED.entity_id,
      metadata = EXCLUDED.metadata,
      updated_at = NOW()
  `;
}

function buildApiFootballPlayerDraft(externalId: string, name: string): ApiFootballPlayerDraft {
  const normalizedName = name.trim() || `API-Football Player ${externalId}`;
  const parts = parseNameParts(normalizedName);

  return {
    externalId,
    firstName: parts.firstName,
    knownAs: normalizedName,
    lastName: parts.lastName,
    slug: `${slugify(normalizedName) || 'api-football-player'}-${externalId}`,
  };
}

function mapApiFootballPositionCode(code: string | null) {
  const normalized = code?.trim().toUpperCase();
  if (!normalized) {
    return null;
  }

  switch (normalized) {
    case 'G':
      return 'goalkeeper';
    case 'D':
      return 'defender';
    case 'M':
      return 'midfielder';
    case 'F':
      return 'forward';
    default:
      return normalized.toLowerCase();
  }
}

function buildPreparedLineupDrafts(
  target: TargetMatchRow,
  payload: ApiFootballEnvelope<ApiFootballFixtureLineupResponseItem>,
) {
  const drafts: ApiFootballPreparedLineupDraft[] = [];

  const appendPlayers = (
    lineupType: 'startXI' | 'substitutes',
    players: Array<{ player?: ApiFootballFixtureLineupPlayer }> | undefined,
    teamResponse: ApiFootballFixtureLineupResponseItem,
  ) => {
    const isStarter = lineupType === 'startXI';
    const teamExternalId = toNullableString(teamResponse.team?.id);
    if (!teamExternalId) {
      return;
    }

    const teamId = teamExternalId === target.home_external_team_id ? target.home_team_id : teamExternalId === target.away_external_team_id ? target.away_team_id : null;
    if (!teamId) {
      return;
    }

    const teamSlug = teamId === target.home_team_id ? 'home' : 'away';

    for (const entry of players ?? []) {
      const playerExternalId = toNullableString(entry.player?.id);
      const playerName = toNullableString(entry.player?.name);
      if (!playerExternalId || !playerName) {
        continue;
      }

      drafts.push({
        endReason: null,
        fromMinute: null,
        gridPosition: toNullableString(entry.player?.grid),
        isStarter,
        matchDate: target.match_date,
        matchId: target.match_id,
        minutesPlayed: null,
        playerExternalId,
        playerName,
        position: mapApiFootballPositionCode(toNullableString(entry.player?.pos)),
        shirtNumber: toNullableNumber(entry.player?.number),
        sourceDetails: {
          source: 'api_football',
          externalFixtureId: target.external_fixture_id,
          externalPlayerId: playerExternalId,
          formation: teamResponse.formation ?? null,
          coachId: teamResponse.coach?.id ?? null,
          coachName: teamResponse.coach?.name ?? null,
          gridPosition: toNullableString(entry.player?.grid),
          lineupType,
          positionCode: toNullableString(entry.player?.pos),
          teamExternalId,
          teamName: teamResponse.team?.name ?? null,
        },
        startReason: isStarter ? 'starting_xi' : 'bench',
        teamId,
        teamSlug,
        toMinute: null,
      });
    }
  };

  for (const teamResponse of payload.response ?? []) {
    appendPlayers('startXI', teamResponse.startXI, teamResponse);
    appendPlayers('substitutes', teamResponse.substitutes, teamResponse);
  }

  return drafts;
}

function collectMissingPlayerDrafts(
  drafts: ApiFootballPreparedLineupDraft[],
  existingPlayerMappings: Map<string, PlayerMappingRow>,
) {
  const missing = new Map<string, ApiFootballPlayerDraft>();

  for (const draft of drafts) {
    if (existingPlayerMappings.has(draft.playerExternalId) || missing.has(draft.playerExternalId)) {
      continue;
    }

    missing.set(draft.playerExternalId, buildApiFootballPlayerDraft(draft.playerExternalId, draft.playerName));
  }

  return [...missing.values()];
}

function toJsonValue(value: unknown) {
  return JSON.parse(JSON.stringify(value)) as JSONValue;
}

function buildLineupInsertDrafts(
  drafts: ApiFootballPreparedLineupDraft[],
  playerMappings: Map<string, PlayerMappingRow>,
) {
  return drafts.flatMap<ApiFootballLineupInsertDraft>((draft) => {
    const playerSlug = playerMappings.get(draft.playerExternalId)?.slug;
    if (!playerSlug) {
      return [];
    }

    return [{
      end_reason: draft.endReason,
      from_minute: draft.fromMinute,
      grid_position: draft.gridPosition,
      is_starter: draft.isStarter,
      match_date: draft.matchDate,
      match_id: draft.matchId,
      minutes_played: draft.minutesPlayed,
      player_slug: playerSlug,
      position: draft.position,
      shirt_number: draft.shirtNumber,
      source_details: draft.sourceDetails,
      start_reason: draft.startReason,
      team_slug: draft.teamSlug,
      to_minute: draft.toMinute,
    }];
  });
}

async function deleteApiFootballLineupsForMatches(sql: Sql, targets: TargetMatchRow[]) {
  if (targets.length === 0) {
    return;
  }

  await sql`
    WITH input AS (
      SELECT *
      FROM jsonb_to_recordset(${sql.json(toJsonValue(targets.map((target) => ({
        match_id: target.match_id,
        match_date: target.match_date,
      }))))}::jsonb) AS item(
        match_id BIGINT,
        match_date DATE
      )
    )
    DELETE FROM match_lineups ml
    USING input
    WHERE ml.match_id = input.match_id
      AND ml.match_date = input.match_date
      AND COALESCE(ml.source_details->>'source', '') = 'api_football'
  `;
}

function serializeLineupInsertDrafts(drafts: ApiFootballLineupInsertDraft[]) {
  return drafts.map((draft) => ({
    ...draft,
    source_details: draft.source_details,
  }));
}

async function upsertMatchLineupsBatch(sql: Sql, drafts: ApiFootballLineupInsertDraft[]) {
  if (drafts.length === 0) {
    return;
  }

  await sql`
    WITH input AS (
      SELECT *
      FROM jsonb_to_recordset(${sql.json(toJsonValue(serializeLineupInsertDrafts(drafts)))}::jsonb) AS item(
        match_id BIGINT,
        match_date DATE,
        team_slug TEXT,
        player_slug TEXT,
        shirt_number INTEGER,
        position TEXT,
        grid_position TEXT,
        is_starter BOOLEAN,
        from_minute INTEGER,
        to_minute INTEGER,
        start_reason TEXT,
        end_reason TEXT,
        minutes_played INTEGER,
        source_details JSONB
      )
    )
    INSERT INTO match_lineups (
      match_id,
      match_date,
      team_id,
      player_id,
      shirt_number,
      position,
      grid_position,
      is_starter,
      from_minute,
      to_minute,
      start_reason,
      end_reason,
      minutes_played,
      source_details
    )
    SELECT
      input.match_id,
      input.match_date,
      team.id,
      player.id,
      input.shirt_number,
      input.position,
      input.grid_position,
      input.is_starter,
      input.from_minute,
      input.to_minute,
      input.start_reason,
      input.end_reason,
      input.minutes_played,
      input.source_details
    FROM input
    JOIN teams team ON team.id = CASE input.team_slug WHEN 'home' THEN (
      SELECT m.home_team_id FROM matches m WHERE m.id = input.match_id AND m.match_date = input.match_date
    ) ELSE (
      SELECT m.away_team_id FROM matches m WHERE m.id = input.match_id AND m.match_date = input.match_date
    ) END
    JOIN players player ON player.slug = input.player_slug
    ON CONFLICT (match_id, match_date, team_id, player_id)
    DO UPDATE SET
      shirt_number = EXCLUDED.shirt_number,
      position = EXCLUDED.position,
      grid_position = EXCLUDED.grid_position,
      is_starter = EXCLUDED.is_starter,
      from_minute = EXCLUDED.from_minute,
      to_minute = EXCLUDED.to_minute,
      start_reason = EXCLUDED.start_reason,
      end_reason = EXCLUDED.end_reason,
      minutes_played = EXCLUDED.minutes_played,
      source_details = COALESCE(match_lineups.source_details, '{}'::jsonb) || EXCLUDED.source_details
  `;
}

export async function syncApiFootballMatchLineups(
  options: SyncApiFootballMatchLineupsOptions,
): Promise<SyncApiFootballMatchLineupsSummary> {
  const startLocalDate = normalizeIsoDate(options.startLocalDate, 'startLocalDate');
  const endLocalDate = normalizeIsoDate(options.endLocalDate, 'endLocalDate');
  const timeZone = options.timeZone.trim();
  if (!timeZone) {
    throw new Error('timeZone is required');
  }

  if (startLocalDate > endLocalDate) {
    throw new Error('startLocalDate must be earlier than or equal to endLocalDate');
  }

  const sql = getApiFootballLineupsDb();
  const dryRun = options.dryRun ?? false;
  const sourceId = await ensureApiFootballSource(sql);
  const requestState = { lastRequestStartedAt: 0 };
  const targets = await resolveTargetMatches(sql, sourceId, startLocalDate, endLocalDate, timeZone, requestState);
  const endpoints = targets.map((target) => buildApiFootballFixtureLineupsPath(target.external_fixture_id));

  if (dryRun) {
    await sql.end({ timeout: 1 }).catch(() => undefined);
    return {
      changedFiles: 0,
      dryRun: true,
      endLocalDate,
      endpoints,
      fetchedFiles: 0,
      lineupRowsPlanned: 0,
      lineupRowsWritten: 0,
      playerUpserts: 0,
      startLocalDate,
      targetMatches: targets.length,
      timeZone,
    };
  }

  const syncRunId = await createSyncRun(sql, sourceId, { endLocalDate, startLocalDate, timeZone });
  let fetchedFiles = 0;
  let changedFiles = 0;
  let playerUpserts = 0;

  try {
    const rawPayloadTargets: RawPayloadTarget[] = [];
    const preparedLineupDrafts: ApiFootballPreparedLineupDraft[] = [];

    for (const target of targets) {
      const endpoint = buildApiFootballFixtureLineupsPath(target.external_fixture_id);
      const payload = await fetchApiFootballEnvelope<ApiFootballFixtureLineupResponseItem>(endpoint, requestState);
      rawPayloadTargets.push({ endpoint, payload, target });
      preparedLineupDrafts.push(...buildPreparedLineupDrafts(target, payload));
      fetchedFiles += 1;
      changedFiles += 1;
    }

    const playerMappings = await loadPlayerMappings(sql, sourceId);
    const missingPlayers = collectMissingPlayerDrafts(preparedLineupDrafts, playerMappings);

    await sql`BEGIN`;
    try {
      for (const payloadTarget of rawPayloadTargets) {
        await upsertRawPayload(sql, {
          endpoint: payloadTarget.endpoint,
          externalId: payloadTarget.target.external_fixture_id,
          payload: payloadTarget.payload,
          seasonContext: payloadTarget.target.season_context,
          sourceId,
          syncRunId,
        });
      }

      for (const player of missingPlayers) {
        await upsertPlayer(sql, sourceId, player);
      }
      playerUpserts = missingPlayers.length;

      const playerMappingsAfterUpsert = await loadPlayerMappings(sql, sourceId);
      const lineupInsertDrafts = buildLineupInsertDrafts(preparedLineupDrafts, playerMappingsAfterUpsert);

      await deleteApiFootballLineupsForMatches(sql, targets);
      await upsertMatchLineupsBatch(sql, lineupInsertDrafts);

      await sql`COMMIT`;

      const summary = {
        changedFiles,
        endLocalDate,
        fetchedFiles,
        lineupRowsPlanned: preparedLineupDrafts.length,
        lineupRowsWritten: lineupInsertDrafts.length,
        playerUpserts,
        startLocalDate,
        targetMatches: targets.length,
        timeZone,
      } satisfies Pick<SyncApiFootballMatchLineupsSummary, 'changedFiles' | 'endLocalDate' | 'fetchedFiles' | 'lineupRowsPlanned' | 'lineupRowsWritten' | 'playerUpserts' | 'startLocalDate' | 'targetMatches' | 'timeZone'>;

      await updateSyncRun(sql, syncRunId, summary, 'completed');
      await sql.end({ timeout: 1 });

      return {
        ...summary,
        dryRun: false,
        endpoints,
      };
    } catch (error) {
      await sql`ROLLBACK`;
      throw error;
    }
  } catch (error) {
    await updateSyncRun(sql, syncRunId, {
      changedFiles,
      endLocalDate,
      fetchedFiles,
      lineupRowsPlanned: 0,
      lineupRowsWritten: 0,
      playerUpserts,
      startLocalDate,
      targetMatches: targets.length,
      timeZone,
    }, 'failed').catch(() => undefined);
    await sql.end({ timeout: 1 }).catch(() => undefined);
    throw error;
  }
}
