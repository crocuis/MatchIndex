import { createHash } from 'node:crypto';
import postgres, { type JSONValue, type Sql } from 'postgres';
import {
  buildApiFootballFixtureEventsPath,
  buildApiFootballFixturesByDatePath,
  fetchApiFootballJson,
  getDefaultApiFootballDataCompetitionTargets,
  getApiFootballSourceConfig,
  type ApiFootballEnvelope,
  type ApiFootballFixtureEventResponseItem,
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

interface ApiFootballPreparedEventDraft {
  detail: string | null;
  eventIndex: number;
  eventTimestamp: string | null;
  eventType: 'goal' | 'own_goal' | 'penalty_scored' | 'yellow_card' | 'red_card' | 'yellow_red_card' | 'substitution';
  isNotable: boolean;
  matchDate: string;
  matchId: number;
  minute: number;
  period: number | null;
  playerExternalId: string | null;
  secondaryPlayerExternalId: string | null;
  second: number | null;
  sourceDetails: Record<string, unknown>;
  sourceEventId: string;
  teamId: number;
}

interface ApiFootballEventInsertDraft {
  detail: string | null;
  event_index: number;
  event_timestamp: string | null;
  event_type: ApiFootballPreparedEventDraft['eventType'];
  is_notable: boolean;
  match_date: string;
  match_id: number;
  minute: number;
  period: number | null;
  player_id: number | null;
  second: number | null;
  secondary_player_id: number | null;
  source_details: Record<string, unknown>;
  source_event_id: string;
  team_id: number;
}

interface RawPayloadTarget {
  endpoint: string;
  payload: ApiFootballEnvelope<ApiFootballFixtureEventResponseItem>;
  target: TargetMatchRow;
}

export interface SyncApiFootballMatchEventsOptions {
  dryRun?: boolean;
  endLocalDate: string;
  startLocalDate: string;
  timeZone: string;
}

export interface SyncApiFootballMatchEventsSummary {
  dryRun: boolean;
  endLocalDate: string;
  endpoints: string[];
  eventRowsPlanned: number;
  eventRowsWritten: number;
  fetchedFiles: number;
  changedFiles: number;
  playerUpserts: number;
  startLocalDate: string;
  targetMatches: number;
  timeZone: string;
}

function getApiFootballEventsDb() {
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

async function createSyncRun(sql: Sql, sourceId: number, summary: Pick<SyncApiFootballMatchEventsSummary, 'endLocalDate' | 'startLocalDate' | 'timeZone'>) {
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
        mode: 'match_event_sync',
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
  summary: Pick<SyncApiFootballMatchEventsSummary, 'changedFiles' | 'endLocalDate' | 'eventRowsPlanned' | 'eventRowsWritten' | 'fetchedFiles' | 'playerUpserts' | 'startLocalDate' | 'targetMatches' | 'timeZone'>,
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
  const status = (fixture.fixture?.status?.short ?? '').trim().toUpperCase();

  if (!leagueId || !targetLeagueIds.has(leagueId) || !kickoffAt) {
    return false;
  }

  const syncableStatuses = new Set(['NS', 'TBD', 'PST', 'CANC', 'ABD', 'SUSP', 'INT', 'LIVE', '1H', 'HT', '2H', 'ET', 'BT', 'P', 'FT', 'AET', 'PEN']);
  if (!syncableStatuses.has(status)) {
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

function formatIntervalFromMinute(minute: number) {
  if (!Number.isFinite(minute) || minute < 0) {
    return null;
  }

  return `${minute} minutes`;
}

function buildStableUuid(value: string) {
  const hash = createHash('sha1').update(value).digest('hex');
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-${hash.slice(12, 16)}-${hash.slice(16, 20)}-${hash.slice(20, 32)}`;
}

function normalizeApiFootballEventType(event: ApiFootballFixtureEventResponseItem) {
  const type = (event.type ?? '').trim().toLowerCase();
  const detail = (event.detail ?? '').trim().toLowerCase();

  if (type === 'goal') {
    if (detail.includes('missed')) {
      return null;
    }

    if (detail.includes('own')) {
      return 'own_goal' as const;
    }

    if (detail.includes('penalty')) {
      return 'penalty_scored' as const;
    }

    return 'goal' as const;
  }

  if (type === 'card') {
    if (detail.includes('second yellow') || detail.includes('yellow red')) {
      return 'yellow_red_card' as const;
    }

    if (detail.includes('red')) {
      return 'red_card' as const;
    }

    if (detail.includes('yellow')) {
      return 'yellow_card' as const;
    }

    return null;
  }

  if (type === 'subst' || type === 'substitution') {
    return 'substitution' as const;
  }

  return null;
}

function resolvePeriod(minute: number) {
  if (minute <= 45) {
    return 1;
  }

  if (minute <= 90) {
    return 2;
  }

  if (minute <= 105) {
    return 3;
  }

  return 4;
}

function buildPreparedEventDrafts(target: TargetMatchRow, payload: ApiFootballEnvelope<ApiFootballFixtureEventResponseItem>) {
  const drafts: ApiFootballPreparedEventDraft[] = [];

  for (const [index, event] of (payload.response ?? []).entries()) {
    const eventType = normalizeApiFootballEventType(event);
    if (!eventType) {
      continue;
    }

    const eventTeamExternalId = toNullableString(event.team?.id);
    if (!eventTeamExternalId) {
      continue;
    }

    let teamId: number | null = null;
    if (eventTeamExternalId === target.home_external_team_id) {
      teamId = target.home_team_id;
    } else if (eventTeamExternalId === target.away_external_team_id) {
      teamId = target.away_team_id;
    }

    if (!teamId) {
      continue;
    }

    const elapsed = toNullableNumber(event.time?.elapsed);
    if (elapsed === null) {
      continue;
    }

    const extra = toNullableNumber(event.time?.extra) ?? 0;
    const minute = elapsed + extra;
    const playerExternalId = toNullableString(event.player?.id);
    const secondaryPlayerExternalId = eventType === 'substitution' || eventType === 'goal' || eventType === 'own_goal' || eventType === 'penalty_scored'
      ? toNullableString(event.assist?.id)
      : null;
    const sourceEventId = buildStableUuid(JSON.stringify({
      detail: event.detail ?? null,
      eventIndex: index,
      externalFixtureId: target.external_fixture_id,
      minute,
      playerExternalId,
      secondaryPlayerExternalId,
      teamExternalId: eventTeamExternalId,
      type: event.type ?? null,
    }));
    const secondaryRole = eventType === 'substitution' ? 'replacement' : 'assist';

    drafts.push({
      detail: toNullableString(event.detail) ?? toNullableString(event.type),
      eventIndex: index,
      eventTimestamp: formatIntervalFromMinute(minute),
      eventType,
      isNotable: true,
      matchDate: target.match_date,
      matchId: target.match_id,
      minute,
      period: resolvePeriod(minute),
      playerExternalId,
      secondaryPlayerExternalId,
      second: null,
      sourceDetails: {
        source: 'api_football',
        externalFixtureId: target.external_fixture_id,
        comments: event.comments ?? null,
        detail: event.detail ?? null,
        playerExternalId,
        playerName: event.player?.name ?? null,
        secondaryPlayerExternalId,
        secondaryPlayerName: event.assist?.name ?? null,
        secondaryPlayerRole: secondaryPlayerExternalId ? secondaryRole : null,
        teamExternalId: eventTeamExternalId,
        teamName: event.team?.name ?? null,
        time: {
          elapsed,
          extra: extra > 0 ? extra : null,
        },
        type: event.type ?? null,
      },
      sourceEventId,
      teamId,
    });
  }

  return drafts;
}

function collectMissingPlayerDrafts(
  drafts: ApiFootballPreparedEventDraft[],
  existingPlayerMappings: Map<string, PlayerMappingRow>,
) {
  const missing = new Map<string, ApiFootballPlayerDraft>();

  for (const draft of drafts) {
    const participants = [
      {
        externalId: draft.playerExternalId,
        name: toNullableString(draft.sourceDetails.playerName),
      },
      {
        externalId: draft.secondaryPlayerExternalId,
        name: toNullableString(draft.sourceDetails.secondaryPlayerName),
      },
    ];

    for (const participant of participants) {
      if (!participant.externalId || !participant.name || existingPlayerMappings.has(participant.externalId) || missing.has(participant.externalId)) {
        continue;
      }

      missing.set(participant.externalId, buildApiFootballPlayerDraft(participant.externalId, participant.name));
    }
  }

  return [...missing.values()];
}

function buildEventInsertDrafts(
  drafts: ApiFootballPreparedEventDraft[],
  playerMappings: Map<string, PlayerMappingRow>,
) {
  return drafts.map<ApiFootballEventInsertDraft>((draft) => ({
    detail: draft.detail,
    event_index: draft.eventIndex,
    event_timestamp: draft.eventTimestamp,
    event_type: draft.eventType,
    is_notable: draft.isNotable,
    match_date: draft.matchDate,
    match_id: draft.matchId,
    minute: draft.minute,
    period: draft.period,
    player_id: draft.playerExternalId ? playerMappings.get(draft.playerExternalId)?.entity_id ?? null : null,
    second: draft.second,
    secondary_player_id: draft.secondaryPlayerExternalId ? playerMappings.get(draft.secondaryPlayerExternalId)?.entity_id ?? null : null,
    source_details: draft.sourceDetails,
    source_event_id: draft.sourceEventId,
    team_id: draft.teamId,
  }));
}

function toJsonValue(value: unknown) {
  return JSON.parse(JSON.stringify(value)) as JSONValue;
}

function serializeEventInsertDrafts(drafts: ApiFootballEventInsertDraft[]) {
  return drafts.map((draft) => ({
    ...draft,
    source_details: draft.source_details,
  }));
}

async function deleteApiFootballEventsForMatches(sql: Sql, targets: TargetMatchRow[]) {
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
    DELETE FROM match_events me
    USING input
    WHERE me.match_id = input.match_id
      AND me.match_date = input.match_date
      AND COALESCE(me.source_details->>'source', '') = 'api_football'
  `;
}

async function upsertMatchEventsBatch(sql: Sql, drafts: ApiFootballEventInsertDraft[]) {
  if (drafts.length === 0) {
    return;
  }

  await sql`
    WITH input AS (
      SELECT *
      FROM jsonb_to_recordset(${sql.json(toJsonValue(serializeEventInsertDrafts(drafts)))}::jsonb) AS item(
        match_id BIGINT,
        match_date DATE,
        source_event_id UUID,
        event_index INTEGER,
        event_type match_event_type,
        period INTEGER,
        event_timestamp INTERVAL,
        minute INTEGER,
        second INTEGER,
        team_id BIGINT,
        player_id BIGINT,
        secondary_player_id BIGINT,
        is_notable BOOLEAN,
        detail TEXT,
        source_details JSONB
      )
    )
    INSERT INTO match_events (
      match_id,
      match_date,
      source_event_id,
      event_index,
      event_type,
      period,
      event_timestamp,
      minute,
      second,
      team_id,
      player_id,
      secondary_player_id,
      is_notable,
      detail,
      source_details
    )
    SELECT
      input.match_id,
      input.match_date,
      input.source_event_id,
      input.event_index,
      input.event_type,
      input.period,
      input.event_timestamp,
      input.minute,
      input.second,
      input.team_id,
      input.player_id,
      input.secondary_player_id,
      input.is_notable,
      input.detail,
      input.source_details
    FROM input
    ON CONFLICT (source_event_id)
    DO UPDATE SET
      event_index = EXCLUDED.event_index,
      event_type = EXCLUDED.event_type,
      period = EXCLUDED.period,
      event_timestamp = EXCLUDED.event_timestamp,
      minute = EXCLUDED.minute,
      second = EXCLUDED.second,
      team_id = EXCLUDED.team_id,
      player_id = EXCLUDED.player_id,
      secondary_player_id = EXCLUDED.secondary_player_id,
      is_notable = EXCLUDED.is_notable,
      detail = EXCLUDED.detail,
      source_details = EXCLUDED.source_details
  `;
}

export async function syncApiFootballMatchEvents(
  options: SyncApiFootballMatchEventsOptions,
): Promise<SyncApiFootballMatchEventsSummary> {
  const startLocalDate = normalizeIsoDate(options.startLocalDate, 'startLocalDate');
  const endLocalDate = normalizeIsoDate(options.endLocalDate, 'endLocalDate');
  const timeZone = options.timeZone.trim();
  if (!timeZone) {
    throw new Error('timeZone is required');
  }

  if (startLocalDate > endLocalDate) {
    throw new Error('startLocalDate must be earlier than or equal to endLocalDate');
  }

  const sql = getApiFootballEventsDb();
  const dryRun = options.dryRun ?? false;
  const sourceId = await ensureApiFootballSource(sql);
  const requestState = { lastRequestStartedAt: 0 };
  const targets = await resolveTargetMatches(sql, sourceId, startLocalDate, endLocalDate, timeZone, requestState);
  const endpoints = targets.map((target) => buildApiFootballFixtureEventsPath(target.external_fixture_id));

  if (dryRun) {
    await sql.end({ timeout: 1 }).catch(() => undefined);
    return {
      dryRun: true,
      endLocalDate,
      endpoints,
      eventRowsPlanned: 0,
      eventRowsWritten: 0,
      fetchedFiles: 0,
      changedFiles: 0,
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
    const preparedEventDrafts: ApiFootballPreparedEventDraft[] = [];

    for (const target of targets) {
      const endpoint = buildApiFootballFixtureEventsPath(target.external_fixture_id);
      const payload = await fetchApiFootballEnvelope<ApiFootballFixtureEventResponseItem>(endpoint, requestState);
      rawPayloadTargets.push({ endpoint, payload, target });
      preparedEventDrafts.push(...buildPreparedEventDrafts(target, payload));
      fetchedFiles += 1;
      changedFiles += 1;
    }

    const playerMappings = await loadPlayerMappings(sql, sourceId);
    const missingPlayers = collectMissingPlayerDrafts(preparedEventDrafts, playerMappings);
    const summary = {
      changedFiles,
      endLocalDate,
      eventRowsPlanned: preparedEventDrafts.length,
      eventRowsWritten: preparedEventDrafts.length,
      fetchedFiles,
      playerUpserts: missingPlayers.length,
      startLocalDate,
      targetMatches: targets.length,
      timeZone,
    } satisfies Pick<SyncApiFootballMatchEventsSummary, 'changedFiles' | 'endLocalDate' | 'eventRowsPlanned' | 'eventRowsWritten' | 'fetchedFiles' | 'playerUpserts' | 'startLocalDate' | 'targetMatches' | 'timeZone'>;

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
      const eventInsertDrafts = buildEventInsertDrafts(preparedEventDrafts, playerMappingsAfterUpsert);

      await deleteApiFootballEventsForMatches(sql, targets);
      await upsertMatchEventsBatch(sql, eventInsertDrafts);

      await sql`COMMIT`;
    } catch (error) {
      await sql`ROLLBACK`;
      throw error;
    }

    await updateSyncRun(sql, syncRunId, {
      ...summary,
      playerUpserts,
    }, 'completed');
    await sql.end({ timeout: 1 });

    return {
      dryRun: false,
      endLocalDate,
      endpoints,
      eventRowsPlanned: preparedEventDrafts.length,
      eventRowsWritten: preparedEventDrafts.length,
      fetchedFiles,
      changedFiles,
      playerUpserts,
      startLocalDate,
      targetMatches: targets.length,
      timeZone,
    };
  } catch (error) {
    await updateSyncRun(sql, syncRunId, {
      changedFiles,
      endLocalDate,
      eventRowsPlanned: 0,
      eventRowsWritten: 0,
      fetchedFiles,
      playerUpserts,
      startLocalDate,
      targetMatches: targets.length,
      timeZone,
    }, 'failed').catch(() => undefined);
    await sql.end({ timeout: 1 }).catch(() => undefined);
    throw error;
  }
}
