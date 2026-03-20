import { createHash } from 'node:crypto';
import postgres, { type Sql } from 'postgres';
import {
  buildApiFootballFixturesByDatePath,
  fetchApiFootballJson,
  getApiFootballSourceConfig,
  getDefaultApiFootballDataCompetitionTargets,
  type ApiFootballEnvelope,
  type ApiFootballFixtureResponseItem,
} from './apiFootball.ts';

interface SourceRow {
  id: number;
}

interface SyncRunRow {
  id: number;
}

interface CandidateMatchRow {
  away_team_id: number;
  away_team_name: string;
  away_team_slug: string;
  competition_slug: string;
  home_team_id: number;
  home_team_name: string;
  home_team_slug: string;
  kickoff_known: boolean;
  local_date: string | null;
  match_date: string;
  match_id: number;
}

interface TargetMatchRow {
  away_external_team_id: string;
  away_team_id: number;
  external_fixture_id: string;
  fixture: ApiFootballFixtureResponseItem;
  home_external_team_id: string;
  home_team_id: number;
  match_date: string;
  match_id: number;
  season_context: string | null;
}

export interface SyncApiFootballDailyFixturesOptions {
  candidateMode?: 'known-kickoff' | 'missing-kickoff' | 'both';
  dryRun?: boolean;
  localDate: string;
  refreshDerivedViews?: boolean;
  timeZone: string;
}

export interface SyncApiFootballDailyFixturesSummary {
  changedFiles: number;
  dryRun: boolean;
  endpoints: string[];
  fetchedFiles: number;
  localDate: string;
  matchesMatched: number;
  matchesUpdated: number;
  timeZone: string;
}

function getApiFootballDailyFixturesDb() {
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

function normalizeIsoDate(value: string, label: string) {
  const normalized = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    throw new Error(`Invalid ${label}: ${value}`);
  }

  return normalized;
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

function buildPayloadHash(payload: unknown) {
  return createHash('sha256').update(JSON.stringify(payload)).digest('hex');
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

async function createSyncRun(sql: Sql, sourceId: number, localDate: string, timeZone: string) {
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
      ${JSON.stringify({ mode: 'daily_fixture_sync', source: 'api_football', localDate, timeZone })}::jsonb
    )
    RETURNING id
  `;

  return rows[0].id;
}

async function updateSyncRun(
  sql: Sql,
  syncRunId: number,
  summary: Pick<SyncApiFootballDailyFixturesSummary, 'changedFiles' | 'fetchedFiles' | 'localDate' | 'matchesMatched' | 'matchesUpdated' | 'timeZone'>,
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

async function loadCandidateMatches(
  sql: Sql,
  localDate: string,
  timeZone: string,
  candidateMode: NonNullable<SyncApiFootballDailyFixturesOptions['candidateMode']>,
) {
  return sql<CandidateMatchRow[]>`
    SELECT
      m.id AS match_id,
      m.match_date::TEXT AS match_date,
      CASE
        WHEN m.kickoff_at IS NULL THEN NULL
        ELSE TIMEZONE(${timeZone}, m.kickoff_at)::DATE::TEXT
      END AS local_date,
      (m.kickoff_at IS NOT NULL) AS kickoff_known,
      m.home_team_id,
      COALESCE(home_tt.name, home_team.slug) AS home_team_name,
      home_team.slug AS home_team_slug,
      m.away_team_id,
      COALESCE(away_tt.name, away_team.slug) AS away_team_name,
      away_team.slug AS away_team_slug,
      c.slug AS competition_slug
    FROM matches m
    JOIN competition_seasons cs ON cs.id = m.competition_season_id
    JOIN competitions c ON c.id = cs.competition_id
    JOIN teams home_team ON home_team.id = m.home_team_id
    LEFT JOIN team_translations home_tt ON home_tt.team_id = home_team.id AND home_tt.locale = 'en'
    JOIN teams away_team ON away_team.id = m.away_team_id
    LEFT JOIN team_translations away_tt ON away_tt.team_id = away_team.id AND away_tt.locale = 'en'
    WHERE c.slug = ANY(${getDefaultApiFootballDataCompetitionTargets().map((target) => target.competitionSlug)})
      AND (
        (
          ${candidateMode !== 'missing-kickoff'}
          AND m.kickoff_at IS NOT NULL
          AND TIMEZONE(${timeZone}, m.kickoff_at)::DATE = ${localDate}::DATE
        )
        OR (
          ${candidateMode !== 'known-kickoff'}
          AND m.kickoff_at IS NULL
          AND m.status IN ('scheduled', 'timed')
          AND m.match_date BETWEEN (${localDate}::DATE - INTERVAL '1 day')::DATE AND (${localDate}::DATE + INTERVAL '1 day')::DATE
        )
      )
  `;
}

function getCompetitionSlugByLeagueId(leagueId?: number | null) {
  if (!leagueId) {
    return null;
  }

  const target = getDefaultApiFootballDataCompetitionTargets().find((item) => item.leagueId === leagueId);
  return target?.competitionSlug ?? null;
}

function getDateDistanceInDays(left: string, right: string) {
  const leftDate = new Date(`${left}T00:00:00Z`);
  const rightDate = new Date(`${right}T00:00:00Z`);
  return Math.round((leftDate.getTime() - rightDate.getTime()) / 86400000);
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

function doesCandidateTeamMatch(candidateName: string, candidateSlug: string, fixtureName: string) {
  const candidateKey = normalizeTeamNameForMatch(candidateName || candidateSlug);
  const fixtureKey = normalizeTeamNameForMatch(fixtureName);
  if (!candidateKey || !fixtureKey) {
    return false;
  }

  return candidateKey === fixtureKey || candidateKey.includes(fixtureKey) || fixtureKey.includes(candidateKey);
}

function resolveCandidateMatch(candidateMatches: CandidateMatchRow[], fixture: ApiFootballFixtureResponseItem, localDate: string) {
  const fixtureHomeName = fixture.teams?.home?.name ?? '';
  const fixtureAwayName = fixture.teams?.away?.name ?? '';
  const fixtureCompetitionSlug = getCompetitionSlugByLeagueId(fixture.league?.id ?? null);

  const matches = candidateMatches
    .filter((candidate) => {
      if (fixtureCompetitionSlug && candidate.competition_slug !== fixtureCompetitionSlug) {
        return false;
      }

      if (!doesCandidateTeamMatch(candidate.home_team_name, candidate.home_team_slug, fixtureHomeName)) {
        return false;
      }

      if (!doesCandidateTeamMatch(candidate.away_team_name, candidate.away_team_slug, fixtureAwayName)) {
        return false;
      }

      if (candidate.kickoff_known) {
        return candidate.local_date === localDate;
      }

      return Math.abs(getDateDistanceInDays(candidate.match_date, localDate)) <= 1;
    })
    .sort((left, right) => {
      const leftDistance = left.kickoff_known ? 0 : Math.abs(getDateDistanceInDays(left.match_date, localDate));
      const rightDistance = right.kickoff_known ? 0 : Math.abs(getDateDistanceInDays(right.match_date, localDate));

      if (leftDistance !== rightDistance) {
        return leftDistance - rightDistance;
      }

      return left.match_id - right.match_id;
    });

  return matches[0] ?? null;
}

function shouldSyncApiFootballFixture(fixture: ApiFootballFixtureResponseItem, localDate: string, timeZone: string) {
  const targetLeagueIds = new Set(getDefaultApiFootballDataCompetitionTargets().map((target) => target.leagueId));
  const leagueId = fixture.league?.id;
  const kickoffAt = fixture.fixture?.date;

  if (!leagueId || !targetLeagueIds.has(leagueId) || !kickoffAt) {
    return false;
  }

  return formatDateInTimeZone(new Date(kickoffAt), timeZone) === localDate;
}

function normalizeMatchStatus(status?: string | null): string {
  switch (status) {
    case 'FT':
      return 'finished';
    case 'AET':
      return 'finished_aet';
    case 'PEN':
      return 'finished_pen';
    case '1H':
    case 'LIVE':
      return 'live_1h';
    case 'HT':
      return 'live_ht';
    case '2H':
      return 'live_2h';
    case 'ET':
      return 'live_et';
    case 'BT':
    case 'P':
      return 'live_pen';
    case 'PST':
      return 'postponed';
    case 'SUSP':
    case 'INT':
      return 'suspended';
    case 'CANC':
      return 'cancelled';
    case 'ABD':
      return 'awarded';
    case 'TBD':
    case 'NS':
    default:
      return 'scheduled';
  }
}

function parseRound(round?: string | null) {
  if (!round) {
    return { stage: 'REGULAR_SEASON', groupName: null as string | null, matchWeek: null as number | null };
  }

  const [stagePart, trailingPart] = round.split(' - ', 2);
  const normalizedStage = stagePart.replace(/\b\w/g, (char) => char.toUpperCase()).replace(/\bof\b/g, 'of');
  const trailingNumber = trailingPart ? Number.parseInt(trailingPart, 10) : Number.NaN;
  const isRegularSeason = /regular season/i.test(stagePart);
  const isLeaguePhase = /league (stage|phase)/i.test(stagePart);
  const isGroup = /group/i.test(stagePart);

  return {
    stage: normalizedStage.replace(/\s+/g, '_').toUpperCase(),
    groupName: isGroup ? trailingPart ?? null : null,
    matchWeek: (isRegularSeason || isLeaguePhase) && Number.isFinite(trailingNumber) ? trailingNumber : null,
  };
}

async function updateMatchFromFixture(sql: Sql, target: TargetMatchRow) {
  const fixture = target.fixture;
  const roundInfo = parseRound(fixture.league?.round ?? null);
  const metadata = {
    api_football: {
      externalFixtureId: fixture.fixture?.id ?? null,
      externalLeagueId: fixture.league?.id ?? null,
      externalSeason: fixture.league?.season ?? null,
      referee: fixture.fixture?.referee ?? null,
      timezone: fixture.fixture?.timezone ?? null,
      venue: fixture.fixture?.venue ?? null,
      status: fixture.fixture?.status ?? null,
      syncedAt: new Date().toISOString(),
    },
  };

  await sql`
    UPDATE matches
    SET
      matchday = COALESCE(${roundInfo.matchWeek}, matchday),
      stage = COALESCE(NULLIF(${roundInfo.stage}, ''), stage),
      group_name = COALESCE(${roundInfo.groupName}, group_name),
      home_score = ${fixture.goals?.home ?? null},
      away_score = ${fixture.goals?.away ?? null},
      status = ${normalizeMatchStatus(fixture.fixture?.status?.short ?? null)},
      kickoff_at = COALESCE(${fixture.fixture?.date ?? null}, kickoff_at),
      source_metadata = COALESCE(source_metadata, '{}'::jsonb) || ${JSON.stringify(metadata)}::jsonb,
      updated_at = NOW()
    WHERE id = ${target.match_id}
      AND match_date = ${target.match_date}
  `;
}

async function upsertSourceEntityMapping(
  sql: Sql,
  params: {
    entityType: 'team' | 'match';
    entityIdSql: ReturnType<Sql>;
    externalId: string;
    externalCode?: string | null;
    metadata: Record<string, unknown>;
    seasonContext?: string | null;
    sourceId: number;
  },
) {
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
      ${params.entityType},
      (${params.entityIdSql}),
      ${params.sourceId},
      ${params.externalId},
      ${params.externalCode ?? null},
      ${params.seasonContext ?? null},
      ${JSON.stringify(params.metadata)}::jsonb,
      NOW()
    )
    ON CONFLICT (entity_type, source_id, external_id)
    DO UPDATE SET
      entity_id = EXCLUDED.entity_id,
      external_code = EXCLUDED.external_code,
      season_context = EXCLUDED.season_context,
      metadata = EXCLUDED.metadata,
      updated_at = NOW()
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
      ${buildPayloadHash(params.payload)}
    )
  `;
}

async function refreshDerivedViews(sql: Sql) {
  await sql`REFRESH MATERIALIZED VIEW mv_team_form`;
  await sql`REFRESH MATERIALIZED VIEW mv_standings`;
  await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;
}

export async function refreshApiFootballDerivedViews() {
  const sql = getApiFootballDailyFixturesDb();

  try {
    await refreshDerivedViews(sql);
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}

export async function syncApiFootballDailyFixtures(
  options: SyncApiFootballDailyFixturesOptions,
): Promise<SyncApiFootballDailyFixturesSummary> {
  const localDate = normalizeIsoDate(options.localDate, 'localDate');
  const timeZone = options.timeZone.trim();
  if (!timeZone) {
    throw new Error('timeZone is required');
  }

  const candidateMode = options.candidateMode ?? 'known-kickoff';
  const dryRun = options.dryRun ?? false;
  const shouldRefreshDerivedViews = options.refreshDerivedViews ?? true;
  const sql = getApiFootballDailyFixturesDb();
  const sourceId = await ensureApiFootballSource(sql);
  const requestState = { lastRequestStartedAt: 0 };
  const fixturesPath = buildApiFootballFixturesByDatePath(localDate, timeZone);
  const fixturesPayload = await fetchApiFootballEnvelope<ApiFootballFixtureResponseItem>(fixturesPath, requestState);
  const candidateMatches = await loadCandidateMatches(sql, localDate, timeZone, candidateMode);
  const targets = new Map<string, TargetMatchRow>();

  for (const fixture of fixturesPayload.response ?? []) {
    if (!shouldSyncApiFootballFixture(fixture, localDate, timeZone)) {
      continue;
    }

    const fixtureId = toNullableString(fixture.fixture?.id);
    const homeExternalTeamId = toNullableString(fixture.teams?.home?.id);
    const awayExternalTeamId = toNullableString(fixture.teams?.away?.id);
    if (!fixtureId || !homeExternalTeamId || !awayExternalTeamId) {
      continue;
    }

    const candidateMatch = resolveCandidateMatch(candidateMatches, fixture, localDate);
    if (!candidateMatch) {
      continue;
    }

    targets.set(fixtureId, {
      away_external_team_id: awayExternalTeamId,
      away_team_id: candidateMatch.away_team_id,
      external_fixture_id: fixtureId,
      fixture,
      home_external_team_id: homeExternalTeamId,
      home_team_id: candidateMatch.home_team_id,
      match_date: candidateMatch.match_date,
      match_id: candidateMatch.match_id,
      season_context: fixture.league?.season ? String(fixture.league.season) : null,
    });
  }

  const endpoints = [...targets.values()].map((target) => `${fixturesPath}:fixture:${target.external_fixture_id}`);

  if (dryRun) {
    await sql.end({ timeout: 1 }).catch(() => undefined);
    return {
      changedFiles: 0,
      dryRun: true,
      endpoints,
      fetchedFiles: 0,
      localDate,
      matchesMatched: targets.size,
      matchesUpdated: 0,
      timeZone,
    };
  }

  const syncRunId = await createSyncRun(sql, sourceId, localDate, timeZone);
  let changedFiles = 0;
  let matchesUpdated = 0;

  try {
    await sql`BEGIN`;
    try {
      for (const target of targets.values()) {
        await upsertRawPayload(sql, {
          endpoint: `${fixturesPath}:fixture:${target.external_fixture_id}`,
          externalId: target.external_fixture_id,
          payload: target.fixture,
          seasonContext: target.season_context,
          sourceId,
          syncRunId,
        });

        await updateMatchFromFixture(sql, target);
        await upsertSourceEntityMapping(sql, {
          entityType: 'match',
          entityIdSql: sql`SELECT id FROM matches WHERE id = ${target.match_id} AND match_date = ${target.match_date}`,
          externalId: target.external_fixture_id,
          metadata: { source: 'api_football', localDate, timeZone },
          seasonContext: target.season_context,
          sourceId,
        });
        await upsertSourceEntityMapping(sql, {
          entityType: 'team',
          entityIdSql: sql`SELECT id FROM teams WHERE id = ${target.home_team_id}`,
          externalId: target.home_external_team_id,
          metadata: { source: 'api_football', localDate, timeZone },
          seasonContext: target.season_context,
          sourceId,
        });
        await upsertSourceEntityMapping(sql, {
          entityType: 'team',
          entityIdSql: sql`SELECT id FROM teams WHERE id = ${target.away_team_id}`,
          externalId: target.away_external_team_id,
          metadata: { source: 'api_football', localDate, timeZone },
          seasonContext: target.season_context,
          sourceId,
        });

        changedFiles += 1;
        matchesUpdated += 1;
      }

      if (shouldRefreshDerivedViews) {
        await refreshDerivedViews(sql);
      }
      await sql`COMMIT`;
    } catch (error) {
      await sql`ROLLBACK`;
      throw error;
    }

    const summary = {
      changedFiles,
      fetchedFiles: 1,
      localDate,
      matchesMatched: targets.size,
      matchesUpdated,
      timeZone,
    } satisfies Pick<SyncApiFootballDailyFixturesSummary, 'changedFiles' | 'fetchedFiles' | 'localDate' | 'matchesMatched' | 'matchesUpdated' | 'timeZone'>;
    await updateSyncRun(sql, syncRunId, summary, 'completed');
    await sql.end({ timeout: 1 });

    return {
      ...summary,
      dryRun: false,
      endpoints,
    };
  } catch (error) {
    await updateSyncRun(sql, syncRunId, {
      changedFiles,
      fetchedFiles: 1,
      localDate,
      matchesMatched: targets.size,
      matchesUpdated,
      timeZone,
    }, 'failed').catch(() => undefined);
    await sql.end({ timeout: 1 }).catch(() => undefined);
    throw error;
  }
}
