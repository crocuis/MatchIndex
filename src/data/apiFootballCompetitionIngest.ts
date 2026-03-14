import { createHash } from 'node:crypto';
import postgres, { type Sql } from 'postgres';
import {
  buildApiFootballFixturesPath,
  buildApiFootballLeaguePath,
  buildApiFootballStandingsPath,
  fetchApiFootballJson,
  getApiFootballRecentSeasonYears,
  getApiFootballSourceConfig,
  parseApiFootballDataCompetitionTargets,
  type ApiFootballCompetitionTarget,
  type ApiFootballEnvelope,
  type ApiFootballFixtureResponseItem,
  type ApiFootballLeagueResponseItem,
} from './apiFootball.ts';

interface SourceRow {
  id: number;
}

interface SyncRunRow {
  id: number;
}

interface LeagueCacheEntry {
  payload: ApiFootballEnvelope<ApiFootballLeagueResponseItem>;
  competition: ApiFootballLeagueResponseItem | null;
}

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

export interface IngestApiFootballCompetitionOptions {
  dryRun?: boolean;
  competitionCodes?: string[];
  seasons?: number[];
}

export interface IngestApiFootballCompetitionSummary {
  dryRun: boolean;
  competitionCodes: string[];
  seasons: number[];
  competitionSeasonsProcessed: number;
  matchManifestsProcessed: number;
  fetchedFiles: number;
  changedFiles: number;
  plannedEndpoints: string[];
}

function buildPayloadHash(payload: unknown) {
  return createHash('sha256').update(JSON.stringify(payload)).digest('hex');
}

function toJsonValue(value: unknown): JsonValue {
  return JSON.parse(JSON.stringify(value)) as JsonValue;
}

function getIngestDb() {
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

function normalizeSeasons(input?: number[]) {
  if (input && input.length > 0) {
    return [...new Set(input)].sort((a, b) => a - b);
  }

  return getApiFootballRecentSeasonYears(2);
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

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getApiFootballErrorMessage(payload: { errors?: Record<string, string> } | null | undefined) {
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
  const rateLimitMessage = payload.errors?.rateLimit;
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

async function createSyncRun(sql: Sql, sourceId: number) {
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
      ${JSON.stringify({ mode: 'competition_manifest_ingest', source: 'api_football' })}::jsonb
    )
    RETURNING id
  `;

  return rows[0].id;
}

async function updateSyncRun(
  sql: Sql,
  syncRunId: number,
  summary: Pick<IngestApiFootballCompetitionSummary, 'changedFiles' | 'fetchedFiles'>,
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
    sourceId: number;
    syncRunId: number;
    endpoint: string;
    entityType: 'competition' | 'match' | null;
    externalId: string | null;
    seasonContext: string | null;
    payload: unknown;
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
      ${params.entityType},
      ${params.externalId},
      ${params.seasonContext},
      200,
      ${sql.json(toJsonValue(params.payload))},
      ${payloadHash}
    )
  `;
}

async function upsertCompetitionSeasonManifest(
  sql: Sql,
  params: {
    sourceId: number;
    syncRunId: number;
    target: ApiFootballCompetitionTarget;
    season: number;
    leaguePayload: ApiFootballEnvelope<ApiFootballLeagueResponseItem>;
    fixturesPayload: ApiFootballEnvelope<ApiFootballFixtureResponseItem>;
    standingsPayload: ApiFootballEnvelope<unknown>;
  },
) {
  const upstreamPath = buildApiFootballFixturesPath(params.target.leagueId, params.season);
  const competition = params.leaguePayload.response?.[0] ?? null;
  const seasonInfo = competition?.seasons?.find((entry) => entry.year === params.season) ?? null;

  await sql`
    INSERT INTO source_sync_manifests (
      source_id,
      sync_run_id,
      manifest_type,
      upstream_path,
      external_id,
      external_parent_id,
      source_updated_at,
      source_available_at,
      metadata
    )
    VALUES (
      ${params.sourceId},
      ${params.syncRunId},
      'competition_season',
      ${upstreamPath},
      ${String(params.season)},
      ${params.target.code},
      NULL,
      NOW(),
      ${sql.json(
        toJsonValue({
          competition: competition,
          competitionCode: params.target.code,
          competitionId: params.target.leagueId,
          competitionName: competition?.league?.name ?? params.target.name,
          competitionType: competition?.league?.type ?? null,
          competitionCountry: competition?.country?.name ?? null,
          season: params.season,
          seasonBounds: seasonInfo ? { start: seasonInfo.start ?? null, end: seasonInfo.end ?? null, current: seasonInfo.current ?? false } : null,
          coverage: seasonInfo?.coverage ?? null,
          resultCount: params.fixturesPayload.results ?? params.fixturesPayload.response?.length ?? 0,
          fixturesCount: params.fixturesPayload.results ?? params.fixturesPayload.response?.length ?? 0,
          standingsCount: params.standingsPayload.results ?? params.standingsPayload.response?.length ?? 0,
        }),
      )}
    )
    ON CONFLICT (source_id, manifest_type, upstream_path)
    DO UPDATE SET
      sync_run_id = EXCLUDED.sync_run_id,
      external_id = EXCLUDED.external_id,
      external_parent_id = EXCLUDED.external_parent_id,
      source_available_at = EXCLUDED.source_available_at,
      metadata = EXCLUDED.metadata,
      updated_at = NOW()
  `;
}

async function upsertMatchManifest(
  sql: Sql,
  params: {
    sourceId: number;
    syncRunId: number;
    target: ApiFootballCompetitionTarget;
    season: number;
    fixture: ApiFootballFixtureResponseItem;
  },
) {
  const fixtureId = params.fixture.fixture?.id;
  if (!fixtureId) {
    return;
  }

  const upstreamPath = `${buildApiFootballFixturesPath(params.target.leagueId, params.season)}:fixture:${fixtureId}`;

  await sql`
    INSERT INTO source_sync_manifests (
      source_id,
      sync_run_id,
      manifest_type,
      upstream_path,
      external_id,
      external_parent_id,
      source_updated_at,
      source_available_at,
      metadata
    )
    VALUES (
      ${params.sourceId},
      ${params.syncRunId},
      'match',
      ${upstreamPath},
      ${String(fixtureId)},
      ${`${params.target.code}:${params.season}`},
      ${params.fixture.fixture?.date ?? null},
      NOW(),
      ${sql.json(
        toJsonValue({
          competitionCode: params.target.code,
          competitionId: params.target.leagueId,
          season: params.season,
          matchId: fixtureId,
          fixtureId,
          utcDate: params.fixture.fixture?.date ?? null,
          round: params.fixture.league?.round ?? null,
          kickoffAt: params.fixture.fixture?.date ?? null,
          status: params.fixture.fixture?.status?.short ?? null,
          homeTeam: params.fixture.teams?.home ?? null,
          awayTeam: params.fixture.teams?.away ?? null,
          homeTeamId: params.fixture.teams?.home?.id ?? null,
          homeTeamName: params.fixture.teams?.home?.name ?? null,
          awayTeamId: params.fixture.teams?.away?.id ?? null,
          awayTeamName: params.fixture.teams?.away?.name ?? null,
          goals: params.fixture.goals ?? null,
        }),
      )}
    )
    ON CONFLICT (source_id, manifest_type, upstream_path)
    DO UPDATE SET
      sync_run_id = EXCLUDED.sync_run_id,
      external_id = EXCLUDED.external_id,
      external_parent_id = EXCLUDED.external_parent_id,
      source_updated_at = EXCLUDED.source_updated_at,
      source_available_at = EXCLUDED.source_available_at,
      metadata = EXCLUDED.metadata,
      updated_at = NOW()
  `;
}

export async function ingestApiFootballCompetitions(
  options: IngestApiFootballCompetitionOptions = {},
): Promise<IngestApiFootballCompetitionSummary> {
  const targets = parseApiFootballDataCompetitionTargets(options.competitionCodes);
  const seasons = normalizeSeasons(options.seasons);
  const plannedEndpoints = [
    ...targets.map((target) => buildApiFootballLeaguePath(target.leagueId)),
    ...targets.flatMap((target) => seasons.flatMap((season) => [
      buildApiFootballFixturesPath(target.leagueId, season),
      buildApiFootballStandingsPath(target.leagueId, season),
    ])),
  ];

  if (options.dryRun ?? true) {
    return {
      dryRun: true,
      competitionCodes: targets.map((target) => target.code),
      seasons,
      competitionSeasonsProcessed: targets.length * seasons.length,
      matchManifestsProcessed: 0,
      fetchedFiles: 0,
      changedFiles: 0,
      plannedEndpoints,
    };
  }

  const sql = getIngestDb();
  const sourceId = await ensureApiFootballSource(sql);
  const syncRunId = await createSyncRun(sql, sourceId);
  const requestState = { lastRequestStartedAt: 0 };
  let fetchedFiles = 0;
  let changedFiles = 0;
  let competitionSeasonsProcessed = 0;
  let matchManifestsProcessed = 0;

  try {
    const leagueCache = new Map<number, LeagueCacheEntry>();

    for (const target of targets) {
      const leaguePath = buildApiFootballLeaguePath(target.leagueId);
      const leaguePayload = await fetchApiFootballEnvelope<ApiFootballLeagueResponseItem>(leaguePath, requestState);
      const competition = leaguePayload.response?.[0] ?? null;

      await upsertRawPayload(sql, {
        sourceId,
        syncRunId,
        endpoint: leaguePath,
        entityType: 'competition',
        externalId: target.code,
        seasonContext: null,
        payload: leaguePayload,
      });

      leagueCache.set(target.leagueId, { payload: leaguePayload, competition });
      fetchedFiles += 1;
      changedFiles += 1;

      for (const season of seasons) {
        const fixturesPath = buildApiFootballFixturesPath(target.leagueId, season);
        const standingsPath = buildApiFootballStandingsPath(target.leagueId, season);
        const fixturesPayload = await fetchApiFootballEnvelope<ApiFootballFixtureResponseItem>(fixturesPath, requestState);
        const standingsPayload = await fetchApiFootballEnvelope<unknown>(standingsPath, requestState);

        await upsertRawPayload(sql, {
          sourceId,
          syncRunId,
          endpoint: fixturesPath,
          entityType: 'match',
          externalId: `${target.code}:${season}`,
          seasonContext: String(season),
          payload: fixturesPayload,
        });

        await upsertRawPayload(sql, {
          sourceId,
          syncRunId,
          endpoint: standingsPath,
          entityType: 'competition',
          externalId: `${target.code}:${season}:standings`,
          seasonContext: String(season),
          payload: standingsPayload,
        });

        await upsertCompetitionSeasonManifest(sql, {
          sourceId,
          syncRunId,
          target,
          season,
          leaguePayload: leagueCache.get(target.leagueId)?.payload ?? leaguePayload,
          fixturesPayload,
          standingsPayload,
        });

        for (const fixture of fixturesPayload.response ?? []) {
          await upsertMatchManifest(sql, {
            sourceId,
            syncRunId,
            target,
            season,
            fixture,
          });
          matchManifestsProcessed += 1;
        }

        fetchedFiles += 2;
        changedFiles += 2;
        competitionSeasonsProcessed += 1;
      }
    }

    const summary = {
      dryRun: false,
      competitionCodes: targets.map((target) => target.code),
      seasons,
      competitionSeasonsProcessed,
      matchManifestsProcessed,
      fetchedFiles,
      changedFiles,
      plannedEndpoints,
    } satisfies IngestApiFootballCompetitionSummary;

    await updateSyncRun(sql, syncRunId, summary, 'completed');
    return summary;
  } catch (error) {
    await updateSyncRun(sql, syncRunId, { fetchedFiles, changedFiles }, 'failed');
    throw error;
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
