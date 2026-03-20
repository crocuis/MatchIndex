import { createHash } from 'node:crypto';
import postgres, { type Sql } from 'postgres';
import {
  buildApiFootballPlayersPath,
  fetchApiFootballJson,
  getApiFootballSourceConfig,
  parseApiFootballCompetitionTargets,
  type ApiFootballPlayersResponse,
} from './apiFootball.ts';
import { normalizePlayerSeasonYears } from './playerSeasonWindow.ts';

interface SourceRow {
  id: number;
}

interface SyncRunRow {
  id: number;
}

interface ApiFootballPageSummary {
  endpoint: string;
  results: number;
  pagingCurrent: number;
  pagingTotal: number;
}

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

export interface IngestApiFootballPlayerStatsOptions {
  dryRun?: boolean;
  competitionCodes?: string[];
  seasons?: number[];
}

export interface IngestApiFootballPlayerStatsSummary {
  dryRun: boolean;
  competitionCodes: string[];
  seasons: number[];
  competitionSeasonsProcessed: number;
  playerPagesProcessed: number;
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
      ${JSON.stringify({ mode: 'player_stats_ingest', source: 'api_football' })}::jsonb
    )
    RETURNING id
  `;

  return rows[0].id;
}

async function updateSyncRun(
  sql: Sql,
  syncRunId: number,
  summary: Pick<IngestApiFootballPlayerStatsSummary, 'changedFiles' | 'fetchedFiles'>,
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
    externalId: string;
    seasonContext: string;
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
      'player',
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
    competitionCode: string;
    leagueId: number;
    season: number;
    pages: ApiFootballPageSummary[];
    truncatedReason?: string | null;
  },
) {
  const upstreamPath = buildApiFootballPlayersPath(params.leagueId, params.season, 1);
  const totalPlayers = params.pages.reduce((sum, page) => sum + page.results, 0);
  const lastPage = params.pages.at(-1);

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
      'player_season_batch',
      ${upstreamPath},
      ${String(params.season)},
      ${String(params.leagueId)},
      NULL,
      NOW(),
      ${sql.json(toJsonValue({
        competitionCode: params.competitionCode,
        leagueId: params.leagueId,
        season: params.season,
        pageCount: params.pages.length,
        totalPlayers,
        truncatedReason: params.truncatedReason ?? null,
        pages: params.pages,
        lastPage: lastPage?.pagingCurrent ?? null,
        totalPages: lastPage?.pagingTotal ?? null,
      }))}
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

function getPlanLimitMessage(payload: ApiFootballPlayersResponse) {
  const planMessage = payload.errors?.plan;
  return typeof planMessage === 'string' && planMessage.trim().length > 0 ? planMessage : null;
}

export async function ingestApiFootballPlayerStats(
  options: IngestApiFootballPlayerStatsOptions = {},
): Promise<IngestApiFootballPlayerStatsSummary> {
  const targets = parseApiFootballCompetitionTargets(options.competitionCodes);
  const seasons = normalizePlayerSeasonYears(options.seasons);
  const plannedEndpoints = targets.flatMap((target) => seasons.map((season) => buildApiFootballPlayersPath(target.leagueId, season, 1)));

  if (options.dryRun ?? true) {
    return {
      dryRun: true,
      competitionCodes: targets.map((target) => target.code),
      seasons,
      competitionSeasonsProcessed: targets.length * seasons.length,
      playerPagesProcessed: 0,
      fetchedFiles: 0,
      changedFiles: 0,
      plannedEndpoints,
    };
  }

  const sql = getIngestDb();
  const sourceId = await ensureApiFootballSource(sql);
  const syncRunId = await createSyncRun(sql, sourceId);
  let fetchedFiles = 0;
  let changedFiles = 0;
  let competitionSeasonsProcessed = 0;
  let playerPagesProcessed = 0;

  try {
    for (const target of targets) {
      for (const season of seasons) {
        const pages: ApiFootballPageSummary[] = [];
        let currentPage = 1;
        let totalPages = 1;
        let truncatedReason: string | null = null;

        while (currentPage <= totalPages) {
          const endpoint = buildApiFootballPlayersPath(target.leagueId, season, currentPage);
          const payload = await fetchApiFootballJson<ApiFootballPlayersResponse>(endpoint);

          if (payload.errors && Object.keys(payload.errors).length > 0) {
            const planLimitMessage = getPlanLimitMessage(payload);
            if (planLimitMessage) {
              truncatedReason = `stopped_at_page_${currentPage}: ${planLimitMessage}`;
              break;
            }

            throw new Error(`API-Football players request returned errors for ${endpoint}: ${JSON.stringify(payload.errors)}`);
          }

          const pagingCurrent = payload.paging?.current ?? currentPage;
          totalPages = payload.paging?.total ?? pagingCurrent;
          const results = payload.results ?? payload.response?.length ?? 0;

          await upsertRawPayload(sql, {
            sourceId,
            syncRunId,
            endpoint,
            externalId: `${target.leagueId}:${season}:page:${pagingCurrent}`,
            seasonContext: String(season),
            payload,
          });

          pages.push({ endpoint, results, pagingCurrent, pagingTotal: totalPages });
          fetchedFiles += 1;
          changedFiles += 1;
          playerPagesProcessed += 1;
          currentPage = pagingCurrent + 1;
        }

        await upsertCompetitionSeasonManifest(sql, {
          sourceId,
          syncRunId,
          competitionCode: target.code,
          leagueId: target.leagueId,
          season,
          pages,
          truncatedReason,
        });
        competitionSeasonsProcessed += 1;
      }
    }

    const summary = {
      dryRun: false,
      competitionCodes: targets.map((target) => target.code),
      seasons,
      competitionSeasonsProcessed,
      playerPagesProcessed,
      fetchedFiles,
      changedFiles,
      plannedEndpoints,
    } satisfies IngestApiFootballPlayerStatsSummary;

    await updateSyncRun(sql, syncRunId, summary, 'completed');
    return summary;
  } catch (error) {
    await updateSyncRun(sql, syncRunId, { fetchedFiles, changedFiles }, 'failed');
    throw error;
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
