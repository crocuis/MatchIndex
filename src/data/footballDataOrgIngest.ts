import { createHash } from 'node:crypto';
import postgres, { type Sql } from 'postgres';
import {
  buildFootballDataCompetitionMatchesPath,
  buildFootballDataCompetitionTeamsPath,
  fetchFootballDataJson,
  getFootballDataSourceConfig,
  getDefaultFootballDataCompetitionTargets,
  type FootballDataOrgCompetitionTarget,
  type FootballDataOrgCompetitionResponse,
  type FootballDataOrgMatchSummary,
  type FootballDataOrgMatchesResponse,
  type FootballDataOrgTeamsResponse,
} from './footballDataOrg.ts';

interface SourceRow {
  id: number;
}

interface SyncRunRow {
  id: number;
}

export interface IngestFootballDataManifestOptions {
  dryRun?: boolean;
  competitionCodes?: string[];
  seasons?: number[];
  status?: string;
}

export interface IngestFootballDataManifestSummary {
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

  const currentYear = new Date().getUTCFullYear();
  return [currentYear];
}

function normalizeCompetitionTargets(rawCodes?: string[]) {
  const defaults = getDefaultFootballDataCompetitionTargets();

  if (!rawCodes || rawCodes.length === 0) {
    return defaults;
  }

  const labels = new Map(defaults.map((target) => [target.code, target.name]));
  const codes = [...new Set(rawCodes.map((code) => code.trim().toUpperCase()).filter(Boolean))];

  return codes.map((code) => ({
    code,
    name: labels.get(code) ?? code,
  } satisfies FootballDataOrgCompetitionTarget));
}

function buildCompetitionPath(code: string) {
  return `/competitions/${code}`;
}

async function ensureFootballDataSource(sql: Sql) {
  const config = getFootballDataSourceConfig();
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, priority)
    VALUES (${config.slug}, ${config.name}, ${config.baseUrl}, 'api', 1)
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
      'v4',
      NULL,
      'running',
      ${JSON.stringify({ mode: 'manifest_ingest', source: 'football-data.org' })}::jsonb
    )
    RETURNING id
  `;

  return rows[0].id;
}

async function updateSyncRun(
  sql: Sql,
  syncRunId: number,
  summary: Pick<IngestFootballDataManifestSummary, 'changedFiles' | 'fetchedFiles'>,
  status: 'completed' | 'failed'
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
  }
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
      ${JSON.stringify(params.payload)}::jsonb,
      ${payloadHash}
    )
  `;
}

async function upsertCompetitionSeasonManifest(
  sql: Sql,
  params: {
    sourceId: number;
    syncRunId: number;
    code: string;
    season: number;
    competitionPayload: unknown;
    matchesPayload: FootballDataOrgMatchesResponse;
  }
) {
  const upstreamPath = buildFootballDataCompetitionMatchesPath(params.code, params.season);
  const competition = params.matchesPayload.competition;

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
      ${params.code},
      NULL,
      NOW(),
      ${JSON.stringify({
        competition: params.competitionPayload,
        competitionCode: params.code,
        competitionId: competition?.id ?? null,
        competitionName: competition?.name ?? null,
        competitionType: competition?.type ?? null,
        season: params.season,
        coverageLevel: 'metadata_only',
        resultCount: params.matchesPayload.resultSet?.count ?? params.matchesPayload.matches?.length ?? 0,
      })}::jsonb
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
    code: string;
    season: number;
    match: FootballDataOrgMatchSummary;
  }
) {
  if (!params.match?.id) {
    return;
  }

  const upstreamPath = `${buildFootballDataCompetitionMatchesPath(params.code, params.season)}:${params.match.id}`;

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
      ${String(params.match.id)},
      ${`${params.code}:${params.season}`},
      ${params.match.utcDate ?? null},
      NOW(),
      ${JSON.stringify({
        competitionCode: params.code,
        season: params.season,
        matchId: params.match.id,
        utcDate: params.match.utcDate ?? null,
        status: params.match.status ?? null,
        matchday: params.match.matchday ?? null,
        stage: params.match.stage ?? null,
        homeTeam: params.match.homeTeam ?? null,
        awayTeam: params.match.awayTeam ?? null,
        coverageLevel: 'metadata_only',
      })}::jsonb
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

export async function ingestFootballDataManifests(
  options: IngestFootballDataManifestOptions = {}
): Promise<IngestFootballDataManifestSummary> {
  const targets = normalizeCompetitionTargets(options.competitionCodes);
  const seasons = normalizeSeasons(options.seasons);
  const status = options.status?.trim().toUpperCase() || 'FINISHED';
  const plannedEndpoints = targets.flatMap((target) => [
    buildCompetitionPath(target.code),
    ...seasons.map((season) => buildFootballDataCompetitionTeamsPath(target.code, season)),
    ...seasons.map((season) => `${buildFootballDataCompetitionMatchesPath(target.code, season)}&status=${status}`),
  ]);

  if (options.dryRun ?? false) {
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
  const sourceId = await ensureFootballDataSource(sql);
  const syncRunId = await createSyncRun(sql, sourceId);
  let fetchedFiles = 0;
  let changedFiles = 0;
  let competitionSeasonsProcessed = 0;
  let matchManifestsProcessed = 0;

  try {
    for (const target of targets) {
      const competitionPath = buildCompetitionPath(target.code);
      const competitionPayload = await fetchFootballDataJson<FootballDataOrgCompetitionResponse>(competitionPath);
      fetchedFiles += 1;
      changedFiles += 1;

      await upsertRawPayload(sql, {
        sourceId,
        syncRunId,
        endpoint: competitionPath,
        entityType: 'competition',
        externalId: target.code,
        seasonContext: null,
        payload: competitionPayload,
      });

      for (const season of seasons) {
        const teamsPath = buildFootballDataCompetitionTeamsPath(target.code, season);
        const matchesPath = `${buildFootballDataCompetitionMatchesPath(target.code, season)}&status=${status}`;
        const teamsPayload = await fetchFootballDataJson<FootballDataOrgTeamsResponse>(teamsPath);
        const matchesPayload = await fetchFootballDataJson<FootballDataOrgMatchesResponse>(matchesPath);
        fetchedFiles += 2;
        changedFiles += 2;
        competitionSeasonsProcessed += 1;

        await upsertRawPayload(sql, {
          sourceId,
          syncRunId,
          endpoint: teamsPath,
          entityType: null,
          externalId: `${target.code}:${season}:teams`,
          seasonContext: String(season),
          payload: teamsPayload,
        });

        await upsertRawPayload(sql, {
          sourceId,
          syncRunId,
          endpoint: matchesPath,
          entityType: 'match',
          externalId: `${target.code}:${season}`,
          seasonContext: String(season),
          payload: matchesPayload,
        });

        await upsertCompetitionSeasonManifest(sql, {
          sourceId,
          syncRunId,
          code: target.code,
          season,
          competitionPayload,
          matchesPayload,
        });

        for (const match of matchesPayload.matches ?? []) {
          await upsertMatchManifest(sql, {
            sourceId,
            syncRunId,
            code: target.code,
            season,
            match,
          });
          matchManifestsProcessed += 1;
        }
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
    } satisfies IngestFootballDataManifestSummary;

    await updateSyncRun(sql, syncRunId, summary, 'completed');
    return summary;
  } catch (error) {
    await updateSyncRun(sql, syncRunId, { fetchedFiles, changedFiles }, 'failed');
    throw error;
  }
}
