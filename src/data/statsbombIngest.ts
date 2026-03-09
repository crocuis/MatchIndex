import { createHash } from 'node:crypto';
import type { Sql } from 'postgres';
import { getSingleConnectionDb } from '@/lib/db';
import type {
  StatsBombCompetitionEntry,
  StatsBombCompetitionSeasonManifest,
  StatsBombMatchEntry,
  StatsBombMatchManifest,
} from './statsbomb';

const STATSBOMB_SOURCE_SLUG = 'statsbomb_open_data';
const STATSBOMB_SOURCE_NAME = 'StatsBomb Open Data';
const STATSBOMB_SOURCE_URL = 'https://github.com/statsbomb/open-data';

interface SourceRow {
  id: number;
}

interface SyncRunRow {
  id: number;
}

export interface IngestStatsBombManifestOptions {
  dryRun?: boolean;
  competitionLimit?: number;
  matchesPerSeasonLimit?: number;
  upstreamRef?: string;
  upstreamCommitSha?: string | null;
}

export interface IngestStatsBombManifestSummary {
  dryRun: boolean;
  competitionsProcessed: number;
  matchesProcessed: number;
  fetchedFiles: number;
  changedFiles: number;
  upstreamRef: string;
}

function buildPayloadHash(payload: unknown) {
  return createHash('sha256').update(JSON.stringify(payload)).digest('hex');
}

function buildCompetitionSeasonPath(entry: StatsBombCompetitionEntry) {
  return `data/competitions.json:${entry.competition_id}:${entry.season_id}`;
}

function buildMatchesPath(entry: StatsBombCompetitionEntry) {
  return `data/matches/${entry.competition_id}/${entry.season_id}.json`;
}

function buildMatchManifestPath(entry: StatsBombMatchEntry) {
  return `data/matches/${entry.competition.competition_id}/${entry.season.season_id}.json:${entry.match_id}`;
}

async function loadStatsBombModule(): Promise<typeof import('./statsbomb')> {
  return import(new URL('./statsbomb.ts', import.meta.url).href);
}

function getIngestDb() {
  return getSingleConnectionDb('statsbomb-ingest');
}

async function ensureStatsBombSource(sql: Sql) {
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
    VALUES (${STATSBOMB_SOURCE_SLUG}, ${STATSBOMB_SOURCE_NAME}, ${STATSBOMB_SOURCE_URL}, 'git', 'master', 0)
    ON CONFLICT (slug)
    DO UPDATE SET
      name = EXCLUDED.name,
      base_url = EXCLUDED.base_url,
      source_kind = EXCLUDED.source_kind,
      upstream_ref = EXCLUDED.upstream_ref,
      priority = EXCLUDED.priority
    RETURNING id
  `;

  return rows[0].id;
}

async function createSyncRun(sql: Sql, sourceId: number, upstreamRef: string, upstreamCommitSha: string | null) {
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
      ${upstreamRef},
      ${upstreamCommitSha},
      'running',
      ${JSON.stringify({ mode: 'manifest_ingest' })}::jsonb
    )
    RETURNING id
  `;

  return rows[0].id;
}

async function updateSyncRun(
  sql: Sql,
  syncRunId: number,
  summary: Pick<IngestStatsBombManifestSummary, 'changedFiles' | 'fetchedFiles'>,
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
    entityType: 'competition' | 'match';
    externalId: string | null;
    seasonContext: string | null;
    payload: unknown;
    upstreamCommitSha: string | null;
    sourceUpdatedAt: string | null;
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
      payload_hash,
      upstream_commit_sha,
      source_updated_at
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
      ${payloadHash},
      ${params.upstreamCommitSha},
      ${params.sourceUpdatedAt}
    )
  `;
}

async function upsertCompetitionSeasonManifest(
  sql: Sql,
  params: {
    sourceId: number;
    syncRunId: number;
    entry: StatsBombCompetitionEntry;
    manifest: StatsBombCompetitionSeasonManifest;
    upstreamCommitSha: string | null;
  }
) {
  await sql`
    INSERT INTO source_sync_manifests (
      source_id,
      sync_run_id,
      manifest_type,
      upstream_path,
      upstream_commit_sha,
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
      ${buildCompetitionSeasonPath(params.entry)},
      ${params.upstreamCommitSha},
      ${params.manifest.sourceSeasonId},
      ${params.manifest.sourceCompetitionId},
      ${params.manifest.matchUpdatedAt},
      ${params.manifest.matchAvailableAt},
      ${JSON.stringify(params.manifest)}::jsonb
    )
    ON CONFLICT (source_id, manifest_type, upstream_path)
    DO UPDATE SET
      sync_run_id = EXCLUDED.sync_run_id,
      upstream_commit_sha = EXCLUDED.upstream_commit_sha,
      external_id = EXCLUDED.external_id,
      external_parent_id = EXCLUDED.external_parent_id,
      source_updated_at = EXCLUDED.source_updated_at,
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
    entry: StatsBombMatchEntry;
    manifest: StatsBombMatchManifest;
    upstreamCommitSha: string | null;
  }
) {
  await sql`
    INSERT INTO source_sync_manifests (
      source_id,
      sync_run_id,
      manifest_type,
      upstream_path,
      upstream_commit_sha,
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
      ${buildMatchManifestPath(params.entry)},
      ${params.upstreamCommitSha},
      ${params.manifest.sourceMatchId},
      ${`${params.manifest.sourceCompetitionId}:${params.manifest.sourceSeasonId}`},
      ${params.manifest.lastUpdatedAt},
      ${params.manifest.lastUpdatedAt},
      ${JSON.stringify(params.manifest)}::jsonb
    )
    ON CONFLICT (source_id, manifest_type, upstream_path)
    DO UPDATE SET
      sync_run_id = EXCLUDED.sync_run_id,
      upstream_commit_sha = EXCLUDED.upstream_commit_sha,
      external_id = EXCLUDED.external_id,
      external_parent_id = EXCLUDED.external_parent_id,
      source_updated_at = EXCLUDED.source_updated_at,
      source_available_at = EXCLUDED.source_available_at,
      metadata = EXCLUDED.metadata,
      updated_at = NOW()
  `;
}

export async function ingestStatsBombManifests(
  options: IngestStatsBombManifestOptions = {}
): Promise<IngestStatsBombManifestSummary> {
  const upstreamRef = options.upstreamRef ?? 'master';
  const { fetchStatsBombJson, buildCompetitionSeasonManifest, buildMatchManifest } = await loadStatsBombModule();
  const competitionEntries = await fetchStatsBombJson<StatsBombCompetitionEntry[]>('data/competitions.json');
  const limitedCompetitionEntries = competitionEntries.slice(0, options.competitionLimit ?? competitionEntries.length);

  let competitionsProcessed = 0;
  let matchesProcessed = 0;
  let fetchedFiles = 1;
  let changedFiles = 1;

  if (options.dryRun ?? false) {
    for (const competitionEntry of limitedCompetitionEntries) {
      competitionsProcessed += 1;
      const matchEntries = await fetchStatsBombJson<StatsBombMatchEntry[]>(buildMatchesPath(competitionEntry));
      fetchedFiles += 1;
      changedFiles += 1;
      matchesProcessed += matchEntries.slice(0, options.matchesPerSeasonLimit ?? matchEntries.length).length;
    }

    return {
      dryRun: true,
      competitionsProcessed,
      matchesProcessed,
      fetchedFiles,
      changedFiles,
      upstreamRef,
    };
  }

  const sql = getIngestDb();
  const sourceId = await ensureStatsBombSource(sql);
  const syncRunId = await createSyncRun(sql, sourceId, upstreamRef, options.upstreamCommitSha ?? null);

  try {
    await upsertRawPayload(sql, {
      sourceId,
      syncRunId,
      endpoint: 'data/competitions.json',
      entityType: 'competition',
      externalId: null,
      seasonContext: null,
      payload: limitedCompetitionEntries,
      upstreamCommitSha: options.upstreamCommitSha ?? null,
      sourceUpdatedAt: limitedCompetitionEntries[0]?.match_updated ?? null,
    });

    for (const competitionEntry of limitedCompetitionEntries) {
      competitionsProcessed += 1;
      const competitionManifest = buildCompetitionSeasonManifest(competitionEntry);
      await upsertCompetitionSeasonManifest(sql, {
        sourceId,
        syncRunId,
        entry: competitionEntry,
        manifest: competitionManifest,
        upstreamCommitSha: options.upstreamCommitSha ?? null,
      });

      const matchEntries = await fetchStatsBombJson<StatsBombMatchEntry[]>(buildMatchesPath(competitionEntry));
      const limitedMatchEntries = matchEntries.slice(0, options.matchesPerSeasonLimit ?? matchEntries.length);
      fetchedFiles += 1;
      changedFiles += 1;

      await upsertRawPayload(sql, {
        sourceId,
        syncRunId,
        endpoint: buildMatchesPath(competitionEntry),
        entityType: 'match',
        externalId: `${competitionEntry.competition_id}:${competitionEntry.season_id}`,
        seasonContext: String(competitionEntry.season_id),
        payload: limitedMatchEntries,
        upstreamCommitSha: options.upstreamCommitSha ?? null,
        sourceUpdatedAt: limitedMatchEntries[0]?.last_updated ?? null,
      });

      for (const matchEntry of limitedMatchEntries) {
        matchesProcessed += 1;
        const matchManifest = buildMatchManifest(matchEntry);
        await upsertMatchManifest(sql, {
          sourceId,
          syncRunId,
          entry: matchEntry,
          manifest: matchManifest,
          upstreamCommitSha: options.upstreamCommitSha ?? null,
        });
      }
    }

    const summary = {
      dryRun: false,
      competitionsProcessed,
      matchesProcessed,
      fetchedFiles,
      changedFiles,
      upstreamRef,
    } satisfies IngestStatsBombManifestSummary;

    await updateSyncRun(sql, syncRunId, summary, 'completed');
    return summary;
  } catch (error) {
    await updateSyncRun(
      sql,
      syncRunId,
      {
        fetchedFiles,
        changedFiles,
      },
      'failed'
    );
    throw error;
  }
}
