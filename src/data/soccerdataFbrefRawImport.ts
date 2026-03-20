import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import postgres, { type Sql } from 'postgres';

type SoccerdataEntityType = 'competition' | 'match' | 'player' | 'team';

const BATCH_SIZE = 500;

interface SourceRow {
  id: number;
}

interface SyncRunRow {
  id: number;
}

interface SoccerdataFbrefRawRecord {
  endpoint: string;
  entityType: SoccerdataEntityType;
  externalId: string | null;
  externalParentId?: string | null;
  manifestType: string;
  metadata?: Record<string, unknown>;
  payload: unknown;
  seasonContext?: string | null;
  sourceAvailableAt?: string | null;
  sourceUpdatedAt?: string | null;
  upstreamPath: string;
}

interface ManifestAggregate {
  entityTypes: Set<SoccerdataEntityType>;
  externalId: string | null;
  externalParentId: string | null;
  manifestType: string;
  metadata: Record<string, unknown>;
  rowCount: number;
  sourceAvailableAt: string | null;
  sourceUpdatedAt: string | null;
  upstreamPath: string;
}

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

export interface ImportSoccerdataFbrefRawOptions {
  dryRun?: boolean;
  competitionCode?: string;
  inputPath?: string;
  season?: string;
  sourceSlug?: string;
}

export interface ImportSoccerdataFbrefRawSummary {
  competitionCode: string | null;
  dryRun: boolean;
  fetchedFiles: number;
  implemented: boolean;
  inputExists: boolean;
  inputPath: string | null;
  manifestCount: number;
  payloadCount: number;
  season: string | null;
  sourceSlug: string;
  syncRunId: number | null;
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

function readJsonLines(inputPath: string) {
  const contents = readFileSync(inputPath, 'utf8');
  const records = contents
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as SoccerdataFbrefRawRecord);

  for (const record of records) {
    if (!record.endpoint || !record.entityType || !record.manifestType || !record.upstreamPath) {
      throw new Error('Invalid soccerdata FBref raw record: endpoint, entityType, manifestType, and upstreamPath are required.');
    }

    if (record.payload === undefined) {
      throw new Error(`Invalid soccerdata FBref raw record for ${record.endpoint}: payload is required.`);
    }
  }

  return records;
}

function buildManifestAggregates(records: SoccerdataFbrefRawRecord[]) {
  const aggregates = new Map<string, ManifestAggregate>();

  for (const record of records) {
    const key = `${record.manifestType}:${record.upstreamPath}`;
    const existing = aggregates.get(key);
    if (existing) {
      existing.rowCount += 1;
      existing.entityTypes.add(record.entityType);
      existing.sourceAvailableAt = existing.sourceAvailableAt ?? record.sourceAvailableAt ?? null;
      existing.sourceUpdatedAt = existing.sourceUpdatedAt ?? record.sourceUpdatedAt ?? null;
      continue;
    }

    aggregates.set(key, {
      entityTypes: new Set([record.entityType]),
      externalId: record.seasonContext ?? record.externalId ?? null,
      externalParentId: record.externalParentId ?? null,
      manifestType: record.manifestType,
      metadata: record.metadata ?? {},
      rowCount: 1,
      sourceAvailableAt: record.sourceAvailableAt ?? null,
      sourceUpdatedAt: record.sourceUpdatedAt ?? null,
      upstreamPath: record.upstreamPath,
    });
  }

  return [...aggregates.values()];
}

async function ensureSoccerdataFbrefSource(sql: Sql, sourceSlug: string) {
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
    VALUES (${sourceSlug}, 'soccerdata FBref', 'https://fbref.com', 'scrape', 'soccerdata-fbref', 3)
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
      'fbref_raw_import',
      NULL,
      'running',
      ${JSON.stringify({ mode: 'raw_import', source: 'soccerdata_fbref' })}::jsonb
    )
    RETURNING id
  `;

  return rows[0].id;
}

async function updateSyncRun(
  sql: Sql,
  syncRunId: number,
  summary: Pick<ImportSoccerdataFbrefRawSummary, 'fetchedFiles' | 'payloadCount'>,
  status: 'completed' | 'failed',
) {
  await sql`
    UPDATE source_sync_runs
    SET
      status = ${status},
      fetched_files = ${summary.fetchedFiles},
      changed_files = ${summary.payloadCount},
      completed_at = NOW(),
      metadata = COALESCE(metadata, '{}'::jsonb) || ${JSON.stringify(summary)}::jsonb
    WHERE id = ${syncRunId}
  `;
}

export async function importSoccerdataFbrefRaw(
  options: ImportSoccerdataFbrefRawOptions = {},
): Promise<ImportSoccerdataFbrefRawSummary> {
  const sourceSlug = options.sourceSlug?.trim() || 'soccerdata_fbref';
  const inputPath = options.inputPath?.trim() || null;
  const inputExists = inputPath ? existsSync(inputPath) : false;

  if (!inputPath || !inputExists) {
    return {
      competitionCode: options.competitionCode?.trim().toUpperCase() || null,
      dryRun: options.dryRun ?? true,
      fetchedFiles: 0,
      implemented: true,
      inputExists,
      inputPath,
      manifestCount: 0,
      payloadCount: 0,
      season: options.season?.trim() || null,
      sourceSlug,
      syncRunId: null,
    };
  }

  const records = readJsonLines(inputPath);
  const manifests = buildManifestAggregates(records);
  const summary: ImportSoccerdataFbrefRawSummary = {
    competitionCode: options.competitionCode?.trim().toUpperCase() || null,
    dryRun: options.dryRun ?? true,
    fetchedFiles: 1,
    implemented: true,
    inputExists,
    inputPath,
    manifestCount: manifests.length,
    payloadCount: records.length,
    season: options.season?.trim() || null,
    sourceSlug,
    syncRunId: null,
  };

  if (summary.dryRun) {
    return summary;
  }

  const sql = getIngestDb();
  const sourceId = await ensureSoccerdataFbrefSource(sql, sourceSlug);
  const syncRunId = await createSyncRun(sql, sourceId);
  summary.syncRunId = syncRunId;

  try {
    await sql`BEGIN`;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
      const chunk = records.slice(i, i + BATCH_SIZE);

      await sql`
        INSERT INTO raw_payloads (
          source_id, sync_run_id, endpoint, entity_type, external_id,
          season_context, http_status, payload, payload_hash, source_updated_at
        )
        SELECT
          ${sourceId}, ${syncRunId},
          t.endpoint,
          t.entity_type,
          t.external_id,
          t.season_context,
          t.http_status,
          t.payload::jsonb,
          t.payload_hash,
          t.source_updated_at
        FROM UNNEST(
          ${chunk.map((r) => r.endpoint)}::text[],
          ${chunk.map((r) => r.entityType)}::entity_type[],
          ${chunk.map((r) => r.externalId ?? null)}::text[],
          ${chunk.map((r) => r.seasonContext ?? null)}::text[],
          ${chunk.map(() => 200)}::int[],
          ${chunk.map((r) => JSON.stringify(toJsonValue(r.payload)))}::text[],
          ${chunk.map((r) => buildPayloadHash(r.payload))}::text[],
          ${chunk.map((r) => r.sourceUpdatedAt ?? null)}::timestamptz[]
        ) AS t(
          endpoint,
          entity_type,
          external_id,
          season_context,
          http_status,
          payload,
          payload_hash,
          source_updated_at
        )
      `;
    }

    for (let i = 0; i < manifests.length; i += BATCH_SIZE) {
      const chunk = manifests.slice(i, i + BATCH_SIZE);

      await sql`
        INSERT INTO source_sync_manifests (
          source_id, sync_run_id, manifest_type, upstream_path,
          external_id, external_parent_id, source_updated_at,
          source_available_at, metadata
        )
        SELECT
          ${sourceId}, ${syncRunId},
          t.manifest_type,
          t.upstream_path,
          t.external_id,
          t.external_parent_id,
          t.source_updated_at,
          t.source_available_at,
          t.metadata::jsonb
        FROM UNNEST(
          ${chunk.map((a) => a.manifestType)}::text[],
          ${chunk.map((a) => a.upstreamPath)}::text[],
          ${chunk.map((a) => a.externalId)}::text[],
          ${chunk.map((a) => a.externalParentId)}::text[],
          ${chunk.map((a) => a.sourceUpdatedAt)}::timestamptz[],
          ${chunk.map((a) => a.sourceAvailableAt)}::timestamptz[],
          ${chunk.map((a) => JSON.stringify({
            ...a.metadata,
            entityTypes: [...a.entityTypes],
            rowCount: a.rowCount,
          }))}::text[]
        ) AS t(
          manifest_type,
          upstream_path,
          external_id,
          external_parent_id,
          source_updated_at,
          source_available_at,
          metadata
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

    await sql`COMMIT`;
    await updateSyncRun(sql, syncRunId, summary, 'completed');
    return summary;
  } catch (error) {
    await sql`ROLLBACK`.catch(() => undefined);
    await updateSyncRun(sql, syncRunId, summary, 'failed');
    throw error;
  }
}
