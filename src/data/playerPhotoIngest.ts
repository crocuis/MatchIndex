import type { Sql } from 'postgres';
import { getSingleConnectionDb } from '@/lib/db';

const API_FOOTBALL_SOURCE_SLUG = 'api_football';
const API_FOOTBALL_SOURCE_NAME = 'API-Football v3';
const API_FOOTBALL_SOURCE_URL = 'https://v3.football.api-sports.io';

interface SourceRow {
  id: number;
}

interface MappingRow {
  player_id: number;
  player_slug: string;
  external_id: string;
}

export interface BackfillApiFootballPlayerPhotoSourceIdsOptions {
  dryRun?: boolean;
  limit?: number;
  playerId?: string;
}

export interface BackfillApiFootballPlayerPhotoSourceIdsSummary {
  dryRun: boolean;
  mappingsFound: number;
  rowsPlanned: number;
  rowsWritten: number;
}

function getIngestDb() {
  return getSingleConnectionDb('photo-ingest');
}

async function ensureApiFootballSource(sql: Sql) {
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, priority)
    VALUES (${API_FOOTBALL_SOURCE_SLUG}, ${API_FOOTBALL_SOURCE_NAME}, ${API_FOOTBALL_SOURCE_URL}, 'api', 2)
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

async function listPlayerMappings(sql: Sql, sourceId: number, options: BackfillApiFootballPlayerPhotoSourceIdsOptions) {
  return sql<MappingRow[]>`
    SELECT
      p.id AS player_id,
      p.slug AS player_slug,
      sem.external_id
    FROM source_entity_mapping sem
    JOIN players p ON p.id = sem.entity_id
    WHERE sem.entity_type = 'player'
      AND sem.source_id = ${sourceId}
      AND (${options.playerId ?? null}::TEXT IS NULL OR p.slug = ${options.playerId ?? null})
    ORDER BY sem.updated_at DESC, p.slug ASC
    LIMIT ${options.limit ?? 100}
  `;
}

async function upsertPlayerPhotoSourceIds(sql: Sql, sourceId: number, mappings: MappingRow[]) {
  for (const mapping of mappings) {
    await sql`
      INSERT INTO player_photo_sources (
        player_id,
        data_source_id,
        external_id,
        status,
        matched_by,
        last_checked_at,
        updated_at
      )
      VALUES (
        ${mapping.player_id},
        ${sourceId},
        ${mapping.external_id},
        'pending',
        'source_entity_mapping',
        NOW(),
        NOW()
      )
      ON CONFLICT (player_id, data_source_id)
      DO UPDATE SET
        external_id = EXCLUDED.external_id,
        matched_by = EXCLUDED.matched_by,
        last_checked_at = EXCLUDED.last_checked_at,
        updated_at = NOW()
    `;
  }
}

export async function backfillApiFootballPlayerPhotoSourceIds(
  options: BackfillApiFootballPlayerPhotoSourceIdsOptions = {}
): Promise<BackfillApiFootballPlayerPhotoSourceIdsSummary> {
  const sql = getIngestDb();
  const sourceId = await ensureApiFootballSource(sql);
  const mappings = await listPlayerMappings(sql, sourceId, options);

  if (options.dryRun ?? true) {
    return {
      dryRun: true,
      mappingsFound: mappings.length,
      rowsPlanned: mappings.length,
      rowsWritten: 0,
    };
  }

  await upsertPlayerPhotoSourceIds(sql, sourceId, mappings);

  return {
    dryRun: false,
    mappingsFound: mappings.length,
    rowsPlanned: mappings.length,
    rowsWritten: mappings.length,
  };
}
