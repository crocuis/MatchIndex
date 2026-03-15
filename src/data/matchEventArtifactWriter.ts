import type { Sql } from 'postgres';
import type {
  MatchAnalysisArtifactPayload,
  MatchEventArtifactType,
  MatchEventFreezeFramesArtifactPayload,
  MatchEventVisibleAreasArtifactPayload,
} from '@/data/types';
import { buildSourceAwareMatchArtifactStorageKey, writeJsonGzipArtifact } from '@/lib/artifactStore';

interface MatchEventArtifactRecordRow {
  table_name: string | null;
}

interface PersistArtifactParams {
  matchId: number;
  matchDate: string;
  sourceVendor: string | null;
  payload:
    | MatchAnalysisArtifactPayload
    | MatchEventFreezeFramesArtifactPayload
    | MatchEventVisibleAreasArtifactPayload;
}

const ARTIFACT_FILE_NAMES: Record<MatchEventArtifactType, string> = {
  analysis_detail: 'analysis-detail.v1.json.gz',
  freeze_frames: 'freeze-frames.v1.json.gz',
  visible_areas: 'visible-areas.v1.json.gz',
  raw_event_bundle: 'raw-event-bundle.v1.json.gz',
};

function getArtifactRowCount(
  payload:
    | MatchAnalysisArtifactPayload
    | MatchEventFreezeFramesArtifactPayload
    | MatchEventVisibleAreasArtifactPayload,
) {
  if (payload.artifactType === 'analysis_detail') {
    return payload.events.length;
  }

  if (payload.artifactType === 'freeze_frames') {
    return payload.freezeFrames.reduce((total, entry) => total + entry.freezeFrames.length, 0);
  }

  return payload.visibleAreas.length;
}

async function hasMatchEventArtifactsTable(sql: Sql) {
  const rows = await sql<MatchEventArtifactRecordRow[]>`
    SELECT to_regclass('public.match_event_artifacts')::TEXT AS table_name
  `;

  return rows[0]?.table_name === 'match_event_artifacts';
}

async function upsertArtifactMetadata(sql: Sql, params: PersistArtifactParams, storageKey: string, byteSize: number, checksumSha256: string) {
  if (!(await hasMatchEventArtifactsTable(sql))) {
    return;
  }

  await sql`
    INSERT INTO match_event_artifacts (
      match_id,
      match_date,
      artifact_type,
      format,
      storage_key,
      version,
      row_count,
      byte_size,
      checksum_sha256,
      source_vendor,
      updated_at
    )
    VALUES (
      ${params.matchId},
      ${params.matchDate},
      ${params.payload.artifactType},
      ${'json.gz'},
      ${storageKey},
      ${params.payload.version},
      ${getArtifactRowCount(params.payload)},
      ${byteSize},
      ${checksumSha256},
      ${params.sourceVendor},
      NOW()
    )
    ON CONFLICT (match_id, artifact_type, version)
    DO UPDATE SET
      storage_key = EXCLUDED.storage_key,
      row_count = EXCLUDED.row_count,
      byte_size = EXCLUDED.byte_size,
      checksum_sha256 = EXCLUDED.checksum_sha256,
      source_vendor = EXCLUDED.source_vendor,
      updated_at = NOW()
  `;
}

export async function persistMatchEventArtifacts(sql: Sql, params: PersistArtifactParams) {
  const storageKey = buildSourceAwareMatchArtifactStorageKey(
    params.sourceVendor,
    params.matchDate,
    params.matchId,
    ARTIFACT_FILE_NAMES[params.payload.artifactType],
  );

  const artifact = await writeJsonGzipArtifact(storageKey, params.payload);
  await upsertArtifactMetadata(sql, params, storageKey, artifact.byteSize, artifact.checksumSha256);

  return {
    storageKey,
    rowCount: getArtifactRowCount(params.payload),
    byteSize: artifact.byteSize,
  };
}
