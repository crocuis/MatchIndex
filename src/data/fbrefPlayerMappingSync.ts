import { readFile } from 'node:fs/promises';
import path from 'node:path';
import postgres from 'postgres';

const BATCH_SIZE = 500;

interface SourceRow {
  id: number;
}

interface PlayerRow {
  id: number;
  slug: string;
}

interface FbrefMappingEntry {
  playerSlug: string;
  sourceUrl: string;
}

export interface SyncFbrefPlayerMappingsOptions {
  dryRun?: boolean;
  filePath?: string;
  sourceSlug?: string;
}

export interface SyncFbrefPlayerMappingsSummary {
  dryRun: boolean;
  filePath: string;
  mappingsRead: number;
  mappingsWritten: number;
  missingPlayers: string[];
  sourceSlug: string;
}

function getDb() {
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

function resolvePathLike(filePath: string) {
  return path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
}

function getMappingsPath(input?: string) {
  return resolvePathLike(input?.trim() || process.env.FBREF_PLAYER_MAPPINGS_FILE?.trim() || 'data/fbref-player-mappings.json');
}

function buildExternalIds(sourceUrl: string) {
  const normalized = sourceUrl.trim();
  const url = new URL(normalized);
  return Array.from(new Set([normalized, url.pathname]));
}

async function ensureSource(sql: postgres.Sql, sourceSlug: string) {
  const rows = await sql<SourceRow[]>`
    SELECT id
    FROM data_sources
    WHERE slug = ${sourceSlug}
    LIMIT 1
  `;

  if (!rows[0]) {
    throw new Error(`Data source '${sourceSlug}' is not initialized`);
  }

  return rows[0].id;
}

async function loadPlayers(sql: postgres.Sql) {
  const rows = await sql<PlayerRow[]>`
    SELECT id, slug
    FROM players
  `;

  return new Map(rows.map((row) => [row.slug, row.id]));
}

export async function syncFbrefPlayerMappings(
  options: SyncFbrefPlayerMappingsOptions = {},
): Promise<SyncFbrefPlayerMappingsSummary> {
  const filePath = getMappingsPath(options.filePath);
  const sourceSlug = options.sourceSlug?.trim() || 'soccerdata_fbref';
  const summary: SyncFbrefPlayerMappingsSummary = {
    dryRun: options.dryRun ?? true,
    filePath,
    mappingsRead: 0,
    mappingsWritten: 0,
    missingPlayers: [],
    sourceSlug,
  };
  const entries = JSON.parse(await readFile(filePath, 'utf8')) as FbrefMappingEntry[];
  const validEntries = entries.filter((entry) => entry.playerSlug?.trim() && entry.sourceUrl?.trim());
  summary.mappingsRead = validEntries.length;

  const sql = getDb();

  try {
    const [sourceId, playerIdBySlug] = await Promise.all([
      ensureSource(sql, sourceSlug),
      loadPlayers(sql),
    ]);

    const missingPlayers: string[] = [];

    type MappingTuple = { playerId: number; externalId: string; metadata: string };
    const tuples: MappingTuple[] = [];

    for (const entry of validEntries) {
      const playerId = playerIdBySlug.get(entry.playerSlug.trim());
      if (!playerId) {
        missingPlayers.push(entry.playerSlug.trim());
        continue;
      }

      const externalIds = buildExternalIds(entry.sourceUrl);
      const metadataJson = JSON.stringify({ source: 'fbref_mapping_file', sourceUrl: entry.sourceUrl });

      if (summary.dryRun) {
        tuples.push(...externalIds.map(externalId => ({ playerId, externalId, metadata: metadataJson })));
        continue;
      }

      tuples.push(...externalIds.map(externalId => ({ playerId, externalId, metadata: metadataJson })));
    }

    const written = summary.dryRun ? tuples.length : 0;

    if (!summary.dryRun) {
      let writtenCount = 0;
      for (let i = 0; i < tuples.length; i += BATCH_SIZE) {
        const chunk = tuples.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO source_entity_mapping (
            entity_type,
            entity_id,
            source_id,
            external_id,
            metadata,
            updated_at
          )
          SELECT 'player', t.player_id, ${sourceId}, t.external_id, t.metadata::jsonb, NOW()
          FROM UNNEST(
            ${sql.array(chunk.map(t => t.playerId))}::int[],
            ${sql.array(chunk.map(t => t.externalId))}::text[],
            ${sql.array(chunk.map(t => t.metadata))}::text[]
          ) AS t(player_id, external_id, metadata)
          ON CONFLICT (entity_type, source_id, external_id)
          DO UPDATE SET
            entity_id = EXCLUDED.entity_id,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        `;
        writtenCount += chunk.length;
      }
      summary.mappingsWritten = writtenCount;
    } else {
      summary.mappingsWritten = written;
    }

    summary.missingPlayers = missingPlayers.sort();
    return summary;
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
