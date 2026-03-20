import { readFile } from 'node:fs/promises';
import path from 'node:path';
import postgres, { type Sql } from 'postgres';

const BATCH_SIZE = 500;

interface ApiFootballPlayerMappingRecord {
  playerId: string;
  externalId: string;
}

interface SourceRow {
  id: number;
}

interface PlayerRow {
  id: number;
  slug: string;
}

export interface SyncApiFootballPlayerMappingsOptions {
  dryRun?: boolean;
  limit?: number;
  playerId?: string;
}

export interface SyncApiFootballPlayerMappingsSummary {
  dryRun: boolean;
  totalMappingsRead: number;
  selectedMappings: number;
  matchedPlayers: number;
  missingPlayers: string[];
  syncedMappings: number;
}

function getMappingsFilePath() {
  const customPath = process.env.API_FOOTBALL_PLAYER_MAPPINGS_FILE?.trim();
  if (customPath) {
    return path.isAbsolute(customPath) ? customPath : path.join(process.cwd(), customPath);
  }

  return path.join(process.cwd(), 'data', 'api-football-player-mappings.json');
}

async function readMappingsFile() {
  const payload = JSON.parse(await readFile(getMappingsFilePath(), 'utf8')) as unknown;
  if (!Array.isArray(payload)) {
    throw new Error('API-Football player mappings file must be an array');
  }

  return payload.filter((entry): entry is ApiFootballPlayerMappingRecord => {
    if (!entry || typeof entry !== 'object') return false;

    const record = entry as Partial<ApiFootballPlayerMappingRecord>;
    return typeof record.playerId === 'string'
      && record.playerId.trim().length > 0
      && typeof record.externalId === 'string'
      && record.externalId.trim().length > 0;
  });
}

function selectMappings(
  mappings: ApiFootballPlayerMappingRecord[],
  options: SyncApiFootballPlayerMappingsOptions,
) {
  const filtered = options.playerId
    ? mappings.filter((mapping) => mapping.playerId === options.playerId)
    : mappings;

  return filtered.slice(0, options.limit ?? filtered.length);
}

async function ensureApiFootballSource(sql: Sql) {
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, priority)
    VALUES ('api_football', 'API-Football v3', 'https://v3.football.api-sports.io', 'api', 2)
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

async function loadPlayersBySlug(sql: Sql, playerSlugs: string[]) {
  return sql<PlayerRow[]>`
    SELECT id, slug
    FROM players
    WHERE slug = ANY(${playerSlugs})
  `;
}

function getMappingDb() {
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

export async function syncApiFootballPlayerMappings(
  options: SyncApiFootballPlayerMappingsOptions = {},
): Promise<SyncApiFootballPlayerMappingsSummary> {
  const mappings = await readMappingsFile();
  const selectedMappings = selectMappings(mappings, options);
  const sql = getMappingDb();
  try {
    const playerRows = await loadPlayersBySlug(sql, selectedMappings.map((mapping) => mapping.playerId));
    const playerIdBySlug = new Map(playerRows.map((row) => [row.slug, row.id]));
    const missingPlayers = selectedMappings
      .map((mapping) => mapping.playerId)
      .filter((playerId) => !playerIdBySlug.has(playerId));

    if (options.dryRun ?? true) {
      return {
        dryRun: true,
        totalMappingsRead: mappings.length,
        selectedMappings: selectedMappings.length,
        matchedPlayers: playerRows.length,
        missingPlayers,
        syncedMappings: 0,
      };
    }

    const sourceId = await ensureApiFootballSource(sql);
    const mappingFilename = path.basename(getMappingsFilePath());
    const metadataJson = JSON.stringify({ source: 'api_football', mappingFile: mappingFilename });

    const matchedMappings = selectedMappings.filter((mapping) => playerIdBySlug.has(mapping.playerId));
    let syncedMappings = 0;

    for (let i = 0; i < matchedMappings.length; i += BATCH_SIZE) {
      const chunk = matchedMappings.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO source_entity_mapping (
          entity_type,
          entity_id,
          source_id,
          external_id,
          metadata,
          updated_at
        )
        SELECT 'player', t.player_id, ${sourceId}, t.external_id, ${metadataJson}::jsonb, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map(m => playerIdBySlug.get(m.playerId)!))}::int[],
          ${sql.array(chunk.map(m => m.externalId))}::text[]
        ) AS t(player_id, external_id)
        ON CONFLICT (entity_type, source_id, external_id)
        DO UPDATE SET
          entity_id = EXCLUDED.entity_id,
          metadata = EXCLUDED.metadata,
          updated_at = NOW()
      `;
      syncedMappings += chunk.length;
    }

    return {
      dryRun: false,
      totalMappingsRead: mappings.length,
      selectedMappings: selectedMappings.length,
      matchedPlayers: playerRows.length,
      missingPlayers,
      syncedMappings,
    };
  } finally {
    await sql.end({ timeout: 1 });
  }
}
