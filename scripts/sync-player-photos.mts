import { readFile } from 'node:fs/promises';
import path from 'node:path';
import postgres from 'postgres';
import type { PhotoSyncProvider } from '../src/data/types.ts';

type ProviderCandidate = {
  provider: PhotoSyncProvider;
  externalId?: string;
  sourceUrl: string;
  mirroredUrl?: string;
  matchedBy: string;
  matchScore?: number;
  etag?: string;
  lastModified?: string;
};

type SyncTarget = {
  id: string;
  name: string;
  firstName: string;
  lastName: string;
  dateOfBirth: string | null;
  nationality: string;
  photoUrl: string | null;
  apiFootballExternalId: string | null;
  sofascoreExternalId: string | null;
};

type SyncTargetRow = {
  id: string;
  name: string;
  first_name: string;
  last_name: string;
  date_of_birth: string | null;
  nationality: string;
  photo_url: string | null;
  api_football_external_id: string | null;
};

type SofascorePlayerMapping = {
  playerSlug: string;
  externalId: string;
};

interface CliOptions {
  dryRun: boolean;
  help: boolean;
  limit: number;
  playerId?: string;
}

interface SyncConfig {
  apiFootballPhotoUrlTemplate: string;
  apiFootballKey?: string;
  mirrorBaseUrl?: string;
  sofascorePhotoUrlTemplate: string;
}

const PROVIDER_PRIORITY: PhotoSyncProvider[] = ['sofascore', 'api_football', 'wikimedia'];

function parsePositiveInt(value: string | undefined, fallbackValue: number) {
  if (!value) {
    return fallbackValue;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallbackValue;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: false,
    help: false,
    limit: 25,
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length), options.limit);
      continue;
    }

    if (arg.startsWith('--player=')) {
      const value = arg.slice('--player='.length).trim();
      if (value) {
        options.playerId = value;
      }
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-player-photos.mts [options]

Options:
  --dry-run        Preview updates without writing to the database
  --limit=<n>      Limit number of players processed (default: 25)
  --player=<slug>  Sync a single player slug
  --help, -h       Show this help message

Environment:
  DATABASE_URL                         PostgreSQL connection string
  API_FOOTBALL_PHOTO_URL_TEMPLATE      Player photo template (default: https://media.api-sports.io/football/players/{id}.png)
  API_FOOTBALL_KEY                     Optional API-Football key for source validation requests
  SOFASCORE_PHOTO_URL_TEMPLATE         Player photo template (default: https://img.sofascore.com/api/v1/player/{id}/image)
  SOFASCORE_PLAYER_MAPPINGS_FILE       Optional JSON file with [{"playerSlug":"...","externalId":"..."}]
  PLAYER_PHOTO_MIRROR_BASE_URL         Optional proxy base URL. When set, final photo_url becomes <base>?source=<encoded>&provider=<provider>
`);
}

function getConfig(): SyncConfig {
  return {
    apiFootballPhotoUrlTemplate:
      process.env.API_FOOTBALL_PHOTO_URL_TEMPLATE?.trim()
      || 'https://media.api-sports.io/football/players/{id}.png',
    apiFootballKey: process.env.API_FOOTBALL_KEY?.trim() || undefined,
    mirrorBaseUrl: process.env.PLAYER_PHOTO_MIRROR_BASE_URL?.trim() || undefined,
    sofascorePhotoUrlTemplate:
      process.env.SOFASCORE_PHOTO_URL_TEMPLATE?.trim()
      || 'https://img.sofascore.com/api/v1/player/{id}/image',
  };
}

function resolveSofascoreMappingsPath() {
  const configuredPath = process.env.SOFASCORE_PLAYER_MAPPINGS_FILE?.trim();
  if (!configuredPath) {
    return path.join(process.cwd(), 'data', 'sofascore-player-mappings.json');
  }

  return path.isAbsolute(configuredPath) ? configuredPath : path.join(process.cwd(), configuredPath);
}

async function loadSofascoreMappings() {
  try {
    const payload = JSON.parse(await readFile(resolveSofascoreMappingsPath(), 'utf8')) as SofascorePlayerMapping[];
    return new Map(
      payload
        .filter((entry) => entry.playerSlug?.trim() && entry.externalId?.trim())
        .map((entry) => [entry.playerSlug.trim(), entry.externalId.trim()])
    );
  } catch {
    return new Map<string, string>();
  }
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    max: 1,
    idle_timeout: 5,
    prepare: false,
  });
}

function normalizeName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .toLowerCase();
}

function getPreferredSearchNames(target: SyncTarget) {
  const candidates = [target.name, `${target.firstName} ${target.lastName}`.trim()];
  return Array.from(new Set(candidates.map((value) => value.trim()).filter(Boolean)));
}

function buildMirroredUrl(sourceUrl: string, provider: PhotoSyncProvider, config: SyncConfig) {
  if (!config.mirrorBaseUrl) {
    return undefined;
  }

  const url = new URL(config.mirrorBaseUrl);
  url.searchParams.set('source', sourceUrl);
  url.searchParams.set('provider', provider);
  return url.toString();
}

async function probeImage(url: string, apiFootballKey?: string) {
  const headers = new Headers();

  if (apiFootballKey && url.includes('api-sports.io')) {
    headers.set('x-apisports-key', apiFootballKey);
  }

  let response = await fetch(url, {
    method: 'HEAD',
    headers,
    redirect: 'follow',
  });

  if (!response.ok || response.status === 405) {
    response = await fetch(url, {
      method: 'GET',
      headers,
      redirect: 'follow',
    });
  }

  if (!response.ok) {
    return {
      ok: false,
      error: `HTTP ${response.status}`,
    } as const;
  }

  const contentType = response.headers.get('content-type') ?? '';
  if (contentType && !contentType.startsWith('image/')) {
    return {
      ok: false,
      error: `Unsupported content-type: ${contentType}`,
    } as const;
  }

  return {
    ok: true,
    etag: response.headers.get('etag') ?? undefined,
    lastModified: response.headers.get('last-modified') ?? undefined,
  } as const;
}

async function resolveApiFootballCandidate(target: SyncTarget, config: SyncConfig): Promise<ProviderCandidate | null> {
  if (!target.apiFootballExternalId) {
    return null;
  }

  const sourceUrl = config.apiFootballPhotoUrlTemplate.replace('{id}', target.apiFootballExternalId);
  const probe = await probeImage(sourceUrl, config.apiFootballKey);
  if (!probe.ok) {
    return null;
  }

  return {
    provider: 'api_football',
    externalId: target.apiFootballExternalId,
    sourceUrl,
    mirroredUrl: buildMirroredUrl(sourceUrl, 'api_football', config),
    matchedBy: 'provider_id',
    etag: probe.etag,
    lastModified: probe.lastModified,
  };
}

async function resolveSofascoreCandidate(target: SyncTarget, config: SyncConfig): Promise<ProviderCandidate | null> {
  if (!target.sofascoreExternalId) {
    return null;
  }

  const sourceUrl = config.sofascorePhotoUrlTemplate.replace('{id}', target.sofascoreExternalId);
  const probe = await probeImage(sourceUrl);
  if (!probe.ok) {
    return null;
  }

  return {
    provider: 'sofascore',
    externalId: target.sofascoreExternalId,
    sourceUrl,
    mirroredUrl: buildMirroredUrl(sourceUrl, 'sofascore', config),
    matchedBy: 'provider_id',
    etag: probe.etag,
    lastModified: probe.lastModified,
  };
}

async function searchWikidata(name: string) {
  const url = new URL('https://www.wikidata.org/w/api.php');
  url.searchParams.set('action', 'wbsearchentities');
  url.searchParams.set('format', 'json');
  url.searchParams.set('language', 'en');
  url.searchParams.set('type', 'item');
  url.searchParams.set('limit', '5');
  url.searchParams.set('search', name);

  const response = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!response.ok) {
    return [] as Array<{ id: string; label?: string }>;
  }

  const payload = await response.json() as { search?: Array<{ id: string; label?: string }> };
  return payload.search ?? [];
}

async function fetchWikidataEntity(entityId: string) {
  const response = await fetch(`https://www.wikidata.org/wiki/Special:EntityData/${entityId}.json`, {
    headers: { Accept: 'application/json' },
  });

  if (!response.ok) {
    return null;
  }

  const payload = await response.json() as {
    entities?: Record<string, {
      labels?: Record<string, { value: string }>;
      claims?: Record<string, Array<{
        mainsnak?: {
          datavalue?: {
            value?: { time?: string } | string;
          };
        };
      }>>;
    }>;
  };

  return payload.entities?.[entityId] ?? null;
}

function getWikidataImageFileName(entity: Awaited<ReturnType<typeof fetchWikidataEntity>>) {
  const claim = entity?.claims?.P18?.[0];
  const value = claim?.mainsnak?.datavalue?.value;
  return typeof value === 'string' ? value : null;
}

function getWikidataBirthDate(entity: Awaited<ReturnType<typeof fetchWikidataEntity>>) {
  const claim = entity?.claims?.P569?.[0];
  const value = claim?.mainsnak?.datavalue?.value;

  if (!value || typeof value === 'string' || !('time' in value) || !value.time) {
    return null;
  }

  return value.time.replace(/^\+/, '').slice(0, 10);
}

function getWikidataLabel(entity: Awaited<ReturnType<typeof fetchWikidataEntity>>) {
  return entity?.labels?.en?.value ?? null;
}

async function resolveWikimediaCandidate(target: SyncTarget, config: SyncConfig): Promise<ProviderCandidate | null> {
  const normalizedTargetName = normalizeName(target.name);
  const targetDob = target.dateOfBirth?.slice(0, 10) ?? null;

  for (const searchName of getPreferredSearchNames(target)) {
    const searchResults = await searchWikidata(searchName);

    for (const result of searchResults) {
      const entity = await fetchWikidataEntity(result.id);
      const imageFileName = getWikidataImageFileName(entity);
      if (!imageFileName) {
        continue;
      }

      const label = getWikidataLabel(entity) ?? result.label ?? '';
      const labelScore = normalizeName(label) === normalizedTargetName ? 1 : 0.5;
      const birthDate = getWikidataBirthDate(entity);

      if (targetDob && birthDate && birthDate !== targetDob) {
        continue;
      }

      const sourceUrl = `https://commons.wikimedia.org/wiki/Special:FilePath/${encodeURIComponent(imageFileName)}`;
      const probe = await probeImage(sourceUrl);
      if (!probe.ok) {
        continue;
      }

      return {
        provider: 'wikimedia',
        externalId: result.id,
        sourceUrl,
        mirroredUrl: buildMirroredUrl(sourceUrl, 'wikimedia', config),
        matchedBy: targetDob ? 'exact_name_dob' : 'exact_name',
        matchScore: labelScore,
        etag: probe.etag,
        lastModified: probe.lastModified,
      };
    }
  }

  return null;
}

async function loadTargets(sql: postgres.Sql, options: CliOptions): Promise<SyncTarget[]> {
  const sofascoreMappings = await loadSofascoreMappings();
  const rows = await sql<SyncTargetRow[]>`
    SELECT
      p.slug AS id,
      COALESCE(pt.known_as, p.slug) AS name,
      COALESCE(pt.first_name, '') AS first_name,
      COALESCE(pt.last_name, '') AS last_name,
      p.date_of_birth::TEXT AS date_of_birth,
      COALESCE(ct.name, country.code_alpha3) AS nationality,
      p.photo_url,
      COALESCE(source_mapping.external_id, api_football_source.external_id) AS api_football_external_id
    FROM players p
    LEFT JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
    LEFT JOIN countries country ON country.id = p.country_id
    LEFT JOIN country_translations ct ON ct.country_id = country.id AND ct.locale = 'en'
    LEFT JOIN data_sources api_football_ds ON api_football_ds.slug = 'api_football'
    LEFT JOIN source_entity_mapping source_mapping
      ON source_mapping.entity_type = 'player'
      AND source_mapping.entity_id = p.id
      AND source_mapping.source_id = api_football_ds.id
    LEFT JOIN player_photo_sources api_football_source
      ON api_football_source.player_id = p.id
      AND api_football_source.data_source_id = api_football_ds.id
    WHERE p.is_active = TRUE
      AND (${options.playerId ?? null}::TEXT IS NULL OR p.slug = ${options.playerId ?? null})
    ORDER BY p.updated_at DESC, p.slug ASC
    LIMIT ${options.limit}
  `;

  return rows.map((row) => ({
    id: row.id,
    name: row.name,
    firstName: row.first_name,
    lastName: row.last_name,
      dateOfBirth: row.date_of_birth,
      nationality: row.nationality,
      photoUrl: row.photo_url,
      apiFootballExternalId: row.api_football_external_id,
      sofascoreExternalId: sofascoreMappings.get(row.id) ?? null,
    }));
}

async function ensureProviderRows(sql: postgres.Sql) {
  await sql`
    INSERT INTO data_sources (slug, name, base_url, source_kind, priority)
    VALUES
      ('api_football', 'API-Football v3', 'https://v3.football.api-sports.io', 'api', 2),
      ('sofascore', 'Sofascore CDN', 'https://img.sofascore.com', 'api', 1),
      ('wikimedia', 'Wikimedia Commons', 'https://commons.wikimedia.org', 'api', 3)
    ON CONFLICT (slug) DO NOTHING
  `;
}

async function upsertPhotoSource(sql: postgres.Sql, playerId: string, candidate: ProviderCandidate) {
  await sql`
    INSERT INTO player_photo_sources (
      player_id,
      data_source_id,
      external_id,
      source_url,
      mirrored_url,
      status,
      matched_by,
      match_score,
      etag,
      last_modified,
      last_checked_at,
      last_synced_at,
      failure_count,
      last_error,
      updated_at
    )
    VALUES (
      (SELECT id FROM players WHERE slug = ${playerId}),
      (SELECT id FROM data_sources WHERE slug = ${candidate.provider}),
      ${candidate.externalId ?? null},
      ${candidate.sourceUrl},
      ${candidate.mirroredUrl ?? null},
      'active',
      ${candidate.matchedBy},
      ${candidate.matchScore ?? null},
      ${candidate.etag ?? null},
      ${candidate.lastModified ?? null},
      NOW(),
      NOW(),
      0,
      NULL,
      NOW()
    )
    ON CONFLICT (player_id, data_source_id)
    DO UPDATE SET
      external_id = EXCLUDED.external_id,
      source_url = EXCLUDED.source_url,
      mirrored_url = EXCLUDED.mirrored_url,
      status = EXCLUDED.status,
      matched_by = EXCLUDED.matched_by,
      match_score = EXCLUDED.match_score,
      etag = EXCLUDED.etag,
      last_modified = EXCLUDED.last_modified,
      last_checked_at = EXCLUDED.last_checked_at,
      last_synced_at = EXCLUDED.last_synced_at,
      failure_count = 0,
      last_error = NULL,
      updated_at = NOW()
  `;
}

async function markProviderFailure(sql: postgres.Sql, playerId: string, provider: PhotoSyncProvider, error: string) {
  await sql`
    INSERT INTO player_photo_sources (
      player_id,
      data_source_id,
      source_url,
      status,
      last_checked_at,
      failure_count,
      last_error,
      updated_at
    )
    VALUES (
      (SELECT id FROM players WHERE slug = ${playerId}),
      (SELECT id FROM data_sources WHERE slug = ${provider}),
      ${`provider:${provider}`},
      'broken',
      NOW(),
      1,
      ${error},
      NOW()
    )
    ON CONFLICT (player_id, data_source_id)
    DO UPDATE SET
      status = 'broken',
      last_checked_at = NOW(),
      failure_count = player_photo_sources.failure_count + 1,
      last_error = EXCLUDED.last_error,
      updated_at = NOW()
  `;
}

async function updatePlayerPhotoUrl(sql: postgres.Sql, playerId: string, photoUrl: string) {
  await sql`
    UPDATE players
    SET photo_url = ${photoUrl}, updated_at = NOW()
    WHERE slug = ${playerId}
  `;
}

async function resolveCandidate(target: SyncTarget, config: SyncConfig) {
  const sofascoreCandidate = await resolveSofascoreCandidate(target, config);
  if (sofascoreCandidate) {
    return sofascoreCandidate;
  }

  const apiFootballCandidate = await resolveApiFootballCandidate(target, config);
  if (apiFootballCandidate) {
    return apiFootballCandidate;
  }

  return resolveWikimediaCandidate(target, config);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const config = getConfig();
  const sql = getSql();

  try {
    await ensureProviderRows(sql);

    const targets = await loadTargets(sql, options);
    console.log(`[player-photos] mode=${options.dryRun ? 'dry-run' : 'write'} limit=${options.limit} targets=${targets.length}`);

    for (const target of targets) {
      const candidate = await resolveCandidate(target, config);
      if (!candidate) {
        console.log(`[player-photos] skip ${target.id} no-provider-match`);
        if (!options.dryRun) {
          await markProviderFailure(sql, target.id, PROVIDER_PRIORITY[0], 'No provider match found');
        }
        continue;
      }

      const finalPhotoUrl = candidate.mirroredUrl ?? candidate.sourceUrl;
      console.log(
        `[player-photos] ${options.dryRun ? 'plan' : 'update'} ${target.id} provider=${candidate.provider} final=${finalPhotoUrl}`
      );

      if (options.dryRun) {
        continue;
      }

      await upsertPhotoSource(sql, target.id, candidate);
      await updatePlayerPhotoUrl(sql, target.id, finalPhotoUrl);
    }
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
