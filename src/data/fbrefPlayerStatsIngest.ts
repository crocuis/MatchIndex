import postgres, { type Sql } from 'postgres';

interface SourceRow {
  id: number;
}

interface SyncRunRow {
  id: number;
}

interface FbrefCompetitionTarget {
  code: string;
  pathId: string;
  statsLabel: string;
}

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

const DEFAULT_FBREF_SOURCE_SLUG = 'fbref_scrape';
const DEFAULT_FBREF_BASE_URL = 'https://fbref.com';
const DEFAULT_STAT_CATEGORIES = ['standard', 'shooting', 'passing', 'defense', 'possession'] as const;
const FBREF_COMPETITION_TARGETS: Record<string, FbrefCompetitionTarget> = {
  BL1: { code: 'BL1', pathId: '20', statsLabel: 'Bundesliga-Stats' },
  EL: { code: 'EL', pathId: '19', statsLabel: 'Europa-League-Stats' },
  FL1: { code: 'FL1', pathId: '13', statsLabel: 'Ligue-1-Stats' },
  PD: { code: 'PD', pathId: '12', statsLabel: 'La-Liga-Stats' },
  PL: { code: 'PL', pathId: '9', statsLabel: 'Premier-League-Stats' },
  SA: { code: 'SA', pathId: '11', statsLabel: 'Serie-A-Stats' },
  UCL: { code: 'UCL', pathId: '8', statsLabel: 'Champions-League-Stats' },
};

export interface IngestFbrefPlayerStatsOptions {
  competitionCode?: string;
  dryRun?: boolean;
  season?: string;
  sourceSlug?: string;
  statCategories?: string[];
}

export interface IngestFbrefPlayerStatsSummary {
  changedFiles: number;
  competitionCode: string | null;
  dryRun: boolean;
  failedEndpoints: Array<{ endpoint: string; error: string }>;
  fetchedFiles: number;
  implemented: boolean;
  nextStep: string;
  payloadCount: number;
  plannedEndpoints: string[];
  season: string | null;
  sourceId: number | null;
  sourceSlug: string;
  statCategories: string[];
  syncRunId: number | null;
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

function normalizeStatCategories(input?: string[]) {
  if (!input || input.length === 0) {
    return [...DEFAULT_STAT_CATEGORIES];
  }

  return Array.from(new Set(input.map((category) => category.trim().toLowerCase()).filter(Boolean)));
}

function toJsonValue(value: unknown): JsonValue {
  return JSON.parse(JSON.stringify(value)) as JsonValue;
}

function buildPayloadHash(payload: unknown) {
  return JSON.stringify(payload);
}

function resolveCompetitionTarget(code: string | null) {
  if (!code) {
    return null;
  }

  return FBREF_COMPETITION_TARGETS[code] ?? null;
}

function buildFbrefPlayersPath(target: FbrefCompetitionTarget, season: string, statCategory: string) {
  return `fbref:/comps/${target.pathId}/${season}/${statCategory}/players`;
}

function buildFbrefPlayersUrl(target: FbrefCompetitionTarget, season: string, statCategory: string) {
  const path = statCategory === 'standard' ? 'stats' : statCategory;
  return `${DEFAULT_FBREF_BASE_URL}/en/comps/${target.pathId}/${season}/${path}/players/${season}-${target.statsLabel}`;
}

async function ensureFbrefSource(sql: Sql, sourceSlug: string) {
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, priority)
    VALUES (${sourceSlug}, 'FBref Scrape', ${DEFAULT_FBREF_BASE_URL}, 'scrape', 4)
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

async function createSyncRun(sql: Sql, sourceId: number, summary: Pick<IngestFbrefPlayerStatsSummary, 'competitionCode' | 'season' | 'statCategories'>) {
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
      'fbref-html',
      NULL,
      'running',
      ${JSON.stringify({
        mode: 'player_stats_ingest',
        source: 'fbref_scrape',
        competitionCode: summary.competitionCode,
        season: summary.season,
        statCategories: summary.statCategories,
      })}::jsonb
    )
    RETURNING id
  `;

  return rows[0].id;
}

async function updateSyncRun(sql: Sql, syncRunId: number, status: 'completed' | 'failed', summary: IngestFbrefPlayerStatsSummary) {
  await sql`
    UPDATE source_sync_runs
    SET
      status = ${status},
      fetched_files = 0,
      changed_files = 0,
      completed_at = NOW(),
      metadata = COALESCE(metadata, '{}'::jsonb) || ${JSON.stringify(summary)}::jsonb
    WHERE id = ${syncRunId}
  `;
}

async function insertRawPayload(
  sql: Sql,
  params: {
    endpoint: string;
    payload: unknown;
    seasonContext: string;
    sourceId: number;
    syncRunId: number;
  },
) {
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
      ${params.endpoint},
      ${params.seasonContext},
      200,
      ${sql.json(toJsonValue(params.payload))},
      ${buildPayloadHash(params.payload)}
    )
  `;
}

async function upsertManifest(
  sql: Sql,
  params: {
    competitionCode: string;
    endpoint: string;
    sourceId: number;
    season: string;
    sourceUrl: string;
    statCategory: string;
    syncRunId: number;
  },
) {
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
      ${params.endpoint},
      ${params.season},
      ${params.competitionCode},
      NULL,
      NOW(),
      ${sql.json(toJsonValue({
        competitionCode: params.competitionCode,
        season: params.season,
        sourceUrl: params.sourceUrl,
        statCategory: params.statCategory,
      }))}
    )
    ON CONFLICT (source_id, manifest_type, upstream_path)
    DO UPDATE SET
      sync_run_id = EXCLUDED.sync_run_id,
      external_id = EXCLUDED.external_id,
      external_parent_id = EXCLUDED.external_parent_id,
      source_available_at = EXCLUDED.source_available_at,
      metadata = EXCLUDED.metadata
  `;
}

async function fetchFbrefPage(url: string) {
  const response = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      Accept: 'text/html,application/xhtml+xml',
      'Accept-Language': 'en-US,en;q=0.9',
    },
  });

  if (!response.ok) {
    throw new Error(`FBref request failed: ${url} (${response.status})`);
  }

  return response.text();
}

export async function ingestFbrefPlayerStats(
  options: IngestFbrefPlayerStatsOptions = {},
): Promise<IngestFbrefPlayerStatsSummary> {
  const competitionCode = options.competitionCode?.trim().toUpperCase() || null;
  const season = options.season?.trim() || null;
  const statCategories = normalizeStatCategories(options.statCategories);
  const sourceSlug = options.sourceSlug?.trim() || DEFAULT_FBREF_SOURCE_SLUG;
  const target = resolveCompetitionTarget(competitionCode);
  const plannedEndpoints = target && season
    ? statCategories.map((category) => buildFbrefPlayersPath(target, season, category))
    : [];

  const summary: IngestFbrefPlayerStatsSummary = {
    changedFiles: 0,
    competitionCode,
    dryRun: options.dryRun ?? true,
    failedEndpoints: [],
    fetchedFiles: 0,
    implemented: false,
    nextStep: target && season
      ? 'Implement FBref HTML parsing/materialization on top of the stored raw payloads.'
      : 'Provide supported competitionCode and season to generate FBref ingest targets.',
    payloadCount: 0,
    plannedEndpoints,
    season,
    sourceId: null,
    sourceSlug,
    statCategories,
    syncRunId: null,
  };

  if (!target || !season) {
    return summary;
  }

  if (summary.dryRun) {
    return summary;
  }

  const sql = getIngestDb();

  try {
    const sourceId = await ensureFbrefSource(sql, sourceSlug);
    const syncRunId = await createSyncRun(sql, sourceId, summary);
    let fetchedFiles = 0;
    const failedEndpoints: IngestFbrefPlayerStatsSummary['failedEndpoints'] = [];

    for (const statCategory of statCategories) {
      const endpoint = buildFbrefPlayersPath(target, season, statCategory);
      const sourceUrl = buildFbrefPlayersUrl(target, season, statCategory);
      let html: string;

      try {
        html = await fetchFbrefPage(sourceUrl);
      } catch (error) {
        failedEndpoints.push({
          endpoint,
          error: error instanceof Error ? error.message : String(error),
        });
        break;
      }

      const payload = {
        source: 'fbref',
        competitionCode: target.code,
        season,
        statCategory,
        pageUrl: sourceUrl,
        fetchedAt: new Date().toISOString(),
        html,
      };

      await insertRawPayload(sql, {
        endpoint,
        payload,
        seasonContext: season,
        sourceId,
        syncRunId,
      });
      await upsertManifest(sql, {
        competitionCode: target.code,
        endpoint,
        sourceId,
        season,
        sourceUrl,
        statCategory,
        syncRunId,
      });
      fetchedFiles += 1;
    }

    const completedSummary: IngestFbrefPlayerStatsSummary = {
      ...summary,
      changedFiles: fetchedFiles,
      failedEndpoints,
      fetchedFiles,
      payloadCount: fetchedFiles,
      implemented: failedEndpoints.length === 0,
      sourceId,
      syncRunId,
    };

    await updateSyncRun(sql, syncRunId, failedEndpoints.length === 0 ? 'completed' : 'failed', completedSummary);

    if (failedEndpoints.length > 0) {
      throw new Error(failedEndpoints.map((item) => `${item.endpoint}: ${item.error}`).join('; '));
    }

    return completedSummary;
  } finally {
    await sql.end({ timeout: 5 });
  }
}
