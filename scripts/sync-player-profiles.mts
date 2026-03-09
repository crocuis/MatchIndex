import { readFile } from 'node:fs/promises';
import path from 'node:path';
import postgres, { type Sql } from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  dryRun: boolean;
  help: boolean;
  inputPath?: string;
  limit?: number;
  playerSlug?: string;
}

interface ProfileSyncPayload {
  provider: string;
  competition?: string;
  fetchedAt?: string;
  rows: ProfileSyncRow[];
  season?: string;
}

interface ProfileSyncRow {
  dateOfBirth?: string | null;
  heightCm?: number | string | null;
  playerName?: string | null;
  playerSlug?: string | null;
  preferredFoot?: string | null;
  raw?: unknown;
  sourceUrl?: string | null;
  weightKg?: number | string | null;
}

interface SourceRow { id: number; }
interface SyncRunRow { id: number; }

function parsePositiveInt(value: string | undefined) {
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { dryRun: false, help: false };
  for (const arg of argv) {
    if (arg === '--dry-run') { options.dryRun = true; continue; }
    if (arg === '--help' || arg === '-h') { options.help = true; continue; }
    if (arg.startsWith('--input=')) { options.inputPath = arg.slice('--input='.length).trim(); continue; }
    if (arg.startsWith('--player=')) { options.playerSlug = arg.slice('--player='.length).trim(); continue; }
    if (arg.startsWith('--limit=')) { options.limit = parsePositiveInt(arg.slice('--limit='.length)); }
  }
  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-player-profiles.mts --input=<path> [options]

Options:
  --input=<path>        Normalized JSON file produced by fetch-player-profiles-fbref.py
  --player=<slug>       Restrict sync to one internal player slug
  --limit=<n>           Limit rows read from the input payload
  --dry-run             Preview updates without writing to the database
  --help, -h            Show this help message

Environment:
  DATABASE_URL          PostgreSQL connection string
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) throw new Error('DATABASE_URL is not set');
  return postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });
}

function resolvePath(inputPath: string) {
  return path.isAbsolute(inputPath) ? inputPath : path.join(process.cwd(), inputPath);
}

async function readPayload(inputPath: string) {
  return JSON.parse(await readFile(resolvePath(inputPath), 'utf8')) as ProfileSyncPayload;
}

function parseDateValue(value: string | null | undefined) {
  if (!value) return undefined;
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;
  const parsed = new Date(trimmed);
  return Number.isNaN(parsed.getTime()) ? undefined : parsed.toISOString().slice(0, 10);
}

function parseIntegerValue(value: number | string | null | undefined) {
  if (typeof value === 'number' && Number.isFinite(value)) return Math.round(value);
  if (typeof value !== 'string') return undefined;
  const digits = value.replace(/[^0-9-]/g, '');
  if (!digits) return undefined;
  const parsed = Number.parseInt(digits, 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parsePreferredFoot(value: string | null | undefined): 'Left' | 'Right' | 'Both' | undefined {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) return undefined;
  if (normalized.startsWith('left')) return 'Left';
  if (normalized.startsWith('right')) return 'Right';
  if (normalized.startsWith('both') || normalized.startsWith('either')) return 'Both';
  return undefined;
}

async function ensureDataSource(sql: Sql, provider: string) {
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
    VALUES (${`${provider}_profile`}, ${provider === 'fbref' ? 'FBref Player Profiles' : provider}, ${provider === 'fbref' ? 'https://fbref.com' : null}, 'scraper', 'profile_sync', 3)
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

async function createSyncRun(sql: Sql, sourceId: number, metadata: Record<string, unknown>) {
  const rows = await sql<SyncRunRow[]>`
    INSERT INTO source_sync_runs (source_id, upstream_ref, status, metadata)
    VALUES (${sourceId}, 'player_profile', 'running', ${JSON.stringify(metadata)}::jsonb)
    RETURNING id
  `;
  return rows[0].id;
}

async function finishSyncRun(sql: Sql, syncRunId: number, status: 'completed' | 'failed', summary: Record<string, unknown>) {
  await sql`
    UPDATE source_sync_runs
    SET
      status = ${status},
      fetched_files = 1,
      changed_files = ${Number(summary.updated ?? 0)},
      completed_at = NOW(),
      metadata = COALESCE(metadata, '{}'::jsonb) || ${JSON.stringify(summary)}::jsonb
    WHERE id = ${syncRunId}
  `;
}

async function insertRawPayload(sql: Sql, sourceId: number, syncRunId: number, playerSlug: string, payload: unknown) {
  await sql`
    INSERT INTO raw_payloads (source_id, sync_run_id, endpoint, entity_type, external_id, http_status, payload)
    VALUES (${sourceId}, ${syncRunId}, 'player_profile', 'player', ${playerSlug}, 200, ${JSON.stringify(payload)}::jsonb)
  `;
}

async function updatePlayer(sql: Sql, row: ProfileSyncRow) {
  const playerSlug = row.playerSlug?.trim();
  if (!playerSlug) return false;
  const dateOfBirth = parseDateValue(row.dateOfBirth);
  const heightCm = parseIntegerValue(row.heightCm);
  const weightKg = parseIntegerValue(row.weightKg);
  const preferredFoot = parsePreferredFoot(row.preferredFoot);
  if (!dateOfBirth && heightCm === undefined && weightKg === undefined && !preferredFoot) return false;

  await sql`
    UPDATE players
    SET
      date_of_birth = COALESCE(date_of_birth, ${dateOfBirth ?? null}),
      height_cm = COALESCE(height_cm, ${heightCm ?? null}),
      weight_kg = COALESCE(weight_kg, ${weightKg ?? null}),
      preferred_foot = COALESCE(preferred_foot, ${preferredFoot ?? null}),
      updated_at = CASE
        WHEN (${dateOfBirth ?? null} IS NOT NULL AND date_of_birth IS NULL)
          OR (${heightCm ?? null} IS NOT NULL AND height_cm IS NULL)
          OR (${weightKg ?? null} IS NOT NULL AND weight_kg IS NULL)
          OR (${preferredFoot ?? null} IS NOT NULL AND preferred_foot IS NULL)
        THEN NOW()
        ELSE updated_at
      END
    WHERE slug = ${playerSlug}
  `;
  return true;
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  if (options.help) { printHelp(); return; }
  if (!options.inputPath) throw new Error('--input is required');

  const payload = await readPayload(options.inputPath);
  const rows = (options.limit ? payload.rows.slice(0, options.limit) : payload.rows)
    .filter((row) => !options.playerSlug || row.playerSlug === options.playerSlug);
  const sql = getSql();
  let syncRunId: number | null = null;
  try {
    const provider = payload.provider.trim().toLowerCase();
    const sourceId = options.dryRun ? null : await ensureDataSource(sql, provider);
    if (!options.dryRun && sourceId !== null) {
      syncRunId = await createSyncRun(sql, sourceId, {
        fetchedAt: payload.fetchedAt ?? null,
        inputPath: resolvePath(options.inputPath),
        provider,
        requestedCompetition: payload.competition ?? null,
        requestedSeason: payload.season ?? null,
      });
    }

    let updated = 0;
    let skipped = 0;
    for (const row of rows) {
      const hasValues = Boolean(parseDateValue(row.dateOfBirth) || parseIntegerValue(row.heightCm) || parseIntegerValue(row.weightKg) || parsePreferredFoot(row.preferredFoot));
      if (!hasValues || !row.playerSlug) { skipped += 1; continue; }
      if (!options.dryRun) {
        const changed = await updatePlayer(sql, row);
        if (changed && sourceId !== null && syncRunId !== null) {
          await insertRawPayload(sql, sourceId, syncRunId, row.playerSlug, row.raw ?? row);
        }
      }
      updated += 1;
    }

    const summary = { provider: payload.provider, skipped, updated };
    if (!options.dryRun && syncRunId !== null) {
      await finishSyncRun(sql, syncRunId, 'completed', summary);
    }
    console.log(JSON.stringify(summary, null, 2));
  } catch (error) {
    if (syncRunId !== null) {
      await finishSyncRun(sql, syncRunId, 'failed', { error: error instanceof Error ? error.message : String(error) });
    }
    throw error;
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
