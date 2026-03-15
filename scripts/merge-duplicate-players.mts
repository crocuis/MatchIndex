import { readFile } from 'node:fs/promises';
import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  aliasSlug?: string;
  batchFile?: string;
  canonicalSlug?: string;
  dryRun: boolean;
  help: boolean;
  skipRefresh: boolean;
}

interface MergePair {
  aliasSlug: string;
  canonicalSlug: string;
}

interface PlayerRow {
  id: number;
  slug: string;
}

interface MergeResult {
  aliasSlug: string;
  canonicalSlug: string;
  merged: boolean;
  reason: string;
}

type DbSql = ReturnType<typeof getSql>;
type TxSql = Awaited<ReturnType<DbSql['reserve']>>;

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: false,
    help: false,
    skipRefresh: false,
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }
    if (arg === '--skip-refresh') {
      options.skipRefresh = true;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--canonical=')) {
      options.canonicalSlug = arg.slice('--canonical='.length).trim() || undefined;
      continue;
    }
    if (arg.startsWith('--alias=')) {
      options.aliasSlug = arg.slice('--alias='.length).trim() || undefined;
      continue;
    }
    if (arg.startsWith('--batch-file=')) {
      options.batchFile = arg.slice('--batch-file='.length).trim() || undefined;
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/merge-duplicate-players.mts [options]

Options:
  --canonical=<slug>   Canonical player slug
  --alias=<slug>       Alias player slug to merge into canonical
  --batch-file=<path>  JSON file with { mergeEntries: [{ canonicalSlug, aliasSlug }] }
  --dry-run            Print merge plan without writing
  --skip-refresh       Skip REFRESH MATERIALIZED VIEW mv_top_scorers
  --help, -h           Show this help message
`);
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

async function withTransaction(sql: DbSql, callback: (tx: TxSql) => Promise<void>) {
  const tx = await sql.reserve();
  try {
    await tx`BEGIN`;
    try {
      await callback(tx);
      await tx`COMMIT`;
    } catch (error) {
      await tx`ROLLBACK`;
      throw error;
    }
  } finally {
    tx.release();
  }
}

async function getPlayerRow(sql: DbSql | TxSql, slug: string) {
  const rows = await sql<PlayerRow[]>`
    SELECT id, slug
    FROM players
    WHERE slug = ${slug}
    LIMIT 1
  `;
  return rows[0];
}

async function loadPairs(options: CliOptions): Promise<MergePair[]> {
  if (options.batchFile) {
    const payload = JSON.parse(await readFile(options.batchFile, 'utf8')) as { mergeEntries?: MergePair[] };
    return (payload.mergeEntries ?? []).filter((entry) => entry.aliasSlug && entry.canonicalSlug);
  }

  if (!options.aliasSlug || !options.canonicalSlug) {
    throw new Error('Either --batch-file or both --canonical and --alias are required');
  }

  return [{ aliasSlug: options.aliasSlug, canonicalSlug: options.canonicalSlug }];
}

async function copyPlayerAliases(sql: DbSql | TxSql, canonicalId: number, aliasId: number, aliasSlug: string) {
  await sql`
    INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
    VALUES ('player', ${canonicalId}, ${aliasSlug}, NULL, 'historical', FALSE, 'approved', 'historical_rule', 'merge-duplicate-players')
    ON CONFLICT (entity_type, entity_id, alias_normalized)
    DO NOTHING
  `;

  await sql`
    INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
    SELECT 'player', ${canonicalId}, pt.known_as, pt.locale, 'common', FALSE, 'pending', 'merge_derived', 'merge-duplicate-players'
    FROM player_translations pt
    WHERE pt.player_id = ${aliasId}
    ON CONFLICT (entity_type, entity_id, alias_normalized)
    DO NOTHING
  `;
}

async function movePlayerTranslations(sql: DbSql | TxSql, canonicalId: number, aliasId: number) {
  await sql`
    INSERT INTO player_translations (player_id, locale, first_name, last_name, known_as)
    SELECT ${canonicalId}, locale, first_name, last_name, known_as
    FROM player_translations
    WHERE player_id = ${aliasId}
    ON CONFLICT (player_id, locale)
    DO UPDATE SET
      first_name = COALESCE(player_translations.first_name, EXCLUDED.first_name),
      last_name = COALESCE(player_translations.last_name, EXCLUDED.last_name),
      known_as = COALESCE(player_translations.known_as, EXCLUDED.known_as)
  `;
}

async function moveSourceEntityMappings(sql: DbSql | TxSql, canonicalId: number, aliasId: number) {
  await sql`
    INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata)
    SELECT entity_type, ${canonicalId}, source_id, external_id, external_code, season_context, metadata
    FROM source_entity_mapping
    WHERE entity_type = 'player'
      AND entity_id = ${aliasId}
    ON CONFLICT (entity_type, source_id, external_id)
    DO UPDATE SET
      entity_id = EXCLUDED.entity_id,
      external_code = COALESCE(source_entity_mapping.external_code, EXCLUDED.external_code),
      season_context = COALESCE(source_entity_mapping.season_context, EXCLUDED.season_context),
      metadata = COALESCE(source_entity_mapping.metadata, EXCLUDED.metadata),
      updated_at = NOW()
  `;
}

async function mergePlayerSurface(sql: DbSql | TxSql, canonicalId: number, aliasId: number) {
  await sql`
    UPDATE players canonical
    SET
      date_of_birth = COALESCE(canonical.date_of_birth, alias.date_of_birth),
      country_id = COALESCE(canonical.country_id, alias.country_id),
      position = COALESCE(canonical.position, alias.position),
      height_cm = COALESCE(canonical.height_cm, alias.height_cm),
      weight_kg = COALESCE(canonical.weight_kg, alias.weight_kg),
      preferred_foot = COALESCE(canonical.preferred_foot, alias.preferred_foot),
      photo_url = COALESCE(canonical.photo_url, alias.photo_url),
      is_active = canonical.is_active OR alias.is_active,
      updated_at = NOW()
    FROM players alias
    WHERE canonical.id = ${canonicalId}
      AND alias.id = ${aliasId}
  `;
}

async function deleteUniqueConflicts(sql: DbSql | TxSql, canonicalId: number, aliasId: number) {
  await sql`
    DELETE FROM player_photo_sources alias_row
    USING player_photo_sources canonical_row
    WHERE alias_row.player_id = ${aliasId}
      AND canonical_row.player_id = ${canonicalId}
      AND canonical_row.data_source_id = alias_row.data_source_id
  `;

  await sql`
    DELETE FROM player_contracts alias_row
    USING player_contracts canonical_row
    WHERE alias_row.player_id = ${aliasId}
      AND canonical_row.player_id = ${canonicalId}
      AND canonical_row.competition_season_id = alias_row.competition_season_id
  `;

  await sql`
    DELETE FROM player_season_stats alias_row
    USING player_season_stats canonical_row
    WHERE alias_row.player_id = ${aliasId}
      AND canonical_row.player_id = ${canonicalId}
      AND canonical_row.competition_season_id = alias_row.competition_season_id
  `;

  await sql`
    DELETE FROM player_market_values alias_row
    USING player_market_values canonical_row
    WHERE alias_row.player_id = ${aliasId}
      AND canonical_row.player_id = ${canonicalId}
      AND canonical_row.source_id = alias_row.source_id
      AND canonical_row.observed_at = alias_row.observed_at
  `;

  await sql`
    DELETE FROM player_transfers alias_row
    USING player_transfers canonical_row
    WHERE alias_row.player_id = ${aliasId}
      AND canonical_row.player_id = ${canonicalId}
      AND canonical_row.source_id = alias_row.source_id
      AND canonical_row.external_transfer_id = alias_row.external_transfer_id
  `;

  await sql`
    DELETE FROM match_lineups alias_row
    USING match_lineups canonical_row
    WHERE alias_row.player_id = ${aliasId}
      AND canonical_row.player_id = ${canonicalId}
      AND canonical_row.match_id = alias_row.match_id
      AND canonical_row.match_date = alias_row.match_date
      AND canonical_row.team_id = alias_row.team_id
  `;
}

async function movePlayerReferences(sql: DbSql | TxSql, canonicalId: number, aliasId: number) {
  await sql`UPDATE player_photo_sources SET player_id = ${canonicalId}, updated_at = NOW() WHERE player_id = ${aliasId}`;
  await sql`UPDATE player_contracts SET player_id = ${canonicalId}, updated_at = NOW() WHERE player_id = ${aliasId}`;
  await sql`UPDATE player_season_stats SET player_id = ${canonicalId}, updated_at = NOW() WHERE player_id = ${aliasId}`;
  await sql`UPDATE player_market_values SET player_id = ${canonicalId}, updated_at = NOW() WHERE player_id = ${aliasId}`;
  await sql`UPDATE player_transfers SET player_id = ${canonicalId}, updated_at = NOW() WHERE player_id = ${aliasId}`;
  await sql`UPDATE match_lineups SET player_id = ${canonicalId} WHERE player_id = ${aliasId}`;
}

async function cleanupAliasPlayer(sql: DbSql | TxSql, aliasId: number) {
  await sql`DELETE FROM source_entity_mapping WHERE entity_type = 'player' AND entity_id = ${aliasId}`;
  await sql`DELETE FROM entity_aliases WHERE entity_type = 'player' AND entity_id = ${aliasId}`;
  await sql`DELETE FROM player_translations WHERE player_id = ${aliasId}`;
  await sql`DELETE FROM players WHERE id = ${aliasId}`;
}

async function refreshViews(sql: DbSql | TxSql) {
  await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;
}

async function mergePair(sql: DbSql, pair: MergePair, options: CliOptions): Promise<MergeResult> {
  const canonical = await getPlayerRow(sql, pair.canonicalSlug);
  const alias = await getPlayerRow(sql, pair.aliasSlug);

  if (!canonical || !alias) {
    return {
      aliasSlug: pair.aliasSlug,
      canonicalSlug: pair.canonicalSlug,
      merged: false,
      reason: 'canonical or alias player row missing',
    };
  }

  if (canonical.id === alias.id) {
    return {
      aliasSlug: pair.aliasSlug,
      canonicalSlug: pair.canonicalSlug,
      merged: false,
      reason: 'canonical and alias resolve to same player row',
    };
  }

  if (options.dryRun) {
    return {
      aliasSlug: pair.aliasSlug,
      canonicalSlug: pair.canonicalSlug,
      merged: true,
      reason: 'dry-run',
    };
  }

  await withTransaction(sql, async (tx) => {
    await mergePlayerSurface(tx, canonical.id, alias.id);
    await copyPlayerAliases(tx, canonical.id, alias.id, alias.slug);
    await movePlayerTranslations(tx, canonical.id, alias.id);
    await moveSourceEntityMappings(tx, canonical.id, alias.id);
    await deleteUniqueConflicts(tx, canonical.id, alias.id);
    await movePlayerReferences(tx, canonical.id, alias.id);
    await cleanupAliasPlayer(tx, alias.id);
    if (!options.skipRefresh) {
      await refreshViews(tx);
    }
  });

  return {
    aliasSlug: pair.aliasSlug,
    canonicalSlug: pair.canonicalSlug,
    merged: true,
    reason: 'merged',
  };
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const pairs = await loadPairs(options);
  const sql = getSql();
  try {
    const results: MergeResult[] = [];
    for (const pair of pairs) {
      results.push(await mergePair(sql, pair, options));
    }
    console.log(JSON.stringify({ results }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
