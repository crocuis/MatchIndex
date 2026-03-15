import postgres from 'postgres';
import { readFile } from 'node:fs/promises';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  aliases?: string[];
  batchFile?: string;
  dryRun: boolean;
  help: boolean;
  skipRefresh: boolean;
  surfaceOnly: boolean;
  teamsOnly: boolean;
}

interface EntityRow {
  id: number;
  slug: string;
}

interface MergePair {
  aliasSlug: string;
  canonicalSlug: string;
}

interface MergeResult {
  aliasSlug: string;
  canonicalSlug: string;
  merged: boolean;
  reason: string;
}

type DbSql = ReturnType<typeof getSql>;
type TxSql = Awaited<ReturnType<DbSql['reserve']>>;

const COMPETITION_MAPPINGS: MergePair[] = [
  { aliasSlug: 'bundesliga', canonicalSlug: '1-bundesliga' },
  { aliasSlug: 'laliga', canonicalSlug: 'la-liga' },
  { aliasSlug: 'ligue1', canonicalSlug: 'ligue-1' },
  { aliasSlug: 'pl', canonicalSlug: 'premier-league' },
  { aliasSlug: 'seriea', canonicalSlug: 'serie-a' },
];

const TEAM_MAPPINGS: MergePair[] = [
  { aliasSlug: 'ac-milan-italy', canonicalSlug: 'milan' },
  { aliasSlug: 'arsenal-england', canonicalSlug: 'arsenal' },
  { aliasSlug: 'as-roma-italy', canonicalSlug: 'roma' },
  { aliasSlug: 'barcelona-spain', canonicalSlug: 'barcelona' },
  { aliasSlug: 'bayer-leverkusen-germany', canonicalSlug: 'leverkusen' },
  { aliasSlug: 'bayern-munich-germany', canonicalSlug: 'bayern' },
  { aliasSlug: 'borussia-dortmund-germany', canonicalSlug: 'dortmund' },
  { aliasSlug: 'chelsea-england', canonicalSlug: 'chelsea' },
  { aliasSlug: 'juventus-italy', canonicalSlug: 'juventus' },
  { aliasSlug: 'krc-genk', canonicalSlug: 'genk' },
  { aliasSlug: 'liverpool-england', canonicalSlug: 'liverpool' },
  { aliasSlug: 'manchester-city-england', canonicalSlug: 'mancity' },
  { aliasSlug: 'napoli-italy', canonicalSlug: 'napoli' },
  { aliasSlug: 'paris-saint-germain-france', canonicalSlug: 'psg' },
  { aliasSlug: 'rb-leipzig-germany', canonicalSlug: 'leipzig' },
  { aliasSlug: 'real-madrid-spain', canonicalSlug: 'realmadrid' },
  { aliasSlug: 'sporting-braga', canonicalSlug: 'sc-braga' },
];

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { dryRun: false, help: false, skipRefresh: false, surfaceOnly: false, teamsOnly: false };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--teams-only') {
      options.teamsOnly = true;
      continue;
    }

    if (arg === '--surface-only') {
      options.surfaceOnly = true;
      continue;
    }

    if (arg.startsWith('--aliases=')) {
      options.aliases = arg.slice('--aliases='.length).split(',').map((value) => value.trim()).filter(Boolean);
      continue;
    }

    if (arg.startsWith('--batch-file=')) {
      options.batchFile = arg.slice('--batch-file='.length).trim() || undefined;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg === '--skip-refresh') {
      options.skipRefresh = true;
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/merge-duplicate-entities.mts [options]

Options:
  --dry-run    Print planned merges without writing to the database
  --teams-only  Skip competition merges and run team merges only
  --aliases=<slug,...>  Restrict merges to specific alias slugs
  --batch-file=<path>  Load explicit team merge pairs from JSON file
  --surface-only  Merge visible references only, archive alias team rows for deep event history
  --skip-refresh  Skip REFRESH MATERIALIZED VIEW steps after merge
  --help, -h   Show this help message
`);
}

function getArchivedTeamSlug(aliasId: number) {
  return `archived-team-${aliasId}`;
}

function filterMappings(mappings: MergePair[], options: CliOptions) {
  if (!options.aliases?.length) {
    return mappings;
  }

  const aliasSet = new Set(options.aliases);
  return mappings.filter((mapping) => aliasSet.has(mapping.aliasSlug));
}

async function loadTeamMappings(options: CliOptions) {
  if (!options.batchFile) {
    return filterMappings(TEAM_MAPPINGS, options);
  }

  const payload = JSON.parse(await readFile(options.batchFile, 'utf8')) as { mergeEntries?: MergePair[] };
  const fileMappings = (payload.mergeEntries ?? []).filter((entry): entry is MergePair => Boolean(entry?.aliasSlug && entry?.canonicalSlug));
  return filterMappings(fileMappings, options);
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

async function getEntityRow(sql: DbSql | TxSql, tableName: 'competitions' | 'teams', slug: string) {
  const rows = await sql<EntityRow[]>`
    SELECT id, slug
    FROM ${sql(tableName)}
    WHERE slug = ${slug}
  `;

  return rows[0];
}

async function copyCompetitionAliases(sql: DbSql | TxSql, canonicalId: number, aliasId: number, aliasSlug: string) {
  await sql`
    INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
    VALUES ('competition', ${canonicalId}, ${aliasSlug}, NULL, 'historical', FALSE, 'approved', 'historical_rule', 'merge-duplicate-entities')
    ON CONFLICT (entity_type, entity_id, alias_normalized)
    DO NOTHING
  `;

  await sql`
    INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
    SELECT 'competition', ${canonicalId}, ct.name, ct.locale, 'common', FALSE, 'pending', 'merge_derived', 'merge-duplicate-entities'
    FROM competition_translations ct
    WHERE ct.competition_id = ${aliasId}
    ON CONFLICT (entity_type, entity_id, alias_normalized)
    DO NOTHING
  `;
}

async function copyTeamAliases(sql: DbSql | TxSql, canonicalId: number, aliasId: number, aliasSlug: string) {
  await sql`
    INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
    VALUES ('team', ${canonicalId}, ${aliasSlug}, NULL, 'historical', FALSE, 'approved', 'historical_rule', 'merge-duplicate-entities')
    ON CONFLICT (entity_type, entity_id, alias_normalized)
    DO NOTHING
  `;

  await sql`
    INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
    SELECT 'team', ${canonicalId}, tt.name, tt.locale, 'common', FALSE, 'pending', 'merge_derived', 'merge-duplicate-entities'
    FROM team_translations tt
    WHERE tt.team_id = ${aliasId}
    ON CONFLICT (entity_type, entity_id, alias_normalized)
    DO NOTHING
  `;
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
  } catch (error) {
    throw error;
  } finally {
    tx.release();
  }
}

async function moveCompetitionTranslationCandidates(sql: DbSql | TxSql, canonicalId: number, aliasId: number) {
  await sql`
    DELETE FROM competition_translation_candidates alias_ctc
    USING competition_translation_candidates canonical_ctc
    WHERE alias_ctc.competition_id = ${aliasId}
      AND canonical_ctc.competition_id = ${canonicalId}
      AND alias_ctc.locale = canonical_ctc.locale
      AND alias_ctc.proposed_name_normalized = canonical_ctc.proposed_name_normalized
      AND alias_ctc.source_key = canonical_ctc.source_key
  `;

  await sql`
    UPDATE competition_translation_candidates
    SET competition_id = ${canonicalId},
        updated_at = NOW()
    WHERE competition_id = ${aliasId}
  `;
}

async function moveTeamTranslationCandidates(sql: DbSql | TxSql, canonicalId: number, aliasId: number) {
  await sql`
    DELETE FROM team_translation_candidates alias_ttc
    USING team_translation_candidates canonical_ttc
    WHERE alias_ttc.team_id = ${aliasId}
      AND canonical_ttc.team_id = ${canonicalId}
      AND alias_ttc.locale = canonical_ttc.locale
      AND alias_ttc.proposed_name_normalized = canonical_ttc.proposed_name_normalized
      AND alias_ttc.source_key = canonical_ttc.source_key
  `;

  await sql`
    UPDATE team_translation_candidates
    SET team_id = ${canonicalId},
        updated_at = NOW()
    WHERE team_id = ${aliasId}
  `;
}

async function mergeCompetition(sql: ReturnType<typeof getSql>, pair: MergePair, dryRun: boolean): Promise<MergeResult> {
  const canonical = await getEntityRow(sql, 'competitions', pair.canonicalSlug);
  const alias = await getEntityRow(sql, 'competitions', pair.aliasSlug);

  if (!canonical || !alias) {
    return {
      aliasSlug: pair.aliasSlug,
      canonicalSlug: pair.canonicalSlug,
      merged: false,
      reason: 'canonical or alias row missing',
    };
  }

  if (dryRun) {
    return {
      aliasSlug: pair.aliasSlug,
      canonicalSlug: pair.canonicalSlug,
      merged: true,
      reason: 'dry-run',
    };
  }

  await withTransaction(sql, async (tx) => {
    await tx`
      UPDATE competitions canonical
      SET
        code = COALESCE(canonical.code, alias.code),
        emblem_url = COALESCE(canonical.emblem_url, alias.emblem_url),
        tier = COALESCE(canonical.tier, alias.tier),
        is_active = canonical.is_active OR alias.is_active,
        updated_at = NOW()
      FROM competitions alias
      WHERE canonical.id = ${canonical.id}
        AND alias.id = ${alias.id}
    `;

    await tx`
      INSERT INTO competition_translations (competition_id, locale, name, short_name)
      SELECT ${canonical.id}, locale, name, short_name
      FROM competition_translations
      WHERE competition_id = ${alias.id}
      ON CONFLICT (competition_id, locale)
      DO UPDATE SET
        short_name = COALESCE(competition_translations.short_name, EXCLUDED.short_name)
    `;

    await moveCompetitionTranslationCandidates(tx, canonical.id, alias.id);

    await copyCompetitionAliases(tx, canonical.id, alias.id, alias.slug);

    await tx`
      INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata)
      SELECT entity_type, ${canonical.id}, source_id, external_id, external_code, season_context, metadata
      FROM source_entity_mapping
      WHERE entity_type = 'competition'
        AND entity_id = ${alias.id}
      ON CONFLICT (entity_type, source_id, external_id)
      DO UPDATE SET
        entity_id = EXCLUDED.entity_id,
        external_code = COALESCE(source_entity_mapping.external_code, EXCLUDED.external_code),
        season_context = COALESCE(source_entity_mapping.season_context, EXCLUDED.season_context),
        metadata = COALESCE(source_entity_mapping.metadata, EXCLUDED.metadata),
        updated_at = NOW()
    `;

    await tx`DELETE FROM source_entity_mapping WHERE entity_type = 'competition' AND entity_id = ${alias.id}`;
    await tx`DELETE FROM entity_aliases WHERE entity_type = 'competition' AND entity_id = ${alias.id}`;
    await tx`UPDATE competition_seasons SET competition_id = ${canonical.id}, updated_at = NOW() WHERE competition_id = ${alias.id}`;
    await tx`DELETE FROM competitions WHERE id = ${alias.id}`;
  });

  return {
    aliasSlug: pair.aliasSlug,
    canonicalSlug: pair.canonicalSlug,
    merged: true,
    reason: 'merged into canonical competition',
  };
}

async function mergeTeam(sql: ReturnType<typeof getSql>, pair: MergePair, options: CliOptions): Promise<MergeResult> {
  const canonical = await getEntityRow(sql, 'teams', pair.canonicalSlug);
  const alias = await getEntityRow(sql, 'teams', pair.aliasSlug);

  if (!canonical || !alias) {
    return {
      aliasSlug: pair.aliasSlug,
      canonicalSlug: pair.canonicalSlug,
      merged: false,
      reason: 'canonical or alias row missing',
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
    await tx`
      UPDATE teams canonical
      SET
        venue_id = COALESCE(canonical.venue_id, alias.venue_id),
        founded_year = COALESCE(canonical.founded_year, alias.founded_year),
        gender = COALESCE(canonical.gender, alias.gender),
        crest_url = COALESCE(canonical.crest_url, alias.crest_url),
        primary_color = COALESCE(canonical.primary_color, alias.primary_color),
        secondary_color = COALESCE(canonical.secondary_color, alias.secondary_color),
        is_active = canonical.is_active OR alias.is_active,
        updated_at = NOW()
      FROM teams alias
      WHERE canonical.id = ${canonical.id}
        AND alias.id = ${alias.id}
    `;

    await tx`
      INSERT INTO team_translations (team_id, locale, name, short_name)
      SELECT ${canonical.id}, locale, name, short_name
      FROM team_translations
      WHERE team_id = ${alias.id}
      ON CONFLICT (team_id, locale)
      DO UPDATE SET
        short_name = COALESCE(team_translations.short_name, EXCLUDED.short_name)
    `;

    await moveTeamTranslationCandidates(tx, canonical.id, alias.id);

    await copyTeamAliases(tx, canonical.id, alias.id, alias.slug);

    await tx`
      INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata)
      SELECT entity_type, ${canonical.id}, source_id, external_id, external_code, season_context, metadata
      FROM source_entity_mapping
      WHERE entity_type = 'team'
        AND entity_id = ${alias.id}
      ON CONFLICT (entity_type, source_id, external_id)
      DO UPDATE SET
        entity_id = EXCLUDED.entity_id,
        external_code = COALESCE(source_entity_mapping.external_code, EXCLUDED.external_code),
        season_context = COALESCE(source_entity_mapping.season_context, EXCLUDED.season_context),
        metadata = COALESCE(source_entity_mapping.metadata, EXCLUDED.metadata),
        updated_at = NOW()
    `;

    await tx`DELETE FROM source_entity_mapping WHERE entity_type = 'team' AND entity_id = ${alias.id}`;
    await tx`DELETE FROM entity_aliases WHERE entity_type = 'team' AND entity_id = ${alias.id}`;

    await tx`
      DELETE FROM team_seasons alias_ts
      USING team_seasons canonical_ts
      WHERE alias_ts.team_id = ${alias.id}
        AND canonical_ts.team_id = ${canonical.id}
        AND canonical_ts.competition_season_id = alias_ts.competition_season_id
    `;

    await tx`
      DELETE FROM player_contracts alias_pc
      USING player_contracts canonical_pc
      WHERE alias_pc.team_id = ${alias.id}
        AND canonical_pc.team_id = ${canonical.id}
        AND canonical_pc.player_id = alias_pc.player_id
        AND canonical_pc.competition_season_id = alias_pc.competition_season_id
    `;

    await tx`UPDATE competition_seasons SET winner_team_id = ${canonical.id}, updated_at = NOW() WHERE winner_team_id = ${alias.id}`;
    await tx`UPDATE team_seasons SET team_id = ${canonical.id}, updated_at = NOW() WHERE team_id = ${alias.id}`;
    await tx`UPDATE player_contracts SET team_id = ${canonical.id}, updated_at = NOW() WHERE team_id = ${alias.id}`;
    await tx`UPDATE matches SET home_team_id = ${canonical.id}, updated_at = NOW() WHERE home_team_id = ${alias.id}`;
    await tx`UPDATE matches SET away_team_id = ${canonical.id}, updated_at = NOW() WHERE away_team_id = ${alias.id}`;
    if (options.surfaceOnly) {
      await tx`DELETE FROM team_translations WHERE team_id = ${alias.id}`;
      await tx`UPDATE teams SET slug = ${getArchivedTeamSlug(alias.id)}, is_active = FALSE, updated_at = NOW() WHERE id = ${alias.id}`;
    } else {
      await tx`UPDATE match_lineups SET team_id = ${canonical.id} WHERE team_id = ${alias.id}`;
      await tx`UPDATE match_stats SET team_id = ${canonical.id} WHERE team_id = ${alias.id}`;
      await tx`DELETE FROM teams WHERE id = ${alias.id}`;
    }
  });

  return {
    aliasSlug: pair.aliasSlug,
    canonicalSlug: pair.canonicalSlug,
    merged: true,
    reason: 'merged into canonical team',
  };
}

async function resolveTeamPairs(sql: DbSql, pairs: MergePair[]) {
  const resolved: Array<{ alias: EntityRow; canonical: EntityRow; pair: MergePair }> = [];

  for (const pair of pairs) {
    const canonical = await getEntityRow(sql, 'teams', pair.canonicalSlug);
    const alias = await getEntityRow(sql, 'teams', pair.aliasSlug);
    if (!canonical || !alias) {
      continue;
    }

    resolved.push({ alias, canonical, pair });
  }

  return resolved;
}

async function mergeTeamsBulk(sql: DbSql, pairs: MergePair[], options: CliOptions): Promise<MergeResult[]> {
  const resolvedPairs = await resolveTeamPairs(sql, pairs);

  if (options.dryRun) {
    return pairs.map((pair) => ({
      aliasSlug: pair.aliasSlug,
      canonicalSlug: pair.canonicalSlug,
      merged: resolvedPairs.some((entry) => entry.pair.aliasSlug === pair.aliasSlug),
      reason: resolvedPairs.some((entry) => entry.pair.aliasSlug === pair.aliasSlug) ? 'dry-run' : 'canonical or alias row missing',
    }));
  }

  if (!resolvedPairs.length) {
    return pairs.map((pair) => ({
      aliasSlug: pair.aliasSlug,
      canonicalSlug: pair.canonicalSlug,
      merged: false,
      reason: 'canonical or alias row missing',
    }));
  }

  await withTransaction(sql, async (tx) => {
    for (const { alias, canonical } of resolvedPairs) {
      await tx`
        UPDATE teams canonical_team
        SET
          venue_id = COALESCE(canonical_team.venue_id, alias_team.venue_id),
          founded_year = COALESCE(canonical_team.founded_year, alias_team.founded_year),
          gender = COALESCE(canonical_team.gender, alias_team.gender),
          crest_url = COALESCE(canonical_team.crest_url, alias_team.crest_url),
          primary_color = COALESCE(canonical_team.primary_color, alias_team.primary_color),
          secondary_color = COALESCE(canonical_team.secondary_color, alias_team.secondary_color),
          is_active = canonical_team.is_active OR alias_team.is_active,
          updated_at = NOW()
        FROM teams alias_team
        WHERE canonical_team.id = ${canonical.id}
          AND alias_team.id = ${alias.id}
      `;

      await tx`
        INSERT INTO team_translations (team_id, locale, name, short_name)
        SELECT ${canonical.id}, locale, name, short_name
        FROM team_translations
        WHERE team_id = ${alias.id}
        ON CONFLICT (team_id, locale)
        DO UPDATE SET
          short_name = COALESCE(team_translations.short_name, EXCLUDED.short_name)
      `;

      await moveTeamTranslationCandidates(tx, canonical.id, alias.id);

      await copyTeamAliases(tx, canonical.id, alias.id, alias.slug);

      await tx`
        INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata)
        SELECT entity_type, ${canonical.id}, source_id, external_id, external_code, season_context, metadata
        FROM source_entity_mapping
        WHERE entity_type = 'team'
          AND entity_id = ${alias.id}
        ON CONFLICT (entity_type, source_id, external_id)
        DO UPDATE SET
          entity_id = EXCLUDED.entity_id,
          external_code = COALESCE(source_entity_mapping.external_code, EXCLUDED.external_code),
          season_context = COALESCE(source_entity_mapping.season_context, EXCLUDED.season_context),
          metadata = COALESCE(source_entity_mapping.metadata, EXCLUDED.metadata),
          updated_at = NOW()
      `;

      await tx`DELETE FROM source_entity_mapping WHERE entity_type = 'team' AND entity_id = ${alias.id}`;
      await tx`DELETE FROM entity_aliases WHERE entity_type = 'team' AND entity_id = ${alias.id}`;

      await tx`
        DELETE FROM team_seasons alias_ts
        USING team_seasons canonical_ts
        WHERE alias_ts.team_id = ${alias.id}
          AND canonical_ts.team_id = ${canonical.id}
          AND canonical_ts.competition_season_id = alias_ts.competition_season_id
      `;

      await tx`
        DELETE FROM player_contracts alias_pc
        USING player_contracts canonical_pc
        WHERE alias_pc.team_id = ${alias.id}
          AND canonical_pc.team_id = ${canonical.id}
          AND canonical_pc.player_id = alias_pc.player_id
          AND canonical_pc.competition_season_id = alias_pc.competition_season_id
      `;
    }

    const valuesSql = resolvedPairs.map(({ alias, canonical }) => `(${alias.id}, ${canonical.id})`).join(', ');

    await tx.unsafe(`
      UPDATE competition_seasons cs
      SET winner_team_id = mapping.canonical_id,
          updated_at = NOW()
      FROM (VALUES ${valuesSql}) AS mapping(alias_id, canonical_id)
      WHERE cs.winner_team_id = mapping.alias_id
    `);

    await tx.unsafe(`
      UPDATE team_seasons ts
      SET team_id = mapping.canonical_id,
          updated_at = NOW()
      FROM (VALUES ${valuesSql}) AS mapping(alias_id, canonical_id)
      WHERE ts.team_id = mapping.alias_id
    `);

    await tx.unsafe(`
      UPDATE player_contracts pc
      SET team_id = mapping.canonical_id,
          updated_at = NOW()
      FROM (VALUES ${valuesSql}) AS mapping(alias_id, canonical_id)
      WHERE pc.team_id = mapping.alias_id
    `);

    await tx.unsafe(`
      UPDATE matches m
      SET home_team_id = mapping.canonical_id,
          updated_at = NOW()
      FROM (VALUES ${valuesSql}) AS mapping(alias_id, canonical_id)
      WHERE m.home_team_id = mapping.alias_id
    `);

    await tx.unsafe(`
      UPDATE matches m
      SET away_team_id = mapping.canonical_id,
          updated_at = NOW()
      FROM (VALUES ${valuesSql}) AS mapping(alias_id, canonical_id)
      WHERE m.away_team_id = mapping.alias_id
    `);

    if (options.surfaceOnly) {
      for (const { alias } of resolvedPairs) {
        await tx`DELETE FROM team_translations WHERE team_id = ${alias.id}`;
        await tx`UPDATE teams SET slug = ${getArchivedTeamSlug(alias.id)}, is_active = FALSE, updated_at = NOW() WHERE id = ${alias.id}`;
      }
    } else {
      await tx.unsafe(`
        UPDATE match_lineups ml
        SET team_id = mapping.canonical_id
        FROM (VALUES ${valuesSql}) AS mapping(alias_id, canonical_id)
        WHERE ml.team_id = mapping.alias_id
      `);

      await tx.unsafe(`
        UPDATE match_stats ms
        SET team_id = mapping.canonical_id
        FROM (VALUES ${valuesSql}) AS mapping(alias_id, canonical_id)
        WHERE ms.team_id = mapping.alias_id
      `);

      for (const { alias } of resolvedPairs) {
        await tx`DELETE FROM teams WHERE id = ${alias.id}`;
      }
    }
  });

  return pairs.map((pair) => ({
    aliasSlug: pair.aliasSlug,
    canonicalSlug: pair.canonicalSlug,
    merged: resolvedPairs.some((entry) => entry.pair.aliasSlug === pair.aliasSlug),
    reason: resolvedPairs.some((entry) => entry.pair.aliasSlug === pair.aliasSlug)
      ? 'merged into canonical team'
      : 'canonical or alias row missing',
  }));
}

async function refreshMaterializedViews(sql: ReturnType<typeof getSql>, options: CliOptions) {
  if (options.dryRun || options.skipRefresh) {
    return;
  }

  await sql`REFRESH MATERIALIZED VIEW mv_team_form`;
  await sql`REFRESH MATERIALIZED VIEW mv_standings`;
  await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const sql = getSql();
  try {
    const competitionResults: MergeResult[] = [];
    if (!options.teamsOnly) {
      for (const pair of filterMappings(COMPETITION_MAPPINGS, options)) {
        console.log(`[competition] ${pair.aliasSlug} -> ${pair.canonicalSlug}`);
        competitionResults.push(await mergeCompetition(sql, pair, options.dryRun));
      }
    }

    const filteredTeamMappings = await loadTeamMappings(options);
    for (const pair of filteredTeamMappings) {
      console.log(`[team] ${pair.aliasSlug} -> ${pair.canonicalSlug}`);
    }
    const teamResults = filteredTeamMappings.length === 1
      ? [await mergeTeam(sql, filteredTeamMappings[0], options)]
      : await mergeTeamsBulk(sql, filteredTeamMappings, options);

    await refreshMaterializedViews(sql, options);

    console.log(JSON.stringify({
      dryRun: options.dryRun,
      skipRefresh: options.skipRefresh,
      surfaceOnly: options.surfaceOnly,
      competitionMergedCount: competitionResults.filter((result) => result.merged).length,
      teamMergedCount: teamResults.filter((result) => result.merged).length,
      competitionResults,
      teamResults,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
