import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

const BATCH_SIZE = 500;

interface CliOptions {
  dryRun: boolean;
  help: boolean;
}

interface MissingTeamRow {
  id: number;
  slug: string;
  name: string;
  country_code: string;
  league_slug: string | null;
  league_name: string | null;
  season_label: string | null;
}

interface MappingDraft {
  entityId: number;
  externalId: string;
  metadata: string;
  seasonContext: string | null;
}

const MANUAL_SOURCE_SLUG = 'manual_team_backfill';

function parseArgs(argv: string[]): CliOptions {
  return {
    dryRun: !argv.includes('--write'),
    help: argv.includes('--help') || argv.includes('-h'),
  };
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/backfill-missing-team-mappings.mts [options]

Options:
  --write     Persist manual team mappings (default: dry-run)
  --help, -h  Show this help message
`);
}

function getSql() {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(databaseUrl, {
    max: 1,
    idle_timeout: 5,
    prepare: false,
  });
}

async function ensureManualSource(sql: postgres.Sql<{}>) {
  const rows = await sql<Array<{ id: number }>>`
    INSERT INTO data_sources (slug, name, source_kind, upstream_ref, priority)
    VALUES (${MANUAL_SOURCE_SLUG}, 'Manual Team Mapping Backfill', 'manual', 'source_entity_mapping', 99)
    ON CONFLICT (slug)
    DO UPDATE SET
      name = EXCLUDED.name,
      source_kind = EXCLUDED.source_kind,
      upstream_ref = EXCLUDED.upstream_ref,
      priority = EXCLUDED.priority
    RETURNING id
  `;

  const sourceId = rows[0]?.id;
  if (!sourceId) {
    throw new Error('Failed to ensure manual team mapping source');
  }

  return sourceId;
}

async function loadMissingTeams(sql: postgres.Sql<{}>) {
  return sql<MissingTeamRow[]>`
    WITH latest_team_season AS (
      SELECT DISTINCT ON (ts.team_id)
        ts.team_id,
        c.slug AS league_slug,
        COALESCE(ct.name, c.slug) AS league_name,
        CASE
          WHEN s.start_date IS NOT NULL AND s.end_date IS NOT NULL THEN
            CASE
              WHEN EXTRACT(YEAR FROM s.start_date) = EXTRACT(YEAR FROM s.end_date)
                THEN EXTRACT(YEAR FROM s.start_date)::INT::TEXT
              ELSE CONCAT(
                EXTRACT(YEAR FROM s.start_date)::INT::TEXT,
                '/',
                LPAD((EXTRACT(YEAR FROM s.end_date)::INT % 100)::TEXT, 2, '0')
              )
            END
          ELSE s.slug
        END AS season_label
      FROM team_seasons ts
      JOIN competition_seasons cs ON cs.id = ts.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      LEFT JOIN competition_translations ct ON ct.competition_id = c.id AND ct.locale = 'en'
      JOIN seasons s ON s.id = cs.season_id
      ORDER BY ts.team_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, cs.id DESC
    )
    SELECT
      t.id,
      t.slug,
      COALESCE(tt.name, t.slug) AS name,
      c.code_alpha3 AS country_code,
      lts.league_slug,
      lts.league_name,
      lts.season_label
    FROM teams t
    JOIN countries c ON c.id = t.country_id
    LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
    LEFT JOIN latest_team_season lts ON lts.team_id = t.id
    WHERE t.is_national = FALSE
      AND t.is_active = TRUE
      AND t.slug NOT LIKE 'archived-team-%'
      AND NOT EXISTS (
        SELECT 1
        FROM source_entity_mapping sem
        WHERE sem.entity_type = 'team'
          AND sem.entity_id = t.id
      )
    ORDER BY c.code_alpha3, COALESCE(lts.league_slug, ''), name
  `;
}

async function writeMappings(sql: postgres.Sql<{}>, sourceId: number, teams: MissingTeamRow[]) {
  const drafts: MappingDraft[] = teams.map((team) => {
    const metadata = {
      source: MANUAL_SOURCE_SLUG,
      strategy: 'canonical-slug-fallback',
      canonicalSlug: team.slug,
      teamName: team.name,
      countryCode: team.country_code,
      leagueSlug: team.league_slug,
      leagueName: team.league_name,
    } satisfies Record<string, string | null>;

    return {
      entityId: team.id,
      externalId: team.slug,
      metadata: JSON.stringify(metadata),
      seasonContext: team.season_label,
    };
  });

  for (let i = 0; i < drafts.length; i += BATCH_SIZE) {
    const chunk = drafts.slice(i, i + BATCH_SIZE);
    await sql`
      INSERT INTO source_entity_mapping (
        entity_type,
        entity_id,
        source_id,
        external_id,
        season_context,
        metadata,
        updated_at
      )
      SELECT
        'team',
        t.entity_id,
        ${sourceId},
        t.external_id,
        t.season_context,
        t.metadata::jsonb,
        NOW()
      FROM UNNEST(
        ${sql.array(chunk.map((draft) => draft.entityId))}::int[],
        ${sql.array(chunk.map((draft) => draft.externalId))}::text[],
        ${sql.array(chunk.map((draft) => draft.seasonContext))}::text[],
        ${sql.array(chunk.map((draft) => draft.metadata))}::text[]
      ) AS t(entity_id, external_id, season_context, metadata)
      ON CONFLICT (entity_type, source_id, external_id)
      DO UPDATE SET
        entity_id = EXCLUDED.entity_id,
        season_context = COALESCE(EXCLUDED.season_context, source_entity_mapping.season_context),
        metadata = COALESCE(source_entity_mapping.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        updated_at = NOW()
    `;
  }

  return drafts.length;
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
    const teams = await loadMissingTeams(sql);

    if (options.dryRun) {
      console.log(JSON.stringify({ dryRun: true, missingTeams: teams.length, sample: teams.slice(0, 20) }, null, 2));
      return;
    }

    await sql`BEGIN`;
    let sourceId = 0;
    try {
      sourceId = await ensureManualSource(sql);
      const inserted = await writeMappings(sql, sourceId, teams);
      await sql`COMMIT`;
      console.log(JSON.stringify({ dryRun: false, sourceId, inserted, teams: teams.length }, null, 2));
    } catch (error) {
      await sql`ROLLBACK`;
      throw error;
    }
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
