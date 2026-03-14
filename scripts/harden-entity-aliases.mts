import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

const REVIEWER = 'harden-entity-aliases';

const SEARCH_ENTITIES_FUNCTION_SQL = `
CREATE OR REPLACE FUNCTION search_entities(
    p_query TEXT,
    p_locale VARCHAR(10) DEFAULT NULL,
    p_entity_type entity_type DEFAULT NULL,
    p_limit INT DEFAULT 20
)
RETURNS TABLE (
    entity_type entity_type,
    entity_id BIGINT,
    matched_alias TEXT,
    match_type TEXT,
    score REAL
) LANGUAGE sql STABLE AS $$
    WITH canonical_terms AS (
        SELECT
            'competition'::entity_type AS entity_type,
            ct.competition_id AS entity_id,
            ct.name AS matched_alias,
            ct.locale,
            to_tsvector('simple', ct.name) AS search_vector
        FROM competition_translations ct
        WHERE ct.name IS NOT NULL

        UNION ALL

        SELECT
            'competition'::entity_type,
            ct.competition_id,
            ct.short_name,
            ct.locale,
            to_tsvector('simple', ct.short_name)
        FROM competition_translations ct
        WHERE ct.short_name IS NOT NULL

        UNION ALL

        SELECT
            'team'::entity_type,
            tt.team_id,
            tt.name,
            tt.locale,
            to_tsvector('simple', tt.name)
        FROM team_translations tt
        WHERE tt.name IS NOT NULL

        UNION ALL

        SELECT
            'team'::entity_type,
            tt.team_id,
            tt.short_name,
            tt.locale,
            to_tsvector('simple', tt.short_name)
        FROM team_translations tt
        WHERE tt.short_name IS NOT NULL

        UNION ALL

        SELECT
            'player'::entity_type,
            pt.player_id,
            pt.known_as,
            pt.locale,
            to_tsvector('simple', pt.known_as)
        FROM player_translations pt
        WHERE pt.known_as IS NOT NULL

        UNION ALL

        SELECT
            'country'::entity_type,
            ctr.country_id,
            ctr.name,
            ctr.locale,
            to_tsvector('simple', ctr.name)
        FROM country_translations ctr
        WHERE ctr.name IS NOT NULL
    ),
    approved_alias_terms AS (
        SELECT
            ea.entity_type,
            ea.entity_id,
            ea.alias AS matched_alias,
            ea.locale,
            ea.search_vector
        FROM entity_aliases ea
        WHERE ea.status = 'approved'
    ),
    ranked AS (
        SELECT
            ct.entity_type,
            ct.entity_id,
            ct.matched_alias,
            'exact'::TEXT AS match_type,
            1.2::REAL AS score,
            1 AS ord
        FROM canonical_terms ct
        WHERE lower(ct.matched_alias) = lower(p_query)
          AND (p_locale IS NULL OR ct.locale = p_locale OR ct.locale = 'en')
          AND (p_entity_type IS NULL OR ct.entity_type = p_entity_type)

        UNION ALL

        SELECT
            ea.entity_type,
            ea.entity_id,
            ea.matched_alias,
            'exact'::TEXT AS match_type,
            1.0::REAL AS score,
            2 AS ord
        FROM approved_alias_terms ea
        WHERE lower(ea.matched_alias) = lower(p_query)
          AND (p_locale IS NULL OR ea.locale = p_locale OR ea.locale = 'en' OR ea.locale IS NULL)
          AND (p_entity_type IS NULL OR ea.entity_type = p_entity_type)

        UNION ALL

        SELECT
            ct.entity_type,
            ct.entity_id,
            ct.matched_alias,
            'fts'::TEXT AS match_type,
            ts_rank(ct.search_vector, plainto_tsquery('simple', p_query))::REAL AS score,
            3 AS ord
        FROM canonical_terms ct
        WHERE ct.search_vector @@ plainto_tsquery('simple', p_query)
          AND lower(ct.matched_alias) <> lower(p_query)
          AND (p_locale IS NULL OR ct.locale = p_locale OR ct.locale = 'en')
          AND (p_entity_type IS NULL OR ct.entity_type = p_entity_type)

        UNION ALL

        SELECT
            ea.entity_type,
            ea.entity_id,
            ea.matched_alias,
            'fts'::TEXT AS match_type,
            ts_rank(ea.search_vector, plainto_tsquery('simple', p_query))::REAL AS score,
            4 AS ord
        FROM approved_alias_terms ea
        WHERE ea.search_vector @@ plainto_tsquery('simple', p_query)
          AND lower(ea.matched_alias) <> lower(p_query)
          AND (p_locale IS NULL OR ea.locale = p_locale OR ea.locale = 'en' OR ea.locale IS NULL)
          AND (p_entity_type IS NULL OR ea.entity_type = p_entity_type)

        UNION ALL

        SELECT
            ct.entity_type,
            ct.entity_id,
            ct.matched_alias,
            'fuzzy'::TEXT AS match_type,
            similarity(ct.matched_alias, p_query)::REAL AS score,
            5 AS ord
        FROM canonical_terms ct
        WHERE ct.matched_alias % p_query
          AND lower(ct.matched_alias) <> lower(p_query)
          AND (p_locale IS NULL OR ct.locale = p_locale OR ct.locale = 'en')
          AND (p_entity_type IS NULL OR ct.entity_type = p_entity_type)

        UNION ALL

        SELECT
            ea.entity_type,
            ea.entity_id,
            ea.matched_alias,
            'fuzzy'::TEXT AS match_type,
            similarity(ea.matched_alias, p_query)::REAL AS score,
            6 AS ord
        FROM approved_alias_terms ea
        WHERE ea.matched_alias % p_query
          AND lower(ea.matched_alias) <> lower(p_query)
          AND (p_locale IS NULL OR ea.locale = p_locale OR ea.locale = 'en' OR ea.locale IS NULL)
          AND (p_entity_type IS NULL OR ea.entity_type = p_entity_type)
    ),
    deduped AS (
        SELECT
            ranked.entity_type,
            ranked.entity_id,
            ranked.matched_alias,
            ranked.match_type,
            ranked.score,
            ranked.ord,
            ROW_NUMBER() OVER (
                PARTITION BY ranked.entity_type, ranked.entity_id
                ORDER BY ranked.ord ASC, ranked.score DESC, ranked.matched_alias ASC
            ) AS rn
        FROM ranked
    )
    SELECT
        deduped.entity_type,
        deduped.entity_id,
        deduped.matched_alias,
        deduped.match_type,
        deduped.score
    FROM deduped
    WHERE deduped.rn = 1
    ORDER BY deduped.ord ASC, deduped.score DESC
    LIMIT p_limit;
$$;
`;

async function main() {
  loadProjectEnv();

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });

  try {
    await sql`BEGIN`;

    try {
      await sql.unsafe(`
        ALTER TABLE entity_aliases
        ADD COLUMN IF NOT EXISTS status TEXT,
        ADD COLUMN IF NOT EXISTS source_type TEXT,
        ADD COLUMN IF NOT EXISTS source_ref TEXT,
        ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS reviewed_by TEXT
      `);

      await sql.unsafe(`
        UPDATE entity_aliases
        SET status = COALESCE(status, 'pending'),
            source_type = COALESCE(source_type, 'legacy')
        WHERE status IS NULL OR source_type IS NULL
      `);

      await sql.unsafe(`
        ALTER TABLE entity_aliases
        ALTER COLUMN status SET DEFAULT 'pending',
        ALTER COLUMN status SET NOT NULL,
        ALTER COLUMN source_type SET DEFAULT 'manual',
        ALTER COLUMN source_type SET NOT NULL
      `);

      await sql.unsafe(`ALTER TABLE entity_aliases DROP CONSTRAINT IF EXISTS entity_aliases_status_check`);
      await sql.unsafe(`ALTER TABLE entity_aliases DROP CONSTRAINT IF EXISTS entity_aliases_source_type_check`);
      await sql.unsafe(`
        ALTER TABLE entity_aliases
        ADD CONSTRAINT entity_aliases_status_check
          CHECK (status IN ('pending', 'approved', 'rejected', 'quarantined')),
        ADD CONSTRAINT entity_aliases_source_type_check
          CHECK (source_type IN ('manual', 'imported', 'merge_derived', 'historical_rule', 'machine_generated', 'legacy'))
      `);

      await sql.unsafe(`
        UPDATE entity_aliases
        SET status = 'approved',
            source_type = 'historical_rule',
            reviewed_at = NOW(),
            reviewed_by = '${REVIEWER}'
        WHERE alias_kind = 'historical'
          AND source_type = 'legacy'
          AND reviewed_at IS NULL
      `);

      await sql.unsafe(`
        UPDATE entity_aliases
        SET status = 'quarantined',
            reviewed_at = NOW(),
            reviewed_by = '${REVIEWER}'
        WHERE entity_type = 'team'
          AND locale = 'ko'
          AND alias_kind <> 'historical'
          AND source_type = 'legacy'
          AND reviewed_at IS NULL
      `);

      await sql.unsafe(`
        UPDATE entity_aliases ea
        SET status = 'quarantined',
            reviewed_at = NOW(),
            reviewed_by = '${REVIEWER}'
        WHERE ea.source_type = 'legacy'
          AND ea.reviewed_at IS NULL
          AND ea.alias_kind <> 'historical'
          AND (
            (ea.entity_type = 'competition' AND EXISTS (
              SELECT 1
              FROM competition_translations ct
              WHERE ct.competition_id = ea.entity_id
                AND ct.locale = COALESCE(ea.locale, 'en')
                AND lower(ct.name) = ea.alias_normalized
            ))
            OR (ea.entity_type = 'competition' AND EXISTS (
              SELECT 1
              FROM competition_translations ct
              WHERE ct.competition_id = ea.entity_id
                AND ct.locale = COALESCE(ea.locale, 'en')
                AND ct.short_name IS NOT NULL
                AND lower(ct.short_name) = ea.alias_normalized
            ))
            OR (ea.entity_type = 'team' AND EXISTS (
              SELECT 1
              FROM team_translations tt
              WHERE tt.team_id = ea.entity_id
                AND tt.locale = COALESCE(ea.locale, 'en')
                AND lower(tt.name) = ea.alias_normalized
            ))
            OR (ea.entity_type = 'team' AND EXISTS (
              SELECT 1
              FROM team_translations tt
              WHERE tt.team_id = ea.entity_id
                AND tt.locale = COALESCE(ea.locale, 'en')
                AND tt.short_name IS NOT NULL
                AND lower(tt.short_name) = ea.alias_normalized
            ))
            OR (ea.entity_type = 'player' AND EXISTS (
              SELECT 1
              FROM player_translations pt
              WHERE pt.player_id = ea.entity_id
                AND pt.locale = COALESCE(ea.locale, 'en')
                AND lower(pt.known_as) = ea.alias_normalized
            ))
            OR (ea.entity_type = 'country' AND EXISTS (
              SELECT 1
              FROM country_translations ctr
              WHERE ctr.country_id = ea.entity_id
                AND ctr.locale = COALESCE(ea.locale, 'en')
                AND lower(ctr.name) = ea.alias_normalized
            ))
          )
      `);

      await sql.unsafe(`CREATE INDEX IF NOT EXISTS idx_entity_aliases_status ON entity_aliases (status, entity_type, locale)`);
      await sql.unsafe(`CREATE INDEX IF NOT EXISTS idx_entity_aliases_approved_fts ON entity_aliases USING GIN (search_vector) WHERE status = 'approved'`);
      await sql.unsafe(`CREATE INDEX IF NOT EXISTS idx_entity_aliases_approved_trgm ON entity_aliases USING GIN (alias gin_trgm_ops) WHERE status = 'approved'`);
      await sql.unsafe(SEARCH_ENTITIES_FUNCTION_SQL);

      await sql`COMMIT`;
    } catch (error) {
      await sql`ROLLBACK`;
      throw error;
    }

    const counts = await sql<{ status: string; count: string }[]>`
      SELECT status, COUNT(*)::TEXT AS count
      FROM entity_aliases
      GROUP BY status
      ORDER BY status ASC
    `;

    const quarantined = await sql<{ entity_type: string; entity_id: string; alias: string; locale: string | null; alias_kind: string; source_type: string }[]>`
      SELECT entity_type::TEXT, entity_id::TEXT, alias, locale, alias_kind::TEXT, source_type
      FROM entity_aliases
      WHERE status = 'quarantined'
      ORDER BY entity_type ASC, locale ASC NULLS LAST, alias ASC
      LIMIT 20
    `;

    console.log(JSON.stringify({ ok: true, counts, quarantined }, null, 2));
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
