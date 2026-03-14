import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

async function main() {
  loadProjectEnv();

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });
  const canonicalSlug = 'psv-netherlands';
  const aliasSlug = 'psv-eindhoven';

  try {
    const canonicalRows = await sql<{ id: number }[]>`SELECT id FROM teams WHERE slug = ${canonicalSlug} LIMIT 1`;
    const aliasRows = await sql<{ id: number }[]>`SELECT id FROM teams WHERE slug = ${aliasSlug} LIMIT 1`;
    const canonicalId = canonicalRows[0]?.id;
    const aliasId = aliasRows[0]?.id;

    if (!canonicalId || !aliasId) {
      throw new Error('canonical or alias PSV team row is missing');
    }

    await sql`BEGIN`;

    try {
      await sql`
        UPDATE teams canonical
        SET
          country_id = COALESCE(canonical.country_id, alias.country_id),
          venue_id = COALESCE(canonical.venue_id, alias.venue_id),
          founded_year = COALESCE(canonical.founded_year, alias.founded_year),
          crest_url = COALESCE(canonical.crest_url, alias.crest_url),
          primary_color = COALESCE(canonical.primary_color, alias.primary_color),
          secondary_color = COALESCE(canonical.secondary_color, alias.secondary_color),
          is_active = canonical.is_active OR alias.is_active,
          updated_at = NOW()
        FROM teams alias
        WHERE canonical.id = ${canonicalId}
          AND alias.id = ${aliasId}
      `;

      await sql`
        INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
        VALUES ('team', ${canonicalId}, ${aliasSlug}, NULL, 'historical', FALSE, 'approved', 'historical_rule', 'fix-psv-canonical')
        ON CONFLICT (entity_type, entity_id, alias_normalized)
        DO NOTHING
      `;

      await sql`
        INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
        SELECT 'team', ${canonicalId}, tt.name, tt.locale, 'common', FALSE, 'pending', 'merge_derived', 'fix-psv-canonical'
        FROM team_translations tt
        WHERE tt.team_id = ${aliasId}
        ON CONFLICT (entity_type, entity_id, alias_normalized)
        DO NOTHING
      `;

      await sql`
        INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata)
        SELECT entity_type, ${canonicalId}, source_id, external_id, external_code, season_context, metadata
        FROM source_entity_mapping
        WHERE entity_type = 'team'
          AND entity_id = ${aliasId}
        ON CONFLICT (entity_type, source_id, external_id)
        DO UPDATE SET
          entity_id = EXCLUDED.entity_id,
          external_code = COALESCE(source_entity_mapping.external_code, EXCLUDED.external_code),
          season_context = COALESCE(source_entity_mapping.season_context, EXCLUDED.season_context),
          metadata = COALESCE(source_entity_mapping.metadata, EXCLUDED.metadata),
          updated_at = NOW()
      `;

      await sql`DELETE FROM source_entity_mapping WHERE entity_type = 'team' AND entity_id = ${aliasId}`;
      await sql`DELETE FROM entity_aliases WHERE entity_type = 'team' AND entity_id = ${aliasId}`;

      await sql`
        DELETE FROM team_seasons alias_ts
        USING team_seasons canonical_ts
        WHERE alias_ts.team_id = ${aliasId}
          AND canonical_ts.team_id = ${canonicalId}
          AND canonical_ts.competition_season_id = alias_ts.competition_season_id
      `;

      await sql`
        UPDATE team_seasons
        SET team_id = ${canonicalId}, updated_at = NOW()
        WHERE team_id = ${aliasId}
      `;

      await sql`
        DELETE FROM player_contracts alias_pc
        USING player_contracts canonical_pc
        WHERE alias_pc.team_id = ${aliasId}
          AND canonical_pc.team_id = ${canonicalId}
          AND canonical_pc.player_id = alias_pc.player_id
          AND canonical_pc.competition_season_id = alias_pc.competition_season_id
      `;

      await sql`
        UPDATE player_contracts
        SET team_id = ${canonicalId}, updated_at = NOW()
        WHERE team_id = ${aliasId}
      `;

      await sql`UPDATE competition_seasons SET winner_team_id = ${canonicalId}, updated_at = NOW() WHERE winner_team_id = ${aliasId}`;
      await sql`UPDATE matches SET home_team_id = ${canonicalId}, updated_at = NOW() WHERE home_team_id = ${aliasId}`;
      await sql`UPDATE matches SET away_team_id = ${canonicalId}, updated_at = NOW() WHERE away_team_id = ${aliasId}`;
      await sql`UPDATE match_events SET possession_team_id = ${canonicalId} WHERE possession_team_id = ${aliasId}`;
      await sql`UPDATE match_events SET team_id = ${canonicalId} WHERE team_id = ${aliasId}`;
      await sql`UPDATE match_event_freeze_frames SET team_id = ${canonicalId} WHERE team_id = ${aliasId}`;
      await sql`UPDATE match_lineups SET team_id = ${canonicalId} WHERE team_id = ${aliasId}`;
      await sql`UPDATE match_stats SET team_id = ${canonicalId} WHERE team_id = ${aliasId}`;

      await sql`DELETE FROM team_translations WHERE team_id = ${aliasId}`;
      await sql`DELETE FROM teams WHERE id = ${aliasId}`;

      await sql`REFRESH MATERIALIZED VIEW mv_team_form`;
      await sql`REFRESH MATERIALIZED VIEW mv_standings`;
      await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;

      await sql`COMMIT`;
      console.log(JSON.stringify({ ok: true, canonicalSlug, aliasSlug }, null, 2));
    } catch (error) {
      await sql`ROLLBACK`;
      throw error;
    }
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
