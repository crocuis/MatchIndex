DO $$
DECLARE
    canonical_id BIGINT;
    alias_slug TEXT;
    alias_id BIGINT;
BEGIN
    SELECT id INTO canonical_id
    FROM teams
    WHERE slug = 'liverpool-fc-england';

    IF canonical_id IS NULL THEN
        RETURN;
    END IF;

    FOREACH alias_slug IN ARRAY ARRAY['liverpool', 'liverpool-england'] LOOP
        SELECT id INTO alias_id
        FROM teams
        WHERE slug = alias_slug;

        IF alias_id IS NULL OR alias_id = canonical_id THEN
            CONTINUE;
        END IF;

        UPDATE teams AS canonical
        SET
            venue_id = COALESCE(canonical.venue_id, alias.venue_id),
            founded_year = COALESCE(canonical.founded_year, alias.founded_year),
            gender = COALESCE(canonical.gender, alias.gender),
            crest_url = COALESCE(canonical.crest_url, alias.crest_url),
            primary_color = COALESCE(canonical.primary_color, alias.primary_color),
            secondary_color = COALESCE(canonical.secondary_color, alias.secondary_color),
            is_active = canonical.is_active OR alias.is_active,
            updated_at = NOW()
        FROM teams AS alias
        WHERE canonical.id = canonical_id
          AND alias.id = alias_id;

        INSERT INTO team_translations (team_id, locale, name, short_name)
        SELECT canonical_id, locale, name, short_name
        FROM team_translations
        WHERE team_id = alias_id
        ON CONFLICT (team_id, locale)
        DO UPDATE SET
            short_name = COALESCE(team_translations.short_name, EXCLUDED.short_name);

        DELETE FROM team_translation_candidates AS alias_ttc
        USING team_translation_candidates AS canonical_ttc
        WHERE alias_ttc.team_id = alias_id
          AND canonical_ttc.team_id = canonical_id
          AND alias_ttc.locale = canonical_ttc.locale
          AND alias_ttc.proposed_name_normalized = canonical_ttc.proposed_name_normalized
          AND alias_ttc.source_key = canonical_ttc.source_key;

        UPDATE team_translation_candidates
        SET
            team_id = canonical_id,
            updated_at = NOW()
        WHERE team_id = alias_id;

        INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
        VALUES ('team', canonical_id, alias_slug, NULL, 'historical', FALSE, 'approved', 'historical_rule', '014_merge_liverpool_team_slugs')
        ON CONFLICT (entity_type, entity_id, alias_normalized)
        DO NOTHING;

        INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
        SELECT 'team', canonical_id, tt.name, tt.locale, 'common', FALSE, 'pending', 'merge_derived', '014_merge_liverpool_team_slugs'
        FROM team_translations AS tt
        WHERE tt.team_id = alias_id
        ON CONFLICT (entity_type, entity_id, alias_normalized)
        DO NOTHING;

        INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata)
        SELECT entity_type, canonical_id, source_id, external_id, external_code, season_context, metadata
        FROM source_entity_mapping
        WHERE entity_type = 'team'
          AND entity_id = alias_id
        ON CONFLICT (entity_type, source_id, external_id)
        DO UPDATE SET
            entity_id = EXCLUDED.entity_id,
            external_code = COALESCE(source_entity_mapping.external_code, EXCLUDED.external_code),
            season_context = COALESCE(source_entity_mapping.season_context, EXCLUDED.season_context),
            metadata = COALESCE(source_entity_mapping.metadata, EXCLUDED.metadata),
            updated_at = NOW();

        DELETE FROM source_entity_mapping
        WHERE entity_type = 'team'
          AND entity_id = alias_id;

        DELETE FROM entity_aliases
        WHERE entity_type = 'team'
          AND entity_id = alias_id;

        DELETE FROM team_seasons AS alias_ts
        USING team_seasons AS canonical_ts
        WHERE alias_ts.team_id = alias_id
          AND canonical_ts.team_id = canonical_id
          AND canonical_ts.competition_season_id = alias_ts.competition_season_id;

        DELETE FROM player_contracts AS alias_pc
        USING player_contracts AS canonical_pc
        WHERE alias_pc.team_id = alias_id
          AND canonical_pc.team_id = canonical_id
          AND canonical_pc.player_id = alias_pc.player_id
          AND canonical_pc.competition_season_id = alias_pc.competition_season_id;

        UPDATE competition_seasons
        SET
            winner_team_id = canonical_id,
            updated_at = NOW()
        WHERE winner_team_id = alias_id;

        UPDATE team_seasons
        SET
            team_id = canonical_id,
            updated_at = NOW()
        WHERE team_id = alias_id;

        UPDATE player_contracts
        SET
            team_id = canonical_id,
            updated_at = NOW()
        WHERE team_id = alias_id;

        UPDATE player_market_values
        SET
            club_id = canonical_id,
            updated_at = NOW()
        WHERE club_id = alias_id;

        UPDATE player_transfers
        SET
            from_team_id = canonical_id,
            updated_at = NOW()
        WHERE from_team_id = alias_id;

        UPDATE player_transfers
        SET
            to_team_id = canonical_id,
            updated_at = NOW()
        WHERE to_team_id = alias_id;

        UPDATE matches
        SET
            home_team_id = canonical_id,
            updated_at = NOW()
        WHERE home_team_id = alias_id;

        UPDATE matches
        SET
            away_team_id = canonical_id,
            updated_at = NOW()
        WHERE away_team_id = alias_id;

        IF to_regclass('public.match_events') IS NOT NULL THEN
            UPDATE match_events
            SET possession_team_id = canonical_id
            WHERE possession_team_id = alias_id;

            UPDATE match_events
            SET team_id = canonical_id
            WHERE team_id = alias_id;
        END IF;

        IF to_regclass('public.match_event_freeze_frames') IS NOT NULL THEN
            UPDATE match_event_freeze_frames
            SET team_id = canonical_id
            WHERE team_id = alias_id;
        END IF;

        IF to_regclass('public.match_lineups') IS NOT NULL THEN
            UPDATE match_lineups
            SET team_id = canonical_id
            WHERE team_id = alias_id;
        END IF;

        IF to_regclass('public.match_stats') IS NOT NULL THEN
            UPDATE match_stats
            SET team_id = canonical_id
            WHERE team_id = alias_id;
        END IF;

        DELETE FROM teams
        WHERE id = alias_id;
    END LOOP;
END $$;

REFRESH MATERIALIZED VIEW mv_team_form;
REFRESH MATERIALIZED VIEW mv_standings;
REFRESH MATERIALIZED VIEW mv_top_scorers;
