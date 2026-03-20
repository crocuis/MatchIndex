DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type
        WHERE typname = 'competition_format_type'
    ) THEN
        CREATE TYPE competition_format_type AS ENUM ('regular_league', 'league_phase', 'group_knockout', 'knockout');
    END IF;
END $$;

ALTER TABLE competition_seasons
    ADD COLUMN IF NOT EXISTS format_type competition_format_type;

CREATE TABLE IF NOT EXISTS competition_format_stage_rules (
    format_type competition_format_type NOT NULL,
    stage_pattern TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (format_type, stage_pattern)
);

CREATE OR REPLACE FUNCTION normalize_match_stage_token(raw_stage TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NULLIF(
        REGEXP_REPLACE(
            REGEXP_REPLACE(UPPER(COALESCE(TRIM(raw_stage), '')), '[^A-Z0-9]+', '_', 'g'),
            '^_+|_+$',
            '',
            'g'
        ),
        ''
    )
$$;

CREATE OR REPLACE FUNCTION infer_competition_season_format(
    p_competition_slug TEXT,
    p_comp_type competition_type,
    p_season_start_date DATE
)
RETURNS competition_format_type
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF p_comp_type = 'league' THEN
        RETURN 'regular_league';
    END IF;

    IF p_competition_slug IN ('champions-league', 'europa-league') THEN
        IF p_season_start_date >= DATE '2024-07-01' THEN
            RETURN 'league_phase';
        END IF;

        RETURN 'group_knockout';
    END IF;

    IF p_competition_slug LIKE '%world-cup%'
        OR p_competition_slug LIKE '%euro%'
        OR p_competition_slug LIKE '%copa-america%'
        OR p_competition_slug LIKE '%african-cup-of-nations%'
    THEN
        RETURN 'group_knockout';
    END IF;

    RETURN 'knockout';
END;
$$;

CREATE OR REPLACE FUNCTION set_competition_season_format()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_competition_slug TEXT;
    v_comp_type competition_type;
    v_season_start_date DATE;
BEGIN
    IF NEW.format_type IS NOT NULL THEN
        RETURN NEW;
    END IF;

    SELECT c.slug, c.comp_type, s.start_date
    INTO v_competition_slug, v_comp_type, v_season_start_date
    FROM competitions c
    JOIN seasons s ON s.id = NEW.season_id
    WHERE c.id = NEW.competition_id;

    NEW.format_type := infer_competition_season_format(v_competition_slug, v_comp_type, v_season_start_date);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_set_competition_season_format ON competition_seasons;
CREATE TRIGGER trg_set_competition_season_format
BEFORE INSERT OR UPDATE OF competition_id, season_id, format_type
ON competition_seasons
FOR EACH ROW
EXECUTE FUNCTION set_competition_season_format();

INSERT INTO competition_format_stage_rules (format_type, stage_pattern, description)
VALUES
    ('regular_league', '^REGULAR_SEASON$', 'Standard league season'),
    ('regular_league', '^RELEGATION_ROUND$', 'Relegation round in split league formats'),
    ('regular_league', '^CHAMPIONSHIP(_.*)?$', 'Championship round or championship final in split leagues'),
    ('regular_league', '^PLAY_OFFS(_.*)?$', 'League playoffs'),
    ('regular_league', '^PLAYOFFS(_.*)?$', 'League playoffs spelling variant'),
    ('regular_league', '^FINAL$', 'League playoff final'),
    ('regular_league', '^APERTURA$', 'Apertura season phase'),
    ('regular_league', '^CLAUSURA$', 'Clausura season phase'),

    ('league_phase', '^LEAGUE_PHASE$', 'League phase'),
    ('league_phase', '^LEAGUE_STAGE$', 'League stage alias'),
    ('league_phase', '^GROUP_STAGE$', 'Legacy group stage'),
    ('league_phase', '^GROUP_[A-Z0-9]+$', 'Legacy group buckets'),
    ('league_phase', '^[0-9]+(ST|ND|RD|TH)_GROUP_STAGE$', 'Ordinal group stage label'),
    ('league_phase', '^KNOCKOUT_ROUND_PLAY_OFFS$', 'Knockout round playoffs'),
    ('league_phase', '^PLAY_OFFS(_.*)?$', 'Playoffs'),
    ('league_phase', '^PLAYOFFS(_.*)?$', 'Playoffs spelling variant'),
    ('league_phase', '^ROUND_OF_[0-9]+$', 'Numbered knockout round'),
    ('league_phase', '^LAST_[0-9]+$', 'Legacy last-N round'),
    ('league_phase', '^QUARTER_FINALS?$', 'Quarter-finals'),
    ('league_phase', '^SEMI_FINALS?$', 'Semi-finals'),
    ('league_phase', '^FINAL$', 'Final'),
    ('league_phase', '^3RD_PLACE_FINAL$', 'Third-place match'),
    ('league_phase', '^THIRD_PLACE_FINAL$', 'Third-place match'),
    ('league_phase', '^QUALIFICATION$', 'Qualification phase'),
    ('league_phase', '^[0-9]+(ST|ND|RD|TH)_QUALIFYING(_ROUND)?(_REPLAYS?)?$', 'Qualifying round'),
    ('league_phase', '^PRELIMINARY_ROUND(_REPLAYS?)?$', 'Preliminary round'),
    ('league_phase', '^EXTRA_PRELIMINARY_ROUND(_REPLAYS?)?$', 'Extra preliminary round'),
    ('league_phase', '^[0-9]+(ST|ND|RD|TH)_ROUND(_QUALIFYING)?(_REPLAYS?)?$', 'Numbered round'),

    ('group_knockout', '^GROUP_STAGE$', 'Group stage'),
    ('group_knockout', '^GROUP_[A-Z0-9]+$', 'Named group'),
    ('group_knockout', '^[0-9]+(ST|ND|RD|TH)_GROUP_STAGE$', 'Ordinal group stage'),
    ('group_knockout', '^ROUND_OF_[0-9]+$', 'Numbered knockout round'),
    ('group_knockout', '^LAST_[0-9]+$', 'Legacy last-N round'),
    ('group_knockout', '^QUARTER_FINALS?$', 'Quarter-finals'),
    ('group_knockout', '^SEMI_FINALS?$', 'Semi-finals'),
    ('group_knockout', '^FINAL$', 'Final'),
    ('group_knockout', '^3RD_PLACE_FINAL$', 'Third-place match'),
    ('group_knockout', '^THIRD_PLACE_FINAL$', 'Third-place match'),
    ('group_knockout', '^PLAY_OFFS(_.*)?$', 'Inter-stage playoffs'),
    ('group_knockout', '^PLAYOFFS(_.*)?$', 'Inter-stage playoffs spelling variant'),
    ('group_knockout', '^KNOCKOUT_ROUND_PLAY_OFFS$', 'Knockout round playoffs'),
    ('group_knockout', '^QUALIFICATION$', 'Qualification phase'),
    ('group_knockout', '^[0-9]+(ST|ND|RD|TH)_QUALIFYING(_ROUND)?(_REPLAYS?)?$', 'Qualifying round'),
    ('group_knockout', '^PRELIMINARY_ROUND(_REPLAYS?)?$', 'Preliminary round'),
    ('group_knockout', '^EXTRA_PRELIMINARY_ROUND(_REPLAYS?)?$', 'Extra preliminary round'),
    ('group_knockout', '^[0-9]+(ST|ND|RD|TH)_ROUND(_QUALIFYING)?(_REPLAYS?)?$', 'Numbered round'),

    ('knockout', '^KNOCKOUT_ROUND_PLAY_OFFS$', 'Knockout round playoffs'),
    ('knockout', '^PLAY_OFFS(_.*)?$', 'Playoffs'),
    ('knockout', '^PLAYOFFS(_.*)?$', 'Playoffs spelling variant'),
    ('knockout', '^ROUND_OF_[0-9]+$', 'Numbered knockout round'),
    ('knockout', '^LAST_[0-9]+$', 'Legacy last-N round'),
    ('knockout', '^QUARTER_FINALS?$', 'Quarter-finals'),
    ('knockout', '^SEMI_FINALS?$', 'Semi-finals'),
    ('knockout', '^FINAL$', 'Final'),
    ('knockout', '^3RD_PLACE_FINAL$', 'Third-place match'),
    ('knockout', '^THIRD_PLACE_FINAL$', 'Third-place match'),
    ('knockout', '^QUALIFICATION$', 'Qualification phase'),
    ('knockout', '^[0-9]+(ST|ND|RD|TH)_QUALIFYING(_ROUND)?(_REPLAYS?)?$', 'Qualifying round'),
    ('knockout', '^PRELIMINARY_ROUND(_REPLAYS?)?$', 'Preliminary round'),
    ('knockout', '^EXTRA_PRELIMINARY_ROUND(_REPLAYS?)?$', 'Extra preliminary round'),
    ('knockout', '^[0-9]+(ST|ND|RD|TH)_ROUND(_QUALIFYING)?(_REPLAYS?)?$', 'Numbered round')
ON CONFLICT (format_type, stage_pattern) DO NOTHING;

WITH stage_observations AS (
    SELECT
        cs.id AS competition_season_id,
        BOOL_OR(normalize_match_stage_token(m.stage) IN ('LEAGUE_PHASE', 'LEAGUE_STAGE')) AS has_league_phase,
        BOOL_OR(
            normalize_match_stage_token(m.stage) = 'GROUP_STAGE'
            OR normalize_match_stage_token(m.stage) ~ '^GROUP_[A-Z0-9]+$'
            OR normalize_match_stage_token(m.stage) ~ '^[0-9]+(ST|ND|RD|TH)_GROUP_STAGE$'
        ) AS has_group_stage
    FROM competition_seasons cs
    LEFT JOIN matches m ON m.competition_season_id = cs.id
    GROUP BY cs.id
)
UPDATE competition_seasons cs
SET format_type = CASE
    WHEN c.comp_type = 'league' THEN 'regular_league'::competition_format_type
    WHEN so.has_league_phase THEN 'league_phase'::competition_format_type
    WHEN so.has_group_stage THEN 'group_knockout'::competition_format_type
    ELSE infer_competition_season_format(c.slug, c.comp_type, s.start_date)
END,
updated_at = NOW()
FROM competitions c
JOIN seasons s ON s.id = cs.season_id
JOIN stage_observations so ON so.competition_season_id = cs.id
WHERE c.id = cs.competition_id
  AND cs.format_type IS DISTINCT FROM CASE
      WHEN c.comp_type = 'league' THEN 'regular_league'::competition_format_type
      WHEN so.has_league_phase THEN 'league_phase'::competition_format_type
      WHEN so.has_group_stage THEN 'group_knockout'::competition_format_type
      ELSE infer_competition_season_format(c.slug, c.comp_type, s.start_date)
  END;

UPDATE matches m
SET stage = 'REGULAR_SEASON',
    updated_at = NOW()
FROM competition_seasons cs
WHERE cs.id = m.competition_season_id
  AND cs.format_type = 'regular_league'
  AND normalize_match_stage_token(m.stage) IN ('REGULAR_SEASON', 'LEAGUE_PHASE', 'LEAGUE_STAGE');

UPDATE matches m
SET stage = 'LEAGUE_PHASE',
    updated_at = NOW()
FROM competition_seasons cs
WHERE cs.id = m.competition_season_id
  AND cs.format_type = 'league_phase'
  AND normalize_match_stage_token(m.stage) IN ('LEAGUE_PHASE', 'LEAGUE_STAGE');

UPDATE matches m
SET stage = 'GROUP_STAGE',
    updated_at = NOW()
FROM competition_seasons cs
WHERE cs.id = m.competition_season_id
  AND cs.format_type = 'group_knockout'
  AND normalize_match_stage_token(m.stage) = 'REGULAR_SEASON';

ALTER TABLE competition_seasons
    ALTER COLUMN format_type SET NOT NULL;

CREATE OR REPLACE FUNCTION validate_match_stage_against_competition_format()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_format_type competition_format_type;
    v_normalized_stage TEXT;
    v_is_allowed BOOLEAN;
BEGIN
    SELECT cs.format_type
    INTO v_format_type
    FROM competition_seasons cs
    WHERE cs.id = NEW.competition_season_id;

    IF v_format_type IS NULL THEN
        RAISE EXCEPTION 'competition_seasons.format_type is missing for competition_season_id=%', NEW.competition_season_id;
    END IF;

    v_normalized_stage := normalize_match_stage_token(NEW.stage);
    IF v_normalized_stage IS NULL THEN
        RAISE EXCEPTION 'match stage is required for competition_season_id=%', NEW.competition_season_id;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM competition_format_stage_rules rule
        WHERE rule.format_type = v_format_type
          AND v_normalized_stage ~ rule.stage_pattern
    ) INTO v_is_allowed;

    IF NOT v_is_allowed THEN
        RAISE EXCEPTION 'stage % is not allowed for format_type % (competition_season_id=%)', NEW.stage, v_format_type, NEW.competition_season_id;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_validate_match_stage_against_competition_format ON matches;
CREATE TRIGGER trg_validate_match_stage_against_competition_format
BEFORE INSERT OR UPDATE OF competition_season_id, stage
ON matches
FOR EACH ROW
EXECUTE FUNCTION validate_match_stage_against_competition_format();

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM matches m
        JOIN competition_seasons cs ON cs.id = m.competition_season_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM competition_format_stage_rules rule
            WHERE rule.format_type = cs.format_type
              AND normalize_match_stage_token(m.stage) ~ rule.stage_pattern
        )
    ) THEN
        RAISE EXCEPTION 'Found match rows with stage values that do not satisfy competition_seasons.format_type';
    END IF;
END $$;

REFRESH MATERIALIZED VIEW mv_team_form;
REFRESH MATERIALIZED VIEW mv_standings;
