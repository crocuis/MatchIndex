-- MatchIndex PostgreSQL schema
-- MVP-first canonical model for football data ingestion and serving.

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TYPE position_type AS ENUM ('GK', 'DEF', 'MID', 'FWD');
CREATE TYPE preferred_foot AS ENUM ('Left', 'Right', 'Both');
CREATE TYPE competition_gender AS ENUM ('male', 'female', 'mixed');
CREATE TYPE competition_type AS ENUM ('league', 'cup', 'league_cup', 'super_cup', 'international');
CREATE TYPE competition_format_type AS ENUM ('regular_league', 'league_phase', 'group_knockout', 'knockout');
CREATE TYPE match_status AS ENUM (
    'scheduled',
    'timed',
    'live_1h',
    'live_ht',
    'live_2h',
    'live_et',
    'live_pen',
    'finished',
    'finished_aet',
    'finished_pen',
    'postponed',
    'suspended',
    'cancelled',
    'awarded'
);
CREATE TYPE match_event_type AS ENUM (
    'pass',
    'shot',
    'carry',
    'pressure',
    'ball_receipt',
    'clearance',
    'interception',
    'block',
    'ball_recovery',
    'foul_won',
    'foul_committed',
    'duel',
    'miscontrol',
    'goalkeeper',
    'offside',
    'dribble',
    'dispossessed',
    'goal',
    'own_goal',
    'penalty_scored',
    'penalty_missed',
    'yellow_card',
    'red_card',
    'yellow_red_card',
    'substitution',
    'var_decision'
);
CREATE TYPE alias_type AS ENUM ('official', 'common', 'abbreviation', 'historical', 'transliteration');
CREATE TYPE entity_type AS ENUM ('competition', 'team', 'player', 'country', 'venue', 'coach', 'match');
CREATE TYPE ingestion_status AS ENUM ('running', 'completed', 'failed');
CREATE TYPE refresh_scope AS ENUM ('master', 'season_current', 'season_finished', 'matchday_hot', 'matchday_warm');

-- Reference

CREATE TABLE locales (
    code VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    fallback_to VARCHAR(10) REFERENCES locales(code),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_locales_single_default
    ON locales (is_default)
    WHERE is_default = TRUE;

CREATE TABLE data_sources (
    id BIGSERIAL PRIMARY KEY,
    slug VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    base_url TEXT,
    source_kind VARCHAR(30) NOT NULL DEFAULT 'api',
    upstream_ref VARCHAR(255),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    priority SMALLINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Master entity data

CREATE TABLE countries (
    id BIGSERIAL PRIMARY KEY,
    code_alpha2 CHAR(2) UNIQUE,
    code_alpha3 CHAR(3) NOT NULL UNIQUE,
    confederation VARCHAR(20),
    fifa_ranking SMALLINT,
    fifa_ranking_women SMALLINT,
    flag_url TEXT,
    crest_url TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE seasons (
    id BIGSERIAL PRIMARY KEY,
    slug VARCHAR(20) NOT NULL UNIQUE,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_season_dates CHECK (end_date > start_date)
);

CREATE TABLE ranking_history (
    id BIGSERIAL PRIMARY KEY,
    country_id BIGINT NOT NULL REFERENCES countries(id) ON DELETE CASCADE,
    ranking_date DATE NOT NULL,
    ranking_category VARCHAR(10) NOT NULL DEFAULT 'men',
    fifa_ranking SMALLINT NOT NULL,
    source VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ranking_history_country_date UNIQUE (country_id, ranking_date, ranking_category)
);

CREATE INDEX idx_ranking_history_country_date ON ranking_history (country_id, ranking_date DESC);

CREATE UNIQUE INDEX idx_seasons_single_current
    ON seasons (is_current)
    WHERE is_current = TRUE;

CREATE TABLE competitions (
    id BIGSERIAL PRIMARY KEY,
    slug VARCHAR(100) NOT NULL UNIQUE,
    code VARCHAR(10) UNIQUE,
    comp_type competition_type NOT NULL DEFAULT 'league',
    gender competition_gender NOT NULL DEFAULT 'male',
    is_youth BOOLEAN NOT NULL DEFAULT FALSE,
    is_international BOOLEAN NOT NULL DEFAULT FALSE,
    country_id BIGINT REFERENCES countries(id),
    emblem_url TEXT,
    tier SMALLINT DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE venues (
    id BIGSERIAL PRIMARY KEY,
    slug VARCHAR(100) NOT NULL UNIQUE,
    city VARCHAR(100),
    country_id BIGINT REFERENCES countries(id),
    capacity INTEGER,
    surface VARCHAR(50),
    image_url TEXT,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE teams (
    id BIGSERIAL PRIMARY KEY,
    slug VARCHAR(100) NOT NULL UNIQUE,
    country_id BIGINT NOT NULL REFERENCES countries(id),
    venue_id BIGINT REFERENCES venues(id),
    founded_year SMALLINT,
    gender competition_gender,
    is_national BOOLEAN NOT NULL DEFAULT FALSE,
    crest_url TEXT,
    primary_color CHAR(7),
    secondary_color CHAR(7),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE players (
    id BIGSERIAL PRIMARY KEY,
    slug VARCHAR(150) NOT NULL UNIQUE,
    date_of_birth DATE,
    country_id BIGINT REFERENCES countries(id),
    position position_type,
    height_cm SMALLINT,
    weight_kg SMALLINT,
    preferred_foot preferred_foot,
    photo_url TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE player_photo_sources (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    data_source_id BIGINT NOT NULL REFERENCES data_sources(id) ON DELETE RESTRICT,
    external_id TEXT,
    source_url TEXT,
    mirrored_url TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    matched_by VARCHAR(30),
    match_score REAL,
    etag TEXT,
    last_modified TEXT,
    last_checked_at TIMESTAMPTZ,
    last_synced_at TIMESTAMPTZ,
    failure_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_player_photo_sources_player_source UNIQUE (player_id, data_source_id),
    CONSTRAINT chk_player_photo_sources_status CHECK (status IN ('active', 'broken', 'pending', 'skipped')),
    CONSTRAINT chk_player_photo_sources_payload CHECK (external_id IS NOT NULL OR source_url IS NOT NULL)
);

CREATE INDEX idx_player_photo_sources_player_id ON player_photo_sources (player_id);
CREATE INDEX idx_player_photo_sources_data_source_id ON player_photo_sources (data_source_id);
CREATE INDEX idx_player_photo_sources_status ON player_photo_sources (status);

CREATE TABLE coaches (
    id BIGSERIAL PRIMARY KEY,
    slug VARCHAR(150) NOT NULL UNIQUE,
    date_of_birth DATE,
    country_id BIGINT REFERENCES countries(id),
    photo_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Localization data

CREATE TABLE country_translations (
    country_id BIGINT NOT NULL REFERENCES countries(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (country_id, locale)
);

CREATE TABLE competition_translations (
    competition_id BIGINT NOT NULL REFERENCES competitions(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    name VARCHAR(255) NOT NULL,
    short_name VARCHAR(50),
    PRIMARY KEY (competition_id, locale)
);

CREATE TABLE competition_translation_candidates (
    id BIGSERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES competitions(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    proposed_name VARCHAR(255) NOT NULL,
    proposed_short_name VARCHAR(50),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'quarantined')),
    source_type TEXT NOT NULL DEFAULT 'manual' CHECK (source_type IN ('manual', 'imported', 'merge_derived', 'historical_rule', 'machine_generated', 'legacy')),
    source_url TEXT,
    source_label TEXT,
    source_ref TEXT,
    notes TEXT,
    reviewed_at TIMESTAMPTZ,
    reviewed_by TEXT,
    promoted_at TIMESTAMPTZ,
    promoted_by TEXT,
    proposed_name_normalized VARCHAR(255) GENERATED ALWAYS AS (lower(proposed_name)) STORED,
    source_key TEXT GENERATED ALWAYS AS (COALESCE(source_url, '') || '|' || COALESCE(source_ref, '')) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE team_translations (
    team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    name VARCHAR(255) NOT NULL,
    short_name VARCHAR(50),
    PRIMARY KEY (team_id, locale)
);

CREATE TABLE team_translation_candidates (
    id BIGSERIAL PRIMARY KEY,
    team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    proposed_name VARCHAR(255) NOT NULL,
    proposed_short_name VARCHAR(50),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'quarantined')),
    source_type TEXT NOT NULL DEFAULT 'manual' CHECK (source_type IN ('manual', 'imported', 'merge_derived', 'historical_rule', 'machine_generated', 'legacy')),
    source_url TEXT,
    source_label TEXT,
    source_ref TEXT,
    notes TEXT,
    reviewed_at TIMESTAMPTZ,
    reviewed_by TEXT,
    promoted_at TIMESTAMPTZ,
    promoted_by TEXT,
    proposed_name_normalized VARCHAR(255) GENERATED ALWAYS AS (lower(proposed_name)) STORED,
    source_key TEXT GENERATED ALWAYS AS (COALESCE(source_url, '') || '|' || COALESCE(source_ref, '')) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE country_translation_candidates (
    id BIGSERIAL PRIMARY KEY,
    country_id BIGINT NOT NULL REFERENCES countries(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    proposed_name VARCHAR(255) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'quarantined')),
    source_type TEXT NOT NULL DEFAULT 'manual' CHECK (source_type IN ('manual', 'imported', 'merge_derived', 'historical_rule', 'machine_generated', 'legacy')),
    source_url TEXT,
    source_label TEXT,
    source_ref TEXT,
    notes TEXT,
    reviewed_at TIMESTAMPTZ,
    reviewed_by TEXT,
    promoted_at TIMESTAMPTZ,
    promoted_by TEXT,
    proposed_name_normalized VARCHAR(255) GENERATED ALWAYS AS (lower(proposed_name)) STORED,
    source_key TEXT GENERATED ALWAYS AS (COALESCE(source_url, '') || '|' || COALESCE(source_ref, '')) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE player_translations (
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    known_as VARCHAR(150) NOT NULL,
    PRIMARY KEY (player_id, locale)
);

CREATE TABLE venue_translations (
    venue_id BIGINT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (venue_id, locale)
);

CREATE TABLE coach_translations (
    coach_id BIGINT NOT NULL REFERENCES coaches(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    known_as VARCHAR(150) NOT NULL,
    PRIMARY KEY (coach_id, locale)
);

CREATE INDEX idx_country_translations_locale ON country_translations (locale, country_id);
CREATE INDEX idx_competition_translations_locale ON competition_translations (locale, competition_id);
CREATE UNIQUE INDEX idx_competition_translation_candidates_unique ON competition_translation_candidates (competition_id, locale, proposed_name_normalized, source_key);
CREATE INDEX idx_competition_translation_candidates_status ON competition_translation_candidates (status, locale, competition_id);
CREATE INDEX idx_competition_translation_candidates_pending ON competition_translation_candidates (locale, competition_id, created_at DESC) WHERE status = 'pending';
CREATE INDEX idx_competition_translation_candidates_approved ON competition_translation_candidates (locale, competition_id, reviewed_at DESC, created_at DESC) WHERE status = 'approved';
CREATE INDEX idx_team_translations_locale ON team_translations (locale, team_id);
CREATE UNIQUE INDEX idx_team_translation_candidates_unique ON team_translation_candidates (team_id, locale, proposed_name_normalized, source_key);
CREATE INDEX idx_team_translation_candidates_status ON team_translation_candidates (status, locale, team_id);
CREATE INDEX idx_team_translation_candidates_pending ON team_translation_candidates (locale, team_id, created_at DESC) WHERE status = 'pending';
CREATE INDEX idx_team_translation_candidates_approved ON team_translation_candidates (locale, team_id, reviewed_at DESC, created_at DESC) WHERE status = 'approved';
CREATE UNIQUE INDEX idx_country_translation_candidates_unique ON country_translation_candidates (country_id, locale, proposed_name_normalized, source_key);
CREATE INDEX idx_country_translation_candidates_status ON country_translation_candidates (status, locale, country_id);
CREATE INDEX idx_country_translation_candidates_pending ON country_translation_candidates (locale, country_id, created_at DESC) WHERE status = 'pending';
CREATE INDEX idx_country_translation_candidates_approved ON country_translation_candidates (locale, country_id, reviewed_at DESC, created_at DESC) WHERE status = 'approved';
CREATE INDEX idx_player_translations_locale ON player_translations (locale, player_id);
CREATE INDEX idx_venue_translations_locale ON venue_translations (locale, venue_id);
CREATE INDEX idx_coach_translations_locale ON coach_translations (locale, coach_id);

CREATE TABLE entity_aliases (
    id BIGSERIAL PRIMARY KEY,
    entity_type entity_type NOT NULL,
    entity_id BIGINT NOT NULL,
    alias VARCHAR(255) NOT NULL,
    locale VARCHAR(10) REFERENCES locales(code),
    alias_kind alias_type NOT NULL DEFAULT 'common',
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'quarantined')),
    source_type TEXT NOT NULL DEFAULT 'manual' CHECK (source_type IN ('manual', 'imported', 'merge_derived', 'historical_rule', 'machine_generated', 'legacy')),
    source_ref TEXT,
    reviewed_at TIMESTAMPTZ,
    reviewed_by TEXT,
    alias_normalized VARCHAR(255) GENERATED ALWAYS AS (lower(alias)) STORED,
    search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple', alias)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_entity_aliases_unique
    ON entity_aliases (entity_type, entity_id, alias_normalized);
CREATE INDEX idx_entity_aliases_type_locale
    ON entity_aliases (entity_type, locale);
CREATE INDEX idx_entity_aliases_status
    ON entity_aliases (status, entity_type, locale);
CREATE INDEX idx_entity_aliases_fts
    ON entity_aliases USING GIN (search_vector);
CREATE INDEX idx_entity_aliases_trgm
    ON entity_aliases USING GIN (alias gin_trgm_ops);
CREATE INDEX idx_entity_aliases_approved_fts
    ON entity_aliases USING GIN (search_vector)
    WHERE status = 'approved';
CREATE INDEX idx_entity_aliases_approved_trgm
    ON entity_aliases USING GIN (alias gin_trgm_ops)
    WHERE status = 'approved';

-- Season-scoped snapshot and current data

CREATE TABLE competition_seasons (
    id BIGSERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES competitions(id),
    season_id BIGINT NOT NULL REFERENCES seasons(id),
    format_type competition_format_type NOT NULL,
    current_matchday SMALLINT,
    total_matchdays SMALLINT,
    source_match_updated_at TIMESTAMPTZ,
    source_match_available_at TIMESTAMPTZ,
    source_match_updated_360_at TIMESTAMPTZ,
    source_match_available_360_at TIMESTAMPTZ,
    source_metadata JSONB,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    winner_team_id BIGINT REFERENCES teams(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (competition_id, season_id)
);

CREATE TABLE team_seasons (
    id BIGSERIAL PRIMARY KEY,
    team_id BIGINT NOT NULL REFERENCES teams(id),
    competition_season_id BIGINT NOT NULL REFERENCES competition_seasons(id),
    coach_id BIGINT REFERENCES coaches(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, competition_season_id)
);

CREATE TABLE player_contracts (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id),
    team_id BIGINT NOT NULL REFERENCES teams(id),
    competition_season_id BIGINT NOT NULL REFERENCES competition_seasons(id),
    shirt_number SMALLINT,
    is_on_loan BOOLEAN NOT NULL DEFAULT FALSE,
    joined_date DATE,
    contract_start_date DATE,
    contract_end_date DATE,
    annual_salary_eur INTEGER,
    weekly_wage_eur INTEGER,
    salary_currency CHAR(3),
    salary_source VARCHAR(50),
    salary_source_url TEXT,
    salary_is_estimated BOOLEAN NOT NULL DEFAULT TRUE,
    salary_updated_at TIMESTAMPTZ,
    left_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, competition_season_id)
);

CREATE INDEX idx_player_contracts_team_season
    ON player_contracts (team_id, competition_season_id);
CREATE INDEX idx_player_contracts_active
    ON player_contracts (team_id, player_id)
    WHERE left_date IS NULL;

CREATE TABLE player_season_stats (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id),
    competition_season_id BIGINT NOT NULL REFERENCES competition_seasons(id),
    appearances SMALLINT NOT NULL DEFAULT 0,
    starts SMALLINT NOT NULL DEFAULT 0,
    minutes_played INTEGER NOT NULL DEFAULT 0,
    goals SMALLINT NOT NULL DEFAULT 0,
    assists SMALLINT NOT NULL DEFAULT 0,
    penalty_goals SMALLINT NOT NULL DEFAULT 0,
    own_goals SMALLINT NOT NULL DEFAULT 0,
    yellow_cards SMALLINT NOT NULL DEFAULT 0,
    red_cards SMALLINT NOT NULL DEFAULT 0,
    yellow_red_cards SMALLINT NOT NULL DEFAULT 0,
    clean_sheets SMALLINT NOT NULL DEFAULT 0,
    goals_conceded SMALLINT NOT NULL DEFAULT 0,
    saves SMALLINT NOT NULL DEFAULT 0,
    avg_rating DECIMAL(3,1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, competition_season_id)
);

CREATE INDEX idx_player_season_stats_goals
    ON player_season_stats (competition_season_id, goals DESC, assists DESC);
CREATE INDEX idx_player_season_stats_assists
    ON player_season_stats (competition_season_id, assists DESC, goals DESC);

CREATE TABLE player_market_values (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    source_id BIGINT NOT NULL REFERENCES data_sources(id) ON DELETE RESTRICT,
    season_id BIGINT REFERENCES seasons(id),
    season_label VARCHAR(20),
    club_id BIGINT REFERENCES teams(id),
    club_name VARCHAR(255),
    external_player_id TEXT,
    external_club_id TEXT,
    observed_at DATE NOT NULL,
    age SMALLINT,
    market_value_eur INTEGER NOT NULL CHECK (market_value_eur >= 0),
    currency_code CHAR(3) NOT NULL DEFAULT 'EUR',
    source_url TEXT,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, source_id, observed_at)
);

CREATE INDEX idx_player_market_values_player_date
    ON player_market_values (player_id, observed_at DESC);
CREATE INDEX idx_player_market_values_season
    ON player_market_values (season_id, market_value_eur DESC);

CREATE TABLE player_transfers (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    source_id BIGINT NOT NULL REFERENCES data_sources(id) ON DELETE RESTRICT,
    season_id BIGINT REFERENCES seasons(id),
    season_label VARCHAR(20),
    external_transfer_id TEXT NOT NULL,
    moved_at DATE,
    age SMALLINT,
    from_team_id BIGINT REFERENCES teams(id),
    from_team_name VARCHAR(255),
    from_team_external_id TEXT,
    to_team_id BIGINT REFERENCES teams(id),
    to_team_name VARCHAR(255),
    to_team_external_id TEXT,
    market_value_eur INTEGER,
    fee_eur INTEGER,
    currency_code CHAR(3),
    fee_display VARCHAR(50),
    transfer_type VARCHAR(30),
    transfer_type_label VARCHAR(100),
    is_pending BOOLEAN NOT NULL DEFAULT FALSE,
    contract_until_date DATE,
    source_url TEXT,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, source_id, external_transfer_id)
);

CREATE INDEX idx_player_transfers_player_date
    ON player_transfers (player_id, moved_at DESC NULLS LAST, id DESC);
CREATE INDEX idx_player_transfers_season
    ON player_transfers (season_id, moved_at DESC NULLS LAST);

CREATE VIEW current_competition_seasons AS
SELECT cs.*
FROM competition_seasons cs
JOIN seasons s ON s.id = cs.season_id
WHERE s.is_current = TRUE;

CREATE VIEW current_player_contracts AS
SELECT pc.*
FROM player_contracts pc
JOIN competition_seasons cs ON cs.id = pc.competition_season_id
JOIN seasons s ON s.id = cs.season_id
WHERE s.is_current = TRUE
  AND pc.left_date IS NULL;

-- Match, event, and live-update data

CREATE TABLE matches (
    id BIGSERIAL,
    match_date DATE NOT NULL,
    competition_season_id BIGINT NOT NULL REFERENCES competition_seasons(id),
    matchday SMALLINT,
    stage VARCHAR(30) DEFAULT 'REGULAR_SEASON',
    group_name VARCHAR(20),
    home_team_id BIGINT NOT NULL REFERENCES teams(id),
    away_team_id BIGINT NOT NULL REFERENCES teams(id),
    home_score SMALLINT,
    away_score SMALLINT,
    home_ht_score SMALLINT,
    away_ht_score SMALLINT,
    status match_status NOT NULL DEFAULT 'scheduled',
    kickoff_at TIMESTAMPTZ,
    venue_id BIGINT REFERENCES venues(id),
    attendance INTEGER,
    referee VARCHAR(100),
    home_formation VARCHAR(20),
    away_formation VARCHAR(20),
    source_last_updated_at TIMESTAMPTZ,
    source_last_updated_360_at TIMESTAMPTZ,
    source_data_version VARCHAR(20),
    source_shot_fidelity_version VARCHAR(20),
    source_xy_fidelity_version VARCHAR(20),
    source_metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, match_date),
    CONSTRAINT chk_match_teams_distinct CHECK (home_team_id <> away_team_id)
) PARTITION BY RANGE (match_date);

CREATE TABLE matches_2024_25 PARTITION OF matches
    FOR VALUES FROM ('2024-07-01') TO ('2025-07-01');
CREATE TABLE matches_2025_26 PARTITION OF matches
    FOR VALUES FROM ('2025-07-01') TO ('2026-07-01');
CREATE TABLE matches_2026_27 PARTITION OF matches
    FOR VALUES FROM ('2026-07-01') TO ('2027-07-01');
CREATE TABLE matches_default PARTITION OF matches DEFAULT;

CREATE INDEX idx_matches_competition_season
    ON matches (competition_season_id, match_date);
CREATE INDEX idx_matches_home_team
    ON matches (home_team_id, match_date DESC);
CREATE INDEX idx_matches_away_team
    ON matches (away_team_id, match_date DESC);
CREATE INDEX idx_matches_kickoff
    ON matches (kickoff_at DESC);
CREATE INDEX idx_matches_status_live_upcoming
    ON matches (status, kickoff_at)
    WHERE status IN ('scheduled', 'timed', 'live_1h', 'live_ht', 'live_2h', 'live_et', 'live_pen');

CREATE TABLE competition_format_stage_rules (
    format_type competition_format_type NOT NULL,
    stage_pattern TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (format_type, stage_pattern)
);

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
    ('knockout', '^[0-9]+(ST|ND|RD|TH)_ROUND(_QUALIFYING)?(_REPLAYS?)?$', 'Numbered round');

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

CREATE TRIGGER trg_set_competition_season_format
BEFORE INSERT OR UPDATE OF competition_id, season_id, format_type
ON competition_seasons
FOR EACH ROW
EXECUTE FUNCTION set_competition_season_format();

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

CREATE TRIGGER trg_validate_match_stage_against_competition_format
BEFORE INSERT OR UPDATE OF competition_season_id, stage
ON matches
FOR EACH ROW
EXECUTE FUNCTION validate_match_stage_against_competition_format();

CREATE TABLE match_stats (
    id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,
    match_date DATE NOT NULL,
    team_id BIGINT NOT NULL REFERENCES teams(id),
    is_home BOOLEAN NOT NULL,
    possession SMALLINT,
    total_passes SMALLINT,
    accurate_passes SMALLINT,
    pass_accuracy SMALLINT,
    total_shots SMALLINT,
    shots_on_target SMALLINT,
    shots_off_target SMALLINT,
    blocked_shots SMALLINT,
    corner_kicks SMALLINT,
    free_kicks SMALLINT,
    throw_ins SMALLINT,
    fouls SMALLINT,
    offsides SMALLINT,
    gk_saves SMALLINT,
    expected_goals DECIMAL(4,2),
    big_chances SMALLINT,
    big_chances_missed SMALLINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date),
    UNIQUE (match_id, match_date, team_id)
);

CREATE INDEX idx_match_stats_match
    ON match_stats (match_id, team_id);
CREATE INDEX idx_match_stats_team_id
    ON match_stats (team_id);

CREATE TABLE match_lineups (
    id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,
    match_date DATE NOT NULL,
    team_id BIGINT NOT NULL REFERENCES teams(id),
    player_id BIGINT NOT NULL REFERENCES players(id),
    shirt_number SMALLINT,
    position VARCHAR(30),
    grid_position VARCHAR(10),
    is_starter BOOLEAN NOT NULL DEFAULT TRUE,
    from_minute SMALLINT,
    to_minute SMALLINT,
    start_reason VARCHAR(50),
    end_reason VARCHAR(50),
    minutes_played SMALLINT,
    rating DECIMAL(3,1),
    source_details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date),
    UNIQUE (match_id, match_date, team_id, player_id)
);

CREATE INDEX idx_match_lineups_match
    ON match_lineups (match_id, team_id, is_starter);
CREATE INDEX idx_match_lineups_player_match
    ON match_lineups (player_id, match_id);
CREATE INDEX idx_match_lineups_team_id
    ON match_lineups (team_id);

CREATE TABLE match_event_artifacts (
    id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,
    match_date DATE NOT NULL,
    artifact_type VARCHAR(40) NOT NULL,
    format VARCHAR(20) NOT NULL,
    storage_key TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    row_count INTEGER,
    byte_size BIGINT,
    checksum_sha256 CHAR(64),
    source_vendor VARCHAR(40),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date),
    CONSTRAINT match_event_artifacts_type_check CHECK (
        artifact_type IN ('analysis_detail', 'freeze_frames', 'visible_areas', 'raw_event_bundle')
    ),
    CONSTRAINT match_event_artifacts_format_check CHECK (format = 'json.gz'),
    UNIQUE (match_id, artifact_type, version)
);

CREATE INDEX idx_match_event_artifacts_match
    ON match_event_artifacts (match_id, artifact_type, version DESC);

-- Source mapping and ingestion support

CREATE TABLE source_entity_mapping (
    id BIGSERIAL PRIMARY KEY,
    entity_type entity_type NOT NULL,
    entity_id BIGINT NOT NULL,
    source_id BIGINT NOT NULL REFERENCES data_sources(id),
    external_id TEXT NOT NULL,
    external_code VARCHAR(20),
    season_context VARCHAR(20),
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_type, source_id, external_id)
);

CREATE INDEX idx_source_entity_mapping_internal
    ON source_entity_mapping (entity_type, entity_id);
CREATE INDEX idx_source_entity_mapping_external
    ON source_entity_mapping (source_id, external_id);

CREATE TABLE source_sync_runs (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES data_sources(id),
    upstream_ref VARCHAR(255) NOT NULL,
    upstream_commit_sha CHAR(40),
    status ingestion_status NOT NULL DEFAULT 'running',
    fetched_files INTEGER NOT NULL DEFAULT 0,
    changed_files INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    metadata JSONB
);

CREATE INDEX idx_source_sync_runs_lookup
    ON source_sync_runs (source_id, started_at DESC);

CREATE TABLE source_sync_manifests (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES data_sources(id),
    sync_run_id BIGINT REFERENCES source_sync_runs(id) ON DELETE SET NULL,
    manifest_type VARCHAR(30) NOT NULL,
    upstream_path TEXT NOT NULL,
    upstream_commit_sha CHAR(40),
    external_id TEXT,
    external_parent_id TEXT,
    source_updated_at TIMESTAMPTZ,
    source_available_at TIMESTAMPTZ,
    metadata JSONB,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, manifest_type, upstream_path)
);

CREATE INDEX idx_source_sync_manifests_external
    ON source_sync_manifests (source_id, manifest_type, external_parent_id, external_id);

CREATE INDEX idx_source_sync_manifests_updated
    ON source_sync_manifests (source_updated_at DESC);

CREATE TABLE raw_payloads (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES data_sources(id),
    sync_run_id BIGINT REFERENCES source_sync_runs(id) ON DELETE SET NULL,
    endpoint TEXT NOT NULL,
    entity_type entity_type,
    external_id TEXT,
    season_context VARCHAR(20),
    http_status SMALLINT,
    payload JSONB NOT NULL,
    payload_hash VARCHAR(64),
    upstream_commit_sha CHAR(40),
    source_updated_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_raw_payloads_lookup
    ON raw_payloads (source_id, entity_type, external_id, fetched_at DESC);

CREATE TABLE ingestion_log (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES data_sources(id),
    job_type VARCHAR(50) NOT NULL,
    status ingestion_status NOT NULL DEFAULT 'running',
    entities_created INTEGER NOT NULL DEFAULT 0,
    entities_updated INTEGER NOT NULL DEFAULT 0,
    errors JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms INTEGER
);

CREATE TABLE refresh_policies (
    id BIGSERIAL PRIMARY KEY,
    slug VARCHAR(100) NOT NULL UNIQUE,
    scope refresh_scope NOT NULL,
    entity_type entity_type,
    current_season_only BOOLEAN NOT NULL DEFAULT FALSE,
    read_mostly_after_finish BOOLEAN NOT NULL DEFAULT FALSE,
    refresh_interval INTERVAL NOT NULL,
    stale_after INTERVAL NOT NULL,
    cache_ttl_seconds INTEGER NOT NULL,
    refresh_on_matchday BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_refresh_policies_cache_ttl CHECK (cache_ttl_seconds > 0),
    CONSTRAINT chk_refresh_policies_stale_after CHECK (stale_after >= refresh_interval)
);

CREATE TABLE data_freshness (
    id BIGSERIAL PRIMARY KEY,
    policy_id BIGINT NOT NULL REFERENCES refresh_policies(id) ON DELETE CASCADE,
    source_id BIGINT REFERENCES data_sources(id),
    entity_type entity_type,
    entity_id BIGINT,
    competition_season_id BIGINT REFERENCES competition_seasons(id),
    match_id BIGINT,
    match_date DATE,
    payload_hash VARCHAR(64),
    payload_changed_at TIMESTAMPTZ,
    last_fetched_at TIMESTAMPTZ,
    last_materialized_at TIMESTAMPTZ,
    next_refresh_at TIMESTAMPTZ,
    refresh_after TIMESTAMPTZ,
    is_live BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date),
    CONSTRAINT chk_data_freshness_scope CHECK (
        entity_id IS NOT NULL OR competition_season_id IS NOT NULL OR match_id IS NOT NULL
    )
);

CREATE UNIQUE INDEX idx_data_freshness_scope_unique
    ON data_freshness (
        policy_id,
        COALESCE(source_id, -1),
        COALESCE(entity_type, 'competition'::entity_type),
        COALESCE(entity_id, -1),
        COALESCE(competition_season_id, -1),
        COALESCE(match_id, -1),
        COALESCE(match_date, DATE '1900-01-01')
    );

CREATE INDEX idx_data_freshness_next_refresh
    ON data_freshness (next_refresh_at)
    WHERE next_refresh_at IS NOT NULL;

CREATE INDEX idx_data_freshness_live_window
    ON data_freshness (is_live, refresh_after)
    WHERE is_live = TRUE;

-- Materialized views for read-heavy pages

CREATE MATERIALIZED VIEW mv_standings AS
WITH match_results AS (
    SELECT
        m.competition_season_id,
        m.home_team_id AS team_id,
        CASE
            WHEN m.home_score > m.away_score THEN 3
            WHEN m.home_score = m.away_score THEN 1
            ELSE 0
        END AS points,
        CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END AS won,
        CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END AS drawn,
        CASE WHEN m.home_score < m.away_score THEN 1 ELSE 0 END AS lost,
        m.home_score AS goals_for,
        m.away_score AS goals_against
    FROM matches m
    WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')

    UNION ALL

    SELECT
        m.competition_season_id,
        m.away_team_id AS team_id,
        CASE
            WHEN m.away_score > m.home_score THEN 3
            WHEN m.away_score = m.home_score THEN 1
            ELSE 0
        END AS points,
        CASE WHEN m.away_score > m.home_score THEN 1 ELSE 0 END AS won,
        CASE WHEN m.away_score = m.home_score THEN 1 ELSE 0 END AS drawn,
        CASE WHEN m.away_score < m.home_score THEN 1 ELSE 0 END AS lost,
        m.away_score AS goals_for,
        m.home_score AS goals_against
    FROM matches m
    WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')
)
SELECT
    mr.competition_season_id,
    mr.team_id,
    COUNT(*)::SMALLINT AS played,
    SUM(mr.won)::SMALLINT AS won,
    SUM(mr.drawn)::SMALLINT AS drawn,
    SUM(mr.lost)::SMALLINT AS lost,
    SUM(mr.goals_for)::SMALLINT AS goals_for,
    SUM(mr.goals_against)::SMALLINT AS goals_against,
    (SUM(mr.goals_for) - SUM(mr.goals_against))::SMALLINT AS goal_difference,
    SUM(mr.points)::SMALLINT AS points,
    RANK() OVER (
        PARTITION BY mr.competition_season_id
        ORDER BY
            SUM(mr.points) DESC,
            SUM(mr.goals_for) - SUM(mr.goals_against) DESC,
            SUM(mr.goals_for) DESC
    )::SMALLINT AS position
FROM match_results mr
GROUP BY mr.competition_season_id, mr.team_id
WITH DATA;

CREATE UNIQUE INDEX idx_mv_standings_unique
    ON mv_standings (competition_season_id, team_id);
CREATE INDEX idx_mv_standings_rank
    ON mv_standings (competition_season_id, position);

CREATE MATERIALIZED VIEW mv_top_scorers AS
SELECT
    pss.competition_season_id,
    pss.player_id,
    pc.team_id,
    pss.goals,
    pss.penalty_goals,
    pss.assists,
    pss.appearances,
    pss.minutes_played,
    RANK() OVER (
        PARTITION BY pss.competition_season_id
        ORDER BY pss.goals DESC, pss.assists DESC, pss.minutes_played ASC
    )::SMALLINT AS rank
FROM player_season_stats pss
JOIN player_contracts pc
    ON pc.player_id = pss.player_id
   AND pc.competition_season_id = pss.competition_season_id
WHERE pc.left_date IS NULL
  AND pss.goals > 0
WITH DATA;

CREATE UNIQUE INDEX idx_mv_top_scorers_unique
    ON mv_top_scorers (competition_season_id, player_id);
CREATE INDEX idx_mv_top_scorers_rank
    ON mv_top_scorers (competition_season_id, rank);

CREATE MATERIALIZED VIEW mv_team_form AS
WITH team_results AS (
    SELECT
        m.id AS match_id,
        m.match_date,
        m.competition_season_id,
        m.home_team_id AS team_id,
        CASE
            WHEN m.home_score > m.away_score THEN 'W'
            WHEN m.home_score = m.away_score THEN 'D'
            ELSE 'L'
        END AS form_result,
        CASE
            WHEN m.home_score > m.away_score THEN 3
            WHEN m.home_score = m.away_score THEN 1
            ELSE 0
        END AS points
    FROM matches m
    WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')

    UNION ALL

    SELECT
        m.id AS match_id,
        m.match_date,
        m.competition_season_id,
        m.away_team_id AS team_id,
        CASE
            WHEN m.away_score > m.home_score THEN 'W'
            WHEN m.away_score = m.home_score THEN 'D'
            ELSE 'L'
        END AS form_result,
        CASE
            WHEN m.away_score > m.home_score THEN 3
            WHEN m.away_score = m.home_score THEN 1
            ELSE 0
        END AS points
    FROM matches m
    WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')
), ranked_results AS (
    SELECT
        tr.*,
        ROW_NUMBER() OVER (
            PARTITION BY tr.competition_season_id, tr.team_id
            ORDER BY tr.match_date DESC, tr.match_id DESC
        ) AS recent_rank
    FROM team_results tr
)
SELECT
    rr.competition_season_id,
    rr.team_id,
    ARRAY_AGG(rr.form_result ORDER BY rr.match_date DESC, rr.match_id DESC) AS last_five_results,
    COUNT(*)::SMALLINT AS matches_counted,
    SUM(rr.points)::SMALLINT AS points_last_five
FROM ranked_results rr
WHERE rr.recent_rank <= 5
GROUP BY rr.competition_season_id, rr.team_id
WITH DATA;

CREATE UNIQUE INDEX idx_mv_team_form_unique
    ON mv_team_form (competition_season_id, team_id);

CREATE OR REPLACE FUNCTION is_matchday_hot_window(
    p_kickoff_at TIMESTAMPTZ,
    p_now TIMESTAMPTZ DEFAULT NOW()
)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
    SELECT p_kickoff_at IS NOT NULL
       AND p_now BETWEEN p_kickoff_at - INTERVAL '6 hours' AND p_kickoff_at + INTERVAL '3 hours';
$$;

CREATE OR REPLACE FUNCTION should_refresh_resource(
    p_policy_slug VARCHAR(100),
    p_last_fetched_at TIMESTAMPTZ,
    p_kickoff_at TIMESTAMPTZ DEFAULT NULL,
    p_now TIMESTAMPTZ DEFAULT NOW()
)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_policy refresh_policies%ROWTYPE;
    v_effective_interval INTERVAL;
BEGIN
    SELECT *
    INTO v_policy
    FROM refresh_policies
    WHERE slug = p_policy_slug;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'refresh policy not found: %', p_policy_slug;
    END IF;

    IF p_last_fetched_at IS NULL THEN
        RETURN TRUE;
    END IF;

    v_effective_interval := v_policy.refresh_interval;

    IF v_policy.refresh_on_matchday AND is_matchday_hot_window(p_kickoff_at, p_now) THEN
        v_effective_interval := LEAST(v_policy.refresh_interval, INTERVAL '30 seconds');
    END IF;

    RETURN p_last_fetched_at + v_effective_interval <= p_now;
END;
$$;

-- Locale-aware alias search with exact -> full-text -> trigram fallback.
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

-- Seed minimum references.
INSERT INTO locales (code, name, is_default, fallback_to)
VALUES
    ('en', 'English', TRUE, NULL),
    ('ko', 'Korean', FALSE, 'en')
ON CONFLICT (code) DO NOTHING;

INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
VALUES
    ('statsbomb_open_data', 'StatsBomb Open Data', 'https://github.com/statsbomb/open-data', 'git', 'master', 0),
    ('football_data_org', 'football-data.org v4', 'https://api.football-data.org/v4', 'api', NULL, 1),
    ('sofascore', 'Sofascore CDN', 'https://img.sofascore.com', 'api', NULL, 1),
    ('api_football', 'API-Football v3', 'https://v3.football.api-sports.io', 'api', NULL, 2),
    ('wikimedia', 'Wikimedia Commons', 'https://commons.wikimedia.org', 'api', NULL, 3)
ON CONFLICT (slug) DO NOTHING;

INSERT INTO refresh_policies (
    slug,
    scope,
    entity_type,
    current_season_only,
    read_mostly_after_finish,
    refresh_interval,
    stale_after,
    cache_ttl_seconds,
    refresh_on_matchday,
    notes
)
VALUES
    (
        'master.competitions',
        'master',
        'competition',
        FALSE,
        TRUE,
        INTERVAL '1 day',
        INTERVAL '7 days',
        86400,
        FALSE,
        '리그/대회 마스터 데이터는 느리게 갱신한다.'
    ),
    (
        'master.teams',
        'master',
        'team',
        FALSE,
        TRUE,
        INTERVAL '12 hours',
        INTERVAL '3 days',
        43200,
        FALSE,
        '구단 마스터 데이터는 하루 1~2회 수준으로 갱신한다.'
    ),
    (
        'master.countries',
        'master',
        'country',
        FALSE,
        TRUE,
        INTERVAL '12 hours',
        INTERVAL '3 days',
        43200,
        FALSE,
        '국가 마스터 데이터는 하루 1~2회 수준으로 갱신한다.'
    ),
    (
        'season.current.standings',
        'season_current',
        'competition',
        TRUE,
        FALSE,
        INTERVAL '10 minutes',
        INTERVAL '30 minutes',
        120,
        TRUE,
        '현재 시즌 순위표는 적극적으로 갱신한다.'
    ),
    (
        'season.finished.read_model',
        'season_finished',
        'competition',
        FALSE,
        TRUE,
        INTERVAL '30 days',
        INTERVAL '90 days',
        86400,
        FALSE,
        '종료 시즌은 사실상 read-mostly로 취급한다.'
    ),
    (
        'match.read_model',
        'matchday_warm',
        'match',
        TRUE,
        FALSE,
        INTERVAL '15 minutes',
        INTERVAL '1 hour',
        300,
        TRUE,
        '일반 경기 읽기 모델은 중간 주기로 갱신한다.'
    ),
    (
        'search.read_model',
        'matchday_warm',
        'search_index',
        FALSE,
        FALSE,
        INTERVAL '15 minutes',
        INTERVAL '1 hour',
        300,
        FALSE,
        '검색 읽기 모델은 중간 주기로 갱신한다.'
    ),
    (
        'match.live.detail',
        'matchday_hot',
        'match',
        TRUE,
        FALSE,
        INTERVAL '15 seconds',
        INTERVAL '2 minutes',
        15,
        TRUE,
        '경기 당일과 라이브 구간 데이터는 짧은 주기로 갱신한다.'
    ),
    (
        'match.upcoming.detail',
        'matchday_warm',
        'match',
        TRUE,
        FALSE,
        INTERVAL '15 minutes',
        INTERVAL '1 hour',
        300,
        TRUE,
        '임박 경기 데이터는 라이브 직전까지 중간 주기로 갱신한다.'
    )
ON CONFLICT (slug) DO NOTHING;
