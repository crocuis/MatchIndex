-- MatchIndex PostgreSQL schema
-- MVP-first canonical model for football data ingestion and serving.

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TYPE position_type AS ENUM ('GK', 'DEF', 'MID', 'FWD');
CREATE TYPE preferred_foot AS ENUM ('Left', 'Right', 'Both');
CREATE TYPE competition_type AS ENUM ('league', 'cup', 'league_cup', 'super_cup', 'international');
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
    flag_url TEXT,
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
    CONSTRAINT chk_season_dates CHECK (end_date > start_date),
    CONSTRAINT no_season_overlap EXCLUDE USING gist (
        daterange(start_date, end_date, '[]') WITH &&
    )
);

CREATE UNIQUE INDEX idx_seasons_single_current
    ON seasons (is_current)
    WHERE is_current = TRUE;

CREATE TABLE competitions (
    id BIGSERIAL PRIMARY KEY,
    slug VARCHAR(100) NOT NULL UNIQUE,
    code VARCHAR(10) UNIQUE,
    comp_type competition_type NOT NULL DEFAULT 'league',
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

CREATE TABLE team_translations (
    team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    name VARCHAR(255) NOT NULL,
    short_name VARCHAR(50),
    PRIMARY KEY (team_id, locale)
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
CREATE INDEX idx_team_translations_locale ON team_translations (locale, team_id);
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
    alias_normalized VARCHAR(255) GENERATED ALWAYS AS (lower(alias)) STORED,
    search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple', alias)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_entity_aliases_unique
    ON entity_aliases (entity_type, entity_id, lower(alias));
CREATE INDEX idx_entity_aliases_type_locale
    ON entity_aliases (entity_type, locale);
CREATE INDEX idx_entity_aliases_fts
    ON entity_aliases USING GIN (search_vector);
CREATE INDEX idx_entity_aliases_trgm
    ON entity_aliases USING GIN (alias gin_trgm_ops);

-- Season-scoped snapshot and current data

CREATE TABLE competition_seasons (
    id BIGSERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES competitions(id),
    season_id BIGINT NOT NULL REFERENCES seasons(id),
    current_matchday SMALLINT,
    total_matchdays SMALLINT,
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

CREATE TABLE match_events (
    id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,
    match_date DATE NOT NULL,
    event_type match_event_type NOT NULL,
    minute SMALLINT NOT NULL,
    extra_minute SMALLINT,
    team_id BIGINT NOT NULL REFERENCES teams(id),
    player_id BIGINT REFERENCES players(id),
    secondary_player_id BIGINT REFERENCES players(id),
    detail VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date)
);

CREATE INDEX idx_match_events_match_timeline
    ON match_events (match_id, minute, extra_minute);
CREATE INDEX idx_match_events_player
    ON match_events (player_id, event_type);
CREATE INDEX idx_match_events_scoring
    ON match_events (team_id, match_date)
    WHERE event_type IN ('goal', 'own_goal', 'penalty_scored');

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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date),
    UNIQUE (match_id, match_date, team_id)
);

CREATE INDEX idx_match_stats_match
    ON match_stats (match_id, team_id);

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
    minutes_played SMALLINT,
    rating DECIMAL(3,1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date),
    UNIQUE (match_id, match_date, team_id, player_id)
);

CREATE INDEX idx_match_lineups_match
    ON match_lineups (match_id, team_id, is_starter);

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

CREATE TABLE raw_payloads (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES data_sources(id),
    endpoint TEXT NOT NULL,
    entity_type entity_type,
    external_id TEXT,
    season_context VARCHAR(20),
    http_status SMALLINT,
    payload JSONB NOT NULL,
    payload_hash VARCHAR(64),
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
    SELECT *
    FROM (
        SELECT
            ea.entity_type,
            ea.entity_id,
            ea.alias AS matched_alias,
            'exact'::TEXT AS match_type,
            1.0::REAL AS score,
            1 AS ord
        FROM entity_aliases ea
        WHERE lower(ea.alias) = lower(p_query)
          AND (p_locale IS NULL OR ea.locale = p_locale OR ea.locale IS NULL)
          AND (p_entity_type IS NULL OR ea.entity_type = p_entity_type)

        UNION ALL

        SELECT
            ea.entity_type,
            ea.entity_id,
            ea.alias AS matched_alias,
            'fts'::TEXT AS match_type,
            ts_rank(ea.search_vector, plainto_tsquery('simple', p_query))::REAL AS score,
            2 AS ord
        FROM entity_aliases ea
        WHERE ea.search_vector @@ plainto_tsquery('simple', p_query)
          AND lower(ea.alias) <> lower(p_query)
          AND (p_locale IS NULL OR ea.locale = p_locale OR ea.locale IS NULL)
          AND (p_entity_type IS NULL OR ea.entity_type = p_entity_type)

        UNION ALL

        SELECT
            ea.entity_type,
            ea.entity_id,
            ea.alias AS matched_alias,
            'fuzzy'::TEXT AS match_type,
            similarity(ea.alias, p_query)::REAL AS score,
            3 AS ord
        FROM entity_aliases ea
        WHERE ea.alias % p_query
          AND lower(ea.alias) <> lower(p_query)
          AND (p_locale IS NULL OR ea.locale = p_locale OR ea.locale IS NULL)
          AND (p_entity_type IS NULL OR ea.entity_type = p_entity_type)
    ) ranked
    ORDER BY ranked.ord, ranked.score DESC
    LIMIT p_limit;
$$;

-- Seed minimum references.
INSERT INTO locales (code, name, is_default, fallback_to)
VALUES
    ('en', 'English', TRUE, NULL),
    ('ko', 'Korean', FALSE, 'en')
ON CONFLICT (code) DO NOTHING;

INSERT INTO data_sources (slug, name, base_url, priority)
VALUES
    ('football_data_org', 'football-data.org v4', 'https://api.football-data.org/v4', 1),
    ('api_football', 'API-Football v3', 'https://v3.football.api-sports.io', 2)
ON CONFLICT (slug) DO NOTHING;
