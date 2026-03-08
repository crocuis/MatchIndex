# MatchIndex — PostgreSQL Data Model

## 1. 설계 원칙

### 1.1 핵심 원칙

| 원칙 | 설명 |
|------|------|
| **Canonical-first** | 외부 API는 원천(source)이지만, 서비스는 내부 DB의 canonical ID를 기준으로 동작한다. 외부 ID는 `source_entity_mapping`에만 존재한다. |
| **Season-scoped isolation** | 시즌별 데이터(통계, 순위, 계약)는 마스터 엔티티와 분리된 별도 테이블에 저장한다. 한 시즌이 종료되어도 마스터 데이터는 불변이다. |
| **Hot/Cold separation** | 현재 시즌 = hot (NVMe tablespace, 짧은 Redis TTL, 빈번한 materialized view refresh). 종료 시즌 = cold (HDD tablespace, 긴 TTL, refresh 없음). |
| **Translation ≠ Canonical** | 엔티티 테이블에는 언어 독립적 데이터만 저장한다. 모든 사람이 읽는 이름/설명은 `*_translations` 테이블에 locale별로 저장한다. |
| **Source-agnostic ingestion** | football-data.org, API-Football 등 여러 소스를 동시 지원한다. raw payload 저장 → normalize → canonical 테이블 upsert 파이프라인. |
| **Partitioned event data** | 매치/이벤트 테이블은 `match_date` 기준 RANGE 파티셔닝. 시즌 경계에 맞춰 파티션 생성. |
| **Materialized reads** | 순위표, 득점 랭킹 등 읽기 집약 데이터는 materialized view + `REFRESH CONCURRENTLY`로 서빙한다. |

### 1.2 ID 전략

```
내부 ID: BIGSERIAL (auto-increment, 모든 테이블)
외부 ID: TEXT (source_entity_mapping.external_id — football-data.org는 integer, apifootball.com은 string이므로 TEXT 통일)
URL slug: VARCHAR(100) UNIQUE (teams.slug = 'manchester-city', SEO/URL용)
```

### 1.3 타임스탬프 규칙

- 모든 테이블에 `created_at TIMESTAMPTZ DEFAULT NOW()` 필수
- 변경 가능한 테이블에 `updated_at TIMESTAMPTZ DEFAULT NOW()` 추가
- 매치 시간은 항상 UTC 저장 (`kickoff_at TIMESTAMPTZ`)
- 시즌 경계는 `DATE` (시간 불필요)

---

## 2. 데이터 분류

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MatchIndex Data Architecture                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Layer 1: MASTER ENTITY (Canonical, language-independent)                   │
│  ┌──────────────┐ ┌──────────┐ ┌──────────┐ ┌───────────┐ ┌────────────┐  │
│  │ competitions  │ │  teams   │ │ players  │ │ countries │ │   venues   │  │
│  └──────────────┘ └──────────┘ └──────────┘ └───────────┘ └────────────┘  │
│  ┌──────────────┐ ┌──────────┐                                             │
│  │   seasons    │ │ coaches  │                                             │
│  └──────────────┘ └──────────┘                                             │
│                                                                             │
│  Layer 2: LOCALIZATION (per-locale translations + search aliases)           │
│  ┌─────────────────────────┐ ┌─────────────────────────┐                   │
│  │ competition_translations│ │   team_translations     │                   │
│  └─────────────────────────┘ └─────────────────────────┘                   │
│  ┌─────────────────────────┐ ┌─────────────────────────┐                   │
│  │  player_translations    │ │  country_translations   │                   │
│  └─────────────────────────┘ └─────────────────────────┘                   │
│  ┌─────────────────────────┐ ┌──────────┐                                  │
│  │   venue_translations    │ │ locales  │                                  │
│  └─────────────────────────┘ └──────────┘                                  │
│  ┌─────────────────────────┐                                               │
│  │    entity_aliases       │ ← 통합 검색 alias (맨시티, Man City, MCFC)     │
│  └─────────────────────────┘                                               │
│                                                                             │
│  Layer 3: SEASON-SCOPED (snapshots, contracts, per-season aggregates)      │
│  ┌─────────────────────────┐ ┌─────────────────────────┐                   │
│  │  competition_seasons    │ │    team_seasons         │                   │
│  └─────────────────────────┘ └─────────────────────────┘                   │
│  ┌─────────────────────────┐ ┌─────────────────────────┐                   │
│  │   player_contracts      │ │  player_season_stats    │                   │
│  └─────────────────────────┘ └─────────────────────────┘                   │
│                                                                             │
│  Layer 4: MATCH/EVENT (high-volume, partitioned by match_date)             │
│  ┌──────────────┐ ┌──────────────┐ ┌────────────────┐                      │
│  │   matches    │ │ match_events │ │  match_stats   │                      │
│  │ (PARTITIONED)│ └──────────────┘ └────────────────┘                      │
│  └──────────────┘ ┌────────────────┐                                       │
│                   │ match_lineups  │                                       │
│                   └────────────────┘                                       │
│                                                                             │
│  Layer 5: MATERIALIZED / COMPUTED (read-optimized views)                   │
│  ┌──────────────────┐ ┌───────────────────┐                                │
│  │  mv_standings     │ │  mv_top_scorers   │                               │
│  └──────────────────┘ └───────────────────┘                                │
│                                                                             │
│  Layer 6: SOURCE MAPPING / INGESTION                                       │
│  ┌──────────────────┐ ┌───────────────────────┐ ┌────────────────┐         │
│  │  data_sources     │ │ source_entity_mapping │ │  raw_payloads  │         │
│  └──────────────────┘ └───────────────────────┘ └────────────────┘         │
│  ┌──────────────────┐                                                      │
│  │  ingestion_log   │                                                      │
│  └──────────────────┘                                                      │
│                                                                             │
│  REDIS (Cache Layer)                                                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ match:{id}:live (HASH, 15s)  │ league:{lid}:season:{sid}:standings  │  │
│  │ match:{id}:events (LIST)     │  (STRING, 60s–5min)                  │  │
│  │ player:{pid}:profile (HASH)  │ standings:sorted (ZSET, no TTL)      │  │
│  │ matches:live (SET)           │ platform:live (PUB/SUB)              │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 데이터 갱신 주기별 분류

| 분류 | 테이블 | 갱신 주기 | 비고 |
|------|--------|-----------|------|
| **Static** | `countries`, `locales` | 거의 없음 | 앱 배포 시 seed |
| **Slow-moving** | `competitions`, `teams`, `players`, `venues`, `coaches` | 일~주 단위 | 이적, 감독 교체 |
| **Seasonal** | `competition_seasons`, `team_seasons`, `player_contracts`, `player_season_stats` | 시즌 단위 생성, 매치 후 갱신 | 시즌 종료 후 immutable |
| **Event-driven** | `matches`, `match_events`, `match_stats`, `match_lineups` | 실시간~분 단위 | 라이브 매치 중 빈번 갱신 |
| **Derived** | `mv_standings`, `mv_top_scorers` | 매치 종료 시 refresh | MATERIALIZED VIEW |
| **Ingestion** | `raw_payloads`, `ingestion_log` | 매 API 호출 시 | 보관 후 cold 이동 가능 |

---

## 3. 테이블 목록

### 전체 테이블 (27개 + 2 materialized views)

| # | Layer | 테이블명 | 유형 | 파티션 | MVP |
|---|-------|----------|------|--------|-----|
| 1 | Master | `locales` | reference | - | ✅ |
| 2 | Master | `countries` | entity | - | ✅ |
| 3 | Master | `competitions` | entity | - | ✅ |
| 4 | Master | `seasons` | entity | - | ✅ |
| 5 | Master | `venues` | entity | - | ⬜ |
| 6 | Master | `teams` | entity | - | ✅ |
| 7 | Master | `players` | entity | - | ✅ |
| 8 | Master | `coaches` | entity | - | ⬜ |
| 9 | i18n | `country_translations` | translation | - | ✅ |
| 10 | i18n | `competition_translations` | translation | - | ✅ |
| 11 | i18n | `team_translations` | translation | - | ✅ |
| 12 | i18n | `player_translations` | translation | - | ✅ |
| 13 | i18n | `venue_translations` | translation | - | ⬜ |
| 14 | i18n | `entity_aliases` | search | - | ✅ |
| 15 | Season | `competition_seasons` | junction | - | ✅ |
| 16 | Season | `team_seasons` | junction | - | ✅ |
| 17 | Season | `player_contracts` | junction | - | ✅ |
| 18 | Season | `player_season_stats` | stats | - | ✅ |
| 19 | Match | `matches` | event | ✅ RANGE | ✅ |
| 20 | Match | `match_events` | event | - | ✅ |
| 21 | Match | `match_stats` | event | - | ✅ |
| 22 | Match | `match_lineups` | event | - | ⬜ |
| 23 | View | `mv_standings` | materialized | - | ✅ |
| 24 | View | `mv_top_scorers` | materialized | - | ✅ |
| 25 | Source | `data_sources` | reference | - | ✅ |
| 26 | Source | `source_entity_mapping` | mapping | - | ✅ |
| 27 | Source | `raw_payloads` | audit | - | ⬜ |
| 28 | Source | `ingestion_log` | audit | - | ⬜ |

**MVP 필수**: 20개 (✅) / **확장**: 8개 (⬜)

---

## 4. 테이블 상세 (DDL)

### 4.0 Extensions & Enums

```sql
-- Required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;      -- trigram similarity search
CREATE EXTENSION IF NOT EXISTS btree_gist;   -- EXCLUDE constraints on seasons

-- Enums
CREATE TYPE position_type AS ENUM ('GK', 'DEF', 'MID', 'FWD');
CREATE TYPE preferred_foot AS ENUM ('Left', 'Right', 'Both');
CREATE TYPE match_status AS ENUM (
    'scheduled', 'timed',
    'live_1h', 'live_ht', 'live_2h', 'live_et', 'live_pen',
    'finished', 'finished_aet', 'finished_pen',
    'postponed', 'suspended', 'cancelled', 'awarded'
);
CREATE TYPE match_event_type AS ENUM (
    'goal', 'own_goal', 'penalty_scored', 'penalty_missed',
    'yellow_card', 'red_card', 'yellow_red_card',
    'substitution', 'var_decision'
);
CREATE TYPE competition_type AS ENUM ('league', 'cup', 'league_cup', 'super_cup', 'international');
CREATE TYPE alias_type AS ENUM ('official', 'common', 'abbreviation', 'historical', 'transliteration');
CREATE TYPE entity_type AS ENUM ('competition', 'team', 'player', 'country', 'venue', 'coach');
```

### 4.1 Layer 1 — Master Entity Tables

```sql
-- ═══════════════════════════════════════════════════════════
-- LOCALES (reference table for valid locales)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE locales (
    code        VARCHAR(10) PRIMARY KEY,          -- 'en', 'ko', 'ja'
    name        VARCHAR(100) NOT NULL,            -- 'English', 'Korean'
    is_default  BOOLEAN NOT NULL DEFAULT FALSE,
    fallback_to VARCHAR(10) REFERENCES locales(code),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Only one default locale allowed
CREATE UNIQUE INDEX idx_locales_single_default
    ON locales (is_default) WHERE is_default = TRUE;

INSERT INTO locales (code, name, is_default, fallback_to) VALUES
    ('en', 'English', TRUE,  NULL),
    ('ko', 'Korean',  FALSE, 'en');


-- ═══════════════════════════════════════════════════════════
-- COUNTRIES (nations / areas)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE countries (
    id              BIGSERIAL PRIMARY KEY,
    code_alpha2     CHAR(2) UNIQUE,               -- ISO 3166-1 alpha-2 (GB, ES)
    code_alpha3     CHAR(3) NOT NULL UNIQUE,       -- ISO 3166-1 alpha-3 (GBR, ESP)
    confederation   VARCHAR(20),                   -- UEFA, CONMEBOL, AFC...
    fifa_ranking     SMALLINT,
    flag_url        TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ═══════════════════════════════════════════════════════════
-- COMPETITIONS (leagues / cups / tournaments)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE competitions (
    id              BIGSERIAL PRIMARY KEY,
    slug            VARCHAR(100) NOT NULL UNIQUE,  -- 'premier-league', 'la-liga'
    code            VARCHAR(10) UNIQUE,            -- 'PL', 'LL' (short code)
    comp_type       competition_type NOT NULL DEFAULT 'league',
    country_id      BIGINT REFERENCES countries(id),
    emblem_url      TEXT,
    tier            SMALLINT DEFAULT 1,            -- 1 = top flight, 2 = second tier
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ═══════════════════════════════════════════════════════════
-- SEASONS
-- ═══════════════════════════════════════════════════════════
CREATE TABLE seasons (
    id              BIGSERIAL PRIMARY KEY,
    slug            VARCHAR(20) NOT NULL UNIQUE,   -- '2025-26'
    start_date      DATE NOT NULL,                 -- 2025-08-01
    end_date        DATE NOT NULL,                 -- 2026-05-31
    is_current      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Prevent overlapping seasons
    CONSTRAINT chk_season_dates CHECK (end_date > start_date),
    CONSTRAINT no_season_overlap EXCLUDE USING gist (
        daterange(start_date, end_date, '[]') WITH &&
    )
);

CREATE UNIQUE INDEX idx_seasons_single_current
    ON seasons (is_current) WHERE is_current = TRUE;


-- ═══════════════════════════════════════════════════════════
-- VENUES (stadiums)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE venues (
    id              BIGSERIAL PRIMARY KEY,
    slug            VARCHAR(100) NOT NULL UNIQUE,
    city            VARCHAR(100),
    country_id      BIGINT REFERENCES countries(id),
    capacity        INTEGER,
    surface         VARCHAR(50),                   -- 'grass', 'artificial'
    image_url       TEXT,
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ═══════════════════════════════════════════════════════════
-- TEAMS (clubs + national teams)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE teams (
    id              BIGSERIAL PRIMARY KEY,
    slug            VARCHAR(100) NOT NULL UNIQUE,  -- 'manchester-city'
    country_id      BIGINT NOT NULL REFERENCES countries(id),
    venue_id        BIGINT REFERENCES venues(id),
    founded_year    SMALLINT,
    is_national     BOOLEAN NOT NULL DEFAULT FALSE,
    crest_url       TEXT,
    primary_color   CHAR(7),                       -- '#6cabdd'
    secondary_color CHAR(7),                       -- '#1c2c5b'
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ═══════════════════════════════════════════════════════════
-- PLAYERS
-- ═══════════════════════════════════════════════════════════
CREATE TABLE players (
    id              BIGSERIAL PRIMARY KEY,
    slug            VARCHAR(150) NOT NULL UNIQUE,  -- 'erling-haaland'
    date_of_birth   DATE,
    country_id      BIGINT REFERENCES countries(id),
    position        position_type,
    height_cm       SMALLINT,
    weight_kg       SMALLINT,
    preferred_foot  preferred_foot,
    photo_url       TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ═══════════════════════════════════════════════════════════
-- COACHES
-- ═══════════════════════════════════════════════════════════
CREATE TABLE coaches (
    id              BIGSERIAL PRIMARY KEY,
    slug            VARCHAR(150) NOT NULL UNIQUE,
    date_of_birth   DATE,
    country_id      BIGINT REFERENCES countries(id),
    photo_url       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 4.2 Layer 2 — Localization Tables

```sql
-- ═══════════════════════════════════════════════════════════
-- TRANSLATION TABLES
-- Pattern: (entity_id, locale) → translated fields
-- PK = composite (entity_id + locale)
-- ═══════════════════════════════════════════════════════════

CREATE TABLE country_translations (
    country_id  BIGINT NOT NULL REFERENCES countries(id) ON DELETE CASCADE,
    locale      VARCHAR(10) NOT NULL REFERENCES locales(code),
    name        VARCHAR(255) NOT NULL,              -- 'England' / '잉글랜드'
    PRIMARY KEY (country_id, locale)
);
CREATE INDEX idx_country_tr_locale ON country_translations (locale);

CREATE TABLE competition_translations (
    competition_id BIGINT NOT NULL REFERENCES competitions(id) ON DELETE CASCADE,
    locale         VARCHAR(10) NOT NULL REFERENCES locales(code),
    name           VARCHAR(255) NOT NULL,           -- 'Premier League' / '프리미어리그'
    short_name     VARCHAR(50),                     -- 'PL' / 'EPL'
    PRIMARY KEY (competition_id, locale)
);
CREATE INDEX idx_comp_tr_locale ON competition_translations (locale);

CREATE TABLE team_translations (
    team_id     BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    locale      VARCHAR(10) NOT NULL REFERENCES locales(code),
    name        VARCHAR(255) NOT NULL,              -- 'Manchester City' / '맨체스터 시티'
    short_name  VARCHAR(50),                        -- 'Man City' / '맨시티'
    PRIMARY KEY (team_id, locale)
);
CREATE INDEX idx_team_tr_locale ON team_translations (locale);

CREATE TABLE player_translations (
    player_id   BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    locale      VARCHAR(10) NOT NULL REFERENCES locales(code),
    first_name  VARCHAR(100),                       -- 'Erling' / '엘링'
    last_name   VARCHAR(100),                       -- 'Haaland' / '홀란드'
    known_as    VARCHAR(150) NOT NULL,              -- 'Erling Haaland' / '엘링 홀란드'
    PRIMARY KEY (player_id, locale)
);
CREATE INDEX idx_player_tr_locale ON player_translations (locale);

CREATE TABLE venue_translations (
    venue_id    BIGINT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
    locale      VARCHAR(10) NOT NULL REFERENCES locales(code),
    name        VARCHAR(255) NOT NULL,              -- 'Etihad Stadium' / '에티하드 스타디움'
    PRIMARY KEY (venue_id, locale)
);
CREATE INDEX idx_venue_tr_locale ON venue_translations (locale);

CREATE TABLE coach_translations (
    coach_id    BIGINT NOT NULL REFERENCES coaches(id) ON DELETE CASCADE,
    locale      VARCHAR(10) NOT NULL REFERENCES locales(code),
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    known_as    VARCHAR(150) NOT NULL,
    PRIMARY KEY (coach_id, locale)
);
CREATE INDEX idx_coach_tr_locale ON coach_translations (locale);


-- ═══════════════════════════════════════════════════════════
-- ENTITY ALIASES (unified search across all entity types)
-- 맨시티, Man City, MCFC, Manchester City FC → team:1
-- ═══════════════════════════════════════════════════════════
CREATE TABLE entity_aliases (
    id                  BIGSERIAL PRIMARY KEY,
    entity_type         entity_type NOT NULL,
    entity_id           BIGINT NOT NULL,
    alias               VARCHAR(255) NOT NULL,
    locale              VARCHAR(10) REFERENCES locales(code),  -- NULL = language-agnostic
    alias_kind          alias_type NOT NULL DEFAULT 'common',
    is_primary          BOOLEAN NOT NULL DEFAULT FALSE,

    -- Auto-computed columns for search
    alias_normalized    VARCHAR(255) GENERATED ALWAYS AS (lower(alias)) STORED,
    search_vector       tsvector GENERATED ALWAYS AS (to_tsvector('simple', alias)) STORED,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Prevent duplicate aliases per entity
CREATE UNIQUE INDEX idx_entity_aliases_unique
    ON entity_aliases (entity_type, entity_id, lower(alias));

-- Full-text search (GIN)
CREATE INDEX idx_entity_aliases_fts
    ON entity_aliases USING GIN (search_vector);

-- Trigram fuzzy search (GIN)
CREATE INDEX idx_entity_aliases_trgm
    ON entity_aliases USING GIN (alias gin_trgm_ops);

-- Filter by entity type + locale
CREATE INDEX idx_entity_aliases_type_locale
    ON entity_aliases (entity_type, locale);
```

### 4.3 Layer 3 — Season-Scoped Tables

```sql
-- ═══════════════════════════════════════════════════════════
-- COMPETITION_SEASONS
-- "Premier League 2025-26" — links a competition to a season
-- ═══════════════════════════════════════════════════════════
CREATE TABLE competition_seasons (
    id              BIGSERIAL PRIMARY KEY,
    competition_id  BIGINT NOT NULL REFERENCES competitions(id),
    season_id       BIGINT NOT NULL REFERENCES seasons(id),
    current_matchday SMALLINT,                     -- 현재 라운드 (e.g., 15)
    total_matchdays  SMALLINT,                     -- 총 라운드 수 (e.g., 38)
    status          VARCHAR(20) NOT NULL DEFAULT 'active',  -- active, completed
    winner_team_id  BIGINT REFERENCES teams(id),   -- 시즌 우승팀 (종료 후 설정)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (competition_id, season_id)
);


-- ═══════════════════════════════════════════════════════════
-- TEAM_SEASONS
-- "Arsenal in Premier League 2025-26"
-- ═══════════════════════════════════════════════════════════
CREATE TABLE team_seasons (
    id                      BIGSERIAL PRIMARY KEY,
    team_id                 BIGINT NOT NULL REFERENCES teams(id),
    competition_season_id   BIGINT NOT NULL REFERENCES competition_seasons(id),
    coach_id                BIGINT REFERENCES coaches(id),

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (team_id, competition_season_id)
);


-- ═══════════════════════════════════════════════════════════
-- PLAYER_CONTRACTS
-- "Haaland at Man City for 2025-26 season, shirt #9"
-- One player can have only one active contract per competition_season
-- ═══════════════════════════════════════════════════════════
CREATE TABLE player_contracts (
    id                      BIGSERIAL PRIMARY KEY,
    player_id               BIGINT NOT NULL REFERENCES players(id),
    team_id                 BIGINT NOT NULL REFERENCES teams(id),
    competition_season_id   BIGINT NOT NULL REFERENCES competition_seasons(id),
    shirt_number            SMALLINT,
    is_on_loan              BOOLEAN NOT NULL DEFAULT FALSE,
    joined_date             DATE,
    left_date               DATE,                  -- NULL if still active

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (player_id, competition_season_id)
);

CREATE INDEX idx_player_contracts_team
    ON player_contracts (team_id, competition_season_id);


-- ═══════════════════════════════════════════════════════════
-- PLAYER_SEASON_STATS
-- Per-player, per-competition_season aggregate statistics
-- Updated after each match (via ingestion or trigger)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE player_season_stats (
    id                      BIGSERIAL PRIMARY KEY,
    player_id               BIGINT NOT NULL REFERENCES players(id),
    competition_season_id   BIGINT NOT NULL REFERENCES competition_seasons(id),

    -- Appearance
    appearances             SMALLINT NOT NULL DEFAULT 0,
    starts                  SMALLINT NOT NULL DEFAULT 0,
    minutes_played          INTEGER NOT NULL DEFAULT 0,

    -- Goals & Assists
    goals                   SMALLINT NOT NULL DEFAULT 0,
    assists                 SMALLINT NOT NULL DEFAULT 0,
    penalty_goals           SMALLINT NOT NULL DEFAULT 0,
    own_goals               SMALLINT NOT NULL DEFAULT 0,

    -- Discipline
    yellow_cards            SMALLINT NOT NULL DEFAULT 0,
    red_cards               SMALLINT NOT NULL DEFAULT 0,
    yellow_red_cards        SMALLINT NOT NULL DEFAULT 0,

    -- GK-specific
    clean_sheets            SMALLINT NOT NULL DEFAULT 0,
    goals_conceded          SMALLINT NOT NULL DEFAULT 0,
    saves                   SMALLINT NOT NULL DEFAULT 0,

    -- Match rating (if available from source)
    avg_rating              DECIMAL(3,1),

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (player_id, competition_season_id)
);

CREATE INDEX idx_player_stats_goals
    ON player_season_stats (competition_season_id, goals DESC);
CREATE INDEX idx_player_stats_assists
    ON player_season_stats (competition_season_id, assists DESC);
```

### 4.4 Layer 4 — Match/Event Tables (Partitioned)

```sql
-- ═══════════════════════════════════════════════════════════
-- MATCHES (PARTITIONED BY RANGE on match_date)
--
-- partition key(match_date)는 PK에 반드시 포함되어야 한다.
-- FK를 걸 때도 (id, match_date) 복합키로 참조한다.
-- ═══════════════════════════════════════════════════════════
CREATE TABLE matches (
    id                      BIGSERIAL,
    match_date              DATE NOT NULL,                     -- partition key
    competition_season_id   BIGINT NOT NULL REFERENCES competition_seasons(id),
    matchday                SMALLINT,                          -- 라운드 번호
    stage                   VARCHAR(30) DEFAULT 'REGULAR_SEASON',
    group_name              VARCHAR(20),                       -- GROUP_A, GROUP_B (cup only)

    home_team_id            BIGINT NOT NULL REFERENCES teams(id),
    away_team_id            BIGINT NOT NULL REFERENCES teams(id),
    home_score              SMALLINT,
    away_score              SMALLINT,
    home_ht_score           SMALLINT,                          -- 전반 스코어
    away_ht_score           SMALLINT,

    status                  match_status NOT NULL DEFAULT 'scheduled',
    kickoff_at              TIMESTAMPTZ,                       -- 정확한 킥오프 시간 (UTC)
    venue_id                BIGINT REFERENCES venues(id),
    attendance              INTEGER,
    referee                 VARCHAR(100),

    -- Duration metadata
    home_formation          VARCHAR(20),                       -- '4-3-3'
    away_formation          VARCHAR(20),

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id, match_date)  -- partition key must be in PK
) PARTITION BY RANGE (match_date);

-- Season-aligned partitions
CREATE TABLE matches_2023_24 PARTITION OF matches
    FOR VALUES FROM ('2023-07-01') TO ('2024-07-01');
CREATE TABLE matches_2024_25 PARTITION OF matches
    FOR VALUES FROM ('2024-07-01') TO ('2025-07-01');
CREATE TABLE matches_2025_26 PARTITION OF matches
    FOR VALUES FROM ('2025-07-01') TO ('2026-07-01');
CREATE TABLE matches_default PARTITION OF matches DEFAULT;

-- Indexes (inherited across all partitions)
CREATE INDEX idx_matches_comp_season ON matches (competition_season_id, match_date);
CREATE INDEX idx_matches_home ON matches (home_team_id, match_date);
CREATE INDEX idx_matches_away ON matches (away_team_id, match_date);
CREATE INDEX idx_matches_status ON matches (status) WHERE status IN ('scheduled', 'live_1h', 'live_ht', 'live_2h');
CREATE INDEX idx_matches_kickoff ON matches (kickoff_at);


-- ═══════════════════════════════════════════════════════════
-- MATCH_EVENTS (goals, cards, substitutions)
--
-- Normalized: 하나의 테이블에 모든 이벤트 유형 저장
-- football-data.org의 goals[]/bookings[]/substitutions[] +
-- API-Football의 events[] 양쪽 모두 이 테이블로 normalize
-- ═══════════════════════════════════════════════════════════
CREATE TABLE match_events (
    id              BIGSERIAL PRIMARY KEY,
    match_id        BIGINT NOT NULL,
    match_date      DATE NOT NULL,                 -- for partition-aware FK
    event_type      match_event_type NOT NULL,
    minute          SMALLINT NOT NULL,
    extra_minute    SMALLINT,                      -- 추가시간 분
    team_id         BIGINT NOT NULL REFERENCES teams(id),
    player_id       BIGINT REFERENCES players(id),
    secondary_player_id BIGINT REFERENCES players(id),  -- assist(goal) / player_in(sub)
    detail          VARCHAR(100),                  -- 'Normal Goal', 'Penalty', etc.

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date)
);

CREATE INDEX idx_match_events_match ON match_events (match_id, minute);
CREATE INDEX idx_match_events_player ON match_events (player_id, event_type);
CREATE INDEX idx_match_events_goals ON match_events (match_date, team_id)
    WHERE event_type IN ('goal', 'penalty_scored');


-- ═══════════════════════════════════════════════════════════
-- MATCH_STATS (team-level per-match statistics)
-- One row per team per match (= 2 rows per match)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE match_stats (
    id              BIGSERIAL PRIMARY KEY,
    match_id        BIGINT NOT NULL,
    match_date      DATE NOT NULL,
    team_id         BIGINT NOT NULL REFERENCES teams(id),
    is_home         BOOLEAN NOT NULL,

    -- Possession & Passing
    possession      SMALLINT,                      -- 0–100 (%)
    total_passes    SMALLINT,
    accurate_passes SMALLINT,
    pass_accuracy   SMALLINT,                      -- %

    -- Shots
    total_shots     SMALLINT,
    shots_on_target SMALLINT,
    shots_off_target SMALLINT,
    blocked_shots   SMALLINT,

    -- Set pieces
    corner_kicks    SMALLINT,
    free_kicks      SMALLINT,
    throw_ins       SMALLINT,

    -- Discipline & Duels
    fouls           SMALLINT,
    offsides        SMALLINT,
    gk_saves        SMALLINT,

    -- Advanced (if available from source)
    expected_goals  DECIMAL(4,2),                  -- xG

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date),
    UNIQUE (match_id, match_date, team_id)
);

CREATE INDEX idx_match_stats_match ON match_stats (match_id);


-- ═══════════════════════════════════════════════════════════
-- MATCH_LINEUPS (starting XI + bench)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE match_lineups (
    id              BIGSERIAL PRIMARY KEY,
    match_id        BIGINT NOT NULL,
    match_date      DATE NOT NULL,
    team_id         BIGINT NOT NULL REFERENCES teams(id),
    player_id       BIGINT NOT NULL REFERENCES players(id),
    shirt_number    SMALLINT,
    position        VARCHAR(30),                   -- 'Goalkeeper', 'Centre-Back', etc.
    grid_position   VARCHAR(10),                   -- '1:1', '2:3' (grid layout)
    is_starter      BOOLEAN NOT NULL DEFAULT TRUE,
    minutes_played  SMALLINT,
    rating          DECIMAL(3,1),

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date),
    UNIQUE (match_id, match_date, team_id, player_id)
);

CREATE INDEX idx_match_lineups_match ON match_lineups (match_id, team_id);
CREATE INDEX idx_match_lineups_player ON match_lineups (player_id);
```

### 4.5 Layer 5 — Materialized Views

```sql
-- ═══════════════════════════════════════════════════════════
-- MV_STANDINGS — 리그 순위표
-- 매치 결과로부터 자동 계산
-- ═══════════════════════════════════════════════════════════
CREATE MATERIALIZED VIEW mv_standings AS
WITH match_results AS (
    -- 매 경기를 홈/어웨이 팀 각각 1행으로 펼침
    SELECT
        m.competition_season_id,
        m.home_team_id                                          AS team_id,
        CASE WHEN m.home_score > m.away_score THEN 3
             WHEN m.home_score = m.away_score THEN 1
             ELSE 0 END                                         AS points,
        CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END AS won,
        CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END AS drawn,
        CASE WHEN m.home_score < m.away_score THEN 1 ELSE 0 END AS lost,
        m.home_score                                            AS goals_for,
        m.away_score                                            AS goals_against,
        m.match_date
    FROM matches m
    WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')

    UNION ALL

    SELECT
        m.competition_season_id,
        m.away_team_id,
        CASE WHEN m.away_score > m.home_score THEN 3
             WHEN m.away_score = m.home_score THEN 1
             ELSE 0 END,
        CASE WHEN m.away_score > m.home_score THEN 1 ELSE 0 END,
        CASE WHEN m.away_score = m.home_score THEN 1 ELSE 0 END,
        CASE WHEN m.away_score < m.home_score THEN 1 ELSE 0 END,
        m.away_score,
        m.home_score,
        m.match_date
    FROM matches m
    WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')
)
SELECT
    mr.competition_season_id,
    mr.team_id,
    COUNT(*)::SMALLINT                                            AS played,
    SUM(mr.won)::SMALLINT                                         AS won,
    SUM(mr.drawn)::SMALLINT                                       AS drawn,
    SUM(mr.lost)::SMALLINT                                        AS lost,
    SUM(mr.goals_for)::SMALLINT                                   AS goals_for,
    SUM(mr.goals_against)::SMALLINT                               AS goals_against,
    (SUM(mr.goals_for) - SUM(mr.goals_against))::SMALLINT         AS goal_difference,
    SUM(mr.points)::SMALLINT                                      AS points,
    RANK() OVER (
        PARTITION BY mr.competition_season_id
        ORDER BY SUM(mr.points) DESC,
                 SUM(mr.goals_for) - SUM(mr.goals_against) DESC,
                 SUM(mr.goals_for) DESC
    )::SMALLINT                                                   AS position
FROM match_results mr
GROUP BY mr.competition_season_id, mr.team_id
WITH DATA;

-- REQUIRED for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX idx_mv_standings_unique
    ON mv_standings (competition_season_id, team_id);
CREATE INDEX idx_mv_standings_position
    ON mv_standings (competition_season_id, position);


-- ═══════════════════════════════════════════════════════════
-- MV_TOP_SCORERS — 득점 랭킹
-- ═══════════════════════════════════════════════════════════
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
    AND pc.left_date IS NULL
WHERE pss.goals > 0
WITH DATA;

CREATE UNIQUE INDEX idx_mv_top_scorers_unique
    ON mv_top_scorers (competition_season_id, player_id);
CREATE INDEX idx_mv_top_scorers_rank
    ON mv_top_scorers (competition_season_id, rank);
```

### 4.6 Layer 6 — Source Mapping & Ingestion

```sql
-- ═══════════════════════════════════════════════════════════
-- DATA_SOURCES (registered external API providers)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE data_sources (
    id          BIGSERIAL PRIMARY KEY,
    slug        VARCHAR(50) NOT NULL UNIQUE,       -- 'football_data_org', 'api_football'
    name        VARCHAR(100) NOT NULL,             -- 'football-data.org v4'
    base_url    TEXT,                              -- 'https://api.football-data.org/v4'
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    priority    SMALLINT NOT NULL DEFAULT 1,       -- 충돌 시 우선순위 (낮을수록 높음)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO data_sources (slug, name, base_url, priority) VALUES
    ('football_data_org', 'football-data.org v4', 'https://api.football-data.org/v4', 1),
    ('api_football',      'API-Football v3',      'https://v3.football.api-sports.io', 2);


-- ═══════════════════════════════════════════════════════════
-- SOURCE_ENTITY_MAPPING
-- 내부 canonical ID ↔ 외부 API ID 매핑
-- ═══════════════════════════════════════════════════════════
CREATE TABLE source_entity_mapping (
    id              BIGSERIAL PRIMARY KEY,
    entity_type     entity_type NOT NULL,
    entity_id       BIGINT NOT NULL,               -- 내부 canonical ID
    source_id       BIGINT NOT NULL REFERENCES data_sources(id),
    external_id     TEXT NOT NULL,                  -- 외부 ID (TEXT: int/string 모두 수용)
    external_code   VARCHAR(20),                    -- 보조 코드 (FD.org의 'PL', 'CL' 등)
    season_context  VARCHAR(20),                    -- '2025' 또는 '2025/2026'
    metadata        JSONB,                          -- source별 추가 정보

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (entity_type, source_id, external_id)
);

CREATE INDEX idx_source_mapping_entity
    ON source_entity_mapping (entity_type, entity_id);
CREATE INDEX idx_source_mapping_external
    ON source_entity_mapping (source_id, external_id);


-- ═══════════════════════════════════════════════════════════
-- RAW_PAYLOADS (외부 API 응답 원본 저장)
-- 디버깅, 재처리, 감사 추적용
-- ═══════════════════════════════════════════════════════════
CREATE TABLE raw_payloads (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT NOT NULL REFERENCES data_sources(id),
    endpoint        TEXT NOT NULL,                  -- '/v4/matches/330299'
    entity_type     entity_type,
    external_id     TEXT,
    season_context  VARCHAR(20),
    http_status     SMALLINT,
    payload         JSONB NOT NULL,
    payload_hash    VARCHAR(64),                    -- SHA-256 for dedup
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_raw_payloads_source_entity
    ON raw_payloads (source_id, entity_type, external_id);
CREATE INDEX idx_raw_payloads_fetched
    ON raw_payloads (fetched_at DESC);


-- ═══════════════════════════════════════════════════════════
-- INGESTION_LOG (ETL 파이프라인 실행 로그)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE ingestion_log (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT NOT NULL REFERENCES data_sources(id),
    job_type        VARCHAR(50) NOT NULL,           -- 'sync_matches', 'sync_standings'
    status          VARCHAR(20) NOT NULL DEFAULT 'running', -- running, completed, failed
    entities_created INTEGER DEFAULT 0,
    entities_updated INTEGER DEFAULT 0,
    errors          JSONB,                          -- [{entity_type, external_id, error}]
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    duration_ms     INTEGER
);
```

---

## 5. i18n 설계

### 5.1 구조

```
locales (locale 레지스트리, fallback chain 정의)
    ↓ FK
*_translations (엔티티별 1:N 번역 테이블)
    ↓
entity_aliases (통합 검색 alias, pg_trgm + tsvector)
```

### 5.2 Locale Fallback 전략

```
요청 locale → fallback chain 순서로 탐색:
  ko-KR → ko → en (default)
  ja    → en (default)
  ar    → en (default)
```

**구현 패턴 A — 고 커버리지 (≥90% 번역 완료) — LEFT JOIN + COALESCE:**

```sql
-- 대부분의 팀에 ko 번역이 있을 때 최적
SELECT
    t.id,
    t.slug,
    COALESCE(tr_ko.name, tr_en.name) AS name,
    COALESCE(tr_ko.short_name, tr_en.short_name) AS short_name
FROM teams t
LEFT JOIN team_translations tr_ko ON tr_ko.team_id = t.id AND tr_ko.locale = 'ko'
LEFT JOIN team_translations tr_en ON tr_en.team_id = t.id AND tr_en.locale = 'en'
WHERE t.id = $1;
```

**구현 패턴 B — 대량 조회 (순위표 20팀 등) — DISTINCT ON:**

```sql
WITH ranked AS (
    SELECT
        tt.team_id,
        tt.name,
        tt.short_name,
        CASE tt.locale
            WHEN 'ko' THEN 1
            WHEN 'en' THEN 2
            ELSE 3
        END AS priority
    FROM team_translations tt
    WHERE tt.team_id = ANY($1::BIGINT[])
      AND tt.locale IN ('ko', 'en')
)
SELECT DISTINCT ON (team_id)
    team_id, name, short_name
FROM ranked
ORDER BY team_id, priority;
```

### 5.3 번역 범위

| 번역 대상 | 번역하는 필드 | 번역하지 않는 필드 |
|-----------|-------------|------------------|
| 팀 | name, short_name | slug, founded_year, colors |
| 선수 | first_name, last_name, known_as | date_of_birth, height, position |
| 대회 | name, short_name | slug, code, tier |
| 국가 | name | code_alpha2, code_alpha3, confederation |
| 경기장 | name | city, capacity, surface |

### 5.4 Alias Search 전략

```
검색 "맨시티" 입력 시:

1단계 (Exact):     entity_aliases WHERE lower(alias) = '맨시티'
                   → team:1 (score 1.0)

2단계 (FTS):       entity_aliases WHERE search_vector @@ plainto_tsquery('simple', '맨시티')
                   → team:1 via '맨체스터 시티' (score 0.8)

3단계 (Fuzzy):     entity_aliases WHERE alias % '맨시티'  (pg_trgm)
                   → team:1 via '맨체스터시티' (score 0.5)
```

**통합 검색 함수:**

```sql
CREATE OR REPLACE FUNCTION search_entities(
    p_query      TEXT,
    p_locale     VARCHAR(10) DEFAULT NULL,
    p_entity_type entity_type DEFAULT NULL,
    p_limit      INT DEFAULT 20
)
RETURNS TABLE (
    entity_type  entity_type,
    entity_id    BIGINT,
    matched_alias TEXT,
    match_type   TEXT,
    score        FLOAT
) LANGUAGE sql STABLE AS $$
    -- Exact match
    SELECT ea.entity_type, ea.entity_id, ea.alias, 'exact'::TEXT, 1.0::FLOAT
    FROM entity_aliases ea
    WHERE lower(ea.alias) = lower(p_query)
      AND (p_locale IS NULL OR ea.locale = p_locale OR ea.locale IS NULL)
      AND (p_entity_type IS NULL OR ea.entity_type = p_entity_type)

    UNION ALL

    -- Full-text search
    SELECT ea.entity_type, ea.entity_id, ea.alias, 'fts',
           ts_rank(ea.search_vector, plainto_tsquery('simple', p_query))::FLOAT
    FROM entity_aliases ea
    WHERE ea.search_vector @@ plainto_tsquery('simple', p_query)
      AND lower(ea.alias) != lower(p_query)
      AND (p_locale IS NULL OR ea.locale = p_locale OR ea.locale IS NULL)
      AND (p_entity_type IS NULL OR ea.entity_type = p_entity_type)

    UNION ALL

    -- Trigram fuzzy (catches typos)
    SELECT ea.entity_type, ea.entity_id, ea.alias, 'fuzzy',
           similarity(ea.alias, p_query)::FLOAT
    FROM entity_aliases ea
    WHERE ea.alias % p_query
      AND lower(ea.alias) != lower(p_query)
      AND (p_locale IS NULL OR ea.locale = p_locale OR ea.locale IS NULL)
      AND (p_entity_type IS NULL OR ea.entity_type = p_entity_type)

    ORDER BY
        CASE match_type WHEN 'exact' THEN 0 WHEN 'fts' THEN 1 ELSE 2 END,
        score DESC
    LIMIT p_limit;
$$;
```

---

## 6. Hot/Cold 전략

### 6.1 데이터 온도 분류

```
┌─────────── HOT (현재 시즌) ───────────┐
│                                        │
│  matches_2025_26 (partition)           │
│  player_season_stats (current season)  │
│  mv_standings (current season)         │
│  mv_top_scorers (current season)       │
│  competition_seasons (is_current)      │
│                                        │
│  NVMe tablespace                       │
│  Redis TTL: 15s–5min                   │
│  MV refresh: 매치 종료 시 즉시         │
│                                        │
├─────────── WARM (직전 시즌) ──────────┤
│                                        │
│  matches_2024_25 (partition)           │
│  player_season_stats (prev season)     │
│                                        │
│  SSD tablespace                        │
│  Redis TTL: 1hr–24hr                   │
│  MV refresh: 없음 (데이터 불변)        │
│                                        │
├─────────── COLD (과거 시즌) ──────────┤
│                                        │
│  matches_2023_24, matches_2022_23...   │
│  historical player_season_stats        │
│  raw_payloads (6개월 이상)             │
│                                        │
│  HDD tablespace                        │
│  Redis TTL: 7–30일                     │
│  MV refresh: 없음                      │
│                                        │
└────────────────────────────────────────┘
```

### 6.2 Tablespace 적용

```sql
-- Tablespace 생성
CREATE TABLESPACE hot_data  LOCATION '/mnt/nvme/pg';
CREATE TABLESPACE warm_data LOCATION '/mnt/ssd/pg';
CREATE TABLESPACE cold_data LOCATION '/mnt/hdd/pg';

-- 시즌 종료 후 파티션 이동 (zero-downtime)
ALTER TABLE matches_2023_24 SET TABLESPACE cold_data;
ALTER TABLE matches_2024_25 SET TABLESPACE warm_data;
-- matches_2025_26는 hot_data에 생성
```

### 6.3 Redis TTL 전략

| 데이터 | Redis Key Pattern | TTL | 무효화 트리거 |
|--------|-------------------|-----|-------------|
| 라이브 스코어 | `match:{id}:live` | 15–30s | 골/카드 이벤트 |
| 라이브 이벤트 | `match:{id}:events` | 2hr | append-only |
| 매치 통계 | `match:{id}:stats` | 30s | 실시간 업데이트 |
| 순위표 (경기 중) | `league:{lid}:season:{sid}:standings` | 60s | 골 이벤트 → DEL |
| 순위표 (경기 없음) | 〃 | 5min | 매치 FT → DEL |
| 선수 시즌 스탯 | `player:{pid}:season:{sid}:stats` | 1–6hr | 매치 FT |
| 선수 프로필 | `player:{pid}:profile` | 24hr | 이적/부상 |
| 팀 프로필 | `team:{tid}:profile` | 24hr | 매우 안정적 |
| 팀 스쿼드 | `team:{tid}:season:{sid}:squad` | 6hr | 이적창 |
| 경기 확정 결과 | `match:{id}:result` | 7일 | 불변 (final) |
| 완료된 시즌 | `league:{lid}:season:{sid}:archive` | 30일 | 불변 |
| 레퍼런스 데이터 | `ref:leagues`, `ref:countries` | 24–72hr | 거의 변동 없음 |

### 6.4 Cache Invalidation Cascade

```
골 이벤트 발생 (match 12345에서)
│
├── IMMEDIATE (pipeline atomic)
│   ├── HSET match:12345:live (score 업데이트)
│   ├── RPUSH match:12345:events (이벤트 추가)
│   ├── DEL league:39:season:2526:standings (순위 무효화)
│   └── ZINCRBY league:39:season:2526:standings:sorted (sorted set 업데이트)
│
├── DEFERRED (FT 휘슬 후)
│   ├── SETEX match:12345:result (확정 결과 저장, 7일 TTL)
│   ├── SREM matches:live "12345" (라이브 셋에서 제거)
│   ├── DEL match:12345:live, match:12345:stats
│   └── DEL player:{scorer_id}:season:2526:stats
│
└── BACKGROUND (async worker)
    ├── REFRESH MATERIALIZED VIEW CONCURRENTLY mv_standings
    └── REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_scorers
```

---

## 7. 읽기 최적화

### 7.1 Materialized View Refresh 전략

| View | Refresh 트리거 | 주기 | 방법 |
|------|--------------|------|------|
| `mv_standings` | 매치 status → 'finished' 변경 시 | 이벤트 기반 + 5분마다 | `pg_notify` → worker |
| `mv_top_scorers` | 매치 종료 후 | 1시간마다 | `pg_cron` |

```sql
-- pg_notify trigger: 매치 종료 시 standings refresh 신호
CREATE OR REPLACE FUNCTION notify_standings_refresh()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('standings_refresh',
        json_build_object(
            'competition_season_id', NEW.competition_season_id,
            'match_id', NEW.id
        )::TEXT
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_match_finished
    AFTER UPDATE OF status ON matches
    FOR EACH ROW
    WHEN (NEW.status IN ('finished', 'finished_aet', 'finished_pen')
          AND OLD.status NOT IN ('finished', 'finished_aet', 'finished_pen'))
    EXECUTE FUNCTION notify_standings_refresh();
```

### 7.2 Redis Sorted Set for Standings

```
# 복합 score 공식: points * 1,000,000 + (goal_diff + 200) * 1,000 + goals_for
# goal_diff에 200을 더해서 음수 방지

ZADD league:39:season:2526:standings:sorted
    87203045  "team:11"   -- Man City: 87pts, +30 GD, 45 GF
    84200038  "team:33"   -- Man Utd: 84pts, +0 GD, 38 GF

# 순위표 조회 (1~20위)
ZREVRANGE league:39:season:2526:standings:sorted 0 19 WITHSCORES

# 특정 팀 순위 조회
ZREVRANK league:39:season:2526:standings:sorted "team:11"
```

### 7.3 Partition Pruning 확인

```sql
-- PostgreSQL은 WHERE 절의 match_date로 불필요한 파티션을 자동 제외
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM matches
WHERE match_date BETWEEN '2025-08-01' AND '2026-05-31'
  AND home_team_id = 42;

-- 예상 출력: "Partitions selected: matches_2025_26"
-- (나머지 파티션은 pruned)
```

---

## 8. 인덱스 전략

### 8.1 인덱스 목록 (전체)

| 테이블 | 인덱스 | 유형 | 용도 |
|--------|--------|------|------|
| **matches** | `(competition_season_id, match_date)` | B-tree | 리그별 + 날짜 범위 조회 |
| | `(home_team_id, match_date)` | B-tree | 팀별 홈 경기 |
| | `(away_team_id, match_date)` | B-tree | 팀별 원정 경기 |
| | `(status) WHERE status IN (scheduled, live_*)` | Partial B-tree | 라이브/예정 경기만 |
| | `(kickoff_at)` | B-tree | 시간순 정렬 |
| **match_events** | `(match_id, minute)` | B-tree | 매치 이벤트 타임라인 |
| | `(player_id, event_type)` | B-tree | 선수별 이벤트 조회 |
| | `(match_date, team_id) WHERE event_type IN (goal, penalty_scored)` | Partial B-tree | 팀별 득점 조회 |
| **match_stats** | `(match_id)` | B-tree | 매치 통계 조회 |
| **player_season_stats** | `(competition_season_id, goals DESC)` | B-tree | 득점 랭킹 |
| | `(competition_season_id, assists DESC)` | B-tree | 어시스트 랭킹 |
| **player_contracts** | `(team_id, competition_season_id)` | B-tree | 팀별 스쿼드 조회 |
| **entity_aliases** | `(entity_type, entity_id, lower(alias))` | Unique B-tree | 중복 방지 |
| | `search_vector` | GIN | 전문 검색 |
| | `alias` | GIN (pg_trgm) | 퍼지 검색 |
| | `(entity_type, locale)` | B-tree | 유형+언어별 필터 |
| **source_entity_mapping** | `(entity_type, entity_id)` | B-tree | 내부 ID → 외부 ID |
| | `(source_id, external_id)` | B-tree | 외부 ID → 내부 ID |
| **mv_standings** | `(competition_season_id, team_id)` | Unique | REFRESH CONCURRENTLY 필수 |
| | `(competition_season_id, position)` | B-tree | 순위 조회 |
| **mv_top_scorers** | `(competition_season_id, player_id)` | Unique | REFRESH CONCURRENTLY 필수 |
| | `(competition_season_id, rank)` | B-tree | 랭킹 조회 |
| ***_translations** | `(locale)` 또는 `(locale, entity_id)` | B-tree | locale별 조회 |

### 8.2 Partial Index 활용

```sql
-- 예정/라이브 경기만 인덱싱 (전체 매치 중 극소수)
CREATE INDEX idx_matches_upcoming ON matches (kickoff_at)
    WHERE status = 'scheduled';

-- 현재 활성 계약만 인덱싱
CREATE INDEX idx_contracts_active ON player_contracts (team_id, player_id)
    WHERE left_date IS NULL;
```

---

## 9. 예시 쿼리 패턴

### 9.1 리그 순위표 (i18n 포함)

```sql
-- /leagues/:id — 순위표 + 팀명 한국어 표시
SELECT
    s.position,
    s.team_id,
    COALESCE(tt_ko.name, tt_en.name) AS team_name,
    COALESCE(tt_ko.short_name, tt_en.short_name) AS team_short_name,
    t.crest_url,
    s.played, s.won, s.drawn, s.lost,
    s.goals_for, s.goals_against, s.goal_difference, s.points
FROM mv_standings s
JOIN teams t ON t.id = s.team_id
LEFT JOIN team_translations tt_ko ON tt_ko.team_id = t.id AND tt_ko.locale = 'ko'
LEFT JOIN team_translations tt_en ON tt_en.team_id = t.id AND tt_en.locale = 'en'
WHERE s.competition_season_id = $1
ORDER BY s.position;
```

### 9.2 선수 프로필 + 시즌 스탯

```sql
-- /players/:id — 선수 상세 페이지
SELECT
    p.id,
    COALESCE(pt_ko.known_as, pt_en.known_as) AS name,
    COALESCE(pt_ko.first_name, pt_en.first_name) AS first_name,
    COALESCE(pt_ko.last_name, pt_en.last_name) AS last_name,
    p.date_of_birth,
    EXTRACT(YEAR FROM age(p.date_of_birth))::INT AS age,
    p.position,
    p.height_cm,
    p.preferred_foot,
    p.photo_url,
    COALESCE(cnt_ko.name, cnt_en.name) AS nationality,
    COALESCE(tt_ko.name, tt_en.name) AS club_name,
    t.crest_url AS club_crest,
    pc.shirt_number,
    -- Season stats
    pss.appearances, pss.starts, pss.minutes_played,
    pss.goals, pss.assists, pss.penalty_goals,
    pss.yellow_cards, pss.red_cards,
    pss.clean_sheets, pss.avg_rating
FROM players p
-- Player name (i18n)
LEFT JOIN player_translations pt_ko ON pt_ko.player_id = p.id AND pt_ko.locale = 'ko'
LEFT JOIN player_translations pt_en ON pt_en.player_id = p.id AND pt_en.locale = 'en'
-- Current contract
LEFT JOIN player_contracts pc
    ON pc.player_id = p.id AND pc.left_date IS NULL
-- Club info
LEFT JOIN teams t ON t.id = pc.team_id
LEFT JOIN team_translations tt_ko ON tt_ko.team_id = t.id AND tt_ko.locale = 'ko'
LEFT JOIN team_translations tt_en ON tt_en.team_id = t.id AND tt_en.locale = 'en'
-- Nationality (i18n)
LEFT JOIN countries c ON c.id = p.country_id
LEFT JOIN country_translations cnt_ko ON cnt_ko.country_id = c.id AND cnt_ko.locale = 'ko'
LEFT JOIN country_translations cnt_en ON cnt_en.country_id = c.id AND cnt_en.locale = 'en'
-- Season stats (current season)
LEFT JOIN player_season_stats pss
    ON pss.player_id = p.id
    AND pss.competition_season_id = pc.competition_season_id
WHERE p.id = $1;
```

### 9.3 팀 스쿼드 목록

```sql
-- /clubs/:id — 스쿼드 (현재 시즌)
SELECT
    p.id AS player_id,
    COALESCE(pt_ko.known_as, pt_en.known_as) AS name,
    p.position,
    p.date_of_birth,
    EXTRACT(YEAR FROM age(p.date_of_birth))::INT AS age,
    COALESCE(cnt_ko.name, cnt_en.name) AS nationality,
    c.flag_url,
    pc.shirt_number,
    pss.appearances, pss.goals, pss.assists
FROM player_contracts pc
JOIN players p ON p.id = pc.player_id
LEFT JOIN player_translations pt_ko ON pt_ko.player_id = p.id AND pt_ko.locale = 'ko'
LEFT JOIN player_translations pt_en ON pt_en.player_id = p.id AND pt_en.locale = 'en'
LEFT JOIN countries c ON c.id = p.country_id
LEFT JOIN country_translations cnt_ko ON cnt_ko.country_id = c.id AND cnt_ko.locale = 'ko'
LEFT JOIN country_translations cnt_en ON cnt_en.country_id = c.id AND cnt_en.locale = 'en'
LEFT JOIN player_season_stats pss
    ON pss.player_id = p.id
    AND pss.competition_season_id = pc.competition_season_id
WHERE pc.team_id = $1
  AND pc.competition_season_id = $2
  AND pc.left_date IS NULL
ORDER BY
    CASE p.position WHEN 'GK' THEN 1 WHEN 'DEF' THEN 2 WHEN 'MID' THEN 3 WHEN 'FWD' THEN 4 END,
    pc.shirt_number;
```

### 9.4 최근 경기 결과

```sql
-- /results — 최근 종료 경기 (리그 필터 가능)
SELECT
    m.id AS match_id,
    m.match_date,
    m.kickoff_at,
    m.matchday,
    m.home_score,
    m.away_score,
    COALESCE(ht_ko.name, ht_en.name) AS home_team_name,
    COALESCE(ht_ko.short_name, ht_en.short_name) AS home_team_short,
    ht.crest_url AS home_crest,
    COALESCE(at_ko.name, at_en.name) AS away_team_name,
    COALESCE(at_ko.short_name, at_en.short_name) AS away_team_short,
    at.crest_url AS away_crest,
    COALESCE(ct_ko.name, ct_en.name) AS competition_name
FROM matches m
JOIN teams ht ON ht.id = m.home_team_id
JOIN teams at ON at.id = m.away_team_id
JOIN competition_seasons cs ON cs.id = m.competition_season_id
JOIN competitions comp ON comp.id = cs.competition_id
LEFT JOIN team_translations ht_ko ON ht_ko.team_id = ht.id AND ht_ko.locale = 'ko'
LEFT JOIN team_translations ht_en ON ht_en.team_id = ht.id AND ht_en.locale = 'en'
LEFT JOIN team_translations at_ko ON at_ko.team_id = at.id AND at_ko.locale = 'ko'
LEFT JOIN team_translations at_en ON at_en.team_id = at.id AND at_en.locale = 'en'
LEFT JOIN competition_translations ct_ko ON ct_ko.competition_id = comp.id AND ct_ko.locale = 'ko'
LEFT JOIN competition_translations ct_en ON ct_en.competition_id = comp.id AND ct_en.locale = 'en'
WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')
  AND ($1::BIGINT IS NULL OR cs.competition_id = $1)  -- 선택적 리그 필터
ORDER BY m.match_date DESC, m.kickoff_at DESC
LIMIT 20;
```

### 9.5 통합 검색 (한국어/영어)

```sql
-- /search?q=맨시티 — 모든 엔티티에서 검색
SELECT * FROM search_entities('맨시티', 'ko', NULL, 20);

-- /search?q=Haaland — 선수만 검색
SELECT * FROM search_entities('Haaland', NULL, 'player', 10);
```

### 9.6 외부 ID로 내부 엔티티 조회 (Ingestion용)

```sql
-- API-Football fixture ID 1208021 → 내부 match 조회
SELECT m.*
FROM matches m
JOIN source_entity_mapping sem
    ON sem.entity_type = 'match'
    AND sem.entity_id = m.id  -- NOTE: 파티션 테이블에서는 match_date도 필요할 수 있음
WHERE sem.source_id = (SELECT id FROM data_sources WHERE slug = 'api_football')
  AND sem.external_id = '1208021';
```

---

## 10. MVP 필수 테이블 vs 확장 테이블

### MVP (Phase 1) — 20개 테이블 + 2 materialized views

서비스 런칭에 반드시 필요한 최소 테이블. 현재 MatchIndex 프론트엔드의 8개 페이지를 모두 지원한다.

```
 ┌─ Reference ──────────────────────────────────┐
 │  locales                                      │
 │  data_sources                                 │
 └───────────────────────────────────────────────┘

 ┌─ Master Entity ──────────────────────────────┐
 │  countries        ← Nation 페이지             │
 │  competitions     ← League 페이지             │
 │  seasons          ← 시즌 스코프 기준           │
 │  teams            ← Club 페이지               │
 │  players          ← Player 페이지             │
 └───────────────────────────────────────────────┘

 ┌─ i18n ───────────────────────────────────────┐
 │  country_translations                         │
 │  competition_translations                     │
 │  team_translations                            │
 │  player_translations                          │
 │  entity_aliases    ← Search 페이지            │
 └───────────────────────────────────────────────┘

 ┌─ Season-Scoped ──────────────────────────────┐
 │  competition_seasons                          │
 │  team_seasons                                 │
 │  player_contracts                             │
 │  player_season_stats                          │
 └───────────────────────────────────────────────┘

 ┌─ Match/Event ────────────────────────────────┐
 │  matches (PARTITIONED)  ← Match/Results 페이지│
 │  match_events           ← Match 이벤트 타임라인│
 │  match_stats            ← Match 통계           │
 └───────────────────────────────────────────────┘

 ┌─ Source Mapping ─────────────────────────────┐
 │  source_entity_mapping  ← 외부 API ID 매핑    │
 └───────────────────────────────────────────────┘

 ┌─ Materialized Views ─────────────────────────┐
 │  mv_standings           ← Dashboard + League  │
 │  mv_top_scorers         ← Dashboard + League  │
 └───────────────────────────────────────────────┘
```

### 확장 (Phase 2) — 8개 테이블

서비스 성장 후 추가:

| 테이블 | 용도 | 우선순위 |
|--------|------|---------|
| `venues` + `venue_translations` | 경기장 상세 정보 (위치, 수용인원, 이미지) | 중 |
| `coaches` + `coach_translations` | 감독 프로필 | 중 |
| `match_lineups` | 선발 라인업 + 교체 명단 | 높음 |
| `raw_payloads` | API 응답 원본 저장 (디버깅/재처리) | 낮음 |
| `ingestion_log` | ETL 파이프라인 모니터링 | 낮음 |

### MVP 마이그레이션 순서

```
1. locales, data_sources                              (reference seed)
2. countries + country_translations                   (entity + i18n)
3. competitions + competition_translations            (entity + i18n)
4. seasons                                            (시즌 정의)
5. teams + team_translations                          (entity + i18n)
6. players + player_translations                      (entity + i18n)
7. entity_aliases                                     (검색)
8. competition_seasons, team_seasons, player_contracts (시즌 연결)
9. player_season_stats                                (통계)
10. matches (+ partitions)                            (경기 데이터)
11. match_events, match_stats                         (이벤트/통계)
12. source_entity_mapping                             (외부 API 매핑)
13. mv_standings, mv_top_scorers                      (materialized views)
```

---

## 부록: 현재 TypeScript ↔ PostgreSQL 매핑

| 현재 TypeScript Interface | PostgreSQL 테이블 | 비고 |
|--------------------------|-------------------|------|
| `League` | `competitions` + `competition_translations` + `competition_seasons` | 시즌을 분리 |
| `Club` | `teams` + `team_translations` + `team_seasons` | leagueId → team_seasons 조인 |
| `Player` | `players` + `player_translations` + `player_contracts` | clubId → player_contracts |
| `Player.seasonStats` | `player_season_stats` | 별도 테이블로 분리 |
| `Nation` | `countries` + `country_translations` | |
| `Match` | `matches` | 파티셔닝 추가 |
| `MatchEvent` | `match_events` | normalized 이벤트 모델 |
| `MatchStats` | `match_stats` | [home,away] → 2행으로 분리 |
| `StandingRow` | `mv_standings` | materialized view |
| `StatLeader` | `mv_top_scorers` | materialized view |
| `SearchResult` | `search_entities()` 함수 | entity_aliases 기반 |
