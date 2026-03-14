import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

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

async function main() {
  loadProjectEnv();
  const sql = getSql();
  try {
    await sql.unsafe(`
      ALTER TABLE player_contracts
        ADD COLUMN IF NOT EXISTS contract_start_date DATE,
        ADD COLUMN IF NOT EXISTS contract_end_date DATE,
        ADD COLUMN IF NOT EXISTS annual_salary_eur INTEGER,
        ADD COLUMN IF NOT EXISTS weekly_wage_eur INTEGER,
        ADD COLUMN IF NOT EXISTS market_value_eur INTEGER,
        ADD COLUMN IF NOT EXISTS salary_currency CHAR(3),
        ADD COLUMN IF NOT EXISTS salary_source VARCHAR(50),
        ADD COLUMN IF NOT EXISTS salary_source_url TEXT,
        ADD COLUMN IF NOT EXISTS salary_is_estimated BOOLEAN NOT NULL DEFAULT TRUE,
        ADD COLUMN IF NOT EXISTS salary_updated_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS market_value_source VARCHAR(50),
        ADD COLUMN IF NOT EXISTS market_value_source_url TEXT,
        ADD COLUMN IF NOT EXISTS market_value_updated_at TIMESTAMPTZ
    `);

    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS player_market_values (
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
      )
    `);

    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS player_transfers (
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
      )
    `);

    await sql.unsafe(`
      CREATE INDEX IF NOT EXISTS idx_player_market_values_player_date
        ON player_market_values (player_id, observed_at DESC)
    `);

    await sql.unsafe(`
      CREATE INDEX IF NOT EXISTS idx_player_market_values_season
        ON player_market_values (season_id, market_value_eur DESC)
    `);

    await sql.unsafe(`
      CREATE INDEX IF NOT EXISTS idx_player_transfers_player_date
        ON player_transfers (player_id, moved_at DESC NULLS LAST, id DESC)
    `);

    await sql.unsafe(`
      CREATE INDEX IF NOT EXISTS idx_player_transfers_season
        ON player_transfers (season_id, moved_at DESC NULLS LAST)
    `);

    await sql.unsafe(`
      INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
      VALUES
        ('sofascore', 'Sofascore CDN', 'https://img.sofascore.com', 'api', NULL, 1),
        ('api_football', 'API-Football v3', 'https://v3.football.api-sports.io', 'api', NULL, 2),
        ('wikimedia', 'Wikimedia Commons', 'https://commons.wikimedia.org', 'api', NULL, 3),
        ('fbref_profile', 'FBref Player Profiles', 'https://fbref.com', 'scraper', 'profile_sync', 3),
        ('transfermarkt', 'Transfermarkt Player Profiles', 'https://www.transfermarkt.com', 'scraper', 'profile_sync', 3),
        ('transfermarkt_scraperfc', 'Transfermarkt via ScraperFC', 'https://www.transfermarkt.com', 'scraper', 'scraperfc', 3)
      ON CONFLICT (slug) DO NOTHING
    `);

    console.log(JSON.stringify({ ok: true }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
