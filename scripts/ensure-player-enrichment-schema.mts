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
        ADD COLUMN IF NOT EXISTS salary_currency CHAR(3),
        ADD COLUMN IF NOT EXISTS salary_source VARCHAR(50),
        ADD COLUMN IF NOT EXISTS salary_source_url TEXT,
        ADD COLUMN IF NOT EXISTS salary_is_estimated BOOLEAN NOT NULL DEFAULT TRUE,
        ADD COLUMN IF NOT EXISTS salary_updated_at TIMESTAMPTZ
    `);

    await sql.unsafe(`
      INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
      VALUES
        ('sofascore', 'Sofascore CDN', 'https://img.sofascore.com', 'api', NULL, 1),
        ('api_football', 'API-Football v3', 'https://v3.football.api-sports.io', 'api', NULL, 2),
        ('wikimedia', 'Wikimedia Commons', 'https://commons.wikimedia.org', 'api', NULL, 3),
        ('fbref_profile', 'FBref Player Profiles', 'https://fbref.com', 'scraper', 'profile_sync', 3),
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
