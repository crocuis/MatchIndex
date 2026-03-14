import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

async function main() {
  loadProjectEnv();

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });
  const currentSlug = 'atletico-madrid-spain';
  const targetSlug = 'club-atletico-de-madrid-spain';

  try {
    await sql`BEGIN`;

    try {
      await sql`
        INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
        VALUES ('team', (SELECT id FROM teams WHERE slug = ${currentSlug}), ${currentSlug}, NULL, 'historical', FALSE, 'approved', 'historical_rule', 'fix-atletico-canonical')
        ON CONFLICT (entity_type, entity_id, alias_normalized)
        DO NOTHING
      `;

      await sql`
        UPDATE teams
        SET slug = ${targetSlug}, updated_at = NOW()
        WHERE slug = ${currentSlug}
      `;

      await sql`
        INSERT INTO team_translations (team_id, locale, name, short_name)
        VALUES ((SELECT id FROM teams WHERE slug = ${targetSlug}), 'en', ${'Club Atlético de Madrid'}, ${'Atlético Madrid'})
        ON CONFLICT (team_id, locale)
        DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
      `;

      await sql`
        INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
        VALUES ('team', (SELECT id FROM teams WHERE slug = ${targetSlug}), ${'Club Atlético de Madrid'}, 'en', 'official', TRUE, 'pending', 'manual', 'fix-atletico-canonical')
        ON CONFLICT (entity_type, entity_id, alias_normalized)
        DO NOTHING
      `;

      await sql`COMMIT`;
      console.log(JSON.stringify({ ok: true, from: currentSlug, to: targetSlug }, null, 2));
    } catch (error) {
      await sql`ROLLBACK`;
      throw error;
    }
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
