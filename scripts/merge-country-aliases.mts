import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { COUNTRY_CODE_ALIASES } from '../src/data/countryCodeAliasSeeds.ts';

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

async function mergeAlias(sql: ReturnType<typeof getSql>, aliasCode: string, canonicalCode: string) {
  const [aliasRow] = await sql<Array<{ id: number }>>`SELECT id FROM countries WHERE code_alpha3 = ${aliasCode}`;
  if (!aliasRow) {
    return { aliasCode, canonicalCode, merged: false, reason: 'alias row missing' };
  }

  const [canonicalRow] = await sql<Array<{ id: number }>>`SELECT id FROM countries WHERE code_alpha3 = ${canonicalCode}`;
  if (!canonicalRow) {
    await sql`UPDATE countries SET code_alpha3 = ${canonicalCode}, updated_at = NOW() WHERE id = ${aliasRow.id}`;
    return { aliasCode, canonicalCode, merged: true, reason: 'renamed alias to canonical' };
  }

  await sql`
    UPDATE countries
    SET confederation = COALESCE(countries.confederation, alias_country.confederation),
        fifa_ranking = COALESCE(countries.fifa_ranking, alias_country.fifa_ranking),
        flag_url = COALESCE(countries.flag_url, alias_country.flag_url),
        crest_url = COALESCE(countries.crest_url, alias_country.crest_url),
        updated_at = NOW()
    FROM countries alias_country
    WHERE countries.id = ${canonicalRow.id}
      AND alias_country.id = ${aliasRow.id}
  `;

  await sql`
    INSERT INTO country_translations (country_id, locale, name)
    SELECT ${canonicalRow.id}, locale, name
    FROM country_translations
    WHERE country_id = ${aliasRow.id}
    ON CONFLICT (country_id, locale)
    DO NOTHING
  `;

  await sql`
    INSERT INTO ranking_history (country_id, ranking_date, fifa_ranking, source)
    SELECT ${canonicalRow.id}, ranking_date, fifa_ranking, source
    FROM ranking_history
    WHERE country_id = ${aliasRow.id}
    ON CONFLICT (country_id, ranking_date, ranking_category)
    DO NOTHING
  `;

  await sql`UPDATE competitions SET country_id = ${canonicalRow.id} WHERE country_id = ${aliasRow.id}`;
  await sql`UPDATE venues SET country_id = ${canonicalRow.id} WHERE country_id = ${aliasRow.id}`;
  await sql`UPDATE teams SET country_id = ${canonicalRow.id} WHERE country_id = ${aliasRow.id}`;
  await sql`UPDATE players SET country_id = ${canonicalRow.id} WHERE country_id = ${aliasRow.id}`;
  await sql`UPDATE coaches SET country_id = ${canonicalRow.id} WHERE country_id = ${aliasRow.id}`;
  await sql`
    DELETE FROM entity_aliases alias_ea
    USING entity_aliases canonical_ea
    WHERE alias_ea.entity_type = 'country'
      AND canonical_ea.entity_type = 'country'
      AND alias_ea.entity_id = ${aliasRow.id}
      AND canonical_ea.entity_id = ${canonicalRow.id}
      AND alias_ea.alias_normalized = canonical_ea.alias_normalized
  `;
  await sql`UPDATE entity_aliases SET entity_id = ${canonicalRow.id} WHERE entity_type = 'country' AND entity_id = ${aliasRow.id}`;
  await sql`DELETE FROM countries WHERE id = ${aliasRow.id}`;

  return { aliasCode, canonicalCode, merged: true, reason: 'merged into canonical row' };
}

async function main() {
  loadProjectEnv();
  const sql = getSql();

  try {
    const results = [] as Array<{ aliasCode: string; canonicalCode: string; merged: boolean; reason: string }>;
    for (const [aliasCode, canonicalCode] of Object.entries(COUNTRY_CODE_ALIASES)) {
      results.push(await mergeAlias(sql, aliasCode, canonicalCode));
    }
    await sql`UPDATE countries SET is_active = FALSE, updated_at = NOW() WHERE code_alpha3 = 'MON'`;
    console.log(JSON.stringify({ mergedCount: results.filter((row) => row.merged).length, results }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
