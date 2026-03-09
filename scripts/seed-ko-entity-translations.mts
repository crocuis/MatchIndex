import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { COMPETITION_NAMES_KO, COUNTRY_TRANSLATIONS, TEAM_NAMES_KO } from './ko-localization-data.mts';
import { resolveNationCodeAlias } from '../src/data/nationCodeAliases.ts';

function printSummary(summary: { countries: number; competitions: number; teams: number; dryRun: boolean }) {
  console.log(JSON.stringify(summary, null, 2));
}

async function main() {
  loadProjectEnv();
  const dryRun = !process.argv.includes('--write');
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(connectionString, { max: 1, prepare: false });

  try {
    const countries = Object.entries(COUNTRY_TRANSLATIONS);
    const competitions = Object.entries(COMPETITION_NAMES_KO);
    const teams = Object.entries(TEAM_NAMES_KO);

    if (dryRun) {
      printSummary({ countries: countries.length, competitions: competitions.length, teams: teams.length, dryRun });
      return;
    }

    for (const [code, translation] of countries) {
      const canonicalCode = resolveNationCodeAlias(code);

      await sql`
        INSERT INTO countries (code_alpha3, confederation, is_active, updated_at)
        VALUES (${canonicalCode}, ${translation.confederation}, TRUE, NOW())
        ON CONFLICT (code_alpha3)
        DO UPDATE SET
          confederation = COALESCE(EXCLUDED.confederation, countries.confederation),
          is_active = TRUE,
          updated_at = NOW()
      `;
    }

    for (const [code, translation] of countries) {
      const canonicalCode = resolveNationCodeAlias(code);

      await sql`
        INSERT INTO country_translations (country_id, locale, name)
        VALUES ((SELECT id FROM countries WHERE code_alpha3 = ${canonicalCode}), 'en', ${translation.en})
        ON CONFLICT (country_id, locale)
        DO UPDATE SET name = EXCLUDED.name
      `;

      await sql`
        INSERT INTO country_translations (country_id, locale, name)
        VALUES ((SELECT id FROM countries WHERE code_alpha3 = ${canonicalCode}), 'ko', ${translation.ko})
        ON CONFLICT (country_id, locale)
        DO UPDATE SET name = EXCLUDED.name
      `;
    }

    for (const [slug, translation] of competitions) {
      await sql`
        INSERT INTO competition_translations (competition_id, locale, name, short_name)
        VALUES ((SELECT id FROM competitions WHERE slug = ${slug}), 'ko', ${translation.name}, ${translation.shortName})
        ON CONFLICT (competition_id, locale)
        DO UPDATE SET
          name = EXCLUDED.name,
          short_name = EXCLUDED.short_name
      `;
    }

    for (const [slug, translation] of teams) {
      await sql`
        INSERT INTO team_translations (team_id, locale, name, short_name)
        VALUES ((SELECT id FROM teams WHERE slug = ${slug}), 'ko', ${translation.name}, ${translation.shortName})
        ON CONFLICT (team_id, locale)
        DO UPDATE SET
          name = EXCLUDED.name,
          short_name = EXCLUDED.short_name
      `;
    }

    printSummary({ countries: countries.length, competitions: competitions.length, teams: teams.length, dryRun });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
