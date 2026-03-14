import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { loadCountryCodeResolver } from '../src/data/countryCodeResolver.ts';
import {
  COMPETITION_NAMES_KO,
  COUNTRY_TRANSLATIONS,
  TEAM_NAMES_KO,
  type NamedLocalizationEntry,
} from './ko-localization-data.mts';

interface SeedSummary {
  countries: number;
  competitions: number;
  teams: number;
  dryRun: boolean;
  skippedCompetitions?: string[];
  skippedTeams?: string[];
}

function printSummary(summary: SeedSummary) {
  console.log(JSON.stringify(summary, null, 2));
}

function loadGeneratedTeamNames(): Record<string, NamedLocalizationEntry> {
  const filePath = path.join(process.cwd(), 'scripts', 'ko-team-names.generated.json');

  if (!existsSync(filePath)) {
    return {};
  }

  return JSON.parse(readFileSync(filePath, 'utf8')) as Record<string, NamedLocalizationEntry>;
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
    const countryCodeResolver = await loadCountryCodeResolver(sql);
    const countries = Object.entries(COUNTRY_TRANSLATIONS);
    const competitions = Object.entries(COMPETITION_NAMES_KO);
    const teams = Object.entries({
      ...loadGeneratedTeamNames(),
      ...TEAM_NAMES_KO,
    });
    const competitionSlugs = competitions.map(([slug]) => slug);
    const teamSlugs = teams.map(([slug]) => slug);

    const existingCompetitions = new Set(
      (await sql`SELECT slug FROM competitions WHERE slug = ANY(${competitionSlugs})`).map((row) => row.slug)
    );
    const existingTeams = new Set(
      (await sql`SELECT slug FROM teams WHERE slug = ANY(${teamSlugs})`).map((row) => row.slug)
    );

    const skippedCompetitions = competitionSlugs.filter((slug) => !existingCompetitions.has(slug));
    const skippedTeams = teamSlugs.filter((slug) => !existingTeams.has(slug));

    if (dryRun) {
      printSummary({
        countries: countries.length,
        competitions: competitions.length,
        teams: teams.length,
        dryRun,
        skippedCompetitions,
        skippedTeams,
      });
      return;
    }

    for (const [code, translation] of countries) {
      const canonicalCode = countryCodeResolver.resolve(code) ?? code;

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
      const canonicalCode = countryCodeResolver.resolve(code) ?? code;

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
      if (!existingCompetitions.has(slug)) {
        continue;
      }

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
      if (!existingTeams.has(slug)) {
        continue;
      }

      await sql`
        INSERT INTO team_translations (team_id, locale, name, short_name)
        VALUES ((SELECT id FROM teams WHERE slug = ${slug}), 'ko', ${translation.name}, ${translation.shortName})
        ON CONFLICT (team_id, locale)
        DO UPDATE SET
          name = EXCLUDED.name,
          short_name = EXCLUDED.short_name
      `;
    }

    printSummary({
      countries: countries.length,
      competitions: competitions.length,
      teams: teams.length,
      dryRun,
      skippedCompetitions,
      skippedTeams,
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
