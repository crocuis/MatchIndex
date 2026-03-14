import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { loadCountryCodeResolver } from '../src/data/countryCodeResolver.ts';
import {
  COMPETITION_NAMES_KO,
  COUNTRY_TRANSLATIONS,
  TEAM_NAMES_KO,
} from './ko-localization-data.mts';
import { upsertCompetitionTranslationCandidate } from './competition-translation-candidates.mts';
import { upsertCountryTranslationCandidate } from './country-translation-candidates.mts';
import { upsertTeamTranslationCandidate } from './team-translation-candidates.mts';

interface SeedSummary {
  countries: number;
  competitions: number;
  teams: number;
  dryRun: boolean;
  skippedCompetitions?: string[];
  skippedTeams?: string[];
}

const LEGACY_TEAM_SLUG_OVERRIDES: Record<string, string> = {
  '1899-hoffenheim': 'tsg-1899-hoffenheim-germany',
  'acf-fiorentina-italy': 'fiorentina-italy',
  'ac-pisa-1909-italy': 'pisa',
  'afc-ajax-netherlands': 'ajax-netherlands',
  'aj-auxerre-france': 'auxerre-france',
  arsenal: 'arsenal-fc-england',
  atletico: 'atletico-madrid',
  'atalanta-bc-italy': 'atalanta',
  bayern: 'fc-bayern-mu-nchen-germany',
  'bayern-munich': 'fc-bayern-mu-nchen-germany',
  'bayer-leverkusen-germany': 'bayer-04-leverkusen-germany',
  'cagliari-calcio-italy': 'cagliari',
  chelsea: 'chelsea-fc-england',
  'club-brugge-kv-belgium': 'club-brugge-kv',
  'deportivo-alaves-spain': 'alaves',
  dortmund: 'borussia-dortmund-germany',
  'fc-lorient-france': 'lorient-france',
  'fc-metz-france': 'metz-france',
  'fc-nantes-france': 'nantes-france',
  'genoa-cfc-italy': 'genoa-italy',
  'hellas-verona-fc-italy': 'hellas-verona-italy',
  inter: 'fc-internazionale-milano-italy',
  'juventus-fc-italy': 'juventus',
  'le-havre-ac-france': 'le-havre',
  leipzig: 'rb-leipzig-germany',
  leverkusen: 'bayer-04-leverkusen-germany',
  'levante-ud-spain': 'levante',
  liverpool: 'liverpool-fc-england',
  mancity: 'manchester-city-fc-england',
  milan: 'ac-milan-italy',
  napoli: 'napoli-italy',
  'olympique-de-marseille-france': 'marseille-france',
  'parma-calcio-1913-italy': 'parma',
  psg: 'paris-saint-germain-fc-france',
  realmadrid: 'real-madrid-cf-spain',
  'sc-freiburg-germany': 'sc-freiburg',
  sevilla: 'sevilla-fc-spain',
  'ss-lazio-italy': 'lazio-italy',
  'ssc-napoli-italy': 'napoli-italy',
  'torino-fc-italy': 'torino-italy',
  'toulouse-fc-france': 'toulouse-france',
  'udinese-calcio-italy': 'udinese-italy',
  'us-cremonese-italy': 'cremonese',
  'us-lecce-italy': 'lecce',
  'us-sassuolo-calcio-italy': 'sassuolo-italy',
  'werder-bremen-germany': 'sv-werder-bremen-germany',
};

function resolveLegacyTeamSlug(slug: string) {
  return LEGACY_TEAM_SLUG_OVERRIDES[slug] ?? slug;
}

function printSummary(summary: SeedSummary) {
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
    const countryCodeResolver = await loadCountryCodeResolver(sql);
    const countries = Object.entries(COUNTRY_TRANSLATIONS);
    const competitions = Object.entries(COMPETITION_NAMES_KO);
    const teams = Object.entries(TEAM_NAMES_KO);
    const competitionSlugs = competitions.map(([slug]) => slug);
    const teamSlugs = teams.map(([slug]) => resolveLegacyTeamSlug(slug));

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

      await upsertCountryTranslationCandidate(sql, {
        countryCode: canonicalCode,
        locale: 'ko',
        proposedName: translation.ko,
        reviewedAt: new Date().toISOString(),
        reviewedBy: 'seed-ko-entity-translations',
        sourceLabel: 'Legacy ko localization seed',
        sourceRef: 'scripts/ko-localization-data.mts',
        sourceType: 'legacy',
        status: 'approved',
      });
    }

    for (const [slug, translation] of competitions) {
      if (!existingCompetitions.has(slug)) {
        continue;
      }

      await upsertCompetitionTranslationCandidate(sql, {
        competitionSlug: slug,
        locale: 'ko',
        proposedName: translation.name,
        proposedShortName: translation.shortName,
        reviewedAt: new Date().toISOString(),
        reviewedBy: 'seed-ko-entity-translations',
        sourceLabel: 'Legacy ko localization seed',
        sourceRef: 'scripts/ko-localization-data.mts',
        sourceType: 'legacy',
        status: 'approved',
      });
    }

    for (const [slug, translation] of teams) {
      const resolvedSlug = resolveLegacyTeamSlug(slug);

      if (!existingTeams.has(resolvedSlug)) {
        continue;
      }

      await upsertTeamTranslationCandidate(sql, {
        locale: 'ko',
        proposedName: translation.name,
        proposedShortName: translation.shortName,
        reviewedAt: new Date().toISOString(),
        reviewedBy: 'seed-ko-entity-translations',
        sourceLabel: 'Legacy ko localization seed',
        sourceRef: 'scripts/ko-localization-data.mts',
        sourceType: 'legacy',
        status: 'approved',
        teamSlug: resolvedSlug,
      });
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
