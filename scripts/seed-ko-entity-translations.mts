import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import {
  promoteApprovedCompetitionTranslationCandidates,
  promoteApprovedCountryTranslationCandidates,
  promoteApprovedTeamTranslationCandidates,
} from './promote-translation-candidates.mts';

interface SeedSummary {
  locale: string;
  dryRun: boolean;
  approvedUnpromoted: {
    countries: number;
    competitions: number;
    teams: number;
  };
  promoted?: {
    countries: number;
    competitions: number;
    teams: number;
  };
}

const TARGET_LOCALE = 'ko';

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
    const [countryPendingRow] = await sql<{ count: number }[]>`
      SELECT COUNT(*)::INT AS count
      FROM (
        SELECT DISTINCT ON (ctc.country_id, ctc.locale) ctc.id
        FROM country_translation_candidates ctc
        WHERE ctc.locale = ${TARGET_LOCALE}
          AND ctc.status = 'approved'
          AND ctc.promoted_at IS NULL
        ORDER BY
          ctc.country_id,
          ctc.locale,
          CASE ctc.source_type
            WHEN 'manual' THEN 5
            WHEN 'imported' THEN 4
            WHEN 'legacy' THEN 3
            WHEN 'merge_derived' THEN 2
            WHEN 'historical_rule' THEN 1
            ELSE 0
          END DESC,
          COALESCE(ctc.reviewed_at, ctc.created_at) DESC,
          ctc.created_at DESC,
          ctc.id DESC
      ) candidates
    `;

    const [competitionPendingRow] = await sql<{ count: number }[]>`
      SELECT COUNT(*)::INT AS count
      FROM (
        SELECT DISTINCT ON (ctc.competition_id, ctc.locale) ctc.id
        FROM competition_translation_candidates ctc
        WHERE ctc.locale = ${TARGET_LOCALE}
          AND ctc.status = 'approved'
          AND ctc.promoted_at IS NULL
        ORDER BY
          ctc.competition_id,
          ctc.locale,
          CASE ctc.source_type
            WHEN 'manual' THEN 5
            WHEN 'imported' THEN 4
            WHEN 'legacy' THEN 3
            WHEN 'merge_derived' THEN 2
            WHEN 'historical_rule' THEN 1
            ELSE 0
          END DESC,
          COALESCE(ctc.reviewed_at, ctc.created_at) DESC,
          ctc.created_at DESC,
          ctc.id DESC
      ) candidates
    `;

    const [teamPendingRow] = await sql<{ count: number }[]>`
      SELECT COUNT(*)::INT AS count
      FROM (
        SELECT DISTINCT ON (ttc.team_id, ttc.locale) ttc.id
        FROM team_translation_candidates ttc
        WHERE ttc.locale = ${TARGET_LOCALE}
          AND ttc.status = 'approved'
          AND ttc.promoted_at IS NULL
        ORDER BY
          ttc.team_id,
          ttc.locale,
          CASE ttc.source_type
            WHEN 'manual' THEN 5
            WHEN 'imported' THEN 4
            WHEN 'legacy' THEN 3
            WHEN 'merge_derived' THEN 2
            WHEN 'historical_rule' THEN 1
            ELSE 0
          END DESC,
          COALESCE(ttc.reviewed_at, ttc.created_at) DESC,
          ttc.created_at DESC,
          ttc.id DESC
      ) candidates
    `;

    if (dryRun) {
      printSummary({
        locale: TARGET_LOCALE,
        dryRun,
        approvedUnpromoted: {
          countries: countryPendingRow?.count ?? 0,
          competitions: competitionPendingRow?.count ?? 0,
          teams: teamPendingRow?.count ?? 0,
        },
      });
      return;
    }

    const promotedCountries = await promoteApprovedCountryTranslationCandidates(sql, TARGET_LOCALE, 'seed-ko-entity-translations');
    const promotedCompetitions = await promoteApprovedCompetitionTranslationCandidates(sql, TARGET_LOCALE, 'seed-ko-entity-translations');
    const promotedTeams = await promoteApprovedTeamTranslationCandidates(sql, TARGET_LOCALE, 'seed-ko-entity-translations');

    printSummary({
      locale: TARGET_LOCALE,
      dryRun,
      approvedUnpromoted: {
        countries: countryPendingRow?.count ?? 0,
        competitions: competitionPendingRow?.count ?? 0,
        teams: teamPendingRow?.count ?? 0,
      },
      promoted: {
        countries: promotedCountries,
        competitions: promotedCompetitions,
        teams: promotedTeams,
      },
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
