import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import {
  promoteApprovedCompetitionTranslationCandidates,
  promoteApprovedCountryTranslationCandidates,
  promoteApprovedTeamTranslationCandidates,
} from './promote-translation-candidates.mts';

interface CliOptions {
  dryRun: boolean;
  help: boolean;
}

interface GapSummary {
  missingOrEnglish: number;
  approvedUnpromoted: number;
}

const TARGET_LOCALE = 'ko';

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: false,
    help: false,
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/repair-tab-localizations.mts [options]

Options:
  --dry-run   Detect localization gaps and preview DB-backed repairs without writing
  --help, -h  Show this help message
`);
}

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

function containsLatin(value: string | null | undefined) {
  return /[A-Za-z]/.test(value ?? '');
}

function hasLocalizedGap(localized: string | null | undefined, english: string) {
  const trimmed = localized?.trim();
  if (!trimmed) {
    return true;
  }

  if (/^[A-Z0-9.&/+' -]{1,8}$/.test(trimmed) && trimmed === english.trim()) {
    return false;
  }

  return containsLatin(trimmed) && trimmed.toLowerCase() === english.trim().toLowerCase();
}

async function getCompetitionGapCount(sql: postgres.Sql<Record<string, never>>) {
  const rows = await sql<Array<{ en_name: string; ko_name: string | null }>>`
    WITH latest_competition_seasons AS (
      SELECT DISTINCT ON (cs.competition_id)
        cs.competition_id
      FROM competition_seasons cs
      JOIN seasons s ON s.id = cs.season_id
      ORDER BY cs.competition_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
    )
    SELECT
      COALESCE(
        (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
        c.slug
      ) AS en_name,
      (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = ${TARGET_LOCALE}) AS ko_name
    FROM competitions c
    JOIN latest_competition_seasons lcs ON lcs.competition_id = c.id
  `;

  return rows.filter((row) => hasLocalizedGap(row.ko_name, row.en_name)).length;
}

async function getTeamGapCount(sql: postgres.Sql<Record<string, never>>) {
  const rows = await sql<Array<{ en_name: string; ko_name: string | null }>>`
    WITH latest_team_seasons AS (
      SELECT DISTINCT ON (ts.team_id)
        ts.team_id
      FROM team_seasons ts
      JOIN competition_seasons cs ON cs.id = ts.competition_season_id
      JOIN seasons s ON s.id = cs.season_id
      ORDER BY ts.team_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, cs.id DESC
    )
    SELECT
      COALESCE(
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
        t.slug
      ) AS en_name,
      (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${TARGET_LOCALE}) AS ko_name
    FROM teams t
    JOIN latest_team_seasons lts ON lts.team_id = t.id
  `;

  return rows.filter((row) => hasLocalizedGap(row.ko_name, row.en_name)).length;
}

async function getCountryGapCount(sql: postgres.Sql<Record<string, never>>) {
  const rows = await sql<Array<{ en_name: string; ko_name: string | null }>>`
    SELECT
      COALESCE(
        (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'en'),
        c.code_alpha3
      ) AS en_name,
      (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = ${TARGET_LOCALE}) AS ko_name
    FROM countries c
    WHERE c.is_active = TRUE
      AND (
        COALESCE(c.confederation, '') <> ''
        OR (c.fifa_ranking IS NOT NULL AND c.fifa_ranking > 0)
        OR (c.fifa_ranking_women IS NOT NULL AND c.fifa_ranking_women > 0)
        OR c.flag_url IS NOT NULL
        OR c.crest_url IS NOT NULL
      )
  `;

  return rows.filter((row) => hasLocalizedGap(row.ko_name, row.en_name)).length;
}

async function getApprovedUnpromotedCount(sql: postgres.Sql<Record<string, never>>, tableName: 'country_translation_candidates' | 'competition_translation_candidates' | 'team_translation_candidates', idColumn: 'country_id' | 'competition_id' | 'team_id') {
  const rows = await sql<{ count: number }[]>`
    SELECT COUNT(*)::INT AS count
    FROM (
      SELECT DISTINCT ON (${sql(idColumn)}, locale) id
      FROM ${sql(tableName)}
      WHERE locale = ${TARGET_LOCALE}
        AND status = 'approved'
        AND promoted_at IS NULL
      ORDER BY
        ${sql(idColumn)},
        locale,
        CASE source_type
          WHEN 'manual' THEN 5
          WHEN 'imported' THEN 4
          WHEN 'legacy' THEN 3
          WHEN 'merge_derived' THEN 2
          WHEN 'historical_rule' THEN 1
          ELSE 0
        END DESC,
        COALESCE(reviewed_at, created_at) DESC,
        created_at DESC,
        id DESC
    ) candidates
  `;

  return rows[0]?.count ?? 0;
}

async function collectSummary(sql: postgres.Sql<Record<string, never>>) {
  const [competitionMissing, teamMissing, countryMissing, competitionApproved, teamApproved, countryApproved] = await Promise.all([
    getCompetitionGapCount(sql),
    getTeamGapCount(sql),
    getCountryGapCount(sql),
    getApprovedUnpromotedCount(sql, 'competition_translation_candidates', 'competition_id'),
    getApprovedUnpromotedCount(sql, 'team_translation_candidates', 'team_id'),
    getApprovedUnpromotedCount(sql, 'country_translation_candidates', 'country_id'),
  ]);

  return {
    competitions: { missingOrEnglish: competitionMissing, approvedUnpromoted: competitionApproved } satisfies GapSummary,
    teams: { missingOrEnglish: teamMissing, approvedUnpromoted: teamApproved } satisfies GapSummary,
    countries: { missingOrEnglish: countryMissing, approvedUnpromoted: countryApproved } satisfies GapSummary,
  };
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const sql = getSql();

  try {
    const before = await collectSummary(sql);

    if (options.dryRun) {
      console.log(JSON.stringify({
        dryRun: true,
        locale: TARGET_LOCALE,
        before,
      }, null, 2));
      return;
    }

    const promoted = {
      competitions: await promoteApprovedCompetitionTranslationCandidates(sql, TARGET_LOCALE, 'repair-tab-localizations'),
      teams: await promoteApprovedTeamTranslationCandidates(sql, TARGET_LOCALE, 'repair-tab-localizations'),
      countries: await promoteApprovedCountryTranslationCandidates(sql, TARGET_LOCALE, 'repair-tab-localizations'),
    };

    const after = await collectSummary(sql);

    console.log(JSON.stringify({
      dryRun: false,
      locale: TARGET_LOCALE,
      promoted,
      before,
      after,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
