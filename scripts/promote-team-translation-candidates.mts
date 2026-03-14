import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { promoteApprovedTeamTranslationCandidates } from './promote-translation-candidates.mts';

interface CliOptions {
  dryRun: boolean;
  locale: string;
  promotedBy: string;
}

function parseArgs(argv: string[]): CliOptions {
  const localeArg = argv.find((arg) => arg.startsWith('--locale='));
  const promotedByArg = argv.find((arg) => arg.startsWith('--promoted-by='));

  return {
    dryRun: argv.includes('--dry-run'),
    locale: localeArg?.split('=')[1] ?? 'ko',
    promotedBy: promotedByArg?.split('=')[1] ?? 'promote-team-translation-candidates',
  };
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, prepare: false, idle_timeout: 5 });
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  const sql = getSql();

  try {
    const promotedCount = options.dryRun
      ? (await sql<{ count: number }[]>`
          SELECT COUNT(*)::INT AS count
          FROM (
            SELECT DISTINCT ON (ttc.team_id, ttc.locale) ttc.id
            FROM team_translation_candidates ttc
            WHERE ttc.locale = ${options.locale}
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
        `)[0]?.count ?? 0
      : await promoteApprovedTeamTranslationCandidates(sql, options.locale, options.promotedBy);

    console.log(JSON.stringify({
      dryRun: options.dryRun,
      locale: options.locale,
      promotedCount,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main();
