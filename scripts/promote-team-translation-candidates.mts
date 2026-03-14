import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  dryRun: boolean;
  locale: string;
  promotedBy: string;
}

interface CandidateRow {
  id: number;
  locale: string;
  proposed_name: string;
  proposed_short_name: string | null;
  team_id: number;
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
    const candidates = await sql<CandidateRow[]>`
      SELECT DISTINCT ON (ttc.team_id, ttc.locale)
        ttc.id,
        ttc.team_id,
        ttc.locale,
        ttc.proposed_name,
        ttc.proposed_short_name
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
    `;

    if (!options.dryRun) {
      for (const candidate of candidates) {
        await sql`
          INSERT INTO team_translations (team_id, locale, name, short_name)
          VALUES (${candidate.team_id}, ${candidate.locale}, ${candidate.proposed_name}, ${candidate.proposed_short_name})
          ON CONFLICT (team_id, locale)
          DO UPDATE SET
            name = EXCLUDED.name,
            short_name = EXCLUDED.short_name
        `;

        await sql`
          UPDATE team_translation_candidates
          SET promoted_at = NOW(),
              promoted_by = ${options.promotedBy},
              updated_at = NOW()
          WHERE id = ${candidate.id}
        `;
      }
    }

    console.log(JSON.stringify({
      dryRun: options.dryRun,
      locale: options.locale,
      promotedCount: candidates.length,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main();
