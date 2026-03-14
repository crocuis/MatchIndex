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
  country_id: number;
}

function parseArgs(argv: string[]): CliOptions {
  const localeArg = argv.find((arg) => arg.startsWith('--locale='));
  const promotedByArg = argv.find((arg) => arg.startsWith('--promoted-by='));

  return {
    dryRun: argv.includes('--dry-run'),
    locale: localeArg?.split('=')[1] ?? 'ko',
    promotedBy: promotedByArg?.split('=')[1] ?? 'promote-country-translation-candidates',
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
      SELECT DISTINCT ON (ctc.country_id, ctc.locale)
        ctc.id,
        ctc.country_id,
        ctc.locale,
        ctc.proposed_name
      FROM country_translation_candidates ctc
      WHERE ctc.locale = ${options.locale}
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
    `;

    if (!options.dryRun) {
      for (const candidate of candidates) {
        await sql`
          INSERT INTO country_translations (country_id, locale, name)
          VALUES (${candidate.country_id}, ${candidate.locale}, ${candidate.proposed_name})
          ON CONFLICT (country_id, locale)
          DO UPDATE SET name = EXCLUDED.name
        `;

        await sql`
          UPDATE country_translation_candidates
          SET promoted_at = NOW(), promoted_by = ${options.promotedBy}, updated_at = NOW()
          WHERE id = ${candidate.id}
        `;
      }
    }

    console.log(JSON.stringify({ dryRun: options.dryRun, locale: options.locale, promotedCount: candidates.length }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main();
