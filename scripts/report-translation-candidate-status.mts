import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  locale: string;
  limit: number;
}

interface SummaryRow {
  pending_count: number;
  approved_unpromoted_count: number;
  promoted_count: number;
}

interface PreviewRow {
  slug: string;
  proposed_name: string;
  proposed_short_name?: string | null;
  status: string;
  source_type: string;
  reviewed_by?: string | null;
  promoted_by?: string | null;
  reviewed_at?: string | null;
  promoted_at?: string | null;
}

function getArgValue(argv: string[], key: string) {
  return argv.find((arg) => arg.startsWith(`${key}=`))?.slice(key.length + 1) ?? null;
}

function parseArgs(argv: string[]): CliOptions {
  const limitRaw = getArgValue(argv, '--limit');
  const limit = limitRaw ? Number.parseInt(limitRaw, 10) : 10;

  return {
    locale: getArgValue(argv, '--locale') ?? 'ko',
    limit: Number.isFinite(limit) && limit > 0 ? limit : 10,
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
    const [teamSummary] = await sql<SummaryRow[]>`
      SELECT
        COUNT(*) FILTER (WHERE status = 'pending')::INT AS pending_count,
        COUNT(*) FILTER (WHERE status = 'approved' AND promoted_at IS NULL)::INT AS approved_unpromoted_count,
        COUNT(*) FILTER (WHERE promoted_at IS NOT NULL)::INT AS promoted_count
      FROM team_translation_candidates
      WHERE locale = ${options.locale}
    `;

    const [competitionSummary] = await sql<SummaryRow[]>`
      SELECT
        COUNT(*) FILTER (WHERE status = 'pending')::INT AS pending_count,
        COUNT(*) FILTER (WHERE status = 'approved' AND promoted_at IS NULL)::INT AS approved_unpromoted_count,
        COUNT(*) FILTER (WHERE promoted_at IS NOT NULL)::INT AS promoted_count
      FROM competition_translation_candidates
      WHERE locale = ${options.locale}
    `;

    const [countrySummary] = await sql<SummaryRow[]>`
      SELECT
        COUNT(*) FILTER (WHERE status = 'pending')::INT AS pending_count,
        COUNT(*) FILTER (WHERE status = 'approved' AND promoted_at IS NULL)::INT AS approved_unpromoted_count,
        COUNT(*) FILTER (WHERE promoted_at IS NOT NULL)::INT AS promoted_count
      FROM country_translation_candidates
      WHERE locale = ${options.locale}
    `;

    const pendingTeams = await sql<PreviewRow[]>`
      SELECT
        t.slug,
        ttc.proposed_name,
        ttc.proposed_short_name,
        ttc.status,
        ttc.source_type,
        ttc.reviewed_by,
        ttc.promoted_by,
        ttc.reviewed_at::TEXT,
        ttc.promoted_at::TEXT
      FROM team_translation_candidates ttc
      JOIN teams t ON t.id = ttc.team_id
      WHERE ttc.locale = ${options.locale}
        AND ttc.status = 'approved'
        AND ttc.promoted_at IS NULL
      ORDER BY COALESCE(ttc.reviewed_at, ttc.created_at) DESC, ttc.id DESC
      LIMIT ${options.limit}
    `;

    const pendingCompetitions = await sql<PreviewRow[]>`
      SELECT
        c.slug,
        ctc.proposed_name,
        ctc.proposed_short_name,
        ctc.status,
        ctc.source_type,
        ctc.reviewed_by,
        ctc.promoted_by,
        ctc.reviewed_at::TEXT,
        ctc.promoted_at::TEXT
      FROM competition_translation_candidates ctc
      JOIN competitions c ON c.id = ctc.competition_id
      WHERE ctc.locale = ${options.locale}
        AND ctc.status = 'approved'
        AND ctc.promoted_at IS NULL
      ORDER BY COALESCE(ctc.reviewed_at, ctc.created_at) DESC, ctc.id DESC
      LIMIT ${options.limit}
    `;

    const pendingCountries = await sql<PreviewRow[]>`
      SELECT
        country.code_alpha3 AS slug,
        ctc.proposed_name,
        NULL::TEXT AS proposed_short_name,
        ctc.status,
        ctc.source_type,
        ctc.reviewed_by,
        ctc.promoted_by,
        ctc.reviewed_at::TEXT,
        ctc.promoted_at::TEXT
      FROM country_translation_candidates ctc
      JOIN countries country ON country.id = ctc.country_id
      WHERE ctc.locale = ${options.locale}
        AND ctc.status = 'approved'
        AND ctc.promoted_at IS NULL
      ORDER BY COALESCE(ctc.reviewed_at, ctc.created_at) DESC, ctc.id DESC
      LIMIT ${options.limit}
    `;

    console.log(JSON.stringify({
      locale: options.locale,
      limit: options.limit,
      summary: {
        teams: teamSummary,
        competitions: competitionSummary,
        countries: countrySummary,
      },
      approvedUnpromotedPreview: {
        teams: pendingTeams,
        competitions: pendingCompetitions,
        countries: pendingCountries,
      },
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main();
