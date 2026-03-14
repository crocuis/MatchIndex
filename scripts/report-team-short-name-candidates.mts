import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { deriveReviewedTeamShortName } from './team-short-name-policy.mts';

interface CliOptions {
  locale: string;
  limit: number;
}

interface TeamTranslationRow {
  slug: string;
  name: string;
  short_name: string | null;
}

function getArgValue(argv: string[], key: string) {
  return argv.find((arg) => arg.startsWith(`${key}=`))?.slice(key.length + 1) ?? null;
}

function parseArgs(argv: string[]): CliOptions {
  const limitRaw = getArgValue(argv, '--limit');
  const limit = limitRaw ? Number.parseInt(limitRaw, 10) : 50;

  return {
    locale: getArgValue(argv, '--locale') ?? 'ko',
    limit: Number.isFinite(limit) && limit > 0 ? limit : 50,
  };
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, prepare: false, idle_timeout: 5 });
}

function isSafeShortNamePolicyCandidate(name: string, shortName: string | null, derivedShortName: string) {
  if (!shortName || shortName !== name || derivedShortName === name) {
    return false;
  }

  return name.startsWith('FC ') || name.endsWith(' FC') || name.endsWith(' WFC');
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  const sql = getSql();

  try {
    const rows = await sql<TeamTranslationRow[]>`
      SELECT t.slug, tt.name, tt.short_name
      FROM team_translations tt
      JOIN teams t ON t.id = tt.team_id
      WHERE tt.locale = ${options.locale}
        AND t.is_national = FALSE
      ORDER BY t.slug
    `;

    const candidates = rows
      .map((row) => {
        const derivedShortName = deriveReviewedTeamShortName(row.name);
        return {
          slug: row.slug,
          name: row.name,
          shortName: row.short_name,
          derivedShortName,
        };
      })
      .filter((row) => isSafeShortNamePolicyCandidate(row.name, row.shortName, row.derivedShortName))
      .slice(0, options.limit);

    console.log(JSON.stringify({
      locale: options.locale,
      candidateCount: candidates.length,
      candidates,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main();
