import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { promoteApprovedTeamTranslationCandidates } from './promote-translation-candidates.mts';
import { deriveReviewedTeamShortName } from './team-short-name-policy.mts';
import { upsertTeamTranslationCandidate } from './team-translation-candidates.mts';

interface CliOptions {
  dryRun: boolean;
  locale: string;
  teamSlug: string;
  name: string;
  shortName: string | null;
  reviewedBy: string;
  notes: string | null;
  sourceUrl: string | null;
  promote: boolean;
}

function getArgValue(argv: string[], key: string) {
  return argv.find((arg) => arg.startsWith(`${key}=`))?.slice(key.length + 1) ?? null;
}

function requireArg(argv: string[], key: string) {
  const value = getArgValue(argv, key);
  if (!value) {
    throw new Error(`Missing required argument: ${key}`);
  }

  return value;
}

function parseArgs(argv: string[]): CliOptions {
  return {
    dryRun: !argv.includes('--write'),
    locale: getArgValue(argv, '--locale') ?? 'ko',
    teamSlug: requireArg(argv, '--team-slug'),
    name: requireArg(argv, '--name'),
    shortName: getArgValue(argv, '--short-name'),
    reviewedBy: getArgValue(argv, '--reviewed-by') ?? 'review-team-translation',
    notes: getArgValue(argv, '--notes'),
    sourceUrl: getArgValue(argv, '--source-url'),
    promote: !argv.includes('--candidate-only'),
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
    const [team] = await sql<{ id: number; slug: string }[]>`
      SELECT id, slug
      FROM teams
      WHERE slug = ${options.teamSlug}
        AND is_national = FALSE
      LIMIT 1
    `;

    if (!team) {
      throw new Error(`Team not found: ${options.teamSlug}`);
    }

    const [current] = await sql<{ name: string; short_name: string | null }[]>`
      SELECT name, short_name
      FROM team_translations
      WHERE team_id = ${team.id}
        AND locale = ${options.locale}
      LIMIT 1
    `;

    const resolvedShortName = options.shortName ?? deriveReviewedTeamShortName(options.name);

    if (options.dryRun) {
      console.log(JSON.stringify({
        dryRun: true,
        locale: options.locale,
        teamSlug: options.teamSlug,
        current: current ?? null,
        candidate: {
          name: options.name,
          shortName: resolvedShortName,
          reviewedBy: options.reviewedBy,
          notes: options.notes,
          sourceUrl: options.sourceUrl,
          promote: options.promote,
        },
      }, null, 2));
      return;
    }

    await upsertTeamTranslationCandidate(sql, {
      teamSlug: options.teamSlug,
      locale: options.locale,
      proposedName: options.name,
      proposedShortName: resolvedShortName,
      reviewedAt: new Date().toISOString(),
      reviewedBy: options.reviewedBy,
      sourceLabel: 'Manual reviewed team localization',
      sourceRef: 'scripts/review-team-translation.mts',
      sourceType: 'manual',
      sourceUrl: options.sourceUrl,
      status: 'approved',
      notes: options.notes,
    });

    const promotedCount = options.promote
      ? await promoteApprovedTeamTranslationCandidates(sql, options.locale, options.reviewedBy)
      : 0;

    const [published] = await sql<{ name: string; short_name: string | null }[]>`
      SELECT name, short_name
      FROM team_translations
      WHERE team_id = ${team.id}
        AND locale = ${options.locale}
      LIMIT 1
    `;

    console.log(JSON.stringify({
      dryRun: false,
      locale: options.locale,
      teamSlug: options.teamSlug,
      promotedCount,
      previous: current ?? null,
      published: published ?? null,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main();
