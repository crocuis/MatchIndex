import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { upsertCompetitionTranslationCandidate } from './competition-translation-candidates.mts';
import { promoteApprovedCompetitionTranslationCandidates } from './promote-translation-candidates.mts';

interface CliOptions {
  dryRun: boolean;
  locale: string;
  competitionSlug: string;
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
    competitionSlug: requireArg(argv, '--competition-slug'),
    name: requireArg(argv, '--name'),
    shortName: getArgValue(argv, '--short-name'),
    reviewedBy: getArgValue(argv, '--reviewed-by') ?? 'review-competition-translation',
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
    const [competition] = await sql<{ id: number; slug: string }[]>`
      SELECT id, slug
      FROM competitions
      WHERE slug = ${options.competitionSlug}
      LIMIT 1
    `;

    if (!competition) {
      throw new Error(`Competition not found: ${options.competitionSlug}`);
    }

    const [current] = await sql<{ name: string; short_name: string | null }[]>`
      SELECT name, short_name
      FROM competition_translations
      WHERE competition_id = ${competition.id}
        AND locale = ${options.locale}
      LIMIT 1
    `;

    if (options.dryRun) {
      console.log(JSON.stringify({
        dryRun: true,
        locale: options.locale,
        competitionSlug: options.competitionSlug,
        current: current ?? null,
        candidate: {
          name: options.name,
          shortName: options.shortName,
          reviewedBy: options.reviewedBy,
          notes: options.notes,
          sourceUrl: options.sourceUrl,
          promote: options.promote,
        },
      }, null, 2));
      return;
    }

    await upsertCompetitionTranslationCandidate(sql, {
      competitionSlug: options.competitionSlug,
      locale: options.locale,
      proposedName: options.name,
      proposedShortName: options.shortName,
      reviewedAt: new Date().toISOString(),
      reviewedBy: options.reviewedBy,
      sourceLabel: 'Manual reviewed competition localization',
      sourceRef: 'scripts/review-competition-translation.mts',
      sourceType: 'manual',
      sourceUrl: options.sourceUrl,
      status: 'approved',
      notes: options.notes,
    });

    const promotedCount = options.promote
      ? await promoteApprovedCompetitionTranslationCandidates(sql, options.locale, options.reviewedBy)
      : 0;

    const [published] = await sql<{ name: string; short_name: string | null }[]>`
      SELECT name, short_name
      FROM competition_translations
      WHERE competition_id = ${competition.id}
        AND locale = ${options.locale}
      LIMIT 1
    `;

    console.log(JSON.stringify({
      dryRun: false,
      locale: options.locale,
      competitionSlug: options.competitionSlug,
      promotedCount,
      previous: current ?? null,
      published: published ?? null,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main();
