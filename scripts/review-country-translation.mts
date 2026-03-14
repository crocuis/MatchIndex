import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { upsertCountryTranslationCandidate } from './country-translation-candidates.mts';
import { promoteApprovedCountryTranslationCandidates } from './promote-translation-candidates.mts';

interface CliOptions {
  dryRun: boolean;
  locale: string;
  countryCode: string;
  name: string;
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
    countryCode: requireArg(argv, '--country-code').toUpperCase(),
    name: requireArg(argv, '--name'),
    reviewedBy: getArgValue(argv, '--reviewed-by') ?? 'review-country-translation',
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
    const [country] = await sql<{ id: number; code_alpha3: string }[]>`
      SELECT id, code_alpha3
      FROM countries
      WHERE code_alpha3 = ${options.countryCode}
      LIMIT 1
    `;

    if (!country) {
      throw new Error(`Country not found: ${options.countryCode}`);
    }

    const [current] = await sql<{ name: string }[]>`
      SELECT name
      FROM country_translations
      WHERE country_id = ${country.id}
        AND locale = ${options.locale}
      LIMIT 1
    `;

    if (options.dryRun) {
      console.log(JSON.stringify({
        dryRun: true,
        locale: options.locale,
        countryCode: options.countryCode,
        current: current ?? null,
        candidate: {
          name: options.name,
          reviewedBy: options.reviewedBy,
          notes: options.notes,
          sourceUrl: options.sourceUrl,
          promote: options.promote,
        },
      }, null, 2));
      return;
    }

    await upsertCountryTranslationCandidate(sql, {
      countryCode: options.countryCode,
      locale: options.locale,
      proposedName: options.name,
      reviewedAt: new Date().toISOString(),
      reviewedBy: options.reviewedBy,
      sourceLabel: 'Manual reviewed country localization',
      sourceRef: 'scripts/review-country-translation.mts',
      sourceType: 'manual',
      sourceUrl: options.sourceUrl,
      status: 'approved',
      notes: options.notes,
    });

    const promotedCount = options.promote
      ? await promoteApprovedCountryTranslationCandidates(sql, options.locale, options.reviewedBy)
      : 0;

    const [published] = await sql<{ name: string }[]>`
      SELECT name
      FROM country_translations
      WHERE country_id = ${country.id}
        AND locale = ${options.locale}
      LIMIT 1
    `;

    console.log(JSON.stringify({
      dryRun: false,
      locale: options.locale,
      countryCode: options.countryCode,
      promotedCount,
      previous: current ?? null,
      published: published ?? null,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main();
