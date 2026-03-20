import { readFileSync } from 'node:fs';
import path from 'node:path';

import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';
import { promoteApprovedTeamTranslationCandidates } from './promote-translation-candidates.mts';
import { upsertTeamTranslationCandidate } from './team-translation-candidates.mts';

interface CliOptions {
  dryRun: boolean;
  inputPath: string;
}

interface ReviewedPendingRow {
  slug: string;
  proposed_name?: string | null;
  proposed_short_name?: string | null;
}

interface ReviewedManualRow {
  slug: string;
  ko_name?: string | null;
  ko_short_name?: string | null;
}

interface ReviewQueueFile {
  deeplPending?: ReviewedPendingRow[];
  manualReviewNeeded?: ReviewedManualRow[];
}

function getArgValue(argv: string[], key: string) {
  return argv.find((arg) => arg.startsWith(`${key}=`))?.slice(key.length + 1) ?? null;
}

function parseArgs(argv: string[]): CliOptions {
  return {
    dryRun: !argv.includes('--write'),
    inputPath: getArgValue(argv, '--input') ?? '.sisyphus/team-ko-review/deepl-pending-all.json',
  };
}

function resolveInputPath(inputPath: string) {
  return path.isAbsolute(inputPath) ? inputPath : path.join(process.cwd(), inputPath);
}

function normalizeRequired(value: string | null | undefined) {
  const normalized = value?.trim() ?? '';
  return normalized;
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
  const inputPath = resolveInputPath(options.inputPath);
  const reviewFile = JSON.parse(readFileSync(inputPath, 'utf8')) as ReviewQueueFile;

  const reviewedPending = (reviewFile.deeplPending ?? [])
    .map((row) => ({
      slug: normalizeRequired(row.slug),
      name: normalizeRequired(row.proposed_name),
      shortName: normalizeRequired(row.proposed_short_name),
    }))
    .filter((row) => row.slug && row.name && row.shortName);

  const reviewedManual = (reviewFile.manualReviewNeeded ?? [])
    .map((row) => ({
      slug: normalizeRequired(row.slug),
      name: normalizeRequired(row.ko_name),
      shortName: normalizeRequired(row.ko_short_name),
    }))
    .filter((row) => row.slug && row.name && row.shortName);

  if (options.dryRun) {
    console.log(JSON.stringify({
      dryRun: true,
      inputPath,
      reviewedPendingCount: reviewedPending.length,
      reviewedManualCount: reviewedManual.length,
      preview: {
        deeplPending: reviewedPending.slice(0, 10),
        manualReviewNeeded: reviewedManual.slice(0, 10),
      },
    }, null, 2));
    return;
  }

  const sql = getSql();

  try {
    for (const row of reviewedPending) {
      await upsertTeamTranslationCandidate(sql, {
        locale: 'ko',
        proposedName: row.name,
        proposedShortName: row.shortName,
        reviewedAt: new Date().toISOString(),
        reviewedBy: 'apply-reviewed-team-ko-queue',
        sourceLabel: 'Reviewed DeepL pending candidate',
        sourceRef: `reviewed-deepl:${row.slug}`,
        sourceType: 'manual',
        status: 'approved',
        teamSlug: row.slug,
        notes: `Applied from ${path.basename(inputPath)} deeplPending`,
      });
    }

    for (const row of reviewedManual) {
      await upsertTeamTranslationCandidate(sql, {
        locale: 'ko',
        proposedName: row.name,
        proposedShortName: row.shortName,
        reviewedAt: new Date().toISOString(),
        reviewedBy: 'apply-reviewed-team-ko-queue',
        sourceLabel: 'Reviewed manual team ko queue',
        sourceRef: `reviewed-manual:${row.slug}`,
        sourceType: 'manual',
        status: 'approved',
        teamSlug: row.slug,
        notes: `Applied from ${path.basename(inputPath)} manualReviewNeeded`,
      });
    }

    const promotedCount = await promoteApprovedTeamTranslationCandidates(sql, 'ko', 'apply-reviewed-team-ko-queue');

    console.log(JSON.stringify({
      dryRun: false,
      inputPath,
      reviewedPendingCount: reviewedPending.length,
      reviewedManualCount: reviewedManual.length,
      promotedCount,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
