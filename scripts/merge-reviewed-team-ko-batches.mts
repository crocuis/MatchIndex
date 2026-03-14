import { existsSync, readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { upsertTeamTranslationCandidate } from './team-translation-candidates.mts';

interface CliOptions {
  dryRun: boolean;
}

interface MissingTeamRow {
  slug: string;
}

interface ReviewedTeamRow {
  slug: string;
  name: string;
  shortName: string;
}

function parseArgs(argv: string[]): CliOptions {
  return {
    dryRun: argv.includes('--dry-run'),
  };
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, prepare: false, idle_timeout: 5 });
}

function readJsonFile<T>(filePath: string): T {
  return JSON.parse(readFileSync(filePath, 'utf8')) as T;
}

function normalizeValue(value: string): string {
  return value.trim();
}

function assertReviewedRow(row: ReviewedTeamRow, filePath: string, index: number) {
  if (typeof row.slug !== 'string' || typeof row.name !== 'string' || typeof row.shortName !== 'string') {
    throw new Error(`Invalid row shape at ${filePath}[${index}]`);
  }

  if (!normalizeValue(row.slug) || !normalizeValue(row.name) || !normalizeValue(row.shortName)) {
    throw new Error(`Blank value at ${filePath}[${index}]`);
  }
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  const root = process.cwd();
  const reviewDir = path.join(root, '.sisyphus', 'team-ko-review');
  const resultsDir = path.join(reviewDir, 'results');
  const sourcePath = path.join(reviewDir, 'latest-team-ko-missing.full.json');

  if (!existsSync(sourcePath)) {
    throw new Error(`Missing source file: ${sourcePath}`);
  }

  if (!existsSync(resultsDir)) {
    throw new Error(`Missing results directory: ${resultsDir}`);
  }

  const sourceRows = readJsonFile<MissingTeamRow[]>(sourcePath);
  const expectedSlugs = sourceRows.map((row) => row.slug);
  const expectedSlugSet = new Set(expectedSlugs);

  const resultFiles = readdirSync(resultsDir)
    .filter((fileName) => /^batch-\d+-ko\.json$/.test(fileName))
    .sort();

  const reviewedRows = resultFiles.flatMap((fileName) => {
    const filePath = path.join(resultsDir, fileName);
    const rows = readJsonFile<ReviewedTeamRow[]>(filePath);

    rows.forEach((row, index) => {
      assertReviewedRow(row, filePath, index);
    });

    return rows;
  });

  const seenSlugs = new Set<string>();
  const duplicateSlugs: string[] = [];
  const unexpectedSlugs: string[] = [];

  for (const row of reviewedRows) {
    if (!expectedSlugSet.has(row.slug)) {
      unexpectedSlugs.push(row.slug);
    }

    if (seenSlugs.has(row.slug)) {
      duplicateSlugs.push(row.slug);
      continue;
    }

    seenSlugs.add(row.slug);
  }

  const missingSlugs = expectedSlugs.filter((slug) => !seenSlugs.has(slug));
  if (duplicateSlugs.length > 0 || unexpectedSlugs.length > 0 || missingSlugs.length > 0) {
    throw new Error(JSON.stringify({ duplicateSlugs, unexpectedSlugs, missingSlugs }, null, 2));
  }

  if (options.dryRun) {
    console.log(JSON.stringify({
      dryRun: true,
      sourceCount: expectedSlugs.length,
      resultFileCount: resultFiles.length,
      candidateCount: reviewedRows.length,
    }, null, 2));
    return;
  }

  const sql = getSql();

  try {
    for (const row of reviewedRows) {
      await upsertTeamTranslationCandidate(sql, {
        locale: 'ko',
        proposedName: normalizeValue(row.name),
        proposedShortName: normalizeValue(row.shortName),
        reviewedAt: new Date().toISOString(),
        reviewedBy: 'merge-reviewed-team-ko-batches',
        sourceLabel: 'Reviewed team ko batch',
        sourceRef: `review:${row.slug}`,
        sourceType: 'manual',
        status: 'approved',
        teamSlug: normalizeValue(row.slug),
      });
    }

    console.log(JSON.stringify({
      dryRun: false,
      sourceCount: expectedSlugs.length,
      resultFileCount: resultFiles.length,
      candidateCount: reviewedRows.length,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main();
