import { existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import type { NamedLocalizationEntry } from './ko-localization-data.mts';

interface MissingTeamRow {
  slug: string;
  en_name: string;
  ko_name: string | null;
  ko_short_name: string | null;
}

interface ReviewedTeamRow {
  slug: string;
  name: string;
  shortName: string;
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

function main() {
  const root = process.cwd();
  const reviewDir = path.join(root, '.sisyphus', 'team-ko-review');
  const resultsDir = path.join(reviewDir, 'results');
  const sourcePath = path.join(reviewDir, 'latest-team-ko-missing.json');
  const outputPath = path.join(root, 'scripts', 'ko-team-names.generated.json');

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
    throw new Error(
      JSON.stringify(
        {
          duplicateSlugs,
          unexpectedSlugs,
          missingSlugs,
        },
        null,
        2,
      ),
    );
  }

  const output: Record<string, NamedLocalizationEntry> = {};
  for (const row of reviewedRows) {
    output[row.slug] = {
      name: normalizeValue(row.name),
      shortName: normalizeValue(row.shortName),
    };
  }

  writeFileSync(outputPath, `${JSON.stringify(output, null, 2)}\n`);

  console.log(
    JSON.stringify(
      {
        sourceCount: expectedSlugs.length,
        resultFileCount: resultFiles.length,
        generatedCount: reviewedRows.length,
        outputPath,
      },
      null,
      2,
    ),
  );
}

main();
