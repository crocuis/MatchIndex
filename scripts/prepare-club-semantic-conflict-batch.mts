import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

interface ConflictEntry {
  key: string;
  aliasSlug: string;
  aliasName: string;
  canonicalSlug: string;
  canonicalName: string;
}

interface ConflictReport {
  generatedAt: string;
  conflictCount: number;
  conflicts: ConflictEntry[];
}

async function main() {
  const inputPath = path.join('logs', 'club-substring-conflicts.json');
  const outputPath = path.join('logs', 'club-semantic-conflicts-merge-batch.json');
  const report = JSON.parse(await readFile(inputPath, 'utf8')) as ConflictReport;
  const output = {
    generatedAt: new Date().toISOString(),
    sourceConflictCount: report.conflictCount,
    selectedCount: report.conflicts.length,
    mergeEntries: report.conflicts.map((entry) => ({
      aliasSlug: entry.aliasSlug,
      canonicalSlug: entry.canonicalSlug,
      aliasName: entry.aliasName,
      canonicalName: entry.canonicalName,
      countryCode: entry.key.split(':')[0] ?? '',
      leagueSlug: null,
      reason: 'promote semantic fullname canonical slug',
    })),
    mergeCommand: `node --experimental-strip-types scripts/apply-team-merge-batch-sequential.mts --surface-only --batch-file=${outputPath}`,
  };

  await writeFile(outputPath, JSON.stringify(output, null, 2), 'utf8');
  console.log(JSON.stringify({ ...output, outputPath }, null, 2));
}

await main();
