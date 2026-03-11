import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

interface CliOptions {
  help: boolean;
  input: string;
  output: string;
}

interface ConflictGroup {
  key: string;
  names: string[];
  slugs: string[];
}

interface ConflictReport {
  generatedAt: string;
  conflictCount: number;
  conflicts: ConflictGroup[];
}

interface MergeEntry {
  aliasSlug: string;
  canonicalSlug: string;
  aliasName: string;
  canonicalName: string;
  countryCode: string;
  leagueSlug: null;
  reason: string;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    help: false,
    input: path.join('logs', 'club-fullname-conflicts.json'),
    output: path.join('logs', 'club-fullname-conflicts-merge-batch.json'),
  };

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--input=')) {
      options.input = arg.slice('--input='.length).trim() || options.input;
      continue;
    }

    if (arg.startsWith('--output=')) {
      options.output = arg.slice('--output='.length).trim() || options.output;
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/prepare-club-fullname-conflict-batch.mts [options]

Options:
  --input=<path>   Input conflict report JSON (default: logs/club-fullname-conflicts.json)
  --output=<path>  Output merge batch JSON (default: logs/club-fullname-conflicts-merge-batch.json)
  --help, -h       Show this help message
`);
}

function scoreFullName(value: string) {
  const hasClubToken = /\b(fc|cf|afc)\b/i.test(value);
  return Number(hasClubToken) * 10 + value.length;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const report = JSON.parse(await readFile(options.input, 'utf8')) as ConflictReport;
  const mergeEntries: MergeEntry[] = report.conflicts.map((conflict) => {
    const scored = conflict.names
      .map((name, index) => ({ name, slug: conflict.slugs[index], score: scoreFullName(name) }))
      .sort((left, right) => right.score - left.score || left.slug.localeCompare(right.slug));

    const canonical = scored[0];
    const alias = scored[scored.length - 1];

    return {
      aliasSlug: alias.slug,
      canonicalSlug: canonical.slug,
      aliasName: alias.name,
      canonicalName: canonical.name,
      countryCode: conflict.key.split(':')[0] ?? '',
      leagueSlug: null,
      reason: 'promote full-name club slug over short-name variant',
    };
  });

  const output = {
    generatedAt: new Date().toISOString(),
    sourceConflictCount: report.conflictCount,
    selectedCount: mergeEntries.length,
    mergeEntries,
    mergeCommand: `node --experimental-strip-types scripts/merge-duplicate-entities.mts --teams-only --batch-file=${options.output}`,
  };

  await writeFile(options.output, JSON.stringify(output, null, 2), 'utf8');
  console.log(JSON.stringify({ ...output, outputPath: options.output }, null, 2));
}

await main();
