import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

interface CliOptions {
  help: boolean;
  input: string;
  output: string;
}

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

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    help: false,
    input: path.join('logs', 'club-substring-conflicts.json'),
    output: path.join('logs', 'club-substring-conflicts-merge-batch.json'),
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
  console.log(`Usage: node --experimental-strip-types scripts/prepare-club-substring-conflict-batch.mts [options]

Options:
  --input=<path>   Input substring conflict JSON (default: logs/club-substring-conflicts.json)
  --output=<path>  Output merge batch JSON (default: logs/club-substring-conflicts-merge-batch.json)
  --help, -h       Show this help message
`);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const report = JSON.parse(await readFile(options.input, 'utf8')) as ConflictReport;
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
      reason: 'promote official expanded full-name slug over substring-style short name',
    })),
    mergeCommand: `node --experimental-strip-types scripts/apply-team-merge-batch-sequential.mts --surface-only --batch-file=${options.output}`,
  };

  await writeFile(options.output, JSON.stringify(output, null, 2), 'utf8');
  console.log(JSON.stringify({ ...output, outputPath: options.output }, null, 2));
}

await main();
