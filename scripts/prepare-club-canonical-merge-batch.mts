import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

interface CliOptions {
  help: boolean;
  input: string;
  output: string;
  limit?: number;
}

interface CandidateMatch {
  aliasSlug: string;
  aliasName: string;
  canonicalSlug: string;
  canonicalName: string;
  countryCode: string;
  gender: 'male' | 'female' | 'mixed';
  leagueSlug: string | null;
  reason: string;
  confidence: 'high';
}

interface CandidateReport {
  generatedAt: string;
  totalTargets: number;
  candidateCount: number;
  candidates: CandidateMatch[];
}

interface MergeBatchEntry {
  aliasSlug: string;
  canonicalSlug: string;
  aliasName: string;
  canonicalName: string;
  countryCode: string;
  leagueSlug: string | null;
  reason: string;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    help: false,
    input: path.join('logs', 'club-canonical-match-candidates.json'),
    output: path.join('logs', 'club-canonical-merge-batch.json'),
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
      continue;
    }

    if (arg.startsWith('--limit=')) {
      const parsed = Number.parseInt(arg.slice('--limit='.length), 10);
      if (Number.isFinite(parsed) && parsed > 0) {
        options.limit = parsed;
      }
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/prepare-club-canonical-merge-batch.mts [options]

Options:
  --input=<path>     Candidate JSON path (default: logs/club-canonical-match-candidates.json)
  --output=<path>    Merge batch JSON path (default: logs/club-canonical-merge-batch.json)
  --limit=<n>        Limit number of merge entries to include
  --help, -h         Show this help message
`);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const input = JSON.parse(await readFile(options.input, 'utf8')) as CandidateReport;
  const selected = options.limit ? input.candidates.slice(0, options.limit) : input.candidates;
  const mergeEntries: MergeBatchEntry[] = selected.map((candidate) => ({
    aliasSlug: candidate.aliasSlug,
    canonicalSlug: candidate.canonicalSlug,
    aliasName: candidate.aliasName,
    canonicalName: candidate.canonicalName,
    countryCode: candidate.countryCode,
    leagueSlug: candidate.leagueSlug,
    reason: candidate.reason,
  }));

  const mergeCommand = mergeEntries.length > 0
    ? `node --experimental-strip-types scripts/merge-duplicate-entities.mts --teams-only --dry-run --aliases=${mergeEntries.map((entry) => entry.aliasSlug).join(',')}`
    : null;

  const report = {
    generatedAt: new Date().toISOString(),
    inputPath: options.input,
    totalCandidateCount: input.candidateCount,
    selectedCount: mergeEntries.length,
    aliases: mergeEntries.map((entry) => entry.aliasSlug),
    mergeEntries,
    mergeCommand,
  };

  await writeFile(options.output, JSON.stringify(report, null, 2), 'utf8');
  console.log(JSON.stringify({ ...report, outputPath: options.output }, null, 2));
}

await main();
