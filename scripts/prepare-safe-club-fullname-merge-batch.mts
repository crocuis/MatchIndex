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

const EXCLUDED_TOKENS = /\b(afc|wfc|fcw|lfc)\b/i;
const PREFERRED_TOKENS = /\b(fc|cf|ac|sc|cfc|rc|rcd|ca|cd|ud|club)\b/i;

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    help: false,
    input: path.join('logs', 'club-fullname-conflicts.json'),
    output: path.join('logs', 'club-safe-fullname-merge-batch.json'),
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
  console.log(`Usage: node --experimental-strip-types scripts/prepare-safe-club-fullname-merge-batch.mts [options]

Options:
  --input=<path>   Input conflict report JSON (default: logs/club-fullname-conflicts.json)
  --output=<path>  Output merge batch JSON (default: logs/club-safe-fullname-merge-batch.json)
  --help, -h       Show this help message
`);
}

function normalizeName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/["'’.]/g, '')
    .replace(/&/g, ' and ')
    .replace(/\b(football club|futbol club|club de futbol)\b/gi, ' ')
    .replace(/\b(fc|cf|ac|sc|afc|cfc|fk|sk|wfc|fcw|lfc|rc|rcd|ca|cd|ud|club)\b/gi, ' ')
    .replace(/\b(de|del|de la|de las|de los)\b/gi, ' ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function scoreName(value: string) {
  return Number(PREFERRED_TOKENS.test(value)) * 100 + value.length;
}

function isSafeConflict(conflict: ConflictGroup) {
  if (conflict.names.length !== 2 || conflict.slugs.length !== 2) {
    return false;
  }

  if (conflict.names.some((name) => EXCLUDED_TOKENS.test(name))) {
    return false;
  }

  const normalized = conflict.names.map((name) => normalizeName(name));
  return normalized[0] === normalized[1];
}

async function main() {
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const report = JSON.parse(await readFile(options.input, 'utf8')) as ConflictReport;
  const mergeEntries: MergeEntry[] = report.conflicts
    .filter(isSafeConflict)
    .map((conflict) => {
      const candidates = conflict.names.map((name, index) => ({ name, slug: conflict.slugs[index], score: scoreName(name) }))
        .sort((left, right) => right.score - left.score || left.slug.localeCompare(right.slug));

      const canonical = candidates[0];
      const alias = candidates[1];

      return {
        aliasSlug: alias.slug,
        canonicalSlug: canonical.slug,
        aliasName: alias.name,
        canonicalName: canonical.name,
        countryCode: conflict.key.split(':')[0] ?? '',
        leagueSlug: null,
        reason: 'safe full-name token merge',
      };
    });

  const output = {
    generatedAt: new Date().toISOString(),
    inputPath: options.input,
    sourceConflictCount: report.conflictCount,
    selectedCount: mergeEntries.length,
    mergeEntries,
    mergeCommand: `node --experimental-strip-types scripts/merge-duplicate-entities.mts --teams-only --batch-file=${options.output}`,
  };

  await writeFile(options.output, JSON.stringify(output, null, 2), 'utf8');
  console.log(JSON.stringify({ ...output, outputPath: options.output }, null, 2));
}

await main();
