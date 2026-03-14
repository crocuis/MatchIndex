import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';

interface CliOptions {
  batchFile: string;
  help: boolean;
  skipRefresh: boolean;
  surfaceOnly: boolean;
}

interface MergeEntry {
  aliasSlug: string;
  canonicalSlug: string;
}

interface MergeBatchFile {
  mergeEntries?: MergeEntry[];
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    batchFile: path.join('logs', 'club-fullname-conflicts-merge-batch.json'),
    help: false,
    skipRefresh: false,
    surfaceOnly: false,
  };

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--batch-file=')) {
      options.batchFile = arg.slice('--batch-file='.length).trim() || options.batchFile;
      continue;
    }

    if (arg === '--surface-only') {
      options.surfaceOnly = true;
      continue;
    }

    if (arg === '--skip-refresh') {
      options.skipRefresh = true;
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/apply-team-merge-batch-sequential.mts [options]

Options:
  --batch-file=<path>  Merge batch JSON path (default: logs/club-fullname-conflicts-merge-batch.json)
  --surface-only       Archive alias team rows instead of rewriting deep event history
  --skip-refresh       Skip per-entry materialized view refresh
  --help, -h           Show this help message
`);
}

async function runMergeForEntry(entry: MergeEntry, tempBatchFile: string, surfaceOnly: boolean, skipRefresh: boolean) {
  await writeFile(tempBatchFile, JSON.stringify({ mergeEntries: [entry] }, null, 2), 'utf8');

  const args = [
    '--experimental-strip-types',
    'scripts/merge-duplicate-entities.mts',
    '--teams-only',
    `--batch-file=${tempBatchFile}`,
  ];

  if (surfaceOnly) {
    args.push('--surface-only');
  }

  if (skipRefresh) {
    args.push('--skip-refresh');
  }

  await new Promise<void>((resolve, reject) => {
    const child = spawn(process.execPath, args, {
      cwd: process.cwd(),
      env: process.env,
      stdio: 'inherit',
    });

    child.on('error', reject);
    child.on('exit', (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(`merge failed for ${entry.aliasSlug} -> ${entry.canonicalSlug} (exit ${code ?? 'unknown'})`));
    });
  });
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const payload = JSON.parse(await readFile(options.batchFile, 'utf8')) as MergeBatchFile;
  const mergeEntries = payload.mergeEntries ?? [];
  const tempDir = path.join('logs', 'tmp-team-merge-batch');
  await mkdir(tempDir, { recursive: true });

  const completed: string[] = [];

  try {
    for (const entry of mergeEntries) {
      const tempBatchFile = path.join(tempDir, `${entry.aliasSlug}.json`);
      await runMergeForEntry(entry, tempBatchFile, options.surfaceOnly, options.skipRefresh);
      completed.push(`${entry.aliasSlug}->${entry.canonicalSlug}`);
      await rm(tempBatchFile, { force: true });
    }

    console.log(JSON.stringify({ ok: true, completedCount: completed.length, completed }, null, 2));
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

await main();
