import { execFile } from 'node:child_process';
import { access, copyFile, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { promisify } from 'node:util';
import { loadProjectEnv } from './load-project-env.mts';

const execFileAsync = promisify(execFile);

interface CliOptions {
  dryRun: boolean;
  help: boolean;
  limit?: number;
  player?: string;
}

interface TargetsPayload {
  mappedTargets?: number;
}

function parsePositiveInt(value: string | undefined) {
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { dryRun: false, help: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }
    if (arg.startsWith('--player=')) {
      options.player = arg.slice('--player='.length).trim();
      continue;
    }
    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
    }
  }
  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/backfill-player-nationalities.mts [options]

Options:
  --player=<slug>       Restrict to one player
  --limit=<n>           Limit missing-player targets processed
  --dry-run             Preview DB writes without persisting updates
  --help, -h            Show this help message
`);
}

function getBaseName(player?: string) {
  return path.join(process.cwd(), 'data', player ? `player-nationality-${player}` : 'player-nationality-backfill');
}

function resolveTransfermarktMappingsPath() {
  const configuredPath = process.env.TRANSFERMARKT_PLAYER_MAPPINGS_FILE?.trim();
  const targetPath = configuredPath || path.join('data', 'transfermarkt-player-mappings.json');
  return path.isAbsolute(targetPath) ? targetPath : path.join(process.cwd(), targetPath);
}

async function resolvePythonCommand() {
  const configuredPath = process.env.SCRAPERFC_PYTHON_PATH?.trim();
  const candidates = [
    configuredPath,
    path.join(process.cwd(), '.venv-scraperfc', 'bin', 'python'),
    'python3',
  ].filter((value): value is string => Boolean(value));

  for (const candidate of candidates) {
    if (candidate === 'python3') {
      return candidate;
    }

    try {
      await access(candidate);
      return candidate;
    } catch {
      continue;
    }
  }

  return 'python3';
}

async function run(command: string, args: string[], label: string, envOverrides?: Partial<NodeJS.ProcessEnv>) {
  const { stdout, stderr } = await execFileAsync(command, args, {
    cwd: process.cwd(),
    env: { ...process.env, ...envOverrides },
    maxBuffer: 10 * 1024 * 1024,
  });
  if (stdout.trim()) {
    console.log(`[${label}] stdout\n${stdout.trim()}`);
  }
  if (stderr.trim()) {
    console.log(`[${label}] stderr\n${stderr.trim()}`);
  }
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const baseName = getBaseName(options.player);
  const targetsPath = `${baseName}-targets.json`;
  const searchPath = `${baseName}-transfermarkt-search.json`;
  const dryRunMappingsPath = `${baseName}-transfermarkt-mappings.json`;
  const profilesPath = `${baseName}-transfermarkt.json`;
  const pythonCommand = await resolvePythonCommand();
  const envOverrides = options.dryRun
    ? { TRANSFERMARKT_PLAYER_MAPPINGS_FILE: dryRunMappingsPath }
    : undefined;

  if (options.dryRun) {
    try {
      await copyFile(resolveTransfermarktMappingsPath(), dryRunMappingsPath);
    } catch {
      await writeFile(dryRunMappingsPath, '[]\n', 'utf8');
    }
  }

  const exportArgs = ['--experimental-strip-types', 'scripts/export-missing-player-nationality-targets.mts', `--output=${targetsPath}`];
  if (options.player) exportArgs.push(`--player=${options.player}`);
  if (options.limit) exportArgs.push(`--limit=${options.limit}`);
  await run('node', exportArgs, 'export-targets', envOverrides);

  await run('node', ['--experimental-strip-types', 'scripts/search-player-profile-mappings.mts', '--provider=transfermarkt', `--input=${targetsPath}`, `--output=${searchPath}`, ...(options.limit ? [`--limit=${options.limit}`] : [])], 'transfermarkt-search', envOverrides);
  await run('node', ['--experimental-strip-types', 'scripts/apply-player-profile-search-candidates.mts', '--provider=transfermarkt', `--input=${searchPath}`], 'transfermarkt-apply', envOverrides);
  await run('node', exportArgs, 'export-targets-refresh', envOverrides);

  const targetsPayload = JSON.parse(await readFile(targetsPath, 'utf8')) as TargetsPayload;
  if (!targetsPayload.mappedTargets) {
    console.log(JSON.stringify({ mappedTargets: 0, message: 'No searchable Transfermarkt targets were resolved for missing country_id players.' }, null, 2));
    return;
  }

  await run(pythonCommand, ['scripts/fetch-player-contracts-transfermarkt.py', `--targets=${targetsPath}`, `--output=${profilesPath}`, ...(options.limit ? [`--limit=${options.limit}`] : [])], 'transfermarkt-fetch');

  const syncArgs = ['--experimental-strip-types', 'scripts/sync-player-nationalities.mts', `--input=${profilesPath}`];
  if (options.player) syncArgs.push(`--player=${options.player}`);
  if (options.limit) syncArgs.push(`--limit=${options.limit}`);
  if (options.dryRun) syncArgs.push('--dry-run');
  await run('node', syncArgs, 'nationality-sync');
}

await main();
