import { execFile } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

interface CliOptions {
  contractOnly: boolean;
  cooldownMs?: number;
  cohortsFilePath?: string;
  help: boolean;
  limit?: number;
  only?: string;
  player?: string;
  retryDelayMs?: number;
  retryLimit?: number;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryInt(value: string | undefined, fallback: number) {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function isRetryableDatabaseError(message: string) {
  const normalized = message.toLowerCase();
  return normalized.includes('too many clients already') || normalized.includes('remaining connection slots are reserved');
}

interface BatchCohort {
  capologyLeague?: string;
  capologySeason?: string;
  competition: string;
  enabled?: boolean;
  season: string;
}

function parsePositiveInt(value: string | undefined) {
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { contractOnly: false, help: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--cohorts-file=')) {
      options.cohortsFilePath = arg.slice('--cohorts-file='.length).trim();
      continue;
    }
    if (arg === '--contract-only') {
      options.contractOnly = true;
      continue;
    }
    if (arg.startsWith('--cooldown-ms=')) {
      options.cooldownMs = parsePositiveInt(arg.slice('--cooldown-ms='.length));
      continue;
    }
    if (arg.startsWith('--retry-limit=')) {
      options.retryLimit = parseRetryInt(arg.slice('--retry-limit='.length), 4);
      continue;
    }
    if (arg.startsWith('--retry-delay-ms=')) {
      options.retryDelayMs = parseRetryInt(arg.slice('--retry-delay-ms='.length), 30000);
      continue;
    }
    if (arg.startsWith('--only=')) {
      options.only = arg.slice('--only='.length).trim();
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
  console.log(`Usage: node --experimental-strip-types scripts/run-player-enrichment-batch.mts [options]

Options:
  --cohorts-file=<path>  JSON array of { competition, season, capologyLeague?, capologySeason?, enabled? }
  --contract-only        Skip mapping search and FBref profile steps
  --cooldown-ms=<n>      Wait time between cohorts in milliseconds (default: 6000)
  --retry-limit=<n>      Retry count for transient DB exhaustion (default: 4)
  --retry-delay-ms=<n>   Wait before retry in milliseconds (default: 30000)
  --only=<competition/season>
                         Run only one cohort from the JSON list
  --player=<slug>        Restrict every cohort to one player slug
  --limit=<n>            Limit exported targets per cohort
  --help, -h             Show this help message
`);
}

function resolveCohortsPath(inputPath?: string) {
  const targetPath = inputPath?.trim() || path.join('data', 'player-enrichment-major-leagues.json');
  return path.isAbsolute(targetPath) ? targetPath : path.join(process.cwd(), targetPath);
}

function normalizeOnlyValue(value: string) {
  return value.trim().toLowerCase();
}

async function loadCohorts(inputPath?: string) {
  const cohortsPath = resolveCohortsPath(inputPath);
  const raw = await readFile(cohortsPath, 'utf8');
  const payload = JSON.parse(raw) as BatchCohort[];
  return {
    cohortsPath,
    cohorts: payload.filter((cohort) => cohort.enabled !== false),
  };
}

async function run(command: string, args: string[], label: string) {
  const { stdout, stderr } = await execFileAsync(command, args, { cwd: process.cwd(), maxBuffer: 10 * 1024 * 1024 });
  if (stdout.trim()) {
    console.log(`[${label}] stdout\n${stdout.trim()}`);
  }
  if (stderr.trim()) {
    console.log(`[${label}] stderr\n${stderr.trim()}`);
  }
}

async function runWithRetry(command: string, args: string[], label: string, retryLimit: number, retryDelayMs: number) {
  for (let attempt = 0; attempt <= retryLimit; attempt += 1) {
    try {
      await run(command, args, label);
      return;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const shouldRetry = attempt < retryLimit && isRetryableDatabaseError(message);
      if (!shouldRetry) {
        throw error;
      }

      console.log(`[${label}] retry ${attempt + 1}/${retryLimit} after transient DB error`);
      await sleep(retryDelayMs);
    }
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const cooldownMs = options.cooldownMs ?? 6000;
  const retryLimit = options.retryLimit ?? 4;
  const retryDelayMs = options.retryDelayMs ?? 30000;
  if (options.help) {
    printHelp();
    return;
  }

  await runWithRetry('node', ['--experimental-strip-types', 'scripts/ensure-player-enrichment-schema.mts'], 'ensure-schema', retryLimit, retryDelayMs);

  const { cohorts, cohortsPath } = await loadCohorts(options.cohortsFilePath);
  const only = options.only ? normalizeOnlyValue(options.only) : undefined;
  const selectedCohorts = only
    ? cohorts.filter((cohort) => normalizeOnlyValue(`${cohort.competition}/${cohort.season}`) === only)
    : cohorts;

  if (!selectedCohorts.length) {
    throw new Error(`No cohorts matched from ${cohortsPath}`);
  }

  const results: Array<{ cohort: string; status: 'ok' | 'failed'; error?: string }> = [];

  for (const cohort of selectedCohorts) {
    const label = `${cohort.competition}/${cohort.season}`;
    const args = [
      '--experimental-strip-types',
      'scripts/auto-populate-player-data.mts',
      `--competition=${cohort.competition}`,
      `--season=${cohort.season}`,
      '--skip-ensure-schema',
    ];

    if (cohort.capologyLeague) args.push(`--capology-league=${cohort.capologyLeague}`);
    if (cohort.capologySeason) args.push(`--capology-season=${cohort.capologySeason}`);
    if (options.player) args.push(`--player=${options.player}`);
    if (options.limit) args.push(`--limit=${options.limit}`);
    if (options.contractOnly) {
      args.push('--skip-search-mappings', '--skip-fbref');
    }

    try {
      await runWithRetry('node', args, label, retryLimit, retryDelayMs);
      results.push({ cohort: label, status: 'ok' });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.log(`[${label}] failed ${message}`);
      results.push({ cohort: label, status: 'failed', error: message });
    }

    await sleep(cooldownMs);
  }

  console.log(JSON.stringify({ cohortsFile: cohortsPath, results }, null, 2));

  if (results.some((result) => result.status === 'failed')) {
    process.exit(1);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
