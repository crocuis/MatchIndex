import { mkdir, open, rm } from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { loadProjectEnv } from './load-project-env.mts';
import { getScheduledJob, SCHEDULED_JOBS } from './scheduled-jobs.config.mts';

interface CliOptions {
  allowOverlap: boolean;
  dryRun: boolean;
  help: boolean;
  jobId?: string;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    allowOverlap: false,
    dryRun: false,
    help: false,
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--allow-overlap') {
      options.allowOverlap = true;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--job=')) {
      const value = arg.slice('--job='.length).trim();
      if (value) {
        options.jobId = value;
      }
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/run-scheduled-job.mts --job=<id> [options]

Options:
  --dry-run         Print the job plan without executing commands
  --allow-overlap   Skip lock acquisition and allow concurrent runs
  --help, -h        Show this help message

Available jobs:
${SCHEDULED_JOBS.map((job) => `  - ${job.id}: ${job.description}`).join('\n')}
`);
}

async function acquireLock(jobId: string) {
  const lockDir = path.join(process.cwd(), '.scheduler-locks');
  await mkdir(lockDir, { recursive: true });
  const lockPath = path.join(lockDir, `${jobId}.lock`);
  const handle = await open(lockPath, 'wx');
  await handle.writeFile(JSON.stringify({ jobId, pid: process.pid, startedAt: new Date().toISOString() }, null, 2));
  return {
    lockPath,
    async release() {
      await handle.close();
      await rm(lockPath, { force: true });
    },
  };
}

async function runCommand(args: string[], description: string) {
  return new Promise<void>((resolve, reject) => {
    const executable = args[0] === 'node' ? process.execPath : args[0];
    const child = spawn(executable, args.slice(1), {
      cwd: process.cwd(),
      env: process.env,
      stdio: 'inherit',
    });

    child.on('exit', (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(`${description} failed with exit code ${code ?? 'unknown'}`));
    });

    child.on('error', reject);
  });
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  if (!options.jobId) {
    throw new Error('Missing --job option');
  }

  const job = getScheduledJob(options.jobId);
  if (!job) {
    throw new Error(`Unknown job: ${options.jobId}`);
  }

  if (options.dryRun) {
    console.log(JSON.stringify({
      dryRun: true,
      jobId: job.id,
      description: job.description,
      schedule: job.schedule,
      commands: job.commands,
    }, null, 2));
    return;
  }

  let lock: Awaited<ReturnType<typeof acquireLock>> | null = null;

  if (!options.allowOverlap) {
    try {
      lock = await acquireLock(job.id);
    } catch {
      console.log(JSON.stringify({
        skipped: true,
        reason: 'job already running',
        jobId: job.id,
      }, null, 2));
      return;
    }
  }

  const startedAt = new Date().toISOString();

  try {
    for (const command of job.commands) {
      await runCommand(command.args, command.description);
    }

    console.log(JSON.stringify({
      ok: true,
      jobId: job.id,
      startedAt,
      finishedAt: new Date().toISOString(),
      commandCount: job.commands.length,
    }, null, 2));
  } finally {
    await lock?.release();
  }
}

await main();
