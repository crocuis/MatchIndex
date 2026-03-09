import { mkdir } from 'node:fs/promises';
import { execFile, spawn } from 'node:child_process';
import path from 'node:path';
import { promisify } from 'node:util';
import { loadProjectEnv } from './load-project-env.mts';
import { getScheduledJob, SCHEDULED_JOBS } from './scheduled-jobs.config.mts';

const execFileAsync = promisify(execFile);
const CRON_BEGIN = '# BEGIN MATCHINDEX SCHEDULED JOBS';
const CRON_END = '# END MATCHINDEX SCHEDULED JOBS';
const CRON_TIMEZONE = 'Asia/Seoul';

interface CliOptions {
  action: 'install' | 'print' | 'remove';
  help: boolean;
  jobIds?: string[];
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    action: 'print',
    help: false,
  };

  for (const arg of argv) {
    if (arg === '--install') {
      options.action = 'install';
      continue;
    }

    if (arg === '--remove') {
      options.action = 'remove';
      continue;
    }

    if (arg === '--print') {
      options.action = 'print';
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--jobs=')) {
      const values = arg.slice('--jobs='.length)
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean);

      if (values.length > 0) {
        options.jobIds = values;
      }
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/manage-scheduled-cron.mts [options]

Options:
  --print          Print the managed cron block (default)
  --install        Install or update the managed cron block in crontab
  --remove         Remove the managed cron block from crontab
  --jobs=<a,b>     Limit to selected jobs
  --help, -h       Show this help message
`);
}

function shellQuote(value: string) {
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}

function resolveJobs(jobIds?: string[]) {
  if (!jobIds || jobIds.length === 0) {
    return SCHEDULED_JOBS;
  }

  return jobIds.map((jobId) => {
    const job = getScheduledJob(jobId);
    if (!job) {
      throw new Error(`Unknown job: ${jobId}`);
    }

    return job;
  });
}

function buildCronBlock(jobIds?: string[]) {
  const repoRoot = process.cwd();
  const runnerPath = path.join(repoRoot, 'scripts', 'run-scheduled-job.mts');
  const logDir = path.join(repoRoot, 'logs', 'scheduler');
  const jobs = resolveJobs(jobIds);
  const lines = jobs.map((job) => {
    const logPath = path.join(logDir, `${job.id}.log`);
    return `${job.schedule} cd ${shellQuote(repoRoot)} && mkdir -p ${shellQuote(logDir)} && ${shellQuote(process.execPath)} --experimental-strip-types ${shellQuote(runnerPath)} --job=${job.id} >> ${shellQuote(logPath)} 2>&1`;
  });

  return [CRON_BEGIN, `CRON_TZ=${CRON_TIMEZONE}`, ...lines, CRON_END].join('\n');
}

async function readCurrentCrontab() {
  try {
    const { stdout } = await execFileAsync('crontab', ['-l'], { cwd: process.cwd() });
    return stdout.trimEnd();
  } catch (error) {
    const output = error as { stderr?: string; stdout?: string; code?: number };
    const stderr = `${output.stderr ?? ''}${output.stdout ?? ''}`;
    if (output.code === 1 && /no crontab for/i.test(stderr)) {
      return '';
    }

    throw error;
  }
}

function stripManagedBlock(contents: string) {
  const pattern = new RegExp(`${CRON_BEGIN}[\\s\\S]*?${CRON_END}\\n?`, 'g');
  return contents.replace(pattern, '').trim();
}

async function installCronBlock(jobIds?: string[]) {
  const current = await readCurrentCrontab();
  const cleaned = stripManagedBlock(current);
  const block = buildCronBlock(jobIds);
  const next = [cleaned, block].filter(Boolean).join('\n\n').trim() + '\n';
  await writeCrontab(next);
}

async function removeCronBlock() {
  const current = await readCurrentCrontab();
  const next = stripManagedBlock(current);
  const payload = next ? `${next}\n` : '';
  await writeCrontab(payload);
}

async function writeCrontab(contents: string) {
  return new Promise<void>((resolve, reject) => {
    const child = spawn('crontab', ['-'], {
      cwd: process.cwd(),
      env: process.env,
      stdio: ['pipe', 'inherit', 'pipe'],
    });

    let stderr = '';

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('error', reject);
    child.on('exit', (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(stderr || `crontab exited with code ${code ?? 'unknown'}`));
    });

    child.stdin.write(contents);
    child.stdin.end();
  });
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  await mkdir(path.join(process.cwd(), 'logs', 'scheduler'), { recursive: true });

  if (options.action === 'print') {
    console.log(buildCronBlock(options.jobIds));
    return;
  }

  if (options.action === 'install') {
    await installCronBlock(options.jobIds);
    console.log(JSON.stringify({ ok: true, action: 'install', jobs: resolveJobs(options.jobIds).map((job) => job.id) }, null, 2));
    return;
  }

  await removeCronBlock();
  console.log(JSON.stringify({ ok: true, action: 'remove' }, null, 2));
}

await main();
