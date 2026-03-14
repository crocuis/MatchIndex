import { spawn } from 'node:child_process';
import { loadProjectEnv } from './load-project-env.mts';

interface MatchEventProfile {
  competitionLimit: number;
  description: string;
  matchesPerSeasonLimit: number;
}

interface CliOptions {
  dryRun: boolean;
  help: boolean;
  profile: 'daily' | 'hourly';
}

const PROFILES: Record<CliOptions['profile'], MatchEventProfile> = {
  hourly: {
    description: '최근 범위 위주의 시간 단위 경기 이벤트 수집',
    competitionLimit: 12,
    matchesPerSeasonLimit: 40,
  },
  daily: {
    description: '더 넓은 범위를 재동기화하는 일 단위 경기 이벤트 수집',
    competitionLimit: 80,
    matchesPerSeasonLimit: 380,
  },
};

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: false,
    help: false,
    profile: 'hourly',
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--profile=')) {
      const value = arg.slice('--profile='.length);
      if (value === 'daily' || value === 'hourly') {
        options.profile = value;
      }
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-match-events.mts [options]

Options:
  --profile=hourly|daily   Select sync profile (default: hourly)
  --dry-run                Print the execution plan without running commands
  --help, -h               Show this help message
`);
}

function buildCommands(profileName: CliOptions['profile'], profile: MatchEventProfile) {
  const limits = [String(profile.competitionLimit), String(profile.matchesPerSeasonLimit)];

  return [
    {
      description: 'API-Football 경기 라인업 동기화',
      args: ['node', '--experimental-strip-types', 'scripts/sync-api-football-match-lineups.mts', `--profile=${profileName}`, '--write'],
    },
    {
      description: 'API-Football 경기 이벤트 동기화',
      args: ['node', '--experimental-strip-types', 'scripts/sync-api-football-match-events.mts', `--profile=${profileName}`, '--write'],
    },
    {
      description: '경기 기본 데이터 적재',
      args: ['node', '--experimental-strip-types', 'scripts/statsbomb-materialize-core.mts', ...limits, '--write'],
    },
    {
      description: '경기 이벤트 상세 적재',
      args: ['node', '--experimental-strip-types', 'scripts/statsbomb-materialize-details.mts', ...limits, '--write'],
    },
  ];
}

async function runCommand(args: string[], description: string) {
  return new Promise<void>((resolve, reject) => {
    const child = spawn(args[0], args.slice(1), {
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

  const profile = PROFILES[options.profile];
  const commands = buildCommands(options.profile, profile);

  if (options.dryRun) {
    console.log(JSON.stringify({
      dryRun: true,
      profile: options.profile,
      ...profile,
      commands,
    }, null, 2));
    return;
  }

  for (const command of commands) {
    await runCommand(command.args, command.description);
  }

  console.log(JSON.stringify({
    ok: true,
    profile: options.profile,
    ...profile,
    commandCount: commands.length,
  }, null, 2));
}

await main();
