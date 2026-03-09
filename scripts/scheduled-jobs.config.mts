export interface ScheduledJobCommand {
  args: string[];
  description: string;
}

export interface ScheduledJobDefinition {
  id: string;
  description: string;
  schedule: string;
  commands: ScheduledJobCommand[];
}

const NODE_EXECUTABLE = process.execPath;

export const SCHEDULED_JOBS: ScheduledJobDefinition[] = [
  {
    id: 'nation-assets-sync',
    description: '국가 국기 및 국가대표 로고 일일 동기화',
    schedule: '12 2 * * *',
    commands: [
      {
        args: [NODE_EXECUTABLE, '--experimental-strip-types', 'scripts/sync-nation-assets.mts'],
        description: '국가 자산 동기화',
      },
    ],
  },
  {
    id: 'fifa-rankings-sync',
    description: '공식 FIFA 랭킹 일일 동기화',
    schedule: '27 2 * * *',
    commands: [
      {
        args: [NODE_EXECUTABLE, '--experimental-strip-types', 'scripts/sync-fifa-rankings.mts'],
        description: '공식 FIFA 랭킹 동기화',
      },
    ],
  },
  {
    id: 'tab-localization-repair',
    description: '리그·구단·국가 탭 로컬라이제이션 누락 일일 보정',
    schedule: '18 3 * * *',
    commands: [
      {
        args: [NODE_EXECUTABLE, '--experimental-strip-types', 'scripts/repair-tab-localizations.mts'],
        description: '탭 로컬라이제이션 누락 보정',
      },
    ],
  },
  {
    id: 'match-events-hourly',
    description: '경기 이벤트 시간 단위 증분 수집',
    schedule: '8 * * * *',
    commands: [
      {
        args: [NODE_EXECUTABLE, '--experimental-strip-types', 'scripts/sync-match-events.mts', '--profile=hourly'],
        description: '시간 단위 경기 이벤트 수집',
      },
    ],
  },
  {
    id: 'match-events-daily',
    description: '경기 이벤트 일 단위 재동기화',
    schedule: '48 2 * * *',
    commands: [
      {
        args: [NODE_EXECUTABLE, '--experimental-strip-types', 'scripts/sync-match-events.mts', '--profile=daily'],
        description: '일 단위 경기 이벤트 재동기화',
      },
    ],
  },
];

export function getScheduledJob(jobId: string) {
  return SCHEDULED_JOBS.find((job) => job.id === jobId);
}
