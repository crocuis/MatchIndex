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
    id: 'api-football-fixtures-daily',
    description: 'API-Football 5대 리그·챔스·유로파 당일 경기 정보 일일 동기화',
    schedule: '42 3 * * *',
    commands: [
      {
        args: [
          NODE_EXECUTABLE,
          '--experimental-strip-types',
          'scripts/sync-api-football-fixtures.mts',
          '--today',
          '--timezone=Asia/Seoul',
        ],
        description: 'API-Football 당일 경기 정보 동기화',
      },
    ],
  },
  {
    id: 'football-data-fixtures-daily',
    description: 'football-data.org 5대 리그·챔스·유로파 당일 경기 일정 일일 동기화',
    schedule: '39 3 * * *',
    commands: [
      {
        args: [
          NODE_EXECUTABLE,
          '--experimental-strip-types',
          'scripts/sync-football-data-fixtures.mts',
          '--today',
          '--timezone=Asia/Seoul',
        ],
        description: 'football-data.org 당일 경기 일정 동기화',
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
