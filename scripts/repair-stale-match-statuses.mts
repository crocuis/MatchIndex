import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

type RepairStatus = 'finished' | 'cancelled' | 'postponed' | 'suspended';

interface CliOptions {
  dryRun: boolean;
  graceDays: number;
  help: boolean;
  noScoreStatus: Exclude<RepairStatus, 'finished'>;
}

interface CandidateRow {
  id: string;
  competition_slug: string;
  match_date: string;
  kickoff_at: string | null;
  status: string;
  home_team: string;
  away_team: string;
  home_score: number | null;
  away_score: number | null;
}

function parsePositiveInt(value: string | undefined, fallback: number) {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNoScoreStatus(value: string | undefined): CliOptions['noScoreStatus'] {
  if (value === 'cancelled' || value === 'postponed' || value === 'suspended') {
    return value;
  }

  return 'cancelled';
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: true,
    graceDays: 14,
    help: false,
    noScoreStatus: 'cancelled',
  };

  for (const arg of argv) {
    if (arg === '--write') {
      options.dryRun = false;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--grace-days=')) {
      options.graceDays = parsePositiveInt(arg.slice('--grace-days='.length), options.graceDays);
      continue;
    }

    if (arg.startsWith('--no-score-status=')) {
      options.noScoreStatus = parseNoScoreStatus(arg.slice('--no-score-status='.length));
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/repair-stale-match-statuses.mts [options]

기본 동작:
  - 과거 경기인데 status가 scheduled/timed로 남은 건수를 점검한다.
  - 스코어가 있는 경기는 finished로 승격한다.
  - 스코어가 없는 오래된 경기는 설정한 상태(cancelled/postponed/suspended)로 전환한다.

Options:
  --write                          실제 UPDATE 반영 (기본값은 dry-run)
  --grace-days=<n>                 무스코어 경기 stale 판정 일수 (기본값: 14)
  --no-score-status=<status>       cancelled | postponed | suspended (기본값: cancelled)
  --help, -h                       도움말 출력
`);
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(process.env.DATABASE_URL, {
    max: 1,
    idle_timeout: 20,
    prepare: false,
  });

  try {
    const candidateRows = await sql<CandidateRow[]>`
      SELECT
        m.id::TEXT AS id,
        c.slug AS competition_slug,
        m.match_date::TEXT AS match_date,
        m.kickoff_at::TEXT AS kickoff_at,
        m.status::TEXT AS status,
        home.slug AS home_team,
        away.slug AS away_team,
        m.home_score,
        m.away_score
      FROM matches m
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      JOIN teams home ON home.id = m.home_team_id
      JOIN teams away ON away.id = m.away_team_id
      WHERE m.status IN ('scheduled', 'timed')
        AND COALESCE(m.kickoff_at, m.match_date::timestamp) < NOW()
      ORDER BY COALESCE(m.kickoff_at, m.match_date::timestamp) DESC, m.id DESC
    `;

    const withScores = candidateRows.filter((row) => row.home_score !== null || row.away_score !== null);
    const scorelessCutoffRows = candidateRows.filter((row) => {
      if (row.home_score !== null || row.away_score !== null) {
        return false;
      }

      const referenceValue = row.kickoff_at ?? `${row.match_date} 00:00:00+00`;
      const referenceTime = Date.parse(referenceValue);
      if (Number.isNaN(referenceTime)) {
        return false;
      }

      const ageMs = Date.now() - referenceTime;
      return ageMs >= options.graceDays * 24 * 60 * 60 * 1000;
    });

    const scoreFixIds = withScores.map((row) => Number(row.id));
    const scorelessFixIds = scorelessCutoffRows.map((row) => Number(row.id));

    if (!options.dryRun) {
      if (scoreFixIds.length > 0) {
        await sql`
          UPDATE matches
          SET status = 'finished'
          WHERE id = ANY(${scoreFixIds})
        `;
      }

      if (scorelessFixIds.length > 0) {
        await sql`
          UPDATE matches
          SET status = ${options.noScoreStatus}
          WHERE id = ANY(${scorelessFixIds})
        `;
      }
    }

    const summary = {
      dryRun: options.dryRun,
      graceDays: options.graceDays,
      noScoreStatus: options.noScoreStatus,
      totalPastScheduledOrTimed: candidateRows.length,
      toFinished: withScores.length,
      toNoScoreStatus: scorelessCutoffRows.length,
      samples: {
        toFinished: withScores.slice(0, 20),
        toNoScoreStatus: scorelessCutoffRows.slice(0, 20),
      },
    };

    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}

await main();
