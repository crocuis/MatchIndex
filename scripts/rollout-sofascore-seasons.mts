import { existsSync } from 'node:fs';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const COMPETITIONS = ['PL', 'BL1', 'PD', 'SA', 'FL1', 'UCL', 'UEL'] as const;

function buildSeasonLabels(count: number) {
  const labels: string[] = [];
  for (let startYear = 2024; startYear > 2024 - count; startYear -= 1) {
    labels.push(`${startYear}-${startYear + 1}`);
  }
  return labels;
}

/** 시작/종료 시즌(예: '2015-2016', '2024-2025')으로 범위 생성. 오름차순 반환. */
function buildSeasonRange(fromSeason: string, toSeason: string): string[] {
  const parseStartYear = (s: string) => Number.parseInt(s.split('-')[0], 10);
  const fromYear = parseStartYear(fromSeason);
  const toYear = parseStartYear(toSeason);
  const labels: string[] = [];
  for (let y = fromYear; y <= toYear; y++) {
    labels.push(`${y}-${y + 1}`);
  }
  return labels;
}

function runCommand(command: string, args: string[]) {
  const result = spawnSync(command, args, {
    cwd: process.cwd(),
    encoding: 'utf8',
    stdio: 'inherit',
  });

  if (result.status !== 0) {
    throw new Error(`Command failed: ${command} ${args.join(' ')}`);
  }
}

function competitionSlug(code: string) {
  switch (code) {
    case 'PL': return 'premier-league';
    case 'BL1': return '1-bundesliga';
    case 'PD': return 'la-liga';
    case 'SA': return 'serie-a';
    case 'FL1': return 'ligue-1';
    case 'UCL': return 'champions-league';
    case 'UEL': return 'europa-league';
    default: return code.toLowerCase();
  }
}

async function main() {
  const argv = process.argv.slice(2);
  const getOption = (name: string) => argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  const hasFlag = (name: string) => argv.includes(`--${name}`);

  // 시즌 목록 결정 (우선순위: --seasons > --from-season/--to-season > --season-count)
  const seasonsOpt = getOption('seasons');
  const fromSeason = getOption('from-season');
  const toSeason = getOption('to-season');
  let seasons: string[];
  if (seasonsOpt) {
    seasons = seasonsOpt.split(',').map((s) => s.trim()).filter(Boolean);
  } else if (fromSeason || toSeason) {
    seasons = buildSeasonRange(fromSeason ?? '2015-2016', toSeason ?? '2024-2025');
  } else {
    seasons = buildSeasonLabels(Number.parseInt(getOption('season-count') ?? '10', 10) || 10);
  }

  const competitions = (getOption('competitions')?.split(',').map((v) => v.trim().toUpperCase()).filter(Boolean) as (typeof COMPETITIONS[number])[] | undefined)
    ?? [...COMPETITIONS];

  // 이미 JSONL이 있는 시즌 건너뜀
  const skipExisting = hasFlag('skip-existing');

  // Python 수집기에 그대로 전달할 선택적 옵션
  const collectPassthrough: string[] = [];
  const timeout = getOption('timeout');
  if (timeout) collectPassthrough.push(`--timeout=${timeout}`);
  if (hasFlag('skip-details')) collectPassthrough.push('--skip-details');
  const maxMatches = getOption('max-matches');
  if (maxMatches) collectPassthrough.push(`--max-matches=${maxMatches}`);
  const retry = getOption('retry');
  if (retry) collectPassthrough.push(`--retry=${retry}`);

  const rolloutLog: Array<{ competition: string; season: string; status: string; cleanupPlan?: string }> = [];

  for (const season of seasons) {
    for (const competition of competitions) {
      const suffix = `${competition.toLowerCase()}-${season}`;
      const rawFile = `data/sofascore-${suffix}.jsonl`;
      const cleanupPlan = `data/sofascore-cleanup-${suffix}.json`;
      const mergeBatch = `data/sofascore-cleanup-${suffix}-batch.json`;

      if (skipExisting && existsSync(path.join(process.cwd(), rawFile))) {
        console.log(`[skip] ${rawFile} already exists`);
        rolloutLog.push({ competition, season, status: 'skipped_existing' });
        continue;
      }

      try {
        runCommand('python3', ['scripts/soccerdata-collect-sofascore.py', `--competition=${competition}`, `--season=${season}`, '--write', `--output=${rawFile}`, ...collectPassthrough]);
        runCommand('node', ['--experimental-strip-types', 'scripts/soccerdata-import-raw.mts', '--source=soccerdata_sofascore', `--competition=${competition}`, `--season=${season}`, `--input=${rawFile}`, '--write']);
        runCommand('node', ['--experimental-strip-types', 'scripts/soccerdata-materialize-sofascore.mts', `--competition=${competition}`, `--season=${season}`, '--source=soccerdata_sofascore', '--write']);
        runCommand('node', ['--experimental-strip-types', 'scripts/soccerdata-materialize-sofascore-details.mts', `--competition=${competition}`, `--season=${season}`, '--source=soccerdata_sofascore', '--write']);

        runCommand('node', ['--experimental-strip-types', 'scripts/plan-sofascore-duplicate-cleanup.mts', `--output=${cleanupPlan}`]);
        const cleanup = JSON.parse(await readFile(path.join(process.cwd(), cleanupPlan), 'utf8')) as { playerMergeEntries: Array<{ aliasSlug: string; canonicalSlug: string }>; duplicateMatchGroups: Array<{ competition_slug: string; season_slug: string }> };

        if (cleanup.playerMergeEntries.length > 0) {
          await writeFile(path.join(process.cwd(), mergeBatch), JSON.stringify({ mergeEntries: cleanup.playerMergeEntries.map((entry) => ({ aliasSlug: entry.aliasSlug, canonicalSlug: entry.canonicalSlug })) }, null, 2));
          runCommand('node', ['--experimental-strip-types', 'scripts/merge-duplicate-players.mts', `--batch-file=${mergeBatch}`, '--skip-refresh']);
        }

        for (const group of cleanup.duplicateMatchGroups.filter((group) => group.competition_slug === competitionSlug(competition))) {
          runCommand('node', ['--experimental-strip-types', 'scripts/dedupe-competition-matches.mts', `--competition=${group.competition_slug}`, `--season=${group.season_slug}`]);
        }

        rolloutLog.push({ competition, season, status: 'completed', cleanupPlan });
      } catch (error) {
        rolloutLog.push({ competition, season, status: error instanceof Error ? error.message : String(error), cleanupPlan });
      }
    }
  }

  const outputPath = getOption('log') || 'data/sofascore-rollout-log.json';
  await writeFile(path.join(process.cwd(), outputPath), `${JSON.stringify(rolloutLog, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({ outputPath, entries: rolloutLog.length }, null, 2));
}

await main();
