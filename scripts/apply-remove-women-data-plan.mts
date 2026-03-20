import { readFileSync } from 'node:fs';

import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface RemovalPlan {
  competitionIds: number[];
  targetCompetitionSeasonIds: number[];
  targetMatchIds: number[];
  targetTeamIds: number[];
  protectedTeamIds: number[];
  deleteTeamIds: number[];
  targetPlayerIds: number[];
  protectedPlayerIds: number[];
  deletePlayerIds: number[];
}

interface CliOptions {
  help: boolean;
  planPath: string;
}

const DEFAULT_PLAN_PATH = '/tmp/remove-women-data-plan.json';
const DEFAULT_BATCH_SIZE = 50;
const PLAYER_DELETE_BATCH_SIZE = 10;

function parseArgs(argv: string[]): CliOptions {
  let help = false;
  let planPath = DEFAULT_PLAN_PATH;

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      help = true;
      continue;
    }

    if (arg.startsWith('--plan=')) {
      planPath = arg.slice('--plan='.length).trim() || DEFAULT_PLAN_PATH;
    }
  }

  return { help, planPath };
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/apply-remove-women-data-plan.mts [options]

Options:
  --plan=<path>   삭제 플랜 JSON 경로 (기본값: ${DEFAULT_PLAN_PATH})
  --help, -h      도움말 출력
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });
}

function uniqueNumbers(values: number[]) {
  return [...new Set(values)].sort((left, right) => left - right);
}

function chunkNumbers(values: number[], batchSize: number) {
  const unique = uniqueNumbers(values);
  const chunks: number[][] = [];

  for (let index = 0; index < unique.length; index += batchSize) {
    chunks.push(unique.slice(index, index + batchSize));
  }

  return chunks;
}

function formatIdList(values: number[]) {
  const unique = uniqueNumbers(values);
  if (unique.length === 0) {
    return '';
  }

  for (const value of unique) {
    if (!Number.isInteger(value) || value <= 0) {
      throw new Error(`Invalid numeric identifier: ${String(value)}`);
    }
  }

  return unique.join(', ');
}

function loadPlan(planPath: string): RemovalPlan {
  const parsed = JSON.parse(readFileSync(planPath, 'utf8')) as RemovalPlan;

  return {
    competitionIds: uniqueNumbers(parsed.competitionIds ?? []),
    targetCompetitionSeasonIds: uniqueNumbers(parsed.targetCompetitionSeasonIds ?? []),
    targetMatchIds: uniqueNumbers(parsed.targetMatchIds ?? []),
    targetTeamIds: uniqueNumbers(parsed.targetTeamIds ?? []),
    protectedTeamIds: uniqueNumbers(parsed.protectedTeamIds ?? []),
    deleteTeamIds: uniqueNumbers(parsed.deleteTeamIds ?? []),
    targetPlayerIds: uniqueNumbers(parsed.targetPlayerIds ?? []),
    protectedPlayerIds: uniqueNumbers(parsed.protectedPlayerIds ?? []),
    deletePlayerIds: uniqueNumbers(parsed.deletePlayerIds ?? []),
  };
}

async function runUnsafe(label: string, query: string) {
  const sql = getSql();

  try {
    console.log(`[apply-remove-women-data-plan] ${label}`);
    await sql.unsafe(query);
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function runBatches(label: string, ids: number[], buildQuery: (idList: string) => string, batchSize: number = DEFAULT_BATCH_SIZE) {
  const batches = chunkNumbers(ids, batchSize);

  for (const [index, batch] of batches.entries()) {
    const idList = formatIdList(batch);
    if (!idList) {
      continue;
    }

    await runUnsafe(`${label} ${index + 1}/${batches.length}`, buildQuery(idList));
  }
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const plan = loadPlan(options.planPath);
  const competitionSeasonIdList = formatIdList(plan.targetCompetitionSeasonIds);
  const competitionIdList = formatIdList(plan.competitionIds);
  const deleteTeamIdList = formatIdList(plan.deleteTeamIds);
  const deletePlayerIdList = formatIdList(plan.deletePlayerIds);

  if (competitionSeasonIdList) {
    await runUnsafe('delete data_freshness by competition season', `DELETE FROM data_freshness WHERE competition_season_id IN (${competitionSeasonIdList})`);
  }

  if (plan.targetMatchIds.length > 0) {
    await runBatches('delete data_freshness by match', plan.targetMatchIds, (idList) => `DELETE FROM data_freshness WHERE match_id IN (${idList})`);
    await runBatches('delete match_stats', plan.targetMatchIds, (idList) => `DELETE FROM match_stats WHERE match_id IN (${idList})`);
    await runBatches('delete match_lineups by match', plan.targetMatchIds, (idList) => `DELETE FROM match_lineups WHERE match_id IN (${idList})`);
    await runBatches('delete match_events by match', plan.targetMatchIds, (idList) => `DELETE FROM match_events WHERE match_id IN (${idList})`);
    await runBatches('delete matches', plan.targetMatchIds, (idList) => `DELETE FROM matches WHERE id IN (${idList})`);
  }

  if (competitionIdList) {
    await runUnsafe('delete data_freshness competition entities', `DELETE FROM data_freshness WHERE entity_type = 'competition' AND entity_id IN (${competitionIdList})`);
  }

  if (deleteTeamIdList) {
    await runUnsafe('delete data_freshness team entities', `DELETE FROM data_freshness WHERE entity_type = 'team' AND entity_id IN (${deleteTeamIdList})`);
  }

  if (deletePlayerIdList) {
    await runUnsafe('delete data_freshness player entities', `DELETE FROM data_freshness WHERE entity_type = 'player' AND entity_id IN (${deletePlayerIdList})`);
  }

  if (competitionSeasonIdList) {
    await runUnsafe('delete player_season_stats by competition season', `DELETE FROM player_season_stats WHERE competition_season_id IN (${competitionSeasonIdList})`);
    await runUnsafe('delete player_contracts by competition season', `DELETE FROM player_contracts WHERE competition_season_id IN (${competitionSeasonIdList})`);
    await runUnsafe('delete team_seasons by competition season', `DELETE FROM team_seasons WHERE competition_season_id IN (${competitionSeasonIdList})`);
    await runUnsafe('delete competition_seasons', `DELETE FROM competition_seasons WHERE id IN (${competitionSeasonIdList})`);
  }

  if (competitionIdList) {
    await runUnsafe('delete competition source mappings', `DELETE FROM source_entity_mapping WHERE entity_type = 'competition' AND entity_id IN (${competitionIdList})`);
    await runUnsafe('delete competition aliases', `DELETE FROM entity_aliases WHERE entity_type = 'competition' AND entity_id IN (${competitionIdList})`);
    await runUnsafe('delete competitions', `DELETE FROM competitions WHERE id IN (${competitionIdList})`);
  }

  if (plan.deletePlayerIds.length > 0) {
    await runBatches('delete freeze frames by player', plan.deletePlayerIds, (idList) => `DELETE FROM match_event_freeze_frames WHERE player_id IN (${idList})`);
    await runBatches('delete match_lineups by player', plan.deletePlayerIds, (idList) => `DELETE FROM match_lineups WHERE player_id IN (${idList})`);
    await runBatches('delete player_season_stats by player', plan.deletePlayerIds, (idList) => `DELETE FROM player_season_stats WHERE player_id IN (${idList})`);
    await runBatches('delete player_contracts by player', plan.deletePlayerIds, (idList) => `DELETE FROM player_contracts WHERE player_id IN (${idList})`);
    await runBatches('delete player_photo_sources', plan.deletePlayerIds, (idList) => `DELETE FROM player_photo_sources WHERE player_id IN (${idList})`);
    await runBatches('delete player_market_values', plan.deletePlayerIds, (idList) => `DELETE FROM player_market_values WHERE player_id IN (${idList})`);
    await runBatches('delete player_transfers', plan.deletePlayerIds, (idList) => `DELETE FROM player_transfers WHERE player_id IN (${idList})`);
    await runBatches('delete player_translations', plan.deletePlayerIds, (idList) => `DELETE FROM player_translations WHERE player_id IN (${idList})`);
    await runBatches('delete player source mappings', plan.deletePlayerIds, (idList) => `DELETE FROM source_entity_mapping WHERE entity_type = 'player' AND entity_id IN (${idList})`);
    await runBatches('delete player aliases', plan.deletePlayerIds, (idList) => `DELETE FROM entity_aliases WHERE entity_type = 'player' AND entity_id IN (${idList})`);
    await runBatches('delete players', plan.deletePlayerIds, (idList) => `DELETE FROM players WHERE id IN (${idList})`, PLAYER_DELETE_BATCH_SIZE);
  }

  if (deleteTeamIdList) {
    await runUnsafe('clear competition winners for deleted teams', `UPDATE competition_seasons SET winner_team_id = NULL, updated_at = NOW() WHERE winner_team_id IN (${deleteTeamIdList})`);
    await runUnsafe('clear player_market_values club references', `UPDATE player_market_values SET club_id = NULL, updated_at = NOW() WHERE club_id IN (${deleteTeamIdList})`);
    await runUnsafe('clear player_transfers from_team references', `UPDATE player_transfers SET from_team_id = NULL, updated_at = NOW() WHERE from_team_id IN (${deleteTeamIdList})`);
    await runUnsafe('clear player_transfers to_team references', `UPDATE player_transfers SET to_team_id = NULL, updated_at = NOW() WHERE to_team_id IN (${deleteTeamIdList})`);
    await runUnsafe('delete team source mappings', `DELETE FROM source_entity_mapping WHERE entity_type = 'team' AND entity_id IN (${deleteTeamIdList})`);
    await runUnsafe('delete team aliases', `DELETE FROM entity_aliases WHERE entity_type = 'team' AND entity_id IN (${deleteTeamIdList})`);
    await runUnsafe('delete teams', `DELETE FROM teams WHERE id IN (${deleteTeamIdList})`);
  }

  await runUnsafe('refresh mv_team_form', 'REFRESH MATERIALIZED VIEW mv_team_form');
  await runUnsafe('refresh mv_standings', 'REFRESH MATERIALIZED VIEW mv_standings');
  await runUnsafe('refresh mv_top_scorers', 'REFRESH MATERIALIZED VIEW mv_top_scorers');

  console.log(JSON.stringify({ applied: true, planPath: options.planPath }, null, 2));
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
