// scripts/ingest-transfermarkt.mts
// transfermarkt-datasets CC0 CSV를 읽어 PostgreSQL에 적재하는 CLI 스크립트
// 대상 시즌: 2015~2026 / 대상 리그: 유럽 5대 리그 (GB1/ES1/IT1/L1/FR1)
//
// 사용법:
//   node --experimental-strip-types scripts/ingest-transfermarkt.mts --dir=<경로> [옵션]
//
// CSV 파일 (transfermarkt-datasets 기준):
//   <dir>/appearances.csv
//   <dir>/clubs.csv
//   <dir>/players.csv
//   <dir>/transfers.csv
//   <dir>/player_valuations.csv

import { createReadStream } from 'node:fs';
import path from 'node:path';
import { createInterface } from 'node:readline';
import postgres, { type Sql } from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

// ============================================================
// 상수
// ============================================================

/** 지원 시즌 시작 연도 범위 (2015/16 ~ 2025/26) */
const SEASON_START_MIN = 2015;
const SEASON_START_MAX = 2025;

/** transfermarkt-datasets competition_id 기준 유럽 5대 리그 코드 */
const TARGET_COMPETITION_IDS = new Set(['GB1', 'ES1', 'IT1', 'L1', 'FR1']);

/** TM competition_id -> 내부 competition slug 매핑 */
const COMPETITION_SLUG_BY_TM_ID: Record<string, string> = {
  GB1: 'premier-league',
  ES1: 'la-liga',
  IT1: 'serie-a',
  L1: 'bundesliga',
  FR1: 'ligue-1',
};

/** 데이터 소스 slug */
const SOURCE_SLUG = 'transfermarkt-datasets';

/** 배치 처리 크기 */
const BATCH_SIZE = 300;

// ============================================================
// CLI 파싱
// ============================================================

interface CliOptions {
  dir?: string;
  dryRun: boolean;
  help: boolean;
  limit?: number;
  skipStats: boolean;
  skipContracts: boolean;
  skipTransfers: boolean;
  skipMarketValues: boolean;
}

function parseArgs(argv: string[]): CliOptions {
  const opts: CliOptions = {
    dryRun: false,
    help: false,
    skipStats: false,
    skipContracts: false,
    skipTransfers: false,
    skipMarketValues: false,
  };

  for (const arg of argv) {
    if (arg === '--dry-run') { opts.dryRun = true; continue; }
    if (arg === '--help' || arg === '-h') { opts.help = true; continue; }
    if (arg === '--skip-stats') { opts.skipStats = true; continue; }
    if (arg === '--skip-contracts') { opts.skipContracts = true; continue; }
    if (arg === '--skip-transfers') { opts.skipTransfers = true; continue; }
    if (arg === '--skip-market-values') { opts.skipMarketValues = true; continue; }
    if (arg.startsWith('--dir=')) { opts.dir = arg.slice('--dir='.length).trim(); continue; }
    if (arg.startsWith('--limit=')) {
      const n = Number.parseInt(arg.slice('--limit='.length), 10);
      if (Number.isFinite(n) && n > 0) opts.limit = n;
    }
  }

  return opts;
}

function printHelp(): void {
  console.log(`사용법: node --experimental-strip-types scripts/ingest-transfermarkt.mts --dir=<경로> [옵션]

옵션:
  --dir=<경로>            transfermarkt-datasets CSV 디렉터리 (필수)
  --dry-run               DB 쓰기 없이 예상 결과 출력
  --skip-stats            player_season_stats 처리 건너뜀
  --skip-contracts        player_contracts 처리 건너뜀
  --skip-transfers        player_transfers 처리 건너뜀
  --skip-market-values    player_market_values 처리 건너뜀
  --limit=<n>             각 CSV 최대 행 수 제한 (디버그용)
  --help, -h              이 도움말 출력

환경 변수:
  DATABASE_URL            PostgreSQL 연결 문자열 (필수)

입력 CSV 파일 (transfermarkt-datasets):
  <dir>/appearances.csv       선수 출전 기록 (시즌 스탯 원천)
  <dir>/clubs.csv             클럽 정보 (이름 fallback용)
  <dir>/players.csv           선수 정보 (이름 fallback용)
  <dir>/transfers.csv         이적 기록
  <dir>/player_valuations.csv 시장가치 이력

dry-run 출력 필드:
  dryRun                  true
  seasonRange             처리 시즌 범위
  competitionIds          처리 리그 코드 목록
  appearanceRowsRead      appearances.csv 읽은 행 수
  targetPlayers           대상 선수 수 (TM ID 기준)
  targetClubs             대상 클럽 수 (TM ID 기준)
  playerMappingsLoaded    source_entity_mapping 선수 매핑 수
  teamMappingsLoaded      source_entity_mapping 팀 매핑 수
  statsPlanned            player_season_stats upsert 예정 행 수
  statsWritten            player_season_stats 실제 upsert 행 수
  contractsPlanned        player_contracts upsert 예정 행 수
  contractsWritten        player_contracts 실제 upsert 행 수
  transfersPlanned        player_transfers upsert 예정 행 수
  transfersWritten        player_transfers 실제 upsert 행 수
  marketValuesPlanned     player_market_values upsert 예정 행 수
  marketValuesWritten     player_market_values 실제 upsert 행 수
`);
}

// ============================================================
// DB 연결
// ============================================================

function getSql(): Sql {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error('DATABASE_URL이 설정되지 않았습니다');
  return postgres(url, { max: 1, idle_timeout: 30, prepare: false });
}

// ============================================================
// CSV 파싱 유틸
// ============================================================

/** CSV 파일을 스트리밍으로 파싱 — 헤더 행 기준 Record 생성 */
async function* parseCsvFile(filePath: string): AsyncGenerator<Record<string, string>> {
  const rl = createInterface({
    input: createReadStream(filePath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });

  let headers: string[] | null = null;
  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const fields = splitCsvLine(trimmed);
    if (!headers) {
      headers = fields;
      continue;
    }
    const record: Record<string, string> = {};
    for (let i = 0; i < headers.length; i++) {
      record[headers[i]] = fields[i] ?? '';
    }
    yield record;
  }
}

/** 따옴표 포함 CSV 라인 분리 */
function splitCsvLine(line: string): string[] {
  const fields: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      fields.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  fields.push(current);
  return fields;
}

// ============================================================
// 텍스트 정규화 유틸
// ============================================================

function normalizeText(value: string | null | undefined): string {
  return (
    value
      ?.normalize('NFKD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-zA-Z0-9]+/g, ' ')
      .trim()
      .toLowerCase() ?? ''
  );
}

/** 팀 이름 키 생성 — FC/AFC 등 접두사 제거 포함 */
function buildTeamKeys(value: string | null | undefined): string[] {
  const normalized = normalizeText(value);
  if (!normalized) return [];
  const compact = normalized
    .replace(/\b(fc|cf|afc|cfc|sc|ac|club|football|futbol|clube)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return [...new Set([normalized, compact].filter(Boolean))];
}

function toInt(value: string | undefined, fallback = 0): number {
  if (!value || !value.trim()) return fallback;
  const n = Number.parseInt(value.trim(), 10);
  return Number.isFinite(n) ? n : fallback;
}

// ============================================================
// 시즌 처리 유틸
// ============================================================

/** ISO 날짜 -> 내부 시즌 slug (예: "2023-09-15" -> "2023/24") */
function dateToSeasonSlug(date: string): string | null {
  const m = date.match(/^(\d{4})-(\d{2})/);
  if (!m) return null;
  const year = Number.parseInt(m[1], 10);
  const month = Number.parseInt(m[2], 10);
  const startYear = month >= 7 ? year : year - 1;
  return `${startYear}/${String((startYear + 1) % 100).padStart(2, '0')}`;
}

/** 시즌 slug에서 start year 추출 */
function slugToStartYear(slug: string): number | null {
  const m = slug.match(/^(\d{4})\//);
  return m ? Number.parseInt(m[1], 10) : null;
}

/** TM transfer_season("2015/2016") -> 내부 slug("2015/16") */
function tmSeasonToSlug(tmSeason: string): string | null {
  const full = tmSeason.match(/^(\d{4})\/(\d{4})$/);
  if (full) {
    const s = Number.parseInt(full[1], 10);
    const e = Number.parseInt(full[2], 10);
    if (e === s + 1) return `${s}/${String(e % 100).padStart(2, '0')}`;
  }
  const single = tmSeason.match(/^(\d{4})$/);
  if (single) {
    const s = Number.parseInt(single[1], 10);
    return `${s}/${String((s + 1) % 100).padStart(2, '0')}`;
  }
  return null;
}

/** 날짜가 대상 시즌 범위(SEASON_START_MIN ~ SEASON_START_MAX)에 속하는지 확인 */
function isDateInRange(date: string): boolean {
  const slug = dateToSeasonSlug(date);
  if (!slug) return false;
  const startYear = slugToStartYear(slug);
  return startYear !== null && startYear >= SEASON_START_MIN && startYear <= SEASON_START_MAX;
}

// ============================================================
// DB 타입 인터페이스
// ============================================================

interface SourceRow { id: number; }
interface SeasonRow { id: number; slug: string; }
interface CompSeasonRow { id: number; competition_slug: string; season_slug: string; }
interface EntityMappingRow { entity_id: number; external_id: string; }
interface TeamFallbackRow { id: number; slug: string; name: string | null; short_name: string | null; }
interface PlayerFallbackRow { id: number; known_as: string | null; first_name: string | null; last_name: string | null; }

// ============================================================
// DB 헬퍼
// ============================================================

/** transfermarkt-datasets 데이터 소스 확보 (없으면 생성) */
async function ensureDataSource(sql: Sql): Promise<number> {
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
    VALUES (
      ${SOURCE_SLUG},
      'transfermarkt-datasets (CC0 오픈 데이터)',
      'https://github.com/dcaribou/transfermarkt-datasets',
      'dataset',
      'transfermarkt-datasets',
      2
    )
    ON CONFLICT (slug) DO UPDATE SET
      name = EXCLUDED.name,
      base_url = EXCLUDED.base_url,
      source_kind = EXCLUDED.source_kind,
      upstream_ref = EXCLUDED.upstream_ref,
      priority = EXCLUDED.priority
    RETURNING id
  `;
  return rows[0].id;
}

/** 기존 데이터 소스 ID 조회 (없으면 null) */
async function findDataSourceId(sql: Sql): Promise<number | null> {
  const rows = await sql<SourceRow[]>`
    SELECT id FROM data_sources WHERE slug = ${SOURCE_SLUG} LIMIT 1
  `;
  return rows[0]?.id ?? null;
}

/** 시즌 slug -> id 맵 */
async function loadSeasonMap(sql: Sql): Promise<Map<string, number>> {
  const rows = await sql<SeasonRow[]>`SELECT id, slug FROM seasons ORDER BY start_date`;
  return new Map(rows.map((r) => [r.slug, r.id]));
}

/** competition_season 조회: key = "competition_slug:season_slug" */
async function loadCompetitionSeasonMap(sql: Sql): Promise<Map<string, CompSeasonRow>> {
  const targetSlugs = Object.values(COMPETITION_SLUG_BY_TM_ID);
  const rows = await sql<CompSeasonRow[]>`
    SELECT
      cs.id,
      c.slug AS competition_slug,
      s.slug AS season_slug
    FROM competition_seasons cs
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    WHERE c.slug = ANY(${targetSlugs})
  `;
  return new Map(rows.map((r) => [`${r.competition_slug}:${r.season_slug}`, r]));
}

/** source_entity_mapping 선수 매핑: TM external_id -> 내부 player_id */
async function loadPlayerMappingMap(sql: Sql, sourceId: number): Promise<Map<string, number>> {
  const rows = await sql<EntityMappingRow[]>`
    SELECT entity_id, external_id
    FROM source_entity_mapping
    WHERE entity_type = 'player' AND source_id = ${sourceId}
  `;
  return new Map(rows.map((r) => [r.external_id, r.entity_id]));
}

/** source_entity_mapping 팀 매핑: TM external_id -> 내부 team_id */
async function loadTeamMappingMap(sql: Sql, sourceId: number): Promise<Map<string, number>> {
  const rows = await sql<EntityMappingRow[]>`
    SELECT entity_id, external_id
    FROM source_entity_mapping
    WHERE entity_type = 'team' AND source_id = ${sourceId}
  `;
  return new Map(rows.map((r) => [r.external_id, r.entity_id]));
}

/** 이름 기반 팀 fallback 맵 — 중복 이름 제거(보수적 처리) */
async function loadTeamNameFallbackMap(sql: Sql): Promise<Map<string, number>> {
  const rows = await sql<TeamFallbackRow[]>`
    SELECT t.id, t.slug, tt.name, tt.short_name
    FROM teams t
    LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
  `;
  const counts = new Map<string, number>();
  const result = new Map<string, number>();

  for (const row of rows) {
    for (const candidate of [row.name, row.short_name, row.slug]) {
      for (const key of buildTeamKeys(candidate)) {
        counts.set(key, (counts.get(key) ?? 0) + 1);
        result.set(key, row.id);
      }
    }
  }
  // 중복 키 제거 (보수적 처리)
  for (const [key, count] of counts) {
    if (count > 1) result.delete(key);
  }
  return result;
}

/** 이름 기반 선수 fallback 맵 — 중복 이름 제거(보수적 처리) */
async function loadPlayerNameFallbackMap(sql: Sql): Promise<Map<string, number>> {
  const rows = await sql<PlayerFallbackRow[]>`
    SELECT p.id, pt.known_as, pt.first_name, pt.last_name
    FROM players p
    LEFT JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
  `;
  const counts = new Map<string, number>();
  const result = new Map<string, number>();

  for (const row of rows) {
    const candidates: Array<string | null> = [
      row.known_as,
      row.first_name && row.last_name ? `${row.first_name} ${row.last_name}` : null,
    ];
    for (const candidate of candidates) {
      const key = normalizeText(candidate);
      if (!key) continue;
      counts.set(key, (counts.get(key) ?? 0) + 1);
      result.set(key, row.id);
    }
  }
  // 중복 이름 제거 (보수적 처리)
  for (const [key, count] of counts) {
    if (count > 1) result.delete(key);
  }
  return result;
}

// ============================================================
// ID 리졸버
// ============================================================

interface IdResolvers {
  resolvePlayerId(tmId: string, name?: string | null): number | undefined;
  resolveTeamId(tmId: string, name?: string | null): number | undefined;
}

function buildResolvers(
  playerMappings: Map<string, number>,
  teamMappings: Map<string, number>,
  playerFallbacks: Map<string, number>,
  teamFallbacks: Map<string, number>,
): IdResolvers {
  return {
    resolvePlayerId(tmId, name) {
      const mapped = playerMappings.get(tmId);
      if (mapped !== undefined) return mapped;
      // 이름 기반 fallback (보수적)
      const key = normalizeText(name);
      return key ? playerFallbacks.get(key) : undefined;
    },
    resolveTeamId(tmId, name) {
      const mapped = teamMappings.get(tmId);
      if (mapped !== undefined) return mapped;
      // 이름 기반 fallback (보수적)
      for (const k of buildTeamKeys(name)) {
        const match = teamFallbacks.get(k);
        if (match !== undefined) return match;
      }
      return undefined;
    },
  };
}

// ============================================================
// Phase 1: appearances.csv 처리
// ============================================================

interface AppearanceAgg {
  tmPlayerId: string;
  tmClubId: string;
  competitionId: string;
  seasonSlug: string;
  appearances: number;
  minutesPlayed: number;
  goals: number;
  assists: number;
  yellowCards: number;
  redCards: number;
}

/**
 * appearances.csv를 스트리밍으로 읽어 집계
 * key: "tmPlayerId:competitionId:seasonSlug:tmClubId"
 */
async function readAppearances(
  dir: string,
  limit: number | undefined,
): Promise<{
  byKey: Map<string, AppearanceAgg>;
  playerIds: Set<string>;
  clubIds: Set<string>;
  rowsRead: number;
}> {
  const filePath = path.join(dir, 'appearances.csv');
  const byKey = new Map<string, AppearanceAgg>();
  const playerIds = new Set<string>();
  const clubIds = new Set<string>();
  let rowsRead = 0;

  for await (const row of parseCsvFile(filePath)) {
    if (limit !== undefined && rowsRead >= limit) break;
    rowsRead++;

    const competitionId = row['competition_id'];
    if (!competitionId || !TARGET_COMPETITION_IDS.has(competitionId)) continue;

    const date = row['date'];
    if (!date || !isDateInRange(date)) continue;

    const seasonSlug = dateToSeasonSlug(date);
    if (!seasonSlug) continue;

    const tmPlayerId = row['player_id'];
    const tmClubId = row['player_club_id'];
    if (!tmPlayerId || !tmClubId) continue;

    playerIds.add(tmPlayerId);
    clubIds.add(tmClubId);

    const key = `${tmPlayerId}:${competitionId}:${seasonSlug}:${tmClubId}`;
    const existing = byKey.get(key) ?? {
      tmPlayerId,
      tmClubId,
      competitionId,
      seasonSlug,
      appearances: 0,
      minutesPlayed: 0,
      goals: 0,
      assists: 0,
      yellowCards: 0,
      redCards: 0,
    };
    existing.appearances += 1;
    existing.minutesPlayed += toInt(row['minutes_played']);
    existing.goals += toInt(row['goals']);
    existing.assists += toInt(row['assists']);
    existing.yellowCards += toInt(row['yellow_cards']);
    existing.redCards += toInt(row['red_cards']);
    byKey.set(key, existing);
  }

  return { byKey, playerIds, clubIds, rowsRead };
}

/** clubs.csv에서 대상 클럽 이름 로드 (이름 fallback용) */
async function readClubNames(
  dir: string,
  targetClubIds: Set<string>,
): Promise<Map<string, string>> {
  const filePath = path.join(dir, 'clubs.csv');
  const result = new Map<string, string>();
  for await (const row of parseCsvFile(filePath)) {
    const clubId = row['club_id'];
    if (clubId && targetClubIds.has(clubId)) {
      const name = row['name'];
      if (name) result.set(clubId, name);
    }
  }
  return result;
}

/** players.csv에서 대상 선수 이름 로드 (이름 fallback용) */
async function readPlayerNames(
  dir: string,
  targetPlayerIds: Set<string>,
): Promise<Map<string, string>> {
  const filePath = path.join(dir, 'players.csv');
  const result = new Map<string, string>();
  for await (const row of parseCsvFile(filePath)) {
    const playerId = row['player_id'];
    if (playerId && targetPlayerIds.has(playerId)) {
      const name = row['name'];
      if (name) result.set(playerId, name);
    }
  }
  return result;
}

// ============================================================
// player_season_stats 처리
// ============================================================

interface SeasonStatsRow {
  playerId: number;
  competitionSeasonId: number;
  appearances: number;
  minutesPlayed: number;
  goals: number;
  assists: number;
  yellowCards: number;
  redCards: number;
}

/**
 * appearances 집계에서 player_season_stats 행 생성
 * (player, competition, season) 단위로 합산 — 클럽 무관
 */
function buildSeasonStatsRows(
  byKey: Map<string, AppearanceAgg>,
  compSeasonMap: Map<string, CompSeasonRow>,
  resolvers: IdResolvers,
  playerNames: Map<string, string>,
): {
  rows: SeasonStatsRow[];
  unmatchedPlayers: number;
  unmatchedCompSeasons: number;
} {
  const aggregated = new Map<string, SeasonStatsRow>();
  const seenUnmatchedPlayers = new Set<string>();
  const seenUnmatchedCompSeasons = new Set<string>();

  for (const agg of byKey.values()) {
    const playerId = resolvers.resolvePlayerId(agg.tmPlayerId, playerNames.get(agg.tmPlayerId));
    if (playerId === undefined) {
      seenUnmatchedPlayers.add(agg.tmPlayerId);
      continue;
    }

    const competitionSlug = COMPETITION_SLUG_BY_TM_ID[agg.competitionId];
    if (!competitionSlug) continue;

    const csKey = `${competitionSlug}:${agg.seasonSlug}`;
    const cs = compSeasonMap.get(csKey);
    if (!cs) {
      seenUnmatchedCompSeasons.add(csKey);
      continue;
    }

    const aggKey = `${playerId}:${cs.id}`;
    const existing = aggregated.get(aggKey) ?? {
      playerId,
      competitionSeasonId: cs.id,
      appearances: 0,
      minutesPlayed: 0,
      goals: 0,
      assists: 0,
      yellowCards: 0,
      redCards: 0,
    };
    existing.appearances += agg.appearances;
    existing.minutesPlayed += agg.minutesPlayed;
    existing.goals += agg.goals;
    existing.assists += agg.assists;
    existing.yellowCards += agg.yellowCards;
    existing.redCards += agg.redCards;
    aggregated.set(aggKey, existing);
  }

  return {
    rows: Array.from(aggregated.values()),
    unmatchedPlayers: seenUnmatchedPlayers.size,
    unmatchedCompSeasons: seenUnmatchedCompSeasons.size,
  };
}

async function upsertSeasonStats(sql: Sql, rows: SeasonStatsRow[]): Promise<number> {
  if (rows.length === 0) return 0;

  await sql`BEGIN`;
  try {
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const chunk = rows.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO player_season_stats (
          player_id, competition_season_id,
          appearances, minutes_played, goals, assists,
          yellow_cards, red_cards, updated_at
        )
        SELECT
          t.player_id, t.competition_season_id,
          t.appearances, t.minutes_played, t.goals, t.assists,
          t.yellow_cards, t.red_cards, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.playerId))}::int[],
          ${sql.array(chunk.map((r) => r.competitionSeasonId))}::int[],
          ${sql.array(chunk.map((r) => r.appearances))}::int[],
          ${sql.array(chunk.map((r) => r.minutesPlayed))}::int[],
          ${sql.array(chunk.map((r) => r.goals))}::int[],
          ${sql.array(chunk.map((r) => r.assists))}::int[],
          ${sql.array(chunk.map((r) => r.yellowCards))}::int[],
          ${sql.array(chunk.map((r) => r.redCards))}::int[]
        ) AS t(player_id, competition_season_id, appearances, minutes_played,
               goals, assists, yellow_cards, red_cards)
        ON CONFLICT (player_id, competition_season_id) DO UPDATE SET
          appearances = EXCLUDED.appearances,
          minutes_played = EXCLUDED.minutes_played,
          goals = EXCLUDED.goals,
          assists = EXCLUDED.assists,
          yellow_cards = EXCLUDED.yellow_cards,
          red_cards = EXCLUDED.red_cards,
          updated_at = NOW()
      `;
    }
    await sql`COMMIT`;
  } catch (err) {
    await sql`ROLLBACK`;
    throw err;
  }

  return rows.length;
}

// ============================================================
// player_contracts 처리
// ============================================================

interface ContractRow {
  playerId: number;
  teamId: number;
  competitionSeasonId: number;
}

/**
 * appearances 집계에서 player_contracts 행 생성
 * (player, competition, season)별로 가장 많은 minutes를 뛴 팀을 선택
 */
function buildContractRows(
  byKey: Map<string, AppearanceAgg>,
  compSeasonMap: Map<string, CompSeasonRow>,
  resolvers: IdResolvers,
  playerNames: Map<string, string>,
  clubNames: Map<string, string>,
): { rows: ContractRow[]; unmatchedTeams: number; } {
  // (player, competition, season) 단위로 팀별 minutes 집계
  const teamMinutes = new Map<string, Map<string, number>>();

  for (const agg of byKey.values()) {
    const playerId = resolvers.resolvePlayerId(agg.tmPlayerId, playerNames.get(agg.tmPlayerId));
    if (playerId === undefined) continue;

    const competitionSlug = COMPETITION_SLUG_BY_TM_ID[agg.competitionId];
    if (!competitionSlug) continue;

    const csKey = `${competitionSlug}:${agg.seasonSlug}`;
    const cs = compSeasonMap.get(csKey);
    if (!cs) continue;

    const aggKey = `${playerId}:${cs.id}`;
    const teams = teamMinutes.get(aggKey) ?? new Map<string, number>();
    teams.set(agg.tmClubId, (teams.get(agg.tmClubId) ?? 0) + agg.minutesPlayed);
    teamMinutes.set(aggKey, teams);
  }

  const rows: ContractRow[] = [];
  let unmatchedTeams = 0;

  for (const [aggKey, teams] of teamMinutes) {
    const colonIdx = aggKey.indexOf(':');
    const playerId = Number(aggKey.slice(0, colonIdx));
    const competitionSeasonId = Number(aggKey.slice(colonIdx + 1));

    // 가장 많은 minutes를 뛴 팀 선택
    let dominantClubTmId: string | undefined;
    let maxMinutes = -1;
    for (const [clubId, minutes] of teams) {
      if (minutes > maxMinutes) {
        maxMinutes = minutes;
        dominantClubTmId = clubId;
      }
    }
    if (!dominantClubTmId) continue;

    const teamId = resolvers.resolveTeamId(dominantClubTmId, clubNames.get(dominantClubTmId));
    if (teamId === undefined) {
      unmatchedTeams++;
      continue;
    }

    rows.push({ playerId, teamId, competitionSeasonId });
  }

  return { rows, unmatchedTeams };
}

async function upsertContracts(sql: Sql, rows: ContractRow[]): Promise<number> {
  if (rows.length === 0) return 0;

  await sql`BEGIN`;
  try {
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const chunk = rows.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO player_contracts (
          player_id, team_id, competition_season_id,
          is_on_loan, updated_at
        )
        SELECT t.player_id, t.team_id, t.competition_season_id, FALSE, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.playerId))}::int[],
          ${sql.array(chunk.map((r) => r.teamId))}::int[],
          ${sql.array(chunk.map((r) => r.competitionSeasonId))}::int[]
        ) AS t(player_id, team_id, competition_season_id)
        ON CONFLICT (player_id, competition_season_id) DO UPDATE SET
          team_id = COALESCE(player_contracts.team_id, EXCLUDED.team_id),
          updated_at = NOW()
      `;
    }
    await sql`COMMIT`;
  } catch (err) {
    await sql`ROLLBACK`;
    throw err;
  }

  return rows.length;
}

// ============================================================
// player_transfers 처리
// ============================================================

interface TransferRow {
  playerId: number;
  sourceId: number;
  seasonId: number | null;
  seasonLabel: string | null;
  externalTransferId: string;
  movedAt: string | null;
  fromTeamId: number | null;
  fromTeamName: string | null;
  fromTeamExternalId: string | null;
  toTeamId: number | null;
  toTeamName: string | null;
  toTeamExternalId: string | null;
  marketValueEur: number | null;
  feeEur: number | null;
  feeDisplay: string | null;
  transferType: string | null;
  rawPayload: Record<string, string>;
}

/** 이적료 문자열 -> 정수 EUR (비숫자 문자열은 null) */
function parseTransferFee(value: string | undefined): number | null {
  if (!value || !value.trim()) return null;
  const cleaned = value.replace(/[^0-9.]/g, '');
  if (!cleaned) return null;
  const n = Number.parseFloat(cleaned);
  return Number.isFinite(n) && n >= 0 ? Math.round(n) : null;
}

async function readTransfers(
  dir: string,
  targetPlayerIds: Set<string>,
  limit: number | undefined,
  resolvers: IdResolvers,
  playerNames: Map<string, string>,
  clubNames: Map<string, string>,
  seasonMap: Map<string, number>,
  sourceId: number,
): Promise<{ rows: TransferRow[]; rowsRead: number; unmatchedPlayers: number; }> {
  const filePath = path.join(dir, 'transfers.csv');
  const rows: TransferRow[] = [];
  let rowsRead = 0;
  let unmatchedPlayers = 0;

  for await (const row of parseCsvFile(filePath)) {
    if (limit !== undefined && rowsRead >= limit) break;
    rowsRead++;

    const tmPlayerId = row['player_id'];
    if (!tmPlayerId || !targetPlayerIds.has(tmPlayerId)) continue;

    // 날짜/시즌 범위 필터
    const transferDate = row['transfer_date'] || null;
    const transferSeason = row['transfer_season'] || null;

    let seasonSlug: string | null = null;
    if (transferDate) {
      if (!isDateInRange(transferDate)) continue;
      seasonSlug = dateToSeasonSlug(transferDate);
    } else if (transferSeason) {
      seasonSlug = tmSeasonToSlug(transferSeason);
      if (!seasonSlug) continue;
      const startYear = slugToStartYear(seasonSlug);
      if (startYear === null || startYear < SEASON_START_MIN || startYear > SEASON_START_MAX) continue;
    } else {
      continue;
    }

    const playerId = resolvers.resolvePlayerId(tmPlayerId, playerNames.get(tmPlayerId));
    if (playerId === undefined) {
      unmatchedPlayers++;
      continue;
    }

    const seasonId = seasonSlug ? (seasonMap.get(seasonSlug) ?? null) : null;

    const fromClubTmId = row['from_club_id'] || null;
    const toClubTmId = row['to_club_id'] || null;
    const fromTeamId = fromClubTmId
      ? (resolvers.resolveTeamId(fromClubTmId, row['from_club_name'] || clubNames.get(fromClubTmId)) ?? null)
      : null;
    const toTeamId = toClubTmId
      ? (resolvers.resolveTeamId(toClubTmId, row['to_club_name'] || clubNames.get(toClubTmId)) ?? null)
      : null;

    // TM에 고유 이적 ID가 없으므로 복합 키로 생성
    const externalTransferId = [
      tmPlayerId,
      transferDate ?? transferSeason ?? 'unknown',
      fromClubTmId ?? 'none',
      toClubTmId ?? 'none',
    ].join('_');

    const feeRaw = row['transfer_fee'] || undefined;
    const feeEur = parseTransferFee(feeRaw);
    // "Free transfer", "Loan" 등 비숫자 문자열은 fee_display에 보존
    const feeDisplay = feeRaw && feeEur === null ? feeRaw.trim() : null;
    const marketValueEur = parseTransferFee(row['market_value_in_eur'] || undefined);

    rows.push({
      playerId,
      sourceId,
      seasonId,
      seasonLabel: seasonSlug,
      externalTransferId,
      movedAt: transferDate,
      fromTeamId,
      fromTeamName: row['from_club_name'] || null,
      fromTeamExternalId: fromClubTmId,
      toTeamId,
      toTeamName: row['to_club_name'] || null,
      toTeamExternalId: toClubTmId,
      marketValueEur,
      feeEur,
      feeDisplay,
      transferType: row['transfer_type'] || null,
      rawPayload: row,
    });
  }

  return { rows, rowsRead, unmatchedPlayers };
}

async function upsertTransfers(sql: Sql, rows: TransferRow[]): Promise<number> {
  if (rows.length === 0) return 0;

  await sql`BEGIN`;
  try {
    for (const row of rows) {
      await sql`
        INSERT INTO player_transfers (
          player_id, source_id, season_id, season_label, external_transfer_id,
          moved_at, from_team_id, from_team_name, from_team_external_id,
          to_team_id, to_team_name, to_team_external_id,
          market_value_eur, fee_eur, currency_code, fee_display,
          transfer_type, is_pending, source_url, raw_payload, updated_at
        )
        VALUES (
          ${row.playerId}, ${row.sourceId}, ${row.seasonId}, ${row.seasonLabel},
          ${row.externalTransferId},
          ${row.movedAt}::date,
          ${row.fromTeamId}, ${row.fromTeamName}, ${row.fromTeamExternalId},
          ${row.toTeamId}, ${row.toTeamName}, ${row.toTeamExternalId},
          ${row.marketValueEur}, ${row.feeEur}, 'EUR', ${row.feeDisplay},
          ${row.transferType}, FALSE, NULL,
          ${JSON.stringify(row.rawPayload)}::jsonb, NOW()
        )
        ON CONFLICT (player_id, source_id, external_transfer_id) DO UPDATE SET
          season_id = COALESCE(EXCLUDED.season_id, player_transfers.season_id),
          season_label = COALESCE(EXCLUDED.season_label, player_transfers.season_label),
          moved_at = COALESCE(EXCLUDED.moved_at, player_transfers.moved_at),
          from_team_id = COALESCE(EXCLUDED.from_team_id, player_transfers.from_team_id),
          from_team_name = COALESCE(EXCLUDED.from_team_name, player_transfers.from_team_name),
          from_team_external_id = COALESCE(EXCLUDED.from_team_external_id, player_transfers.from_team_external_id),
          to_team_id = COALESCE(EXCLUDED.to_team_id, player_transfers.to_team_id),
          to_team_name = COALESCE(EXCLUDED.to_team_name, player_transfers.to_team_name),
          to_team_external_id = COALESCE(EXCLUDED.to_team_external_id, player_transfers.to_team_external_id),
          market_value_eur = COALESCE(EXCLUDED.market_value_eur, player_transfers.market_value_eur),
          fee_eur = COALESCE(EXCLUDED.fee_eur, player_transfers.fee_eur),
          fee_display = COALESCE(EXCLUDED.fee_display, player_transfers.fee_display),
          transfer_type = COALESCE(EXCLUDED.transfer_type, player_transfers.transfer_type),
          raw_payload = EXCLUDED.raw_payload,
          updated_at = NOW()
      `;
    }
    await sql`COMMIT`;
  } catch (err) {
    await sql`ROLLBACK`;
    throw err;
  }

  return rows.length;
}

// ============================================================
// player_market_values 처리
// ============================================================

interface MarketValueRow {
  playerId: number;
  sourceId: number;
  seasonId: number | null;
  seasonLabel: string | null;
  clubId: number | null;
  clubName: string | null;
  externalPlayerId: string;
  externalClubId: string | null;
  observedAt: string;
  marketValueEur: number;
  rawPayload: Record<string, string>;
}

async function readMarketValues(
  dir: string,
  targetPlayerIds: Set<string>,
  limit: number | undefined,
  resolvers: IdResolvers,
  playerNames: Map<string, string>,
  clubNames: Map<string, string>,
  seasonMap: Map<string, number>,
  sourceId: number,
): Promise<{ rows: MarketValueRow[]; rowsRead: number; unmatchedPlayers: number; }> {
  const filePath = path.join(dir, 'player_valuations.csv');
  const rows: MarketValueRow[] = [];
  let rowsRead = 0;
  let unmatchedPlayers = 0;

  for await (const row of parseCsvFile(filePath)) {
    if (limit !== undefined && rowsRead >= limit) break;
    rowsRead++;

    const tmPlayerId = row['player_id'];
    if (!tmPlayerId || !targetPlayerIds.has(tmPlayerId)) continue;

    const date = row['date'];
    if (!date || !isDateInRange(date)) continue;

    // 5대 리그 소속 클럽 필터 (필드가 있을 때만 적용)
    const domesticCompId = row['player_club_domestic_competition_id'];
    if (domesticCompId && domesticCompId.trim() && !TARGET_COMPETITION_IDS.has(domesticCompId.trim())) continue;

    const marketValueStr = row['market_value_in_eur'];
    if (!marketValueStr || !marketValueStr.trim()) continue;
    const marketValueEur = Math.round(Number.parseFloat(marketValueStr));
    if (!Number.isFinite(marketValueEur) || marketValueEur < 0) continue;

    const playerId = resolvers.resolvePlayerId(tmPlayerId, playerNames.get(tmPlayerId));
    if (playerId === undefined) {
      unmatchedPlayers++;
      continue;
    }

    const seasonSlug = dateToSeasonSlug(date);
    const seasonId = seasonSlug ? (seasonMap.get(seasonSlug) ?? null) : null;

    const tmClubId = row['current_club_id'] || null;
    const clubId = tmClubId
      ? (resolvers.resolveTeamId(tmClubId, clubNames.get(tmClubId)) ?? null)
      : null;

    rows.push({
      playerId,
      sourceId,
      seasonId,
      seasonLabel: seasonSlug,
      clubId,
      clubName: tmClubId ? (clubNames.get(tmClubId) ?? null) : null,
      externalPlayerId: tmPlayerId,
      externalClubId: tmClubId,
      observedAt: date,
      marketValueEur,
      rawPayload: row,
    });
  }

  return { rows, rowsRead, unmatchedPlayers };
}

async function upsertMarketValues(sql: Sql, rows: MarketValueRow[]): Promise<number> {
  if (rows.length === 0) return 0;

  await sql`BEGIN`;
  try {
    for (const row of rows) {
      await sql`
        INSERT INTO player_market_values (
          player_id, source_id, season_id, season_label,
          club_id, club_name, external_player_id, external_club_id,
          observed_at, market_value_eur, currency_code,
          source_url, raw_payload, updated_at
        )
        VALUES (
          ${row.playerId}, ${row.sourceId}, ${row.seasonId}, ${row.seasonLabel},
          ${row.clubId}, ${row.clubName}, ${row.externalPlayerId}, ${row.externalClubId},
          ${row.observedAt}::date, ${row.marketValueEur}, 'EUR',
          NULL, ${JSON.stringify(row.rawPayload)}::jsonb, NOW()
        )
        ON CONFLICT (player_id, source_id, observed_at) DO UPDATE SET
          season_id = COALESCE(EXCLUDED.season_id, player_market_values.season_id),
          season_label = COALESCE(EXCLUDED.season_label, player_market_values.season_label),
          club_id = COALESCE(EXCLUDED.club_id, player_market_values.club_id),
          club_name = COALESCE(EXCLUDED.club_name, player_market_values.club_name),
          external_player_id = COALESCE(EXCLUDED.external_player_id, player_market_values.external_player_id),
          external_club_id = COALESCE(EXCLUDED.external_club_id, player_market_values.external_club_id),
          market_value_eur = EXCLUDED.market_value_eur,
          raw_payload = EXCLUDED.raw_payload,
          updated_at = NOW()
      `;
    }
    await sql`COMMIT`;
  } catch (err) {
    await sql`ROLLBACK`;
    throw err;
  }

  return rows.length;
}

// ============================================================
// 메인
// ============================================================

async function main(): Promise<void> {
  loadProjectEnv();
  const opts = parseArgs(process.argv.slice(2));

  if (opts.help) {
    printHelp();
    return;
  }

  if (!opts.dir) {
    throw new Error('--dir=<경로> 옵션이 필요합니다. --help로 사용법 확인');
  }

  const dir = path.isAbsolute(opts.dir) ? opts.dir : path.join(process.cwd(), opts.dir);

  console.log('\n[transfermarkt-datasets 인제스트]');
  console.log(`  CSV 디렉터리: ${dir}`);
  console.log(`  Dry-run: ${opts.dryRun}`);
  console.log(`  시즌 범위: ${SEASON_START_MIN}/${String((SEASON_START_MIN + 1) % 100).padStart(2, '0')} ~ ${SEASON_START_MAX}/${String((SEASON_START_MAX + 1) % 100).padStart(2, '0')}`);
  console.log(`  리그: ${[...TARGET_COMPETITION_IDS].join(', ')}\n`);

  const sql = getSql();

  try {
    // 1. 데이터 소스 ID 확보
    let sourceId: number;
    if (opts.dryRun) {
      // dry-run: 기존 소스 조회만 (없으면 -1로 매핑 스킵)
      sourceId = (await findDataSourceId(sql)) ?? -1;
    } else {
      sourceId = await ensureDataSource(sql);
    }

    // 2. DB 참조 데이터 로드
    console.log('DB 참조 데이터 로드 중...');
    const [seasonMap, compSeasonMap, playerFallbacks, teamFallbacks] = await Promise.all([
      loadSeasonMap(sql),
      loadCompetitionSeasonMap(sql),
      loadPlayerNameFallbackMap(sql),
      loadTeamNameFallbackMap(sql),
    ]);

    // source_entity_mapping 매핑 로드 (소스가 존재할 때만)
    const [playerMappings, teamMappings] = sourceId !== -1
      ? await Promise.all([
          loadPlayerMappingMap(sql, sourceId),
          loadTeamMappingMap(sql, sourceId),
        ])
      : [new Map<string, number>(), new Map<string, number>()];

    console.log(`  시즌: ${seasonMap.size}개, competition_seasons: ${compSeasonMap.size}개`);
    console.log(`  선수 매핑(source_entity_mapping): ${playerMappings.size}개`);
    console.log(`  팀 매핑(source_entity_mapping): ${teamMappings.size}개`);
    console.log(`  선수 이름 fallback: ${playerFallbacks.size}개`);
    console.log(`  팀 이름 fallback: ${teamFallbacks.size}개`);

    const resolvers = buildResolvers(playerMappings, teamMappings, playerFallbacks, teamFallbacks);

    // 3. appearances.csv 처리 (Phase 1)
    console.log('\nappearances.csv 읽는 중...');
    const { byKey, playerIds, clubIds, rowsRead: appRowsRead } = await readAppearances(dir, opts.limit);
    console.log(`  읽은 행: ${appRowsRead}, 대상 선수: ${playerIds.size}, 대상 클럽: ${clubIds.size}`);

    // 4. 이름 데이터 로드 (fallback용)
    console.log('clubs.csv / players.csv 읽는 중...');
    const [clubNames, playerNames] = await Promise.all([
      readClubNames(dir, clubIds),
      readPlayerNames(dir, playerIds),
    ]);
    console.log(`  클럽 이름: ${clubNames.size}개, 선수 이름: ${playerNames.size}개`);

    // 5. player_season_stats 처리
    let statsPlanned = 0;
    let statsWritten = 0;
    if (!opts.skipStats) {
      const { rows: statsRows, unmatchedPlayers, unmatchedCompSeasons } = buildSeasonStatsRows(
        byKey, compSeasonMap, resolvers, playerNames,
      );
      statsPlanned = statsRows.length;
      console.log(`\nplayer_season_stats: ${statsPlanned}행 계획 (미매핑 선수: ${unmatchedPlayers}, 미매핑 시즌: ${unmatchedCompSeasons})`);
      if (!opts.dryRun) {
        statsWritten = await upsertSeasonStats(sql, statsRows);
        console.log(`  → ${statsWritten}행 upsert 완료`);
      }
    }

    // 6. player_contracts 처리
    let contractsPlanned = 0;
    let contractsWritten = 0;
    if (!opts.skipContracts) {
      const { rows: contractRows, unmatchedTeams } = buildContractRows(
        byKey, compSeasonMap, resolvers, playerNames, clubNames,
      );
      contractsPlanned = contractRows.length;
      console.log(`\nplayer_contracts: ${contractsPlanned}행 계획 (미매핑 팀: ${unmatchedTeams})`);
      if (!opts.dryRun) {
        contractsWritten = await upsertContracts(sql, contractRows);
        console.log(`  → ${contractsWritten}행 upsert 완료`);
      }
    }

    // 7. transfers.csv 처리
    let transfersPlanned = 0;
    let transfersWritten = 0;
    if (!opts.skipTransfers) {
      console.log('\ntransfers.csv 읽는 중...');
      const { rows: transferRows, rowsRead: trRowsRead, unmatchedPlayers: trUnmatched } =
        await readTransfers(dir, playerIds, opts.limit, resolvers, playerNames, clubNames, seasonMap, sourceId);
      transfersPlanned = transferRows.length;
      console.log(`  읽은 행: ${trRowsRead}, 계획 행: ${transfersPlanned}, 미매핑 선수: ${trUnmatched}`);
      if (!opts.dryRun) {
        transfersWritten = await upsertTransfers(sql, transferRows);
        console.log(`  → ${transfersWritten}행 upsert 완료`);
      }
    }

    // 8. player_valuations.csv 처리
    let marketValuesPlanned = 0;
    let marketValuesWritten = 0;
    if (!opts.skipMarketValues) {
      console.log('\nplayer_valuations.csv 읽는 중...');
      const { rows: mvRows, rowsRead: mvRowsRead, unmatchedPlayers: mvUnmatched } =
        await readMarketValues(dir, playerIds, opts.limit, resolvers, playerNames, clubNames, seasonMap, sourceId);
      marketValuesPlanned = mvRows.length;
      console.log(`  읽은 행: ${mvRowsRead}, 계획 행: ${marketValuesPlanned}, 미매핑 선수: ${mvUnmatched}`);
      if (!opts.dryRun) {
        marketValuesWritten = await upsertMarketValues(sql, mvRows);
        console.log(`  → ${marketValuesWritten}행 upsert 완료`);
      }
    }

    // 9. dry-run summary 출력
    const summary = {
      dryRun: opts.dryRun,
      seasonRange: `${SEASON_START_MIN}-${SEASON_START_MAX + 1}`,
      competitionIds: [...TARGET_COMPETITION_IDS],
      appearanceRowsRead: appRowsRead,
      targetPlayers: playerIds.size,
      targetClubs: clubIds.size,
      playerMappingsLoaded: playerMappings.size,
      teamMappingsLoaded: teamMappings.size,
      statsPlanned,
      statsWritten,
      contractsPlanned,
      contractsWritten,
      transfersPlanned,
      transfersWritten,
      marketValuesPlanned,
      marketValuesWritten,
    };

    console.log('\n=== 요약 ===');
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
