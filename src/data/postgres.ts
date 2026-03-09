import 'server-only';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { readThroughCache, buildCacheKey, deleteCacheKey } from '@/lib/cache';
import { getDb } from '@/lib/db';
import type {
  Club,
  ClubListItem,
  ClubSeasonHistoryEntry,
  League,
  LeagueSeasonEntry,
  Match,
  MatchEvent,
  MatchAnalysisData,
  MatchAnalysisEvent,
  MatchAnalysisEventType,
  MatchLineup,
  MatchStats,
  Nation,
  NationListItem,
  PaginatedResult,
  Player,
  PlayerListItem,
  PlayerPhotoSource,
  PlayerPhotoSourceStatus,
  PlayerPhotoSyncTarget,
  PhotoSyncProvider,
  SearchResult,
  StandingRow,
  StatLeader,
  WorldCupTournament,
} from '@/data/types';
import { clubLogoMap, leagueLogoMap } from '@/data/entityImages.generated.ts';
import { deriveCompetitionType } from '@/data/competitionTypes';
import { getNationFlagUrl } from '@/data/nationVisuals';
import { resolveTournamentSlots } from '@/data/tournamentSlots';

// Korean team name translations in the DB are machine-translated and incorrect
// (e.g. "Levante" → "당신을 키우다"). Club names are proper nouns — for Korean locale,
// we skip team_translations entirely and fall back to the English name.
// This is enforced via `AND ${locale} <> 'ko'` in all team_translations SQL lookups.

const KOREAN_NATION_NAME_FALLBACKS: Record<string, string> = {
  BHR: '바레인',
  BIH: '보스니아 헤르체고비나',
  BLR: '벨라루스',
  BEN: '베냉',
  BFA: '부르키나파소',
  BUL: '불가리아',
  CHN: '중국',
  CMR: '카메룬',
  CUB: '쿠바',
  FIJ: '피지',
  GAB: '가봉',
  GUA: '과테말라',
  GUY: '가이아나',
  GUM: '괌',
  HKG: '홍콩',
  HON: '온두라스',
  IRQ: '이라크',
  ISR: '이스라엘',
  KOS: '코소보',
  MAS: '말레이시아',
  MKD: '북마케도니아',
  MLT: '몰타',
  MNE: '몬테네그로',
  MYA: '미얀마',
  NCA: '니카라과',
  NCL: '뉴칼레도니아',
  NEP: '네팔',
  NGA: '나이지리아',
  NIR: '북아일랜드',
  OMA: '오만',
  PLE: '팔레스타인',
  PNG: '파푸아뉴기니',
  PRK: '북한',
  PUR: '푸에르토리코',
  SAM: '사모아',
  SLV: '엘살바도르',
  SOL: '솔로몬 제도',
  SVN: '슬로베니아',
  SYR: '시리아',
  TJK: '타지키스탄',
  TPE: '중화 타이베이',
  TRI: '트리니다드 토바고',
  UAE: '아랍에미리트',
  UGA: '우간다',
  VAN: '바누아투',
};

let worldCup2026SourcePromise: Promise<WorldCupTournament> | null = null;

interface LeagueRow {
  id: string;
  name: string;
  country: string;
  season: string;
  gender: League['gender'];
  emblem_url: string | null;
  number_of_clubs: number;
}

interface ClubRow {
  id: string;
  name: string;
  short_name: string;
  country: string;
  gender: Club['gender'];
  founded: number | null;
  stadium: string;
  stadium_capacity: number | null;
  league_id: string;
  crest_url: string | null;
}

interface ClubListRow extends ClubRow {
  league_name: string;
}

interface ClubRepresentativeRow extends ClubListRow {
  season_start_date: string | null;
  season_end_date: string | null;
}

interface ClubSeasonMetaRow {
  coach_name: string | null;
}

interface PlayerRow {
  id: string;
  name: string;
  first_name: string;
  last_name: string;
  date_of_birth: string | null;
  age: number | null;
  nationality: string;
  nation_id: string | null;
  club_id: string;
  position: 'GK' | 'DEF' | 'MID' | 'FWD' | null;
  photo_url: string | null;
  shirt_number: number | null;
  joined_date: string | null;
  contract_start_date: string | null;
  contract_end_date: string | null;
  annual_salary_eur: number | null;
  weekly_wage_eur: number | null;
  salary_currency: string | null;
  salary_source: string | null;
  salary_source_url: string | null;
  salary_is_estimated: boolean | null;
  height: number | null;
  preferred_foot: 'Left' | 'Right' | 'Both' | null;
  appearances: number | null;
  goals: number | null;
  assists: number | null;
  minutes_played: number | null;
  yellow_cards: number | null;
  red_cards: number | null;
  clean_sheets: number | null;
}

interface PlayerListRow extends PlayerRow {
  club_name: string;
  club_short_name: string;
  club_logo: string | null;
  nation_name: string;
  nation_code: string;
  nation_flag: string | null;
}

interface NationRow {
  id: string;
  name: string;
  code: string;
  confederation: string;
  fifa_ranking: number | null;
  previous_fifa_ranking: number | null;
  flag_url: string | null;
  crest_url: string | null;
}

interface NationListRow extends NationRow {
  player_count: number;
}

type NationRankingCategory = 'men' | 'women';

interface MatchRow {
  id: string;
  home_team_id: string;
  away_team_id: string;
  home_team_name: string;
  away_team_name: string;
  home_team_code: string;
  away_team_code: string;
  home_team_logo: string | null;
  away_team_logo: string | null;
  home_score: number | null;
  away_score: number | null;
  date: string;
  time: string | null;
  venue: string;
  league_id: string;
  match_week: number | null;
  stage: string | null;
  group_name: string | null;
  competition_name: string;
  team_type: 'club' | 'nation';
  status: Match['status'];
}

interface StandingRowDb {
  position: number;
  club_id: string;
  club_name: string;
  club_short_name: string;
  club_logo: string | null;
  played: number;
  won: number;
  drawn: number;
  lost: number;
  goals_for: number;
  goals_against: number;
  goal_difference: number;
  points: number;
  form: Array<'W' | 'D' | 'L'> | null;
}

interface TopScorerRow {
  player_id: string;
  club_id: string;
  goals: number;
  assists: number;
}

interface LeagueSeasonRowDb {
  season_id: string;
  season_label: string;
  is_current: boolean;
}

interface TopScorerDisplayRowDb extends TopScorerRow {
  player_name: string;
  club_short_name: string;
}

interface LeagueStandingRowDb extends StandingRowDb {
  league_id: string;
}

interface ClubSeasonHistoryRowDb {
  season_id: string;
  season_label: string;
  league_id: string;
  league_name: string;
  position: number | null;
  played: number | null;
  won: number | null;
  drawn: number | null;
  lost: number | null;
  goals_for: number | null;
  goals_against: number | null;
  goal_difference: number | null;
  points: number | null;
  form: Array<'W' | 'D' | 'L'> | null;
}

interface SearchRow {
  result_type: SearchResult['type'];
  result_id: string;
  result_name: string;
  subtitle: string;
  gender: SearchResult['gender'] | null;
}

interface MatchEventRowDb {
  source_event_id: string;
  minute: number;
  event_type: 'goal' | 'own_goal' | 'penalty_scored' | 'penalty_missed' | 'yellow_card' | 'red_card' | 'yellow_red_card' | 'substitution' | 'var_decision';
  player_id: string | null;
  player_name: string | null;
  secondary_player_id: string | null;
  secondary_player_name: string | null;
  assist_player_id: string | null;
  assist_player_name: string | null;
  team_id: string;
  detail: string | null;
  source_details: unknown;
}

interface MatchAnalysisEventRowDb {
  source_event_id: string;
  minute: number;
  second: number | null;
  event_type: MatchAnalysisEventType;
  player_id: string | null;
  player_name: string | null;
  secondary_player_id: string | null;
  secondary_player_name: string | null;
  team_id: string;
  location_x: number | null;
  location_y: number | null;
  end_location_x: number | null;
  end_location_y: number | null;
  end_location_z: number | null;
  under_pressure: boolean | null;
  statsbomb_xg: number | null;
  detail: string | null;
  source_details: unknown;
}

interface MatchStatsRowDb {
  team_id: string;
  possession: number | null;
  total_shots: number;
  shots_on_target: number;
  corner_kicks: number | null;
  fouls: number | null;
}

interface MatchLineupRowDb {
  team_id: string;
  player_id: string;
  player_name: string;
  shirt_number: number | null;
  position: string | null;
  is_starter: boolean;
}

interface PlayerPhotoSourceRow {
  player_id: string;
  provider: PhotoSyncProvider;
  external_id: string | null;
  source_url: string | null;
  mirrored_url: string | null;
  status: PlayerPhotoSourceStatus;
  matched_by: string | null;
  match_score: number | null;
  etag: string | null;
  last_modified: string | null;
  last_checked_at: string | null;
  last_synced_at: string | null;
  failure_count: number | null;
  last_error: string | null;
}

interface PlayerPhotoSyncTargetRow {
  id: string;
  name: string;
  first_name: string;
  last_name: string;
  date_of_birth: string | null;
  nationality: string;
  photo_url: string | null;
}

interface PaginationOptions {
  page?: number;
  pageSize?: number;
}

const EMPTY_WORLD_CUP_2026: WorldCupTournament = {
  year: '2026',
  host: '',
  subtitle: '',
  groups: [],
  stages: [],
  spotlights: [],
  matches: [],
};

function isFormValue(value: unknown): value is 'W' | 'D' | 'L' {
  return value === 'W' || value === 'D' || value === 'L';
}

function isStringRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isMatchValue(value: unknown): value is Match {
  if (!isStringRecord(value)) return false;

  return typeof value.id === 'string'
    && typeof value.homeTeamId === 'string'
    && typeof value.awayTeamId === 'string'
    && (typeof value.homeTeamName === 'string' || value.homeTeamName === undefined)
    && (typeof value.awayTeamName === 'string' || value.awayTeamName === undefined)
    && (typeof value.homeTeamCode === 'string' || value.homeTeamCode === undefined)
    && (typeof value.awayTeamCode === 'string' || value.awayTeamCode === undefined)
    && (typeof value.homeScore === 'number' || value.homeScore === null)
    && (typeof value.awayScore === 'number' || value.awayScore === null)
    && typeof value.date === 'string'
    && typeof value.time === 'string'
    && typeof value.venue === 'string'
    && typeof value.leagueId === 'string'
    && (value.teamType === 'club' || value.teamType === 'nation' || value.teamType === undefined)
    && (value.status === 'scheduled' || value.status === 'live' || value.status === 'finished');
}

function isWorldCupTournamentValue(value: unknown): value is WorldCupTournament {
  if (!isStringRecord(value)) return false;
  if (typeof value.year !== 'string' || typeof value.host !== 'string' || typeof value.subtitle !== 'string') return false;
  if (!Array.isArray(value.groups) || !Array.isArray(value.stages) || !Array.isArray(value.spotlights) || !Array.isArray(value.matches)) return false;

  return value.groups.every((group) =>
    isStringRecord(group)
      && typeof group.id === 'string'
      && typeof group.name === 'string'
      && Array.isArray(group.standings)
      && group.standings.every((row) =>
        isStringRecord(row)
          && typeof row.position === 'number'
          && typeof row.nationId === 'string'
          && (typeof row.nationName === 'string' || row.nationName === undefined)
          && (typeof row.nationCode === 'string' || row.nationCode === undefined)
          && typeof row.played === 'number'
          && typeof row.won === 'number'
          && typeof row.drawn === 'number'
          && typeof row.lost === 'number'
          && typeof row.goalsFor === 'number'
          && typeof row.goalsAgainst === 'number'
          && typeof row.goalDifference === 'number'
          && typeof row.points === 'number'
          && Array.isArray(row.form)
          && row.form.every(isFormValue)
      )
  )
    && value.stages.every((stage) =>
      isStringRecord(stage)
        && typeof stage.name === 'string'
        && Array.isArray(stage.matchIds)
        && stage.matchIds.every((matchId) => typeof matchId === 'string')
    )
    && value.spotlights.every((spotlight) =>
      isStringRecord(spotlight)
        && typeof spotlight.nationId === 'string'
        && typeof spotlight.playerId === 'string'
        && typeof spotlight.note === 'string'
    )
    && value.matches.every(isMatchValue);
}

function normalizeWorldCupTournament(value: unknown): WorldCupTournament | null {
  if (!isStringRecord(value)) return null;

  const tournament = isStringRecord(value.tournament) ? { ...value.tournament, matches: value.matches } : value;

  if (!isWorldCupTournamentValue(tournament)) return null;

  return tournament;
}

function hasDatabaseUrl() {
  return Boolean(process.env.DATABASE_URL);
}

function getWorldCup2026FilePath() {
  const customPath = process.env.WORLD_CUP_2026_DATA_FILE?.trim();
  if (customPath) {
    return path.isAbsolute(customPath) ? customPath : path.join(process.cwd(), customPath);
  }

  return path.join(process.cwd(), 'data', 'worldcup-2026.json');
}

async function loadWorldCup2026FromUrl(): Promise<WorldCupTournament | null> {
  const url = process.env.WORLD_CUP_2026_DATA_URL?.trim();
  if (!url) return null;

  try {
    const response = await fetch(url, {
      headers: { Accept: 'application/json' },
      next: { revalidate: 3600 },
    });

    if (!response.ok) return null;

    const payload = await response.json();
    return normalizeWorldCupTournament(payload);
  } catch {
    return null;
  }
}

async function loadWorldCup2026FromFile(): Promise<WorldCupTournament | null> {
  try {
    const filePath = getWorldCup2026FilePath();
    const payload = JSON.parse(await readFile(filePath, 'utf8')) as unknown;
    return normalizeWorldCupTournament(payload);
  } catch {
    return null;
  }
}

async function loadWorldCup2026Source(): Promise<WorldCupTournament> {
  if (!worldCup2026SourcePromise) {
    worldCup2026SourcePromise = (async () => {
      const tournament = (await loadWorldCup2026FromUrl())
        ?? (await loadWorldCup2026FromFile())
        ?? EMPTY_WORLD_CUP_2026;

      return resolveTournamentSlots(tournament);
    })();
  }

  return worldCup2026SourcePromise;
}

function mergeMatches(primary: Match[], secondary: Match[]) {
  return Array.from(new Map([...primary, ...secondary].map((match) => [match.id, match])).values());
}

function addWorldCupNationSeed(
  seeds: Map<string, Pick<Nation, 'id' | 'name' | 'code'>>,
  id: string | undefined,
  name: string | undefined,
  code: string | undefined,
) {
  if (!id || !name || !code) {
    return;
  }

  const normalizedId = id.toLowerCase();
  const normalizedCode = code.toUpperCase();
  const normalizedName = name.trim();

  if (!normalizedId || !normalizedName || !/^[A-Z]{3}$/.test(normalizedCode)) {
    return;
  }

  const lowerName = normalizedName.toLowerCase();
  const isPlaceholder = lowerName.includes('winner')
    || lowerName.includes('runners-up')
    || lowerName.includes('third place')
    || lowerName.includes('loser match')
    || lowerName.includes('group ')
    || lowerName.includes('path ');

  if (isPlaceholder) {
    return;
  }

  if (!seeds.has(normalizedId)) {
    seeds.set(normalizedId, {
      id: normalizedId,
      name: normalizedName,
      code: normalizedCode,
    });
  }
}

function collectWorldCupNationSeeds(tournament: WorldCupTournament) {
  const seeds = new Map<string, Pick<Nation, 'id' | 'name' | 'code'>>();

  for (const group of tournament.groups) {
    for (const row of group.standings) {
      addWorldCupNationSeed(seeds, row.nationId, row.nationName, row.nationCode);
    }
  }

  for (const match of tournament.matches) {
    if (match.teamType !== 'nation') {
      continue;
    }

    addWorldCupNationSeed(seeds, match.homeTeamId, match.homeTeamName, match.homeTeamCode);
    addWorldCupNationSeed(seeds, match.awayTeamId, match.awayTeamName, match.awayTeamCode);
  }

  return seeds;
}

function mergeNationsWithWorldCup(nations: Nation[], tournament: WorldCupTournament) {
  const merged = new Map(nations.map((nation) => [nation.id, nation]));
  const existingCodes = new Set(nations.map((nation) => nation.code.toUpperCase()));

  for (const seed of collectWorldCupNationSeeds(tournament).values()) {
    if (merged.has(seed.id) || existingCodes.has(seed.code.toUpperCase())) {
      continue;
    }

    merged.set(seed.id, {
      id: seed.id,
      name: seed.name,
      code: seed.code,
      confederation: '',
      fifaRanking: 0,
      flag: getNationFlagUrl(seed.code),
    });
    existingCodes.add(seed.code.toUpperCase());
  }

  return sortNations(Array.from(merged.values()));
}

function sortNations(nations: Nation[]) {
  return nations.toSorted((left, right) => {
    const leftHasRank = left.fifaRanking > 0;
    const rightHasRank = right.fifaRanking > 0;

    if (leftHasRank !== rightHasRank) {
      return leftHasRank ? -1 : 1;
    }

    if (leftHasRank && rightHasRank && left.fifaRanking !== right.fifaRanking) {
      return left.fifaRanking - right.fifaRanking;
    }

    return left.name.localeCompare(right.name);
  });
}

async function localizeNationMatchNames(matches: Match[], locale: string): Promise<Match[]> {
  if (locale === 'en') {
    return matches;
  }

  const nations = await getNationsDb(locale);
  const nationMap = new Map(nations.map((nation) => [nation.id, nation.name]));
  const nationCodeMap = new Map(nations.map((nation) => [nation.code.toUpperCase(), nation.name]));

  return matches.map((match) => {
    if (match.teamType !== 'nation') {
      return match;
    }

    return {
      ...match,
      homeTeamName: nationMap.get(match.homeTeamId) ?? nationCodeMap.get(match.homeTeamCode?.toUpperCase() ?? '') ?? match.homeTeamName,
      awayTeamName: nationMap.get(match.awayTeamId) ?? nationCodeMap.get(match.awayTeamCode?.toUpperCase() ?? '') ?? match.awayTeamName,
    };
  });
}

async function withFallback<T>(loader: () => Promise<T>, fallback: () => T | Promise<T>, context?: string) {
  if (!hasDatabaseUrl()) {
    return fallback();
  }

  try {
    return await loader();
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[data:fallback]', context ?? 'unknown', error);
    }

    return fallback();
  }
}

function mapLeague(row: LeagueRow): League {
  return {
    id: row.id,
    name: row.name,
    country: row.country,
    season: row.season,
    gender: row.gender ?? undefined,
    logo: leagueLogoMap[row.id] ?? row.emblem_url ?? undefined,
    numberOfClubs: row.number_of_clubs,
    competitionType: deriveCompetitionType(row.id, row.name),
  };
}

function mapClub(row: ClubRow): Club {
  return {
    id: row.id,
    name: row.name,
    shortName: row.short_name,
    country: row.country,
    gender: row.gender ?? undefined,
    founded: row.founded ?? 0,
    stadium: row.stadium,
    stadiumCapacity: row.stadium_capacity ?? 0,
    leagueId: row.league_id,
    logo: clubLogoMap[row.id] ?? row.crest_url ?? undefined,
  };
}

function mapClubListItem(row: ClubListRow): ClubListItem {
  return {
    ...mapClub(row),
    leagueName: deriveCompetitionType(row.league_id, row.league_name) === 'league'
      ? row.league_name
      : undefined,
  };
}

function getClubRepresentativeTimestamp(row: Pick<ClubRepresentativeRow, 'season_end_date' | 'season_start_date'>) {
  const value = row.season_end_date ?? row.season_start_date;
  if (!value) return 0;

  const timestamp = new Date(value).getTime();
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function getClubIdentityKey(row: Pick<ClubRepresentativeRow, 'name' | 'country' | 'gender'>) {
  return [row.name.trim().toLowerCase(), row.country.trim().toLowerCase(), row.gender ?? 'unknown'].join('::');
}

function compareClubRepresentativeRows(left: ClubRepresentativeRow, right: ClubRepresentativeRow) {
  const leftIsLeague = deriveCompetitionType(left.league_id, left.league_name) === 'league';
  const rightIsLeague = deriveCompetitionType(right.league_id, right.league_name) === 'league';

  if (leftIsLeague !== rightIsLeague) {
    return leftIsLeague ? -1 : 1;
  }

  const seasonDifference = getClubRepresentativeTimestamp(right) - getClubRepresentativeTimestamp(left);
  if (seasonDifference !== 0) {
    return seasonDifference;
  }

  const leftHasLogo = Boolean(left.crest_url || clubLogoMap[left.id]);
  const rightHasLogo = Boolean(right.crest_url || clubLogoMap[right.id]);
  if (leftHasLogo !== rightHasLogo) {
    return leftHasLogo ? -1 : 1;
  }

  if (left.id.length !== right.id.length) {
    return left.id.length - right.id.length;
  }

  return left.id.localeCompare(right.id);
}

function selectRepresentativeClubRows(rows: ClubRepresentativeRow[]) {
  const bestByTeam = new Map<string, ClubRepresentativeRow>();

  for (const row of rows) {
    const existing = bestByTeam.get(row.id);
    if (!existing || compareClubRepresentativeRows(row, existing) < 0) {
      bestByTeam.set(row.id, row);
    }
  }

  const bestByIdentity = new Map<string, ClubRepresentativeRow>();

  for (const row of bestByTeam.values()) {
    const key = getClubIdentityKey(row);
    const existing = bestByIdentity.get(key);
    if (!existing || compareClubRepresentativeRows(row, existing) < 0) {
      bestByIdentity.set(key, row);
    }
  }

  return Array.from(bestByIdentity.values()).sort((left, right) => left.name.localeCompare(right.name));
}

async function getClubRepresentativeRowsDb(locale: string) {
  const sql = getDb();

  return sql<ClubRepresentativeRow[]>`
    SELECT
      t.slug AS id,
      COALESCE(
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
        t.slug
      ) AS name,
      COALESCE(
        (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
        (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
        t.slug
      ) AS short_name,
      COALESCE(
        (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
        (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
        country.code_alpha3
      ) AS country,
      t.gender,
      t.founded_year AS founded,
      COALESCE(
        (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
        (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
        v.slug,
        ''
      ) AS stadium,
      v.capacity AS stadium_capacity,
      c.slug AS league_id,
      COALESCE(
        (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = ${locale}),
        (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
        c.slug
      ) AS league_name,
      t.crest_url,
      s.start_date::TEXT AS season_start_date,
      s.end_date::TEXT AS season_end_date
    FROM team_seasons ts
    JOIN teams t ON t.id = ts.team_id
    JOIN countries country ON country.id = t.country_id
    LEFT JOIN venues v ON v.id = t.venue_id
    JOIN competition_seasons cs ON cs.id = ts.competition_season_id
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    WHERE t.is_national = FALSE
  `;
}

async function getClubRepresentativeRowBySlugDb(id: string, locale: string) {
  const sql = getDb();

  const rows = await sql<ClubRepresentativeRow[]>`
    SELECT
      t.slug AS id,
      COALESCE(
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
        t.slug
      ) AS name,
      COALESCE(
        (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
        (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
        t.slug
      ) AS short_name,
      COALESCE(
        (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
        (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
        country.code_alpha3
      ) AS country,
      t.gender,
      t.founded_year AS founded,
      COALESCE(
        (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
        (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
        v.slug,
        ''
      ) AS stadium,
      v.capacity AS stadium_capacity,
      c.slug AS league_id,
      COALESCE(
        (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = ${locale}),
        (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
        c.slug
      ) AS league_name,
      t.crest_url,
      s.start_date::TEXT AS season_start_date,
      s.end_date::TEXT AS season_end_date
    FROM team_seasons ts
    JOIN teams t ON t.id = ts.team_id
    JOIN countries country ON country.id = t.country_id
    LEFT JOIN venues v ON v.id = t.venue_id
    JOIN competition_seasons cs ON cs.id = ts.competition_season_id
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    WHERE t.slug = ${id}
      AND t.is_national = FALSE
  `;

  return rows.sort(compareClubRepresentativeRows)[0];
}

function mapPlayer(row: PlayerRow): Player {
  const contractStartDate = row.contract_start_date ?? row.joined_date ?? undefined;
  const contractEndDate = row.contract_end_date ?? undefined;
  const annualSalary = row.annual_salary_eur ?? undefined;
  const weeklyWage = row.weekly_wage_eur ?? undefined;
  const hasContractData = Boolean(
    contractStartDate
    || contractEndDate
    || annualSalary !== undefined
    || weeklyWage !== undefined
    || row.salary_source
    || row.salary_source_url
  );

  return {
    id: row.id,
    name: row.name,
    firstName: row.first_name,
    lastName: row.last_name,
    dateOfBirth: row.date_of_birth ?? '',
    age: row.age ?? 0,
    nationality: row.nationality ?? '',
    nationId: row.nation_id?.toLowerCase() ?? '',
    clubId: row.club_id,
    position: row.position ?? 'MID',
    photoUrl: row.photo_url ?? undefined,
    shirtNumber: row.shirt_number ?? 0,
    height: row.height ?? 0,
    preferredFoot: row.preferred_foot ?? 'Right',
    contract: hasContractData ? {
      startDate: contractStartDate,
      endDate: contractEndDate,
      annualSalary,
      weeklyWage,
      currencyCode: row.salary_currency ?? 'EUR',
      source: row.salary_source ?? undefined,
      sourceUrl: row.salary_source_url ?? undefined,
      isEstimated: row.salary_is_estimated ?? undefined,
    } : undefined,
    seasonStats: {
      appearances: row.appearances ?? 0,
      goals: row.goals ?? 0,
      assists: row.assists ?? 0,
      minutesPlayed: row.minutes_played ?? 0,
      yellowCards: row.yellow_cards ?? 0,
      redCards: row.red_cards ?? 0,
      cleanSheets: row.clean_sheets ?? undefined,
    },
  };
}

function mapPlayerListItem(row: PlayerListRow): PlayerListItem {
  return {
    ...mapPlayer(row),
    clubName: row.club_name,
    clubShortName: row.club_short_name,
    clubLogo: clubLogoMap[row.club_id] ?? row.club_logo ?? undefined,
    nationName: row.nation_name,
    nationCode: row.nation_code,
    nationFlag: row.nation_flag ?? getNationFlagUrl(row.nation_code),
  };
}

function mapNation(row: NationRow, rankingCategory: NationRankingCategory): Nation {
  const currentRanking = row.fifa_ranking ?? 0;
  const previousRanking = row.previous_fifa_ranking ?? undefined;

  return {
    id: row.id?.toLowerCase() ?? '',
    name: row.name,
    code: row.code,
    confederation: row.confederation,
    previousFifaRanking: previousRanking,
    rankingChange: previousRanking && currentRanking ? previousRanking - currentRanking : undefined,
    fifaRanking: currentRanking,
    rankingCategory,
    flag: row.flag_url ?? getNationFlagUrl(row.code),
    crest: row.crest_url ?? undefined,
  };
}

function mapNationListItem(row: NationListRow, rankingCategory: NationRankingCategory): NationListItem {
  return {
    ...mapNation(row, rankingCategory),
    playerCount: row.player_count,
  };
}

function createPaginatedResult<T>(items: T[], totalCount: number, currentPage: number, pageSize: number): PaginatedResult<T> {
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));

  return {
    items,
    totalCount,
    currentPage: Math.min(currentPage, totalPages),
    pageSize,
    totalPages,
  };
}

function applyKoreanNationNameFallback(rows: NationRow[], locale: string) {
  if (locale !== 'ko') {
    return rows;
  }

  return rows.map((row) => ({
    ...row,
    name: KOREAN_NATION_NAME_FALLBACKS[row.code] ?? row.name,
  }));
}

function mapPlayerPhotoSource(row: PlayerPhotoSourceRow): PlayerPhotoSource {
  return {
    playerId: row.player_id,
    provider: row.provider,
    externalId: row.external_id ?? undefined,
    sourceUrl: row.source_url ?? undefined,
    mirroredUrl: row.mirrored_url ?? undefined,
    status: row.status,
    matchedBy: row.matched_by ?? undefined,
    matchScore: row.match_score ?? undefined,
    etag: row.etag ?? undefined,
    lastModified: row.last_modified ?? undefined,
    lastCheckedAt: row.last_checked_at ?? undefined,
    lastSyncedAt: row.last_synced_at ?? undefined,
    failureCount: row.failure_count ?? 0,
    lastError: row.last_error ?? undefined,
  };
}

function mapPlayerPhotoSyncTarget(row: PlayerPhotoSyncTargetRow): PlayerPhotoSyncTarget {
  return {
    id: row.id,
    name: row.name,
    firstName: row.first_name,
    lastName: row.last_name,
    dateOfBirth: row.date_of_birth ?? '',
    nationality: row.nationality,
    photoUrl: row.photo_url ?? undefined,
  };
}

function normalizeMatchStatus(status: string): Match['status'] {
  if (status.startsWith('live')) {
    return 'live';
  }

  if (status.startsWith('finished')) {
    return 'finished';
  }

  return 'scheduled';
}

function mapMatch(row: MatchRow): Match {
  return {
    id: row.id,
    homeTeamId: row.home_team_id,
    awayTeamId: row.away_team_id,
    homeTeamName: row.home_team_name,
    awayTeamName: row.away_team_name,
    homeTeamCode: row.home_team_code,
    awayTeamCode: row.away_team_code,
    homeTeamLogo: clubLogoMap[row.home_team_id] ?? row.home_team_logo ?? undefined,
    awayTeamLogo: clubLogoMap[row.away_team_id] ?? row.away_team_logo ?? undefined,
    homeScore: row.home_score,
    awayScore: row.away_score,
    date: row.date,
    time: row.time ?? '00:00',
    venue: row.venue,
    leagueId: row.league_id,
    matchWeek: row.match_week ?? undefined,
    stage: row.stage ?? undefined,
    groupName: row.group_name ?? undefined,
    competitionName: row.competition_name,
    teamType: row.team_type,
    status: normalizeMatchStatus(row.status),
  };
}

function mapStanding(row: StandingRowDb): StandingRow {
  return {
    position: row.position,
    clubId: row.club_id,
    clubName: row.club_name,
    clubShortName: row.club_short_name,
    clubLogo: clubLogoMap[row.club_id] ?? row.club_logo ?? undefined,
    played: row.played,
    won: row.won,
    drawn: row.drawn,
    lost: row.lost,
    goalsFor: row.goals_for,
    goalsAgainst: row.goals_against,
    goalDifference: row.goal_difference,
    points: row.points,
    form: row.form ?? [],
  };
}

function mapClubSeasonHistoryEntry(row: ClubSeasonHistoryRowDb): ClubSeasonHistoryEntry {
  return {
    seasonId: row.season_id,
    seasonLabel: row.season_label,
    leagueId: row.league_id,
    leagueName: row.league_name,
    position: row.position ?? undefined,
    played: row.played ?? 0,
    won: row.won ?? 0,
    drawn: row.drawn ?? 0,
    lost: row.lost ?? 0,
    goalsFor: row.goals_for ?? 0,
    goalsAgainst: row.goals_against ?? 0,
    goalDifference: row.goal_difference ?? 0,
    points: row.points ?? 0,
    form: (row.form ?? []).filter(isFormValue),
  };
}

function mapMatchEventType(type: MatchEventRowDb['event_type']): 'goal' | 'yellow_card' | 'red_card' | 'substitution' {
  if (type === 'goal' || type === 'own_goal' || type === 'penalty_scored' || type === 'penalty_missed') {
    return 'goal';
  }

  if (type === 'yellow_card' || type === 'yellow_red_card') {
    return 'yellow_card';
  }

  if (type === 'red_card') {
    return 'red_card';
  }

  return 'substitution';
}

function readNestedName(value: unknown, ...keys: string[]) {
  let current: unknown = value;

  for (const key of keys) {
    if (!isStringRecord(current) || !(key in current)) {
      return undefined;
    }

    current = current[key];
  }

  return typeof current === 'string' ? current : undefined;
}

function parseStoredSourceDetails(value: unknown): unknown {
  if (typeof value !== 'string') {
    return value;
  }

  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

function extractAnalysisOutcome(eventType: MatchAnalysisEventType, sourceDetails: unknown) {
  if (eventType === 'pass') {
    return readNestedName(sourceDetails, 'pass', 'outcome', 'name') ?? 'Complete';
  }

  if (eventType === 'shot' || eventType === 'goal' || eventType === 'penalty_scored' || eventType === 'penalty_missed') {
    return readNestedName(sourceDetails, 'shot', 'outcome', 'name')
      ?? (eventType === 'goal' || eventType === 'penalty_scored' ? 'Goal' : undefined);
  }

  return undefined;
}

function extractMatchEventDetail(
  type: MatchEventRowDb['event_type'],
  detail: string | null,
  sourceDetails: unknown,
  playerName: string | null,
  secondaryPlayerName: string | null
) {
  if (type === 'substitution') {
    if (playerName && secondaryPlayerName) {
      return `${playerName} OUT · ${secondaryPlayerName} IN`;
    }

    return secondaryPlayerName ? `${secondaryPlayerName} IN` : detail ?? 'Substitution';
  }

  if (type === 'goal' || type === 'penalty_scored' || type === 'penalty_missed' || type === 'own_goal') {
    return readNestedName(sourceDetails, 'shot', 'outcome', 'name') ?? detail ?? undefined;
  }

  if (type === 'yellow_card' || type === 'red_card' || type === 'yellow_red_card') {
    return readNestedName(sourceDetails, 'bad_behaviour', 'card', 'name')
      ?? readNestedName(sourceDetails, 'foul_committed', 'card', 'name')
      ?? detail
      ?? undefined;
  }

  return detail ?? undefined;
}

function mapMatchAnalysisEvent(row: MatchAnalysisEventRowDb): MatchAnalysisEvent {
  return {
    id: row.source_event_id,
    minute: row.minute,
    second: row.second,
    type: row.event_type,
    teamId: row.team_id,
    playerId: row.player_id ?? undefined,
    playerName: row.player_name ?? row.player_id ?? undefined,
    secondaryPlayerId: row.secondary_player_id ?? undefined,
    secondaryPlayerName: row.secondary_player_name ?? row.secondary_player_id ?? undefined,
    locationX: row.location_x ?? undefined,
    locationY: row.location_y ?? undefined,
    endLocationX: row.end_location_x ?? undefined,
    endLocationY: row.end_location_y ?? undefined,
    endLocationZ: row.end_location_z ?? undefined,
    underPressure: row.under_pressure ?? false,
    statsbombXg: row.statsbomb_xg ?? undefined,
    outcome: extractAnalysisOutcome(row.event_type, row.source_details),
    detail: row.detail ?? undefined,
  };
}

function mapMatchLineup(row: MatchLineupRowDb): MatchLineup {
  return {
    teamId: row.team_id,
    playerId: row.player_id,
    playerName: row.player_name,
    shirtNumber: row.shirt_number ?? undefined,
    position: row.position ?? undefined,
    isStarter: row.is_starter,
  };
}

function mapTimelineMatchEvent(row: MatchEventRowDb): MatchEvent {
  const parsedSourceDetails = parseStoredSourceDetails(row.source_details);

  return {
    sourceEventId: row.source_event_id,
    minute: row.minute,
    type: mapMatchEventType(row.event_type),
    rawType: row.event_type,
    playerId: row.player_id ?? 'unknown',
    playerName: row.player_name ?? row.player_id ?? 'Unknown',
    teamId: row.team_id,
    secondaryPlayerId: row.secondary_player_id ?? undefined,
    secondaryPlayerName: row.secondary_player_name ?? row.secondary_player_id ?? undefined,
    assistPlayerId: row.assist_player_id ?? undefined,
    assistPlayerName: row.assist_player_name ?? row.assist_player_id ?? undefined,
    detail: extractMatchEventDetail(row.event_type, row.detail, parsedSourceDetails, row.player_name, row.secondary_player_name),
  };
}

function normalizePagination(options: PaginationOptions = {}) {
  const pageSize = Math.max(1, Math.min(options.pageSize ?? 50, 100));
  const currentPage = Math.max(1, options.page ?? 1);
  const offset = (currentPage - 1) * pageSize;

  return { currentPage, pageSize, offset };
}

export async function getLeaguesDb(locale: string = 'en'): Promise<League[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'leagues', locale });

    return readThroughCache({
      key,
      tier: 'master',
      loader: async () => {
        const rows = await sql<LeagueRow[]>`
          WITH latest_competition_seasons AS (
            SELECT DISTINCT ON (cs.competition_id)
              cs.id,
              cs.competition_id,
              cs.season_id
            FROM competition_seasons cs
            JOIN seasons s ON s.id = cs.season_id
            ORDER BY cs.competition_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
          )
          SELECT
            c.slug AS id,
            COALESCE(
              (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = ${locale}),
              (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
              c.slug
            ) AS name,
            COALESCE(
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
              country.code_alpha3
            ) AS country,
            CASE
              WHEN s.start_date IS NOT NULL AND s.end_date IS NOT NULL THEN
                CASE
                  WHEN EXTRACT(YEAR FROM s.start_date) = EXTRACT(YEAR FROM s.end_date)
                    THEN EXTRACT(YEAR FROM s.start_date)::INT::TEXT
                  ELSE CONCAT(
                    EXTRACT(YEAR FROM s.start_date)::INT::TEXT,
                    '/',
                    LPAD((EXTRACT(YEAR FROM s.end_date)::INT % 100)::TEXT, 2, '0')
                  )
                END
              ELSE REGEXP_REPLACE(s.slug, '-[0-9]+$', '')
            END AS season,
            c.gender,
            c.emblem_url,
            COUNT(ts.id)::INT AS number_of_clubs
          FROM competitions c
          LEFT JOIN countries country ON country.id = c.country_id
          JOIN latest_competition_seasons lcs ON lcs.competition_id = c.id
          JOIN seasons s ON s.id = lcs.season_id
          LEFT JOIN team_seasons ts ON ts.competition_season_id = lcs.id
          GROUP BY c.id, country.id, s.id
          ORDER BY name ASC
        `;

        return rows.map(mapLeague);
      },
    });
  }, () => []);
}

export async function getPaginatedLeaguesDb(
  locale: string = 'en',
  query: string = '',
  gender: 'male' | 'female' = 'male',
  options: PaginationOptions = {}
): Promise<PaginatedResult<League>> {
  const { currentPage, pageSize, offset } = normalizePagination(options);
  const trimmedQuery = query.trim();
  const searchPattern = `%${trimmedQuery}%`;

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'leagues-paginated', locale, params: { page: currentPage, pageSize, q: trimmedQuery, gender } });

    return readThroughCache({
      key,
      tier: 'master',
      loader: async () => {
        const [countRows, rows] = await Promise.all([
          sql<{ total_count: number }[]>`
            SELECT COUNT(*)::INT AS total_count
            FROM competitions c
            LEFT JOIN competition_translations ct_locale ON ct_locale.competition_id = c.id AND ct_locale.locale = ${locale}
            LEFT JOIN competition_translations ct_en ON ct_en.competition_id = c.id AND ct_en.locale = 'en'
            LEFT JOIN countries country ON country.id = c.country_id
            LEFT JOIN country_translations ctr_locale ON ctr_locale.country_id = country.id AND ctr_locale.locale = ${locale}
            LEFT JOIN country_translations ctr_en ON ctr_en.country_id = country.id AND ctr_en.locale = 'en'
            WHERE c.gender = ${gender}
              AND (
                ${trimmedQuery === ''}
                OR COALESCE(ct_locale.name, ct_en.name, c.slug) ILIKE ${searchPattern}
                OR COALESCE(ctr_locale.name, ctr_en.name, country.code_alpha3, '') ILIKE ${searchPattern}
                OR c.slug ILIKE ${searchPattern}
              )
          `,
          sql<LeagueRow[]>`
            WITH latest_competition_seasons AS (
              SELECT DISTINCT ON (cs.competition_id)
                cs.id,
                cs.competition_id,
                cs.season_id
              FROM competition_seasons cs
              JOIN seasons s ON s.id = cs.season_id
              ORDER BY cs.competition_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
            )
            SELECT
              c.slug AS id,
              COALESCE(
                (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = ${locale}),
                (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
                c.slug
              ) AS name,
              COALESCE(
                (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
                (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
                country.code_alpha3
              ) AS country,
              CASE
                WHEN s.start_date IS NOT NULL AND s.end_date IS NOT NULL THEN
                  CASE
                    WHEN EXTRACT(YEAR FROM s.start_date) = EXTRACT(YEAR FROM s.end_date)
                      THEN EXTRACT(YEAR FROM s.start_date)::INT::TEXT
                    ELSE CONCAT(
                      EXTRACT(YEAR FROM s.start_date)::INT::TEXT,
                      '/',
                      LPAD((EXTRACT(YEAR FROM s.end_date)::INT % 100)::TEXT, 2, '0')
                    )
                  END
                ELSE REGEXP_REPLACE(s.slug, '-[0-9]+$', '')
              END AS season,
              c.gender,
              c.emblem_url,
              COUNT(ts.id)::INT AS number_of_clubs
            FROM competitions c
            LEFT JOIN countries country ON country.id = c.country_id
            JOIN latest_competition_seasons lcs ON lcs.competition_id = c.id
            JOIN seasons s ON s.id = lcs.season_id
            LEFT JOIN team_seasons ts ON ts.competition_season_id = lcs.id
            WHERE c.gender = ${gender}
              AND (
                ${trimmedQuery === ''}
                OR COALESCE(
                  (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = ${locale}),
                  (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
                  c.slug
                ) ILIKE ${searchPattern}
                OR COALESCE(
                  (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
                  (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
                  country.code_alpha3
                ) ILIKE ${searchPattern}
                OR c.slug ILIKE ${searchPattern}
              )
            GROUP BY c.id, country.id, s.id
            ORDER BY name ASC
            LIMIT ${pageSize}
            OFFSET ${offset}
          `,
        ]);

        return createPaginatedResult(rows.map(mapLeague), countRows[0]?.total_count ?? 0, currentPage, pageSize);
      },
    });
  }, () => createPaginatedResult([], 0, currentPage, pageSize));
}

export async function getLeagueByIdDb(id: string, locale: string = 'en'): Promise<League | undefined> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'league-by-id', locale, id });

    return readThroughCache({
      key,
      tier: 'master',
      loader: async () => {
        const rows = await sql<LeagueRow[]>`
          WITH latest_competition_season AS (
            SELECT DISTINCT ON (cs.competition_id)
              cs.id,
              cs.competition_id,
              cs.season_id
            FROM competition_seasons cs
            JOIN seasons s ON s.id = cs.season_id
            WHERE cs.competition_id = (SELECT c.id FROM competitions c WHERE c.slug = ${id} LIMIT 1)
            ORDER BY cs.competition_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
          )
          SELECT
            c.slug AS id,
            COALESCE(
              (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = ${locale}),
              (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
              c.slug
            ) AS name,
            COALESCE(
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
              country.code_alpha3
            ) AS country,
            CASE
              WHEN s.start_date IS NOT NULL AND s.end_date IS NOT NULL THEN
                CASE
                  WHEN EXTRACT(YEAR FROM s.start_date) = EXTRACT(YEAR FROM s.end_date)
                    THEN EXTRACT(YEAR FROM s.start_date)::INT::TEXT
                  ELSE CONCAT(
                    EXTRACT(YEAR FROM s.start_date)::INT::TEXT,
                    '/',
                    LPAD((EXTRACT(YEAR FROM s.end_date)::INT % 100)::TEXT, 2, '0')
                  )
                END
              ELSE REGEXP_REPLACE(s.slug, '-[0-9]+$', '')
            END AS season,
            c.gender,
            c.emblem_url,
            COUNT(ts.id)::INT AS number_of_clubs
          FROM competitions c
          LEFT JOIN countries country ON country.id = c.country_id
          JOIN latest_competition_season lcs ON lcs.competition_id = c.id
          JOIN seasons s ON s.id = lcs.season_id
          LEFT JOIN team_seasons ts ON ts.competition_season_id = lcs.id
          WHERE c.slug = ${id}
          GROUP BY c.id, country.id, s.id
          LIMIT 1
        `;

        return rows[0] ? mapLeague(rows[0]) : undefined;
      },
    });
  }, () => undefined);
}

export async function getClubsDb(locale: string = 'en'): Promise<Club[]> {
  return withFallback(async () => {
    const key = buildCacheKey({ namespace: 'clubs-v2', locale });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await getClubRepresentativeRowsDb(locale);
        return selectRepresentativeClubRows(rows).map(mapClub);
      },
    });
  }, () => []);
}

export async function getPaginatedClubsDb(
  locale: string = 'en',
  query: string = '',
  gender: 'male' | 'female' = 'male',
  options: PaginationOptions = {}
): Promise<PaginatedResult<ClubListItem>> {
  const { currentPage, pageSize, offset } = normalizePagination(options);
  const trimmedQuery = query.trim().toLowerCase();

  return withFallback(async () => {
    const key = buildCacheKey({ namespace: 'clubs-paginated-v2', locale, params: { page: currentPage, pageSize, q: trimmedQuery, gender } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = selectRepresentativeClubRows(await getClubRepresentativeRowsDb(locale)).filter((row) => {
          if (row.gender !== gender) {
            return false;
          }

          if (!trimmedQuery) {
            return true;
          }

          const haystack = [row.name, row.short_name, row.country, row.stadium, row.league_name, row.id]
            .filter(Boolean)
            .join(' ')
            .toLowerCase();

          return haystack.includes(trimmedQuery);
        });
        const pagedRows = rows.slice(offset, offset + pageSize);

        return createPaginatedResult(pagedRows.map(mapClubListItem), rows.length, currentPage, pageSize);
      },
    });
  }, () => createPaginatedResult([], 0, currentPage, pageSize));
}

export async function getClubByIdDb(id: string, locale: string = 'en'): Promise<Club | undefined> {
  return withFallback(async () => {
    const key = buildCacheKey({ namespace: 'club-by-id-v3', locale, id });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const row = await getClubRepresentativeRowBySlugDb(id, locale);
        return row ? mapClub(row) : undefined;
      },
    });
  }, () => undefined);
}

export async function getClubsByIdsDb(ids: string[], locale: string = 'en'): Promise<Club[]> {
  const normalizedIds = Array.from(new Set(ids.filter(Boolean)));

  if (normalizedIds.length === 0) {
    return [];
  }

  return withFallback(async () => {
    const sql = getDb();
    const rows = await sql<ClubRepresentativeRow[]>`
      SELECT
        t.slug AS id,
        COALESCE(
          (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
          (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
          t.slug
        ) AS name,
        COALESCE(
          (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
          (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
          t.slug
        ) AS short_name,
        COALESCE(
          (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
          (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
          country.code_alpha3
        ) AS country,
        t.gender,
        t.founded_year AS founded,
        COALESCE(
          (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
          (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
          v.slug,
          ''
        ) AS stadium,
        v.capacity AS stadium_capacity,
        c.slug AS league_id,
        COALESCE(
          (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = ${locale}),
          (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = c.id AND ct.locale = 'en'),
          c.slug
        ) AS league_name,
        t.crest_url,
        s.start_date::TEXT AS season_start_date,
        s.end_date::TEXT AS season_end_date
      FROM team_seasons ts
      JOIN teams t ON t.id = ts.team_id
      JOIN countries country ON country.id = t.country_id
      LEFT JOIN venues v ON v.id = t.venue_id
      JOIN competition_seasons cs ON cs.id = ts.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      JOIN seasons s ON s.id = cs.season_id
      WHERE t.is_national = FALSE
        AND t.slug = ANY(${normalizedIds})
    `;

    return selectRepresentativeClubRows(rows).map(mapClub);
  }, () => []);
}

export async function getClubSeasonHistoryDb(clubId: string, locale: string = 'en'): Promise<ClubSeasonHistoryEntry[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'club-season-history', locale, id: clubId });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<ClubSeasonHistoryRowDb[]>`
          WITH club_team AS (
            SELECT id, slug
            FROM teams
            WHERE slug = ${clubId}
          ), club_competition_seasons AS (
            SELECT
              cs.id AS competition_season_id,
              c.id AS competition_id,
              c.slug AS league_id,
              s.slug AS season_id,
              CASE
                WHEN s.start_date IS NOT NULL AND s.end_date IS NOT NULL THEN
                  CASE
                    WHEN EXTRACT(YEAR FROM s.start_date) = EXTRACT(YEAR FROM s.end_date)
                      THEN EXTRACT(YEAR FROM s.start_date)::INT::TEXT
                    ELSE CONCAT(
                      EXTRACT(YEAR FROM s.start_date)::INT::TEXT,
                      '/',
                      LPAD((EXTRACT(YEAR FROM s.end_date)::INT % 100)::TEXT, 2, '0')
                    )
                  END
                ELSE REGEXP_REPLACE(s.slug, '-[0-9]+$', '')
              END AS season_label,
              s.start_date,
              s.end_date
            FROM club_team team
            JOIN team_seasons ts ON ts.team_id = team.id
            JOIN competition_seasons cs ON cs.id = ts.competition_season_id
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
          ), match_results AS (
            SELECT
              m.competition_season_id,
              m.home_team_id AS team_id,
              CASE
                WHEN m.home_score > m.away_score THEN 3
                WHEN m.home_score = m.away_score THEN 1
                ELSE 0
              END AS points,
              CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END AS won,
              CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END AS drawn,
              CASE WHEN m.home_score < m.away_score THEN 1 ELSE 0 END AS lost,
              m.home_score AS goals_for,
              m.away_score AS goals_against,
              m.match_date,
              m.id AS match_id,
              CASE
                WHEN m.home_score > m.away_score THEN 'W'
                WHEN m.home_score = m.away_score THEN 'D'
                ELSE 'L'
              END::TEXT AS form_result
            FROM matches m
            JOIN club_competition_seasons ccs ON ccs.competition_season_id = m.competition_season_id
            WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')

            UNION ALL

            SELECT
              m.competition_season_id,
              m.away_team_id AS team_id,
              CASE
                WHEN m.away_score > m.home_score THEN 3
                WHEN m.away_score = m.home_score THEN 1
                ELSE 0
              END AS points,
              CASE WHEN m.away_score > m.home_score THEN 1 ELSE 0 END AS won,
              CASE WHEN m.away_score = m.home_score THEN 1 ELSE 0 END AS drawn,
              CASE WHEN m.away_score < m.home_score THEN 1 ELSE 0 END AS lost,
              m.away_score AS goals_for,
              m.home_score AS goals_against,
              m.match_date,
              m.id AS match_id,
              CASE
                WHEN m.away_score > m.home_score THEN 'W'
                WHEN m.away_score = m.home_score THEN 'D'
                ELSE 'L'
              END::TEXT AS form_result
            FROM matches m
            JOIN club_competition_seasons ccs ON ccs.competition_season_id = m.competition_season_id
            WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')
          ), standings AS (
            SELECT
              mr.competition_season_id,
              mr.team_id,
              COUNT(*)::INT AS played,
              SUM(mr.won)::INT AS won,
              SUM(mr.drawn)::INT AS drawn,
              SUM(mr.lost)::INT AS lost,
              SUM(mr.goals_for)::INT AS goals_for,
              SUM(mr.goals_against)::INT AS goals_against,
              (SUM(mr.goals_for) - SUM(mr.goals_against))::INT AS goal_difference,
              SUM(mr.points)::INT AS points,
              RANK() OVER (
                PARTITION BY mr.competition_season_id
                ORDER BY
                  SUM(mr.points) DESC,
                  SUM(mr.goals_for) - SUM(mr.goals_against) DESC,
                  SUM(mr.goals_for) DESC
              )::INT AS position
            FROM match_results mr
            GROUP BY mr.competition_season_id, mr.team_id
          ), ranked_results AS (
            SELECT
              mr.competition_season_id,
              mr.team_id,
              mr.form_result,
              mr.match_date,
              mr.match_id,
              ROW_NUMBER() OVER (
                PARTITION BY mr.competition_season_id, mr.team_id
                ORDER BY mr.match_date DESC, mr.match_id DESC
              ) AS recent_rank
            FROM match_results mr
          ), team_form AS (
            SELECT
              rr.competition_season_id,
              rr.team_id,
              ARRAY_AGG(rr.form_result::TEXT ORDER BY rr.match_date DESC, rr.match_id DESC) AS form
            FROM ranked_results rr
            WHERE rr.recent_rank <= 5
            GROUP BY rr.competition_season_id, rr.team_id
          )
          SELECT
            ccs.season_id,
            ccs.season_label,
            ccs.league_id,
            COALESCE(
              (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = ccs.competition_id AND ct.locale = ${locale}),
              (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = ccs.competition_id AND ct.locale = 'en'),
              ccs.league_id
            ) AS league_name,
            standings.position,
            standings.played,
            standings.won,
            standings.drawn,
            standings.lost,
            standings.goals_for,
            standings.goals_against,
            standings.goal_difference,
            standings.points,
            team_form.form::TEXT[]
          FROM club_competition_seasons ccs
          JOIN club_team team ON TRUE
          JOIN standings
            ON standings.competition_season_id = ccs.competition_season_id
            AND standings.team_id = team.id
          LEFT JOIN team_form
            ON team_form.competition_season_id = ccs.competition_season_id
            AND team_form.team_id = team.id
          ORDER BY ccs.end_date DESC NULLS LAST, ccs.start_date DESC NULLS LAST, ccs.competition_season_id DESC, ccs.league_id ASC
        `;

        return rows.map(mapClubSeasonHistoryEntry);
      },
    });
  }, () => []);
}

export async function getClubsByLeagueDb(leagueId: string, locale: string = 'en'): Promise<Club[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'clubs-by-league-v2', locale, id: leagueId });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<ClubRow[]>`
          WITH latest_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug = ${leagueId}
            ORDER BY s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
            LIMIT 1
          )
          SELECT
            t.slug AS id,
            COALESCE(
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
              t.slug
            ) AS name,
            COALESCE(
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
              t.slug
            ) AS short_name,
            COALESCE(
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
              country.code_alpha3
            ) AS country,
            t.gender,
            t.founded_year AS founded,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS stadium,
            v.capacity AS stadium_capacity,
            c.slug AS league_id,
            t.crest_url
          FROM latest_competition_season lcs
          JOIN team_seasons ts ON ts.competition_season_id = lcs.id
          JOIN teams t ON t.id = ts.team_id
          JOIN countries country ON country.id = t.country_id
          LEFT JOIN venues v ON v.id = t.venue_id
          JOIN competition_seasons cs ON cs.id = ts.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          WHERE t.is_national = FALSE
          ORDER BY name ASC
        `;

        return rows.map(mapClub);
      },
    });
  }, () => []);
}

export async function getClubNameDb(id: string, locale: string = 'en'): Promise<string> {
  const club = await getClubByIdDb(id, locale);
  return club?.name ?? 'Unknown';
}

export async function getClubShortNameDb(id: string, locale: string = 'en'): Promise<string> {
  const club = await getClubByIdDb(id, locale);
  return club?.shortName ?? '???';
}

export async function getPlayersDb(locale: string = 'en'): Promise<Player[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'players', locale });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<PlayerRow[]>`
          WITH latest_player_contracts AS (
            SELECT DISTINCT ON (pc.player_id)
              pc.player_id,
              pc.team_id,
              pc.competition_season_id,
              pc.shirt_number,
              pc.joined_date
            FROM player_contracts pc
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            ORDER BY
              pc.player_id,
              COALESCE(pc.left_date, s.end_date, s.start_date) DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              pc.joined_date DESC NULLS LAST,
              pc.competition_season_id DESC
          )
          SELECT
            p.slug AS id,
            COALESCE(
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              p.slug
            ) AS name,
            COALESCE(
              (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              ''
            ) AS first_name,
            COALESCE(
              (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              ''
            ) AS last_name,
            p.date_of_birth::TEXT AS date_of_birth,
            CASE
              WHEN p.date_of_birth IS NULL THEN NULL
              ELSE EXTRACT(YEAR FROM age(NOW(), p.date_of_birth))::INT
            END AS age,
            COALESCE(
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
              country.code_alpha3
            ) AS nationality,
            country.code_alpha3::TEXT AS nation_id,
            team.slug AS club_id,
            p.position,
            p.photo_url,
            lpc.shirt_number,
            lpc.joined_date::TEXT AS joined_date,
            NULL::TEXT AS contract_start_date,
            NULL::TEXT AS contract_end_date,
            NULL::INT AS annual_salary_eur,
            NULL::INT AS weekly_wage_eur,
            NULL::TEXT AS salary_currency,
            NULL::TEXT AS salary_source,
            NULL::TEXT AS salary_source_url,
            NULL::BOOLEAN AS salary_is_estimated,
            p.height_cm AS height,
            p.preferred_foot,
            pss.appearances,
            pss.goals,
            pss.assists,
            pss.minutes_played,
            pss.yellow_cards,
            pss.red_cards,
            pss.clean_sheets
          FROM latest_player_contracts lpc
          JOIN players p ON p.id = lpc.player_id
          LEFT JOIN countries country ON country.id = p.country_id
          JOIN competition_seasons cs ON cs.id = lpc.competition_season_id
          JOIN teams team ON team.id = lpc.team_id
          LEFT JOIN player_season_stats pss ON pss.player_id = p.id AND pss.competition_season_id = lpc.competition_season_id
          ORDER BY name ASC
        `;

        return rows.map(mapPlayer);
      },
    });
  }, () => [], 'getPlayersDb');
}

export async function getPaginatedPlayersDb(
  locale: string = 'en',
  query: string = '',
  gender: 'male' | 'female' = 'male',
  options: PaginationOptions = {}
): Promise<PaginatedResult<PlayerListItem>> {
  const { currentPage, pageSize, offset } = normalizePagination(options);
  const trimmedQuery = query.trim();
  const searchPattern = `%${trimmedQuery}%`;

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'players-paginated', locale, params: { page: currentPage, pageSize, q: trimmedQuery, gender } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const latestPlayerContracts = sql`
          WITH latest_player_contracts AS (
            SELECT DISTINCT ON (pc.player_id)
              pc.player_id,
              pc.team_id,
              pc.competition_season_id,
              pc.shirt_number,
              pc.joined_date
            FROM player_contracts pc
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            ORDER BY
              pc.player_id,
              COALESCE(pc.left_date, s.end_date, s.start_date) DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              pc.joined_date DESC NULLS LAST,
              pc.competition_season_id DESC
          )
        `;

        const [countRows, rows] = await Promise.all([
          sql<{ total_count: number }[]>`
            ${latestPlayerContracts}
            SELECT COUNT(*)::INT AS total_count
            FROM latest_player_contracts
            JOIN players p ON p.id = latest_player_contracts.player_id
            LEFT JOIN player_translations pt_locale ON pt_locale.player_id = p.id AND pt_locale.locale = ${locale}
            LEFT JOIN player_translations pt_en ON pt_en.player_id = p.id AND pt_en.locale = 'en'
            LEFT JOIN countries country ON country.id = p.country_id
            LEFT JOIN country_translations ctr_locale ON ctr_locale.country_id = country.id AND ctr_locale.locale = ${locale}
            LEFT JOIN country_translations ctr_en ON ctr_en.country_id = country.id AND ctr_en.locale = 'en'
            JOIN teams team ON team.id = latest_player_contracts.team_id
            LEFT JOIN team_translations tt_locale ON tt_locale.team_id = team.id AND tt_locale.locale = ${locale}
            LEFT JOIN team_translations tt_en ON tt_en.team_id = team.id AND tt_en.locale = 'en'
            WHERE team.gender = ${gender}
              AND (
                ${trimmedQuery === ''}
                OR COALESCE(pt_locale.known_as, pt_en.known_as, p.slug) ILIKE ${searchPattern}
                OR CONCAT_WS(' ', COALESCE(pt_locale.first_name, pt_en.first_name, ''), COALESCE(pt_locale.last_name, pt_en.last_name, '')) ILIKE ${searchPattern}
                OR COALESCE(tt_locale.name, tt_en.name, team.slug) ILIKE ${searchPattern}
                OR COALESCE(ctr_locale.name, ctr_en.name, country.code_alpha3, '') ILIKE ${searchPattern}
                OR p.slug ILIKE ${searchPattern}
              )
          `,
          sql<PlayerListRow[]>`
            ${latestPlayerContracts}
            SELECT
              p.slug AS id,
              COALESCE(
                (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
                (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
                p.slug
              ) AS name,
              COALESCE(
                (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
                (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
                ''
              ) AS first_name,
              COALESCE(
                (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
                (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
                ''
              ) AS last_name,
              p.date_of_birth::TEXT AS date_of_birth,
              CASE
                WHEN p.date_of_birth IS NULL THEN NULL
                ELSE EXTRACT(YEAR FROM age(NOW(), p.date_of_birth))::INT
              END AS age,
              COALESCE(
                (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
                (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
                country.code_alpha3
              ) AS nationality,
              country.code_alpha3::TEXT AS nation_id,
              team.slug AS club_id,
              p.position,
              p.photo_url,
              lpc.shirt_number,
              lpc.joined_date::TEXT AS joined_date,
              NULL::TEXT AS contract_start_date,
              NULL::TEXT AS contract_end_date,
              NULL::INT AS annual_salary_eur,
              NULL::INT AS weekly_wage_eur,
              NULL::TEXT AS salary_currency,
              NULL::TEXT AS salary_source,
              NULL::TEXT AS salary_source_url,
              NULL::BOOLEAN AS salary_is_estimated,
              p.height_cm AS height,
              p.preferred_foot,
              pss.appearances,
              pss.goals,
              pss.assists,
              pss.minutes_played,
              pss.yellow_cards,
              pss.red_cards,
              pss.clean_sheets,
              COALESCE(
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
                team.slug
              ) AS club_name,
              COALESCE(
                (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
                (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
                team.slug
              ) AS club_short_name,
              team.crest_url AS club_logo,
              COALESCE(
                (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
                (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
                country.code_alpha3
              ) AS nation_name,
              country.code_alpha3::TEXT AS nation_code,
              country.flag_url AS nation_flag
            FROM latest_player_contracts lpc
            JOIN players p ON p.id = lpc.player_id
            LEFT JOIN countries country ON country.id = p.country_id
            JOIN competition_seasons cs ON cs.id = lpc.competition_season_id
            JOIN teams team ON team.id = lpc.team_id
            LEFT JOIN player_season_stats pss ON pss.player_id = p.id AND pss.competition_season_id = lpc.competition_season_id
            WHERE team.gender = ${gender}
              AND (
                ${trimmedQuery === ''}
                OR COALESCE(
                  (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
                  (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
                  p.slug
                ) ILIKE ${searchPattern}
                OR CONCAT_WS(
                  ' ',
                  COALESCE((SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}), (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'), ''),
                  COALESCE((SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}), (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'), '')
                ) ILIKE ${searchPattern}
                OR COALESCE(
                  (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
                  (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
                  team.slug
                ) ILIKE ${searchPattern}
                OR COALESCE(
                  (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
                  (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
                  country.code_alpha3
                ) ILIKE ${searchPattern}
                OR p.slug ILIKE ${searchPattern}
              )
            ORDER BY name ASC
            LIMIT ${pageSize}
            OFFSET ${offset}
          `,
        ]);

        return createPaginatedResult(rows.map(mapPlayerListItem), countRows[0]?.total_count ?? 0, currentPage, pageSize);
      },
    });
  }, () => createPaginatedResult([], 0, currentPage, pageSize), 'getPaginatedPlayersDb');
}

export async function getPlayerByIdDb(id: string, locale: string = 'en'): Promise<Player | undefined> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'player-by-id', locale, id });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<PlayerRow[]>`
          WITH latest_player_contracts AS (
            SELECT DISTINCT ON (pc.player_id)
              pc.player_id,
              pc.team_id,
              pc.competition_season_id,
              pc.shirt_number,
              pc.joined_date
            FROM player_contracts pc
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            ORDER BY
              pc.player_id,
              COALESCE(pc.left_date, s.end_date, s.start_date) DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              pc.joined_date DESC NULLS LAST,
              pc.competition_season_id DESC
          )
          SELECT
            p.slug AS id,
            COALESCE(
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              p.slug
            ) AS name,
            COALESCE(
              (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              ''
            ) AS first_name,
            COALESCE(
              (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              ''
            ) AS last_name,
            p.date_of_birth::TEXT AS date_of_birth,
            CASE
              WHEN p.date_of_birth IS NULL THEN NULL
              ELSE EXTRACT(YEAR FROM age(NOW(), p.date_of_birth))::INT
            END AS age,
            COALESCE(
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
              country.code_alpha3
            ) AS nationality,
            country.code_alpha3::TEXT AS nation_id,
            team.slug AS club_id,
            p.position,
            p.photo_url,
            lpc.shirt_number,
            lpc.joined_date::TEXT AS joined_date,
            NULL::TEXT AS contract_start_date,
            NULL::TEXT AS contract_end_date,
            NULL::INT AS annual_salary_eur,
            NULL::INT AS weekly_wage_eur,
            NULL::TEXT AS salary_currency,
            NULL::TEXT AS salary_source,
            NULL::TEXT AS salary_source_url,
            NULL::BOOLEAN AS salary_is_estimated,
            p.height_cm AS height,
            p.preferred_foot,
            pss.appearances,
            pss.goals,
            pss.assists,
            pss.minutes_played,
            pss.yellow_cards,
            pss.red_cards,
            pss.clean_sheets
          FROM latest_player_contracts lpc
          JOIN players p ON p.id = lpc.player_id
          LEFT JOIN countries country ON country.id = p.country_id
          JOIN teams team ON team.id = lpc.team_id
          LEFT JOIN player_season_stats pss ON pss.player_id = p.id AND pss.competition_season_id = lpc.competition_season_id
          WHERE p.slug = ${id}
          LIMIT 1
        `;

        return rows[0] ? mapPlayer(rows[0]) : undefined;
      },
    });
  }, () => undefined);
}

export async function getPlayersByClubDb(clubId: string, locale: string = 'en'): Promise<Player[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'players-by-club', locale, id: clubId });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<PlayerRow[]>`
          WITH latest_player_contracts AS (
            SELECT DISTINCT ON (pc.player_id)
              pc.player_id,
              pc.team_id,
              pc.competition_season_id,
              pc.shirt_number,
              pc.joined_date
            FROM player_contracts pc
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            ORDER BY
              pc.player_id,
              COALESCE(pc.left_date, s.end_date, s.start_date) DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              pc.joined_date DESC NULLS LAST,
              pc.competition_season_id DESC
          )
          SELECT
            p.slug AS id,
            COALESCE(
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              p.slug
            ) AS name,
            COALESCE(
              (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              ''
            ) AS first_name,
            COALESCE(
              (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              ''
            ) AS last_name,
            p.date_of_birth::TEXT AS date_of_birth,
            CASE
              WHEN p.date_of_birth IS NULL THEN NULL
              ELSE EXTRACT(YEAR FROM age(NOW(), p.date_of_birth))::INT
            END AS age,
            COALESCE(
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
              country.code_alpha3
            ) AS nationality,
            country.code_alpha3::TEXT AS nation_id,
            team.slug AS club_id,
            p.position,
            p.photo_url,
            lpc.shirt_number,
            lpc.joined_date::TEXT AS joined_date,
            NULL::TEXT AS contract_start_date,
            NULL::TEXT AS contract_end_date,
            NULL::INT AS annual_salary_eur,
            NULL::INT AS weekly_wage_eur,
            NULL::TEXT AS salary_currency,
            NULL::TEXT AS salary_source,
            NULL::TEXT AS salary_source_url,
            NULL::BOOLEAN AS salary_is_estimated,
            p.height_cm AS height,
            p.preferred_foot,
            pss.appearances,
            pss.goals,
            pss.assists,
            pss.minutes_played,
            pss.yellow_cards,
            pss.red_cards,
            pss.clean_sheets
          FROM latest_player_contracts lpc
          JOIN players p ON p.id = lpc.player_id
          LEFT JOIN countries country ON country.id = p.country_id
          JOIN teams team ON team.id = lpc.team_id
          LEFT JOIN player_season_stats pss ON pss.player_id = p.id AND pss.competition_season_id = lpc.competition_season_id
          WHERE team.slug = ${clubId}
          ORDER BY name ASC
        `;

        return rows.map(mapPlayer);
      },
    });
  }, () => []);
}

export async function getPlayersByClubAndSeasonDb(
  clubId: string,
  seasonId: string,
  locale: string = 'en'
): Promise<Player[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'players-by-club-season', locale, id: clubId, params: { season: seasonId } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<PlayerRow[]>`
          WITH target_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug IN (
              SELECT c2.slug
              FROM teams team
              JOIN team_seasons ts ON ts.team_id = team.id
              JOIN competition_seasons cs2 ON cs2.id = ts.competition_season_id
              JOIN competitions c2 ON c2.id = cs2.competition_id
              JOIN seasons s2 ON s2.id = cs2.season_id
              WHERE team.slug = ${clubId}
                AND s2.slug = ${seasonId}
              ORDER BY cs2.id DESC
              LIMIT 1
            )
              AND s.slug = ${seasonId}
            ORDER BY cs.id DESC
            LIMIT 1
          ), season_player_contracts AS (
            SELECT DISTINCT ON (pc.player_id)
              pc.player_id,
              pc.team_id,
              pc.competition_season_id,
              pc.shirt_number,
              pc.joined_date
            FROM player_contracts pc
            JOIN teams team ON team.id = pc.team_id
            JOIN target_competition_season tcs ON tcs.id = pc.competition_season_id
            WHERE team.slug = ${clubId}
            ORDER BY pc.player_id, pc.joined_date DESC NULLS LAST, pc.competition_season_id DESC
          )
          SELECT
            p.slug AS id,
            COALESCE(
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              p.slug
            ) AS name,
            COALESCE(
              (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              ''
            ) AS first_name,
            COALESCE(
              (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              ''
            ) AS last_name,
            p.date_of_birth::TEXT AS date_of_birth,
            CASE
              WHEN p.date_of_birth IS NULL THEN NULL
              ELSE EXTRACT(YEAR FROM age(NOW(), p.date_of_birth))::INT
            END AS age,
            COALESCE(
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
              country.code_alpha3
            ) AS nationality,
            country.code_alpha3::TEXT AS nation_id,
            team.slug AS club_id,
            p.position,
            p.photo_url,
            spc.shirt_number,
            spc.joined_date::TEXT AS joined_date,
            NULL::TEXT AS contract_start_date,
            NULL::TEXT AS contract_end_date,
            NULL::INT AS annual_salary_eur,
            NULL::INT AS weekly_wage_eur,
            NULL::TEXT AS salary_currency,
            NULL::TEXT AS salary_source,
            NULL::TEXT AS salary_source_url,
            NULL::BOOLEAN AS salary_is_estimated,
            p.height_cm AS height,
            p.preferred_foot,
            pss.appearances,
            pss.goals,
            pss.assists,
            pss.minutes_played,
            pss.yellow_cards,
            pss.red_cards,
            pss.clean_sheets
          FROM season_player_contracts spc
          JOIN players p ON p.id = spc.player_id
          LEFT JOIN countries country ON country.id = p.country_id
          JOIN teams team ON team.id = spc.team_id
          LEFT JOIN player_season_stats pss ON pss.player_id = p.id AND pss.competition_season_id = spc.competition_season_id
          ORDER BY name ASC
        `;

        return rows.map(mapPlayer);
      },
    });
  }, () => []);
}

export async function getClubSeasonMetaDb(
  clubId: string,
  seasonId: string,
  locale: string = 'en'
): Promise<{ coachName?: string }> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'club-season-meta', locale, id: clubId, params: { season: seasonId } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<ClubSeasonMetaRow[]>`
          SELECT
            COALESCE(
              (SELECT ct.known_as FROM coach_translations ct WHERE ct.coach_id = coach.id AND ct.locale = ${locale}),
              (SELECT ct.known_as FROM coach_translations ct WHERE ct.coach_id = coach.id AND ct.locale = 'en'),
              coach.slug
            ) AS coach_name
          FROM teams team
          JOIN team_seasons ts ON ts.team_id = team.id
          JOIN competition_seasons cs ON cs.id = ts.competition_season_id
          JOIN seasons s ON s.id = cs.season_id
          LEFT JOIN coaches coach ON coach.id = ts.coach_id
          WHERE team.slug = ${clubId}
            AND s.slug = ${seasonId}
          ORDER BY cs.id DESC
          LIMIT 1
        `;

        return {
          coachName: rows[0]?.coach_name ?? undefined,
        };
      },
    });
  }, () => ({}));
}

export async function getPlayersByNationDb(nationId: string, locale: string = 'en'): Promise<Player[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'players-by-nation', locale, id: nationId.toLowerCase() });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<PlayerRow[]>`
          WITH latest_player_contracts AS (
            SELECT DISTINCT ON (pc.player_id)
              pc.player_id,
              pc.team_id,
              pc.competition_season_id,
              pc.shirt_number,
              pc.joined_date
            FROM player_contracts pc
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            ORDER BY
              pc.player_id,
              COALESCE(pc.left_date, s.end_date, s.start_date) DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              pc.joined_date DESC NULLS LAST,
              pc.competition_season_id DESC
          )
          SELECT
            p.slug AS id,
            COALESCE(
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              p.slug
            ) AS name,
            COALESCE(
              (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.first_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              ''
            ) AS first_name,
            COALESCE(
              (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = ${locale}),
              (SELECT pt.last_name FROM player_translations pt WHERE pt.player_id = p.id AND pt.locale = 'en'),
              ''
            ) AS last_name,
            p.date_of_birth::TEXT AS date_of_birth,
            CASE
              WHEN p.date_of_birth IS NULL THEN NULL
              ELSE EXTRACT(YEAR FROM age(NOW(), p.date_of_birth))::INT
            END AS age,
            COALESCE(
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
              country.code_alpha3
            ) AS nationality,
            country.code_alpha3::TEXT AS nation_id,
            team.slug AS club_id,
            p.position,
            p.photo_url,
            lpc.shirt_number,
            lpc.joined_date::TEXT AS joined_date,
            NULL::TEXT AS contract_start_date,
            NULL::TEXT AS contract_end_date,
            NULL::INT AS annual_salary_eur,
            NULL::INT AS weekly_wage_eur,
            NULL::TEXT AS salary_currency,
            NULL::TEXT AS salary_source,
            NULL::TEXT AS salary_source_url,
            NULL::BOOLEAN AS salary_is_estimated,
            p.height_cm AS height,
            p.preferred_foot,
            pss.appearances,
            pss.goals,
            pss.assists,
            pss.minutes_played,
            pss.yellow_cards,
            pss.red_cards,
            pss.clean_sheets
          FROM latest_player_contracts lpc
          JOIN players p ON p.id = lpc.player_id
          LEFT JOIN countries country ON country.id = p.country_id
          JOIN teams team ON team.id = lpc.team_id
          LEFT JOIN player_season_stats pss ON pss.player_id = p.id AND pss.competition_season_id = lpc.competition_season_id
          WHERE UPPER(country.code_alpha3) = UPPER(${nationId})
          ORDER BY name ASC
        `;

        return rows.map(mapPlayer);
      },
    });
  }, () => []);
}

export async function getPlayerNameDb(id: string, locale: string = 'en'): Promise<string> {
  const player = await getPlayerByIdDb(id, locale);
  return player?.name ?? 'Unknown';
}

export async function listPlayersForPhotoSyncDb(limit: number = 50): Promise<PlayerPhotoSyncTarget[]> {
  const sql = getDb();

  const rows = await sql<PlayerPhotoSyncTargetRow[]>`
    SELECT
      p.slug AS id,
      COALESCE(pt.known_as, p.slug) AS name,
      COALESCE(pt.first_name, '') AS first_name,
      COALESCE(pt.last_name, '') AS last_name,
      p.date_of_birth::TEXT AS date_of_birth,
      COALESCE(ct.name, country.code_alpha3) AS nationality,
      p.photo_url
    FROM players p
    LEFT JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
    LEFT JOIN countries country ON country.id = p.country_id
    LEFT JOIN country_translations ct ON ct.country_id = country.id AND ct.locale = 'en'
    WHERE p.is_active = TRUE
    ORDER BY p.updated_at DESC, p.slug ASC
    LIMIT ${limit}
  `;

  return rows.map(mapPlayerPhotoSyncTarget);
}

export async function getPlayerPhotoSourcesDb(playerId?: string): Promise<PlayerPhotoSource[]> {
  const sql = getDb();

  const rows = playerId
    ? await sql<PlayerPhotoSourceRow[]>`
        SELECT
          p.slug AS player_id,
          ds.slug::TEXT AS provider,
          pps.external_id,
          pps.source_url,
          pps.mirrored_url,
          pps.status,
          pps.matched_by,
          pps.match_score,
          pps.etag,
          pps.last_modified,
          pps.last_checked_at::TEXT,
          pps.last_synced_at::TEXT,
          pps.failure_count,
          pps.last_error
        FROM player_photo_sources pps
        JOIN players p ON p.id = pps.player_id
        JOIN data_sources ds ON ds.id = pps.data_source_id
        WHERE p.slug = ${playerId}
        ORDER BY ds.priority ASC, ds.slug ASC
      `
    : await sql<PlayerPhotoSourceRow[]>`
        SELECT
          p.slug AS player_id,
          ds.slug::TEXT AS provider,
          pps.external_id,
          pps.source_url,
          pps.mirrored_url,
          pps.status,
          pps.matched_by,
          pps.match_score,
          pps.etag,
          pps.last_modified,
          pps.last_checked_at::TEXT,
          pps.last_synced_at::TEXT,
          pps.failure_count,
          pps.last_error
        FROM player_photo_sources pps
        JOIN players p ON p.id = pps.player_id
        JOIN data_sources ds ON ds.id = pps.data_source_id
        ORDER BY p.slug ASC, ds.priority ASC, ds.slug ASC
      `;

  return rows.map(mapPlayerPhotoSource);
}

export interface UpsertPlayerPhotoSourceInput {
  playerId: string;
  provider: PhotoSyncProvider;
  externalId?: string;
  sourceUrl?: string;
  mirroredUrl?: string;
  status: PlayerPhotoSourceStatus;
  matchedBy?: string;
  matchScore?: number;
  etag?: string;
  lastModified?: string;
  lastCheckedAt?: string;
  lastSyncedAt?: string;
  failureCount?: number;
  lastError?: string;
}

export async function upsertPlayerPhotoSourceDb(input: UpsertPlayerPhotoSourceInput): Promise<void> {
  const sql = getDb();

  await sql`
    INSERT INTO player_photo_sources (
      player_id,
      data_source_id,
      external_id,
      source_url,
      mirrored_url,
      status,
      matched_by,
      match_score,
      etag,
      last_modified,
      last_checked_at,
      last_synced_at,
      failure_count,
      last_error,
      updated_at
    )
    VALUES (
      (SELECT id FROM players WHERE slug = ${input.playerId}),
      (SELECT id FROM data_sources WHERE slug = ${input.provider}),
      ${input.externalId ?? null},
      ${input.sourceUrl ?? null},
      ${input.mirroredUrl ?? null},
      ${input.status},
      ${input.matchedBy ?? null},
      ${input.matchScore ?? null},
      ${input.etag ?? null},
      ${input.lastModified ?? null},
      ${input.lastCheckedAt ?? null},
      ${input.lastSyncedAt ?? null},
      ${input.failureCount ?? 0},
      ${input.lastError ?? null},
      NOW()
    )
    ON CONFLICT (player_id, data_source_id)
    DO UPDATE SET
      external_id = EXCLUDED.external_id,
      source_url = EXCLUDED.source_url,
      mirrored_url = EXCLUDED.mirrored_url,
      status = EXCLUDED.status,
      matched_by = EXCLUDED.matched_by,
      match_score = EXCLUDED.match_score,
      etag = EXCLUDED.etag,
      last_modified = EXCLUDED.last_modified,
      last_checked_at = EXCLUDED.last_checked_at,
      last_synced_at = EXCLUDED.last_synced_at,
      failure_count = EXCLUDED.failure_count,
      last_error = EXCLUDED.last_error,
      updated_at = NOW()
  `;
}

export async function updatePlayerPhotoUrlDb(playerId: string, photoUrl?: string): Promise<void> {
  const sql = getDb();

  await sql`
    UPDATE players
    SET photo_url = ${photoUrl ?? null}, updated_at = NOW()
    WHERE slug = ${playerId}
  `;

  await Promise.allSettled([
    deleteCacheKey(buildCacheKey({ namespace: 'players', locale: 'en' })),
    deleteCacheKey(buildCacheKey({ namespace: 'players', locale: 'ko' })),
  ]);
}

export async function getNationsDb(locale: string = 'en', rankingCategory: NationRankingCategory = 'men'): Promise<Nation[]> {
  const tournament = await loadWorldCup2026Source();
  const cacheNamespace = rankingCategory === 'women' ? 'nations-women' : 'nations';

  return withFallback(async () => {
    const sql = getDb();
    const rankingColumn = rankingCategory === 'women' ? sql`c.fifa_ranking_women` : sql`c.fifa_ranking`;
    const key = buildCacheKey({ namespace: cacheNamespace, locale });

    return readThroughCache({
      key,
      tier: 'master',
      loader: async () => {
        const rows = await sql<NationRow[]>`
          WITH localized_countries AS (
            SELECT
              c.id,
              c.code_alpha3,
              COALESCE(
                (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = ${locale}),
                (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'en'),
                c.code_alpha3
              ) AS localized_name,
              COALESCE(
                (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'en'),
                c.code_alpha3
              ) AS canonical_name,
              COALESCE(c.confederation, '') AS confederation,
              ${rankingColumn} AS fifa_ranking,
              (
                SELECT rh.fifa_ranking
                FROM ranking_history rh
                WHERE rh.country_id = c.id
                  AND rh.ranking_category = ${rankingCategory}
                ORDER BY rh.ranking_date DESC
                LIMIT 1
              ) AS previous_fifa_ranking,
              c.flag_url,
              c.crest_url,
              ROW_NUMBER() OVER (
                PARTITION BY LOWER(COALESCE(
                  (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'en'),
                  c.code_alpha3
                ))
                ORDER BY
                  CASE WHEN c.confederation IS NOT NULL AND c.confederation <> '' THEN 0 ELSE 1 END,
                  CASE WHEN ${rankingColumn} IS NOT NULL AND ${rankingColumn} > 0 THEN 0 ELSE 1 END,
                  CASE WHEN ${rankingColumn} IS NOT NULL AND ${rankingColumn} > 0 THEN ${rankingColumn} ELSE 32767 END,
                  CASE WHEN EXISTS(SELECT 1 FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'ko') THEN 0 ELSE 1 END,
                  c.updated_at DESC,
                  c.id DESC
              ) AS duplicate_rank
            FROM countries c
          )
          SELECT
            code_alpha3::TEXT AS id,
            localized_name AS name,
            code_alpha3::TEXT AS code,
            confederation,
            fifa_ranking,
            previous_fifa_ranking,
            flag_url,
            crest_url
          FROM localized_countries
          WHERE duplicate_rank = 1
            AND (
              confederation <> ''
              OR (fifa_ranking IS NOT NULL AND fifa_ranking > 0)
              OR flag_url IS NOT NULL
              OR crest_url IS NOT NULL
            )
          ORDER BY
            CASE WHEN fifa_ranking IS NULL OR fifa_ranking <= 0 THEN 1 ELSE 0 END,
            fifa_ranking ASC NULLS LAST,
            name ASC
        `;

        return mergeNationsWithWorldCup(
          applyKoreanNationNameFallback(rows, locale).map((row) => mapNation(row, rankingCategory)),
          tournament,
        );
      },
    });
  }, () => mergeNationsWithWorldCup([], tournament));
}

export async function getPaginatedNationsDb(
  locale: string = 'en',
  rankingCategory: NationRankingCategory = 'men',
  options: PaginationOptions = {}
): Promise<PaginatedResult<NationListItem>> {
  const cacheNamespace = rankingCategory === 'women' ? 'nations-women-paginated' : 'nations-paginated';
  const { currentPage, pageSize, offset } = normalizePagination(options);

  return withFallback(async () => {
    const sql = getDb();
    const rankingColumn = rankingCategory === 'women' ? sql`c.fifa_ranking_women` : sql`c.fifa_ranking`;
    const key = buildCacheKey({ namespace: cacheNamespace, locale, params: { page: currentPage, pageSize } });

    return readThroughCache({
      key,
      tier: 'master',
      loader: async () => {
        const nationBase = sql`
          WITH latest_player_contracts AS (
            SELECT DISTINCT ON (pc.player_id)
              pc.player_id
            FROM player_contracts pc
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            ORDER BY
              pc.player_id,
              COALESCE(pc.left_date, s.end_date, s.start_date) DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              pc.joined_date DESC NULLS LAST,
              pc.competition_season_id DESC
          ),
          player_counts AS (
            SELECT p.country_id, COUNT(*)::INT AS player_count
            FROM latest_player_contracts lpc
            JOIN players p ON p.id = lpc.player_id
            WHERE p.country_id IS NOT NULL
            GROUP BY p.country_id
          ),
          localized_countries AS (
            SELECT
              c.id,
              c.code_alpha3,
              COALESCE(
                (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = ${locale}),
                (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'en'),
                c.code_alpha3
              ) AS localized_name,
              COALESCE(c.confederation, '') AS confederation,
              ${rankingColumn} AS fifa_ranking,
              (
                SELECT rh.fifa_ranking
                FROM ranking_history rh
                WHERE rh.country_id = c.id
                  AND rh.ranking_category = ${rankingCategory}
                ORDER BY rh.ranking_date DESC
                LIMIT 1
              ) AS previous_fifa_ranking,
              c.flag_url,
              c.crest_url,
              COALESCE(pc.player_count, 0)::INT AS player_count,
              ROW_NUMBER() OVER (
                PARTITION BY LOWER(COALESCE(
                  (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'en'),
                  c.code_alpha3
                ))
                ORDER BY
                  CASE WHEN c.confederation IS NOT NULL AND c.confederation <> '' THEN 0 ELSE 1 END,
                  CASE WHEN ${rankingColumn} IS NOT NULL AND ${rankingColumn} > 0 THEN 0 ELSE 1 END,
                  CASE WHEN ${rankingColumn} IS NOT NULL AND ${rankingColumn} > 0 THEN ${rankingColumn} ELSE 32767 END,
                  CASE WHEN EXISTS(SELECT 1 FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'ko') THEN 0 ELSE 1 END,
                  c.updated_at DESC,
                  c.id DESC
              ) AS duplicate_rank
            FROM countries c
            LEFT JOIN player_counts pc ON pc.country_id = c.id
          )
        `;

        const [countRows, rows] = await Promise.all([
          sql<{ total_count: number }[]>`
            ${nationBase}
            SELECT COUNT(*)::INT AS total_count
            FROM localized_countries
            WHERE duplicate_rank = 1
              AND fifa_ranking > 0
              AND fifa_ranking <= 100
          `,
          sql<NationListRow[]>`
            ${nationBase}
            SELECT
              code_alpha3::TEXT AS id,
              localized_name AS name,
              code_alpha3::TEXT AS code,
              confederation,
              fifa_ranking,
              previous_fifa_ranking,
              flag_url,
              crest_url,
              player_count
            FROM localized_countries
            WHERE duplicate_rank = 1
              AND fifa_ranking > 0
              AND fifa_ranking <= 100
            ORDER BY fifa_ranking ASC, name ASC
            LIMIT ${pageSize}
            OFFSET ${offset}
          `,
        ]);

        const localizedRows = applyKoreanNationNameFallback(rows, locale) as NationListRow[];

        return createPaginatedResult(
          localizedRows.map((row) => mapNationListItem(row, rankingCategory)),
          countRows[0]?.total_count ?? 0,
          currentPage,
          pageSize,
        );
      },
    });
  }, () => createPaginatedResult([], 0, currentPage, pageSize));
}

export async function getNationByIdDb(id: string, locale: string = 'en', rankingCategory: NationRankingCategory = 'men'): Promise<Nation | undefined> {
  const normalizedId = id.toLowerCase();

  return withFallback(async () => {
    const sql = getDb();
    const rankingColumn = rankingCategory === 'women' ? sql`c.fifa_ranking_women` : sql`c.fifa_ranking`;
    const key = buildCacheKey({ namespace: `nation-by-id-${rankingCategory}`, locale, id: normalizedId });

    return readThroughCache({
      key,
      tier: 'master',
      loader: async () => {
        const rows = await sql<NationRow[]>`
          SELECT
            c.code_alpha3::TEXT AS id,
            COALESCE(
              (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = ${locale}),
              (SELECT ct.name FROM country_translations ct WHERE ct.country_id = c.id AND ct.locale = 'en'),
              c.code_alpha3
            ) AS name,
            c.code_alpha3::TEXT AS code,
            COALESCE(c.confederation, '') AS confederation,
            ${rankingColumn} AS fifa_ranking,
            (
              SELECT rh.fifa_ranking
              FROM ranking_history rh
              WHERE rh.country_id = c.id
                AND rh.ranking_category = ${rankingCategory}
              ORDER BY rh.ranking_date DESC
              LIMIT 1
            ) AS previous_fifa_ranking,
            c.flag_url,
            c.crest_url
          FROM countries c
          WHERE LOWER(c.code_alpha3) = ${normalizedId}
          LIMIT 1
        `;

        if (rows[0]) {
          const localizedRow = applyKoreanNationNameFallback(rows, locale)[0] ?? rows[0];
          return mapNation(localizedRow, rankingCategory);
        }

        const tournament = await loadWorldCup2026Source();
        return mergeNationsWithWorldCup([], tournament).find((nation) => nation.id === normalizedId);
      },
    });
  }, async () => {
    const nations = await getNationsDb(locale, rankingCategory);
    return nations.find((nation) => nation.id === normalizedId);
  });
}

export async function getMatchesDb(locale: string = 'en'): Promise<Match[]> {
  const tournament = await loadWorldCup2026Source();

  const matches = await withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'matches', locale });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, c.slug) AS competition_name,
            'club'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM matches m
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST
        `;

        return mergeMatches(rows.map(mapMatch), tournament.matches);
      },
    });
  }, () => mergeMatches([], tournament.matches));

  return localizeNationMatchNames(matches, locale);
}

export async function getMatchByIdDb(id: string, locale: string = 'en'): Promise<Match | undefined> {
  const tournament = await loadWorldCup2026Source();

  return withFallback(async () => {
    const sql = getDb();
    const rows = await sql<MatchRow[]>`
      SELECT
        m.id::TEXT AS id,
        home.slug AS home_team_id,
        away.slug AS away_team_id,
        COALESCE(home_name.name, home.slug) AS home_team_name,
        COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
        COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
        COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
        home.crest_url AS home_team_logo,
        away.crest_url AS away_team_logo,
        m.home_score,
        m.away_score,
        m.match_date::TEXT AS date,
        TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
        COALESCE(vt.name, vt_en.name, v.slug, '') AS venue,
        c.slug AS league_id,
        m.matchday AS match_week,
        m.stage,
        m.group_name,
        COALESCE(ct.name, ct_en.name, c.slug) AS competition_name,
        'club'::TEXT AS team_type,
        m.status::TEXT AS status
      FROM matches m
      JOIN teams home ON home.id = m.home_team_id
      JOIN teams away ON away.id = m.away_team_id
      LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
      LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
      LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
      LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      LEFT JOIN competition_translations ct ON ct.competition_id = c.id AND ct.locale = ${locale}
      LEFT JOIN competition_translations ct_en ON ct_en.competition_id = c.id AND ct_en.locale = 'en'
      LEFT JOIN venues v ON v.id = m.venue_id
      LEFT JOIN venue_translations vt ON vt.venue_id = v.id AND vt.locale = ${locale}
      LEFT JOIN venue_translations vt_en ON vt_en.venue_id = v.id AND vt_en.locale = 'en'
      WHERE m.id::TEXT = ${id}
      LIMIT 1
    `;

    const row = rows[0];
    if (!row) {
      const fallbackMatch = tournament.matches.find((match) => match.id === id);
      if (!fallbackMatch) {
        return undefined;
      }

      return (await localizeNationMatchNames([fallbackMatch], locale))[0];
    }

    const [eventRows, statsRows] = await Promise.all([
      sql<MatchEventRowDb[]>`
        SELECT
          me.source_event_id::TEXT AS source_event_id,
          me.minute,
          me.event_type,
          player.slug AS player_id,
          COALESCE(player_name.known_as, player.slug) AS player_name,
          secondary_player.slug AS secondary_player_id,
          COALESCE(secondary_player_name.known_as, secondary_player.slug) AS secondary_player_name,
          assist_player.slug AS assist_player_id,
          COALESCE(assist_player_name.known_as, assist_player.slug) AS assist_player_name,
          team.slug AS team_id,
          me.detail,
          me.source_details
        FROM match_events me
        JOIN teams team ON team.id = me.team_id
        LEFT JOIN players player ON player.id = me.player_id
        LEFT JOIN player_translations player_name ON player_name.player_id = player.id AND player_name.locale = ${locale}
        LEFT JOIN players secondary_player ON secondary_player.id = me.secondary_player_id
        LEFT JOIN player_translations secondary_player_name ON secondary_player_name.player_id = secondary_player.id AND secondary_player_name.locale = ${locale}
        LEFT JOIN match_events assist_event ON assist_event.source_event_id::TEXT = me.source_details->'shot'->>'key_pass_id'
        LEFT JOIN players assist_player ON assist_player.id = assist_event.player_id
        LEFT JOIN player_translations assist_player_name ON assist_player_name.player_id = assist_player.id AND assist_player_name.locale = ${locale}
        WHERE me.match_id = ${Number(id)}
          AND me.is_notable = TRUE
        ORDER BY me.minute ASC, me.event_index ASC
      `,
      sql<MatchStatsRowDb[]>`
        SELECT
          team.slug AS team_id,
          ms.possession,
          ms.total_shots,
          ms.shots_on_target,
          ms.corner_kicks,
          ms.fouls
        FROM match_stats ms
        JOIN teams team ON team.id = ms.team_id
        WHERE ms.match_id = ${Number(id)}
      `,
    ]);

    const match = mapMatch(row);
    const homeStats = statsRows.find((item) => item.team_id === match.homeTeamId);
    const awayStats = statsRows.find((item) => item.team_id === match.awayTeamId);

    const localizedMatch = (await localizeNationMatchNames([{
      ...match,
      events: eventRows
        .filter((event) => event.player_id)
        .map(mapTimelineMatchEvent),
      stats: homeStats && awayStats ? {
        possession: [homeStats.possession ?? 0, awayStats.possession ?? 0],
        shots: [homeStats.total_shots, awayStats.total_shots],
        shotsOnTarget: [homeStats.shots_on_target, awayStats.shots_on_target],
        corners: [homeStats.corner_kicks ?? 0, awayStats.corner_kicks ?? 0],
        fouls: [homeStats.fouls ?? 0, awayStats.fouls ?? 0],
      } : undefined,
    }], locale))[0];

    return localizedMatch;
  }, async () => {
    const matches = await getMatchesDb(locale);
    return matches.find((match) => match.id === id);
  });
}

export async function getMatchTimelineDb(id: string, locale: string = 'en'): Promise<MatchEvent[]> {
  return withFallback(async () => {
    const sql = getDb();
    const rows = await sql<MatchEventRowDb[]>`
      SELECT
        me.source_event_id::TEXT AS source_event_id,
        me.minute,
        me.event_type,
        player.slug AS player_id,
        COALESCE(player_name.known_as, player.slug) AS player_name,
        secondary_player.slug AS secondary_player_id,
        COALESCE(secondary_player_name.known_as, secondary_player.slug) AS secondary_player_name,
        assist_player.slug AS assist_player_id,
        COALESCE(assist_player_name.known_as, assist_player.slug) AS assist_player_name,
        team.slug AS team_id,
        me.detail,
        me.source_details
      FROM match_events me
      JOIN teams team ON team.id = me.team_id
      LEFT JOIN players player ON player.id = me.player_id
      LEFT JOIN player_translations player_name ON player_name.player_id = player.id AND player_name.locale = ${locale}
      LEFT JOIN players secondary_player ON secondary_player.id = me.secondary_player_id
      LEFT JOIN player_translations secondary_player_name ON secondary_player_name.player_id = secondary_player.id AND secondary_player_name.locale = ${locale}
      LEFT JOIN match_events assist_event ON assist_event.source_event_id::TEXT = me.source_details->'shot'->>'key_pass_id'
      LEFT JOIN players assist_player ON assist_player.id = assist_event.player_id
      LEFT JOIN player_translations assist_player_name ON assist_player_name.player_id = assist_player.id AND assist_player_name.locale = ${locale}
      WHERE me.match_id = ${Number(id)}
        AND me.is_notable = TRUE
      ORDER BY me.minute ASC, me.event_index ASC
    `;

    return rows.filter((event) => event.player_id).map(mapTimelineMatchEvent);
  }, () => []);
}

export async function getMatchStatsDb(id: string): Promise<MatchStats | undefined> {
  return withFallback(async () => {
    const sql = getDb();
    const rows = await sql<MatchStatsRowDb[]>`
      SELECT
        team.slug AS team_id,
        ms.possession,
        ms.total_shots,
        ms.shots_on_target,
        ms.corner_kicks,
        ms.fouls
      FROM match_stats ms
      JOIN teams team ON team.id = ms.team_id
      WHERE ms.match_id = ${Number(id)}
    `;

    if (rows.length < 2) {
      return undefined;
    }

    return {
      possession: [rows[0].possession ?? 0, rows[1].possession ?? 0],
      shots: [rows[0].total_shots, rows[1].total_shots],
      shotsOnTarget: [rows[0].shots_on_target, rows[1].shots_on_target],
      corners: [rows[0].corner_kicks ?? 0, rows[1].corner_kicks ?? 0],
      fouls: [rows[0].fouls ?? 0, rows[1].fouls ?? 0],
    };
  }, () => undefined);
}

export async function getMatchLineupsDb(id: string, locale: string = 'en'): Promise<MatchLineup[]> {
  if (!/^\d+$/.test(id)) {
    return [];
  }

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'match-lineups', locale, id });

    return readThroughCache({
      key,
      tier: 'master',
      loader: async () => {
        const rows = await sql<MatchLineupRowDb[]>`
          SELECT
            team.slug AS team_id,
            player.slug AS player_id,
            COALESCE(
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = player.id AND pt.locale = ${locale}),
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = player.id AND pt.locale = 'en'),
              player.slug
            ) AS player_name,
            ml.shirt_number,
            ml.position,
            ml.is_starter
          FROM match_lineups ml
          JOIN teams team ON team.id = ml.team_id
          JOIN players player ON player.id = ml.player_id
          WHERE ml.match_id = ${Number(id)}
          ORDER BY team.slug, ml.is_starter DESC, ml.shirt_number ASC NULLS LAST, player_name ASC
        `;

        return rows.map(mapMatchLineup);
      },
    });
  }, () => [], 'getMatchLineupsDb');
}

export async function getMatchAnalysisDataDb(id: string): Promise<MatchAnalysisData> {
  return withFallback(async () => {
    const sql = getDb();
    const rows = await sql<MatchAnalysisEventRowDb[]>`
      SELECT
        me.source_event_id::TEXT AS source_event_id,
        me.minute,
        me.second,
        me.event_type::TEXT AS event_type,
        player.slug AS player_id,
        COALESCE(player_name.known_as, player.slug) AS player_name,
        secondary_player.slug AS secondary_player_id,
        COALESCE(secondary_player_name.known_as, secondary_player.slug) AS secondary_player_name,
        team.slug AS team_id,
        me.location_x::FLOAT8 AS location_x,
        me.location_y::FLOAT8 AS location_y,
        me.end_location_x::FLOAT8 AS end_location_x,
        me.end_location_y::FLOAT8 AS end_location_y,
        me.end_location_z::FLOAT8 AS end_location_z,
        me.under_pressure,
        me.statsbomb_xg::FLOAT8 AS statsbomb_xg,
        me.detail,
        me.source_details
      FROM match_events me
      JOIN teams team ON team.id = me.team_id
      LEFT JOIN players player ON player.id = me.player_id
      LEFT JOIN player_translations player_name ON player_name.player_id = player.id AND player_name.locale = 'en'
      LEFT JOIN players secondary_player ON secondary_player.id = me.secondary_player_id
      LEFT JOIN player_translations secondary_player_name ON secondary_player_name.player_id = secondary_player.id AND secondary_player_name.locale = 'en'
      WHERE me.match_id = ${Number(id)}
        AND me.location_x IS NOT NULL
      ORDER BY me.event_index ASC
    `;

    return {
      events: rows.map(mapMatchAnalysisEvent),
    };
  }, () => ({ events: [] }));
}

export async function getMatchesByLeagueDb(leagueId: string, locale: string = 'en'): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'matches-by-league', locale, id: leagueId });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          WITH latest_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug = ${leagueId}
            ORDER BY s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
            LIMIT 1
          )
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, c.slug) AS competition_name,
            'club'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM latest_competition_season lcs
          JOIN matches m ON m.competition_season_id = lcs.id
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST
        `;

        return rows.map(mapMatch);
      },
    });
  }, () => []);
}

export async function getMatchesByClubDb(clubId: string, locale: string = 'en'): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'matches-by-club', locale, id: clubId });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
            'club'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM matches m
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          WHERE home.slug = ${clubId}
            OR away.slug = ${clubId}
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
        `;

        return rows.map(mapMatch);
      },
    });
  }, () => []);
}

export async function getRecentFinishedMatchesByClubDb(
  clubId: string,
  locale: string = 'en',
  limit: number = 10,
): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'recent-finished-matches-by-club', locale, id: clubId, params: { limit } });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
            'club'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM matches m
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          WHERE (home.slug = ${clubId} OR away.slug = ${clubId})
            AND m.status IN ('finished', 'finished_aet', 'finished_pen')
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
          LIMIT ${limit}
        `;

        return rows.map(mapMatch);
      },
    });
  }, () => []);
}

export async function getUpcomingScheduledMatchesByLeagueIdsDb(
  leagueIds: string[],
  locale: string = 'en',
  limit: number = 4,
): Promise<Match[]> {
  const normalizedIds = Array.from(new Set(leagueIds.filter(Boolean)));

  if (normalizedIds.length === 0) {
    return [];
  }

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'upcoming-scheduled-matches-by-league-ids', locale, params: { ids: normalizedIds.join(','), limit } });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
            'club'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM matches m
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          WHERE c.slug = ANY(${normalizedIds})
            AND m.status = 'scheduled'
          ORDER BY m.match_date ASC, m.kickoff_at ASC NULLS LAST, m.id ASC
          LIMIT ${limit}
        `;

        return rows.map(mapMatch);
      },
    });
  }, () => []);
}

export async function getRecentFinishedMatchesByLeagueIdsDb(
  leagueIds: string[],
  locale: string = 'en',
  limit: number = 6,
): Promise<Match[]> {
  const normalizedIds = Array.from(new Set(leagueIds.filter(Boolean)));

  if (normalizedIds.length === 0) {
    return [];
  }

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'recent-finished-matches-by-league-ids', locale, params: { ids: normalizedIds.join(','), limit } });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
            'club'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM matches m
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          WHERE c.slug = ANY(${normalizedIds})
            AND m.status IN ('finished', 'finished_aet', 'finished_pen')
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
          LIMIT ${limit}
        `;

        return rows.map(mapMatch);
      },
    });
  }, () => []);
}

export async function getMatchesByClubAndSeasonDb(
  clubId: string,
  seasonId: string,
  locale: string = 'en',
): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'matches-by-club-season', locale, id: clubId, params: { season: seasonId } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
            'club'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM matches m
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN seasons s ON s.id = cs.season_id
          JOIN competitions c ON c.id = cs.competition_id
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          WHERE s.slug = ${seasonId}
            AND (home.slug = ${clubId} OR away.slug = ${clubId})
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
        `;

        return rows.map(mapMatch);
      },
    });
  }, () => []);
}

export async function getRecentFinishedMatchesByClubAndSeasonDb(
  clubId: string,
  seasonId: string,
  locale: string = 'en',
  limit: number = 10,
): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'recent-finished-matches-by-club-season', locale, id: clubId, params: { season: seasonId, limit } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          WITH target_competition_seasons AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN seasons s ON s.id = cs.season_id
            WHERE s.slug = ${seasonId}
          )
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
            'club'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM matches m
          JOIN target_competition_seasons tcs ON tcs.id = m.competition_season_id
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          WHERE (home.slug = ${clubId} OR away.slug = ${clubId})
            AND m.status IN ('finished', 'finished_aet', 'finished_pen')
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
          LIMIT ${limit}
        `;

        return rows.map(mapMatch);
      },
    });
  }, () => []);
}

export async function getUpcomingScheduledMatchesByClubAndSeasonDb(
  clubId: string,
  seasonId: string,
  locale: string = 'en',
  limit: number = 10,
): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'upcoming-scheduled-matches-by-club-season', locale, id: clubId, params: { season: seasonId, limit } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          WITH target_competition_seasons AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN seasons s ON s.id = cs.season_id
            WHERE s.slug = ${seasonId}
          )
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
            'club'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM matches m
          JOIN target_competition_seasons tcs ON tcs.id = m.competition_season_id
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          WHERE (home.slug = ${clubId} OR away.slug = ${clubId})
            AND m.status = 'scheduled'
          ORDER BY m.match_date ASC, m.kickoff_at ASC NULLS LAST, m.id ASC
          LIMIT ${limit}
        `;

        return rows.map(mapMatch);
      },
    });
  }, () => []);
}

export async function getMatchesByNationDb(nationId: string, locale: string = 'en'): Promise<Match[]> {
  const normalizedId = nationId.toLowerCase();
  const tournament = await loadWorldCup2026Source();

  const matches = await withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'matches-by-nation', locale, id: normalizedId });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
            'nation'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM matches m
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          WHERE home.is_national = TRUE
            AND away.is_national = TRUE
            AND (LOWER(home.slug) = ${normalizedId} OR LOWER(away.slug) = ${normalizedId})
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
        `;

        return mergeMatches(
          rows.map(mapMatch),
          tournament.matches.filter(
            (match) => match.teamType === 'nation' && (match.homeTeamId === normalizedId || match.awayTeamId === normalizedId)
          ),
        );
      },
    });
  }, () => tournament.matches.filter(
    (match) => match.teamType === 'nation' && (match.homeTeamId === normalizedId || match.awayTeamId === normalizedId)
  ));

  return localizeNationMatchNames(matches, locale);
}

export async function getFinishedMatchesDb(locale: string = 'en'): Promise<Match[]> {
  const matches = await getMatchesDb(locale);
  return matches.filter((match) => match.status === 'finished');
}

export async function getPaginatedFinishedMatchesDb(
  locale: string = 'en',
  leagueId?: string,
  query: string = '',
  gender: 'male' | 'female' = 'male',
  options: PaginationOptions = {}
): Promise<PaginatedResult<Match>> {
  const { currentPage, pageSize, offset } = normalizePagination(options);
  const trimmedQuery = query.trim();
  const searchPattern = `%${trimmedQuery}%`;

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({
      namespace: 'finished-matches-paginated',
      locale,
      params: { page: currentPage, pageSize, leagueId: leagueId ?? 'all', q: trimmedQuery, gender },
    });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      loader: async () => {
        const leagueFilter = leagueId ? sql`AND c.slug = ${leagueId}` : sql``;
        const queryFilter = trimmedQuery ? sql`
          AND (
            COALESCE(home_name.name, home_name_en.name, home.slug) ILIKE ${searchPattern}
            OR COALESCE(away_name.name, away_name_en.name, away.slug) ILIKE ${searchPattern}
            OR COALESCE(comp_name.name, comp_name_en.name, c.slug) ILIKE ${searchPattern}
            OR COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) ILIKE ${searchPattern}
          )
        ` : sql``;

        const [countRows, rows] = await Promise.all([
          sql<{ total_count: number }[]>`
            SELECT COUNT(*)::INT AS total_count
            FROM matches m
            JOIN teams home ON home.id = m.home_team_id
            JOIN teams away ON away.id = m.away_team_id
            LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
            LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
            LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
            LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
            JOIN competition_seasons cs ON cs.id = m.competition_season_id
            JOIN competitions c ON c.id = cs.competition_id
            LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
            LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
            LEFT JOIN venues v ON v.id = m.venue_id
            WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')
              AND c.gender = ${gender}
              ${leagueFilter}
              ${queryFilter}
          `,
          sql<MatchRow[]>`
            SELECT
              m.id::TEXT AS id,
              home.slug AS home_team_id,
              away.slug AS away_team_id,
              COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
              COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
              COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
              COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
              home.crest_url AS home_team_logo,
              away.crest_url AS away_team_logo,
              m.home_score,
              m.away_score,
              m.match_date::TEXT AS date,
              TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
              COALESCE(
                (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
                (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
                v.slug,
                ''
              ) AS venue,
              c.slug AS league_id,
              m.matchday AS match_week,
              m.stage,
              m.group_name,
              COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
              'club'::TEXT AS team_type,
              m.status::TEXT AS status
            FROM matches m
            JOIN teams home ON home.id = m.home_team_id
            JOIN teams away ON away.id = m.away_team_id
            LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
            LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
            LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
            LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
            JOIN competition_seasons cs ON cs.id = m.competition_season_id
            JOIN competitions c ON c.id = cs.competition_id
            LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
            LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
            LEFT JOIN venues v ON v.id = m.venue_id
            WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')
              AND c.gender = ${gender}
              ${leagueFilter}
              ${queryFilter}
            ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
            LIMIT ${pageSize}
            OFFSET ${offset}
          `,
        ]);

        return createPaginatedResult(rows.map(mapMatch), countRows[0]?.total_count ?? 0, currentPage, pageSize);
      },
    });
  }, () => createPaginatedResult([], 0, currentPage, pageSize));
}

export async function getScheduledMatchesDb(locale: string = 'en'): Promise<Match[]> {
  const matches = await getMatchesDb(locale);
  return matches.filter((match) => match.status === 'scheduled');
}

export async function getFinishedMatchesByNationDb(nationId: string, locale: string = 'en'): Promise<Match[]> {
  const matches = await getMatchesByNationDb(nationId, locale);
  return matches.filter((match) => match.status === 'finished');
}

export async function getScheduledMatchesByNationDb(nationId: string, locale: string = 'en'): Promise<Match[]> {
  const matches = await getMatchesByNationDb(nationId, locale);
  return matches.filter((match) => match.status === 'scheduled');
}

export async function getWorldCup2026Db(): Promise<WorldCupTournament> {
  return loadWorldCup2026Source();
}

export async function getFinishedMatchesByLeagueDb(leagueId: string, locale: string = 'en'): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const rows = await sql<MatchRow[]>`
      SELECT
        m.id::TEXT AS id,
        home.slug AS home_team_id,
        away.slug AS away_team_id,
        COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
        COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
        COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
        COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
        home.crest_url AS home_team_logo,
        away.crest_url AS away_team_logo,
        m.home_score,
        m.away_score,
        m.match_date::TEXT AS date,
        TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
        COALESCE(
          (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
          (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
          v.slug,
          ''
        ) AS venue,
        c.slug AS league_id,
        m.matchday AS match_week,
        m.stage,
        m.group_name,
        COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
        'club'::TEXT AS team_type,
        m.status::TEXT AS status
      FROM matches m
      JOIN teams home ON home.id = m.home_team_id
      JOIN teams away ON away.id = m.away_team_id
      LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
      LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
      LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
      LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
      LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
      LEFT JOIN venues v ON v.id = m.venue_id
      WHERE c.slug = ${leagueId}
        AND m.status IN ('finished', 'finished_aet', 'finished_pen')
      ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
    `;

    return rows.map(mapMatch);
  }, () => []);
}

export async function getScheduledMatchesByLeagueDb(leagueId: string, locale: string = 'en'): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const rows = await sql<MatchRow[]>`
      SELECT
        m.id::TEXT AS id,
        home.slug AS home_team_id,
        away.slug AS away_team_id,
        COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
        COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
        COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
        COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
        home.crest_url AS home_team_logo,
        away.crest_url AS away_team_logo,
        m.home_score,
        m.away_score,
        m.match_date::TEXT AS date,
        TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
        COALESCE(
          (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
          (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
          v.slug,
          ''
        ) AS venue,
        c.slug AS league_id,
        m.matchday AS match_week,
        m.stage,
        m.group_name,
        COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
        'club'::TEXT AS team_type,
        m.status::TEXT AS status
      FROM matches m
      JOIN teams home ON home.id = m.home_team_id
      JOIN teams away ON away.id = m.away_team_id
      LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
      LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
      LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
      LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
      LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
      LEFT JOIN venues v ON v.id = m.venue_id
      WHERE c.slug = ${leagueId}
        AND m.status = 'scheduled'
      ORDER BY m.match_date ASC, m.kickoff_at ASC NULLS LAST, m.id ASC
    `;

    return rows.map(mapMatch);
  }, () => []);
}

export async function getStandingsByLeagueDb(leagueId: string, locale: string = 'en'): Promise<StandingRow[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'standings', locale, id: leagueId });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<StandingRowDb[]>`
          WITH latest_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug = ${leagueId}
            ORDER BY s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
            LIMIT 1
          )
          SELECT
            standings.position,
            team.slug AS club_id,
            COALESCE(
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_name,
            COALESCE(
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_short_name,
            team.crest_url AS club_logo,
            standings.played,
            standings.won,
            standings.drawn,
            standings.lost,
            standings.goals_for,
            standings.goals_against,
            standings.goal_difference,
            standings.points,
            form.last_five_results AS form
          FROM mv_standings standings
          JOIN latest_competition_season lcs ON lcs.id = standings.competition_season_id
          JOIN teams team ON team.id = standings.team_id
          LEFT JOIN mv_team_form form
            ON form.competition_season_id = standings.competition_season_id
            AND form.team_id = standings.team_id
          ORDER BY standings.position ASC
        `;

        return rows.map(mapStanding);
      },
    });
  }, () => []);
}

export async function getStandingsByLeagueIdsDb(
  leagueIds: string[],
  locale: string = 'en',
): Promise<Record<string, StandingRow[]>> {
  const normalizedIds = Array.from(new Set(leagueIds.filter(Boolean)));

  if (normalizedIds.length === 0) {
    return {};
  }

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'standings-by-league-ids', locale, params: { ids: normalizedIds.join(',') } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<LeagueStandingRowDb[]>`
          WITH target_competitions AS (
            SELECT c.id, c.slug
            FROM competitions c
            WHERE c.slug = ANY(${normalizedIds})
          ), latest_competition_seasons AS (
            SELECT DISTINCT ON (cs.competition_id)
              cs.id,
              cs.competition_id,
              tc.slug AS league_id
            FROM competition_seasons cs
            JOIN target_competitions tc ON tc.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            ORDER BY cs.competition_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
          )
          SELECT
            lcs.league_id,
            standings.position,
            team.slug AS club_id,
            COALESCE(
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_name,
            COALESCE(
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_short_name,
            team.crest_url AS club_logo,
            standings.played,
            standings.won,
            standings.drawn,
            standings.lost,
            standings.goals_for,
            standings.goals_against,
            standings.goal_difference,
            standings.points,
            form.last_five_results AS form
          FROM latest_competition_seasons lcs
          JOIN mv_standings standings ON standings.competition_season_id = lcs.id
          JOIN teams team ON team.id = standings.team_id
          LEFT JOIN mv_team_form form
            ON form.competition_season_id = standings.competition_season_id
            AND form.team_id = standings.team_id
          ORDER BY lcs.league_id ASC, standings.position ASC
        `;

        return rows.reduce<Record<string, StandingRow[]>>((acc, row) => {
          acc[row.league_id] ??= [];
          acc[row.league_id].push(mapStanding(row));
          return acc;
        }, {});
      },
    });
  }, () => ({}));
}

export async function getStandingsByLeagueAndSeasonDb(
  leagueId: string,
  seasonId: string,
  locale: string = 'en',
): Promise<StandingRow[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'standings-by-season', locale, id: leagueId, params: { season: seasonId } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<StandingRowDb[]>`
          WITH target_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug = ${leagueId}
              AND s.slug = ${seasonId}
          ), match_results AS (
            SELECT
              m.competition_season_id,
              m.home_team_id AS team_id,
              CASE
                WHEN m.home_score > m.away_score THEN 3
                WHEN m.home_score = m.away_score THEN 1
                ELSE 0
              END AS points,
              CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END AS won,
              CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END AS drawn,
              CASE WHEN m.home_score < m.away_score THEN 1 ELSE 0 END AS lost,
              m.home_score AS goals_for,
              m.away_score AS goals_against,
              m.match_date,
              m.id AS match_id,
              CASE
                WHEN m.home_score > m.away_score THEN 'W'
                WHEN m.home_score = m.away_score THEN 'D'
                ELSE 'L'
              END::TEXT AS form_result
            FROM matches m
            JOIN target_competition_season tcs ON tcs.id = m.competition_season_id
            WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')

            UNION ALL

            SELECT
              m.competition_season_id,
              m.away_team_id AS team_id,
              CASE
                WHEN m.away_score > m.home_score THEN 3
                WHEN m.away_score = m.home_score THEN 1
                ELSE 0
              END AS points,
              CASE WHEN m.away_score > m.home_score THEN 1 ELSE 0 END AS won,
              CASE WHEN m.away_score = m.home_score THEN 1 ELSE 0 END AS drawn,
              CASE WHEN m.away_score < m.home_score THEN 1 ELSE 0 END AS lost,
              m.away_score AS goals_for,
              m.home_score AS goals_against,
              m.match_date,
              m.id AS match_id,
              CASE
                WHEN m.away_score > m.home_score THEN 'W'
                WHEN m.away_score = m.home_score THEN 'D'
                ELSE 'L'
              END::TEXT AS form_result
            FROM matches m
            JOIN target_competition_season tcs ON tcs.id = m.competition_season_id
            WHERE m.status IN ('finished', 'finished_aet', 'finished_pen')
          ), standings AS (
            SELECT
              mr.competition_season_id,
              mr.team_id,
              COUNT(*)::INT AS played,
              SUM(mr.won)::INT AS won,
              SUM(mr.drawn)::INT AS drawn,
              SUM(mr.lost)::INT AS lost,
              SUM(mr.goals_for)::INT AS goals_for,
              SUM(mr.goals_against)::INT AS goals_against,
              (SUM(mr.goals_for) - SUM(mr.goals_against))::INT AS goal_difference,
              SUM(mr.points)::INT AS points,
              RANK() OVER (
                PARTITION BY mr.competition_season_id
                ORDER BY
                  SUM(mr.points) DESC,
                  SUM(mr.goals_for) - SUM(mr.goals_against) DESC,
                  SUM(mr.goals_for) DESC
              )::INT AS position
            FROM match_results mr
            GROUP BY mr.competition_season_id, mr.team_id
          ), ranked_results AS (
            SELECT
              mr.competition_season_id,
              mr.team_id,
              mr.form_result,
              mr.match_date,
              mr.match_id,
              ROW_NUMBER() OVER (
                PARTITION BY mr.competition_season_id, mr.team_id
                ORDER BY mr.match_date DESC, mr.match_id DESC
              ) AS recent_rank
            FROM match_results mr
          ), team_form AS (
            SELECT
              rr.competition_season_id,
              rr.team_id,
              ARRAY_AGG(rr.form_result::TEXT ORDER BY rr.match_date DESC, rr.match_id DESC) AS form
            FROM ranked_results rr
            WHERE rr.recent_rank <= 5
            GROUP BY rr.competition_season_id, rr.team_id
          )
          SELECT
            standings.position,
            team.slug AS club_id,
            COALESCE(
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_name,
            COALESCE(
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_short_name,
            team.crest_url AS club_logo,
            standings.played,
            standings.won,
            standings.drawn,
            standings.lost,
            standings.goals_for,
            standings.goals_against,
            standings.goal_difference,
            standings.points,
            team_form.form::TEXT[]
          FROM standings
          JOIN teams team ON team.id = standings.team_id
          LEFT JOIN team_form
            ON team_form.competition_season_id = standings.competition_season_id
            AND team_form.team_id = standings.team_id
          ORDER BY standings.position ASC
        `;

        return rows.map(mapStanding);
      },
    });
  }, () => []);
}

export async function getTopScorersDb(leagueId: string, limit: number = 10): Promise<StatLeader[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'top-scorers', id: leagueId, params: { limit } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<TopScorerRow[]>`
          WITH latest_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug = ${leagueId}
            ORDER BY s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
            LIMIT 1
          )
          SELECT
            player.slug AS player_id,
            team.slug AS club_id,
            scorers.goals,
            scorers.assists
          FROM mv_top_scorers scorers
          JOIN latest_competition_season lcs ON lcs.id = scorers.competition_season_id
          JOIN players player ON player.id = scorers.player_id
          JOIN teams team ON team.id = scorers.team_id
          ORDER BY scorers.rank ASC
          LIMIT ${limit}
        `;

        return rows.map((row) => ({
          playerId: row.player_id,
          clubId: row.club_id,
          leagueId,
          goals: row.goals,
          assists: row.assists,
        }));
      },
    });
  }, () => []);
}

export async function getTopScorerRowsDb(
  leagueId: string,
  locale: string = 'en',
  limit: number = 10,
): Promise<Array<StatLeader & { playerName: string; clubShortName: string }>> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'top-scorer-rows', locale, id: leagueId, params: { limit } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<TopScorerDisplayRowDb[]>`
          WITH latest_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug = ${leagueId}
            ORDER BY s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
            LIMIT 1
          )
          SELECT
            player.slug AS player_id,
            team.slug AS club_id,
            scorers.goals,
            scorers.assists,
            COALESCE(
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = player.id AND pt.locale = ${locale}),
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = player.id AND pt.locale = 'en'),
              player.slug
            ) AS player_name,
            COALESCE(
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_short_name
          FROM mv_top_scorers scorers
          JOIN latest_competition_season lcs ON lcs.id = scorers.competition_season_id
          JOIN players player ON player.id = scorers.player_id
          JOIN teams team ON team.id = scorers.team_id
          ORDER BY scorers.rank ASC
          LIMIT ${limit}
        `;

        return rows.map((row) => ({
          playerId: row.player_id,
          clubId: row.club_id,
          leagueId,
          goals: row.goals,
          assists: row.assists,
          playerName: row.player_name,
          clubShortName: row.club_short_name,
        }));
      },
    });
  }, () => []);
}

export async function getTopScorerRowsBySeasonDb(
  leagueId: string,
  seasonId: string,
  locale: string = 'en',
  limit: number = 10,
): Promise<Array<StatLeader & { playerName: string; clubShortName: string }>> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'top-scorer-rows-by-season', locale, id: leagueId, params: { season: seasonId, limit } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<TopScorerDisplayRowDb[]>`
          WITH target_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug = ${leagueId}
              AND s.slug = ${seasonId}
          )
          SELECT
            player.slug AS player_id,
            team.slug AS club_id,
            scorers.goals,
            scorers.assists,
            COALESCE(
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = player.id AND pt.locale = ${locale}),
              (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = player.id AND pt.locale = 'en'),
              player.slug
            ) AS player_name,
            COALESCE(
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_short_name
          FROM mv_top_scorers scorers
          JOIN target_competition_season tcs ON tcs.id = scorers.competition_season_id
          JOIN players player ON player.id = scorers.player_id
          JOIN teams team ON team.id = scorers.team_id
          ORDER BY scorers.rank ASC
          LIMIT ${limit}
        `;

        return rows.map((row) => ({
          playerId: row.player_id,
          clubId: row.club_id,
          leagueId,
          goals: row.goals,
          assists: row.assists,
          playerName: row.player_name,
          clubShortName: row.club_short_name,
        }));
      },
    });
  }, () => []);
}

export async function getSeasonsByLeagueDb(leagueId: string): Promise<LeagueSeasonEntry[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'seasons-by-league', id: leagueId });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<LeagueSeasonRowDb[]>`
          SELECT
            s.slug AS season_id,
            CASE
              WHEN EXTRACT(YEAR FROM s.start_date) = EXTRACT(YEAR FROM s.end_date)
                THEN EXTRACT(YEAR FROM s.start_date)::INT::TEXT
              ELSE CONCAT(
                EXTRACT(YEAR FROM s.start_date)::INT::TEXT,
                '/',
                LPAD((EXTRACT(YEAR FROM s.end_date)::INT % 100)::TEXT, 2, '0')
              )
            END AS season_label,
            s.is_current
          FROM competition_seasons cs
          JOIN competitions c ON c.id = cs.competition_id
          JOIN seasons s ON s.id = cs.season_id
          WHERE c.slug = ${leagueId}
          ORDER BY s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, s.id DESC
        `;

        return rows.map((row) => ({
          seasonId: row.season_id,
          seasonLabel: row.season_label,
          isCurrent: row.is_current,
        }));
      },
    });
  }, () => []);
}

export async function getMatchesByLeagueAndSeasonDb(
  leagueId: string,
  seasonId: string,
  locale: string = 'en',
): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'matches-by-league-season', locale, id: leagueId, params: { season: seasonId } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          WITH target_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug = ${leagueId}
              AND s.slug = ${seasonId}
          )
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
            COALESCE(home_name.short_name, home_name_en.short_name, home.slug) AS home_team_code,
            COALESCE(away_name.short_name, away_name_en.short_name, away.slug) AS away_team_code,
            home.crest_url AS home_team_logo,
            away.crest_url AS away_team_logo,
            m.home_score,
            m.away_score,
            m.match_date::TEXT AS date,
            TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS venue,
            c.slug AS league_id,
            m.matchday AS match_week,
            m.stage,
            m.group_name,
            COALESCE(comp_name.name, comp_name_en.name, c.slug) AS competition_name,
            'club'::TEXT AS team_type,
            m.status::TEXT AS status
          FROM target_competition_season tcs
          JOIN matches m ON m.competition_season_id = tcs.id
          JOIN teams home ON home.id = m.home_team_id
          JOIN teams away ON away.id = m.away_team_id
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale} AND ${locale} <> 'ko'
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST
        `;

        return rows.map(mapMatch);
      },
    });
  }, () => []);
}

export async function getClubsByLeagueAndSeasonDb(
  leagueId: string,
  seasonId: string,
  locale: string = 'en',
): Promise<Club[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'clubs-by-league-season', locale, id: leagueId, params: { season: seasonId } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<ClubRow[]>`
          WITH target_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug = ${leagueId}
              AND s.slug = ${seasonId}
          )
          SELECT
            t.slug AS id,
            COALESCE(
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
              t.slug
            ) AS name,
            COALESCE(
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
              t.slug
            ) AS short_name,
            COALESCE(
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = ${locale}),
              (SELECT ctr.name FROM country_translations ctr WHERE ctr.country_id = country.id AND ctr.locale = 'en'),
              country.code_alpha3
            ) AS country,
            t.gender,
            t.founded_year AS founded,
            COALESCE(
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = ${locale}),
              (SELECT vt.name FROM venue_translations vt WHERE vt.venue_id = v.id AND vt.locale = 'en'),
              v.slug,
              ''
            ) AS stadium,
            v.capacity AS stadium_capacity,
            c.slug AS league_id,
            t.crest_url
          FROM target_competition_season tcs
          JOIN team_seasons ts ON ts.competition_season_id = tcs.id
          JOIN teams t ON t.id = ts.team_id
          JOIN countries country ON country.id = t.country_id
          LEFT JOIN venues v ON v.id = t.venue_id
          JOIN competition_seasons cs ON cs.id = ts.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          WHERE t.is_national = FALSE
          ORDER BY name ASC
        `;

        return rows.map(mapClub);
      },
    });
  }, () => []);
}

export async function getTopScorersBySeasonDb(
  leagueId: string,
  seasonId: string,
  limit: number = 10,
): Promise<StatLeader[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'top-scorers-by-season', id: leagueId, params: { season: seasonId, limit } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<TopScorerRow[]>`
          WITH target_competition_season AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN competitions c ON c.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE c.slug = ${leagueId}
              AND s.slug = ${seasonId}
          )
          SELECT
            player.slug AS player_id,
            team.slug AS club_id,
            scorers.goals,
            scorers.assists
          FROM mv_top_scorers scorers
          JOIN target_competition_season tcs ON tcs.id = scorers.competition_season_id
          JOIN players player ON player.id = scorers.player_id
          JOIN teams team ON team.id = scorers.team_id
          ORDER BY scorers.rank ASC
          LIMIT ${limit}
        `;

        return rows.map((row) => ({
          playerId: row.player_id,
          clubId: row.club_id,
          leagueId,
          goals: row.goals,
          assists: row.assists,
        }));
      },
    });
  }, () => []);
}

export async function searchAllDb(
  query: string,
  locale: string = 'en',
  gender?: 'male' | 'female'
): Promise<SearchResult[]> {
  const normalizedQuery = query.trim();

  if (!normalizedQuery) {
    return [];
  }

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'search', locale, params: { q: normalizedQuery, gender: gender ?? 'all' } });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      loader: async () => {
        const rows = await sql<SearchRow[]>`
          WITH matched AS (
            SELECT *
            FROM search_entities(${normalizedQuery}, ${locale}, CAST(NULL AS entity_type), 20)
          )
          SELECT
            CASE matched.entity_type
              WHEN 'competition' THEN 'league'
              WHEN 'team' THEN 'club'
              WHEN 'country' THEN 'nation'
              ELSE matched.entity_type::TEXT
            END AS result_type,
            CASE matched.entity_type
              WHEN 'competition' THEN (SELECT c.slug FROM competitions c WHERE c.id = matched.entity_id)
              WHEN 'team' THEN (SELECT t.slug FROM teams t WHERE t.id = matched.entity_id)
              WHEN 'player' THEN (SELECT p.slug FROM players p WHERE p.id = matched.entity_id)
              WHEN 'country' THEN (SELECT c.code_alpha3::TEXT FROM countries c WHERE c.id = matched.entity_id)
              ELSE matched.entity_id::TEXT
            END AS result_id,
            CASE matched.entity_type
              WHEN 'competition' THEN COALESCE(
                (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = matched.entity_id AND ct.locale = ${locale}),
                (SELECT ct.name FROM competition_translations ct WHERE ct.competition_id = matched.entity_id AND ct.locale = 'en'),
                matched.matched_alias
              )
              WHEN 'team' THEN COALESCE(
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = matched.entity_id AND tt.locale = ${locale} AND ${locale} <> 'ko'),
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = matched.entity_id AND tt.locale = 'en'),
                matched.matched_alias
              )
              WHEN 'player' THEN COALESCE(
                (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = matched.entity_id AND pt.locale = ${locale}),
                (SELECT pt.known_as FROM player_translations pt WHERE pt.player_id = matched.entity_id AND pt.locale = 'en'),
                matched.matched_alias
              )
              WHEN 'country' THEN COALESCE(
                (SELECT ct.name FROM country_translations ct WHERE ct.country_id = matched.entity_id AND ct.locale = ${locale}),
                (SELECT ct.name FROM country_translations ct WHERE ct.country_id = matched.entity_id AND ct.locale = 'en'),
                matched.matched_alias
              )
              ELSE matched.matched_alias
            END AS result_name,
            matched.match_type AS subtitle,
            CASE matched.entity_type
              WHEN 'competition' THEN (SELECT c.gender::TEXT FROM competitions c WHERE c.id = matched.entity_id)
              WHEN 'team' THEN (SELECT t.gender::TEXT FROM teams t WHERE t.id = matched.entity_id)
              WHEN 'player' THEN (
                SELECT team.gender::TEXT
                FROM player_contracts pc
                JOIN competition_seasons cs ON cs.id = pc.competition_season_id
                JOIN seasons s ON s.id = cs.season_id
                JOIN teams team ON team.id = pc.team_id
                WHERE pc.player_id = matched.entity_id
                ORDER BY
                  COALESCE(pc.left_date, s.end_date, s.start_date) DESC NULLS LAST,
                  s.start_date DESC NULLS LAST,
                  pc.joined_date DESC NULLS LAST,
                  pc.competition_season_id DESC
                LIMIT 1
              )
              ELSE NULL
            END AS gender
          FROM matched
        `;

        return rows
          .map((row) => ({
            type: row.result_type,
            id: row.result_id,
            name: row.result_name,
            subtitle: row.subtitle,
            gender: row.gender ?? undefined,
          }))
          .filter((row) => {
            if (!gender) {
              return true;
            }

            if (row.type === 'nation') {
              return true;
            }

            return row.gender === gender;
          });
      },
    });
  }, () => []);
}
