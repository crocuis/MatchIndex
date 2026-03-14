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
  PlayerClubHistoryEntry,
  PlayerListItem,
  PlayerMarketValueEntry,
  PlayerNationalTeamSummary,
  PlayerPhotoSource,
  PlayerPhotoSourceStatus,
  PlayerPhotoSyncTarget,
  PlayerSeasonHistoryEntry,
  PlayerTransferEntry,
  PhotoSyncProvider,
  SearchResult,
  StandingRow,
  StatLeader,
  WorldCupTournament,
} from '@/data/types';
import { clubLogoMap, leagueLogoMap } from '@/data/entityImages.generated.ts';
import { getNationFlagUrl } from '@/data/nationVisuals';
import { resolveTournamentSlots } from '@/data/tournamentSlots';

let worldCup2026SourcePromise: Promise<WorldCupTournament> | null = null;

interface LeagueRow {
  id: string;
  name: string;
  country: string;
  season: string;
  gender: League['gender'];
  comp_type: 'league' | 'cup' | 'league_cup' | 'super_cup' | 'international';
  emblem_url: string | null;
  number_of_clubs: number;
}

interface ClubRow {
  id: string;
  name: string;
  korean_name: string | null;
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
  league_comp_type: LeagueRow['comp_type'];
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
  gender?: 'male' | 'female' | 'mixed' | null;
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
  latest_season_end_date: string | null;
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

interface PlayerSeasonHistoryRowDb {
  season_id: string;
  season_label: string;
  club_id: string;
  club_name: string;
  appearances: number | null;
  goals: number | null;
  assists: number | null;
  minutes_played: number | null;
  yellow_cards: number | null;
  red_cards: number | null;
  clean_sheets: number | null;
}

interface PlayerClubHistoryRowDb {
  club_id: string | null;
  club_name: string;
  start_year: number;
  end_year: number;
}

interface PlayerMarketValueRowDb {
  season_label: string | null;
  observed_at: string;
  age: number | null;
  club_id: string | null;
  club_name: string | null;
  market_value_eur: number;
  currency_code: string | null;
  source_url: string | null;
}

interface PlayerTransferRowDb {
  external_transfer_id: string;
  season_label: string | null;
  moved_at: string | null;
  age: number | null;
  from_team_id: string | null;
  from_team_name: string | null;
  to_team_id: string | null;
  to_team_name: string | null;
  market_value_eur: number | null;
  fee_eur: number | null;
  fee_display: string | null;
  currency_code: string | null;
  transfer_type: string | null;
  transfer_type_label: string | null;
  contract_until_date: string | null;
  source_url: string | null;
}

interface PlayerNationalTeamSummaryRowDb {
  caps: number;
  goals: number;
}

interface PlayerFallbackSeasonHistoryRowDb extends PlayerSeasonHistoryRowDb {
  start_year: number | null;
  end_year: number | null;
}

interface PlayerFallbackStatsRowDb {
  player_id: string;
  season_id: string;
  club_id: string;
  start_date: string | null;
  end_date: string | null;
  appearances: number | null;
  goals: number | null;
  assists: number | null;
  minutes_played: number | null;
  yellow_cards: number | null;
  red_cards: number | null;
  clean_sheets: number | null;
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
  home_team_korean_name?: string | null;
  away_team_korean_name?: string | null;
  home_team_code: string;
  away_team_code: string;
  home_team_logo: string | null;
  away_team_logo: string | null;
  home_score: number | null;
  away_score: number | null;
  date: string;
  time: string | null;
  venue: string;
  attendance: number | null;
  referee: string | null;
  home_formation: string | null;
  away_formation: string | null;
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
  club_korean_name?: string | null;
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

export interface DashboardTournamentSummary {
  recentResults: Match[];
  upcomingFixtures: Match[];
  stageTrail: string[];
}

export interface LeagueFilterOption {
  id: string;
  name: string;
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

interface TournamentStageTrailRowDb {
  stage: string;
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
  image_url: string | null;
  short_name: string | null;
  nation_code: string | null;
  player_position: Player['position'] | null;
}

const CANONICAL_SEARCH_CLUB_ID_MAP: Record<string, string> = {
  'borussia-mo-nchengladbach': 'borussia-mo-nchengladbach-germany',
  'borussia-monchengladbach': 'borussia-mo-nchengladbach-germany',
  'borussia-monchengladbach-germany': 'borussia-mo-nchengladbach-germany',
};

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
  expected_goals: number | null;
  total_passes: number | null;
  accurate_passes: number | null;
  pass_accuracy: number | null;
  total_shots: number;
  shots_on_target: number;
  corner_kicks: number | null;
  fouls: number | null;
  offsides: number | null;
  gk_saves: number | null;
}

interface MatchLineupRowDb {
  team_id: string;
  player_id: string;
  player_name: string;
  grid_position: string | null;
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
      signal: AbortSignal.timeout(5000),
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
  if (locale === 'en' || matches.every((match) => match.teamType !== 'nation')) {
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
    competitionType: row.comp_type === 'league' ? 'league' : 'tournament',
  };
}

function mapClub(row: ClubRow): Club {
  return {
    id: row.id,
    name: row.name,
    koreanName: row.korean_name ?? row.name,
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
    leagueName: row.league_comp_type === 'league'
      ? row.league_name
      : undefined,
  };
}

function getLocalizedClubName(_id: string, name: string, _locale: string): string {
  void _id;
  void _locale;
  return name;
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
  const leftIsLeague = left.league_comp_type === 'league';
  const rightIsLeague = right.league_comp_type === 'league';

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
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
        t.slug
      ) AS name,
      (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'ko') AS korean_name,
      COALESCE(
        (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
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
      c.comp_type::TEXT AS league_comp_type,
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
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
        t.slug
      ) AS name,
      (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'ko') AS korean_name,
      COALESCE(
        (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
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
      c.comp_type::TEXT AS league_comp_type,
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
  const latestSeasonEndDate = row.latest_season_end_date ? new Date(row.latest_season_end_date) : null;
  const retiredThreshold = new Date();
  retiredThreshold.setUTCFullYear(retiredThreshold.getUTCFullYear() - 3);
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
    gender: row.gender ?? undefined,
    clubId: row.club_id,
    position: row.position ?? 'MID',
    photoUrl: row.photo_url ?? undefined,
    shirtNumber: row.shirt_number ?? 0,
    height: row.height ?? 0,
    preferredFoot: row.preferred_foot ?? 'Right',
    isRetired: latestSeasonEndDate ? latestSeasonEndDate < retiredThreshold : undefined,
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

function mapPlayerSeasonHistoryEntry(row: PlayerSeasonHistoryRowDb, locale: string = 'en'): PlayerSeasonHistoryEntry {
  return {
    seasonId: row.season_id,
    seasonLabel: row.season_label,
    clubId: row.club_id,
    clubName: getLocalizedClubName(row.club_id, row.club_name, locale),
    appearances: row.appearances ?? 0,
    goals: row.goals ?? 0,
    assists: row.assists ?? 0,
    minutesPlayed: row.minutes_played ?? 0,
    yellowCards: row.yellow_cards ?? 0,
    redCards: row.red_cards ?? 0,
    cleanSheets: row.clean_sheets ?? undefined,
  };
}

function mapPlayerClubHistoryEntry(row: PlayerClubHistoryRowDb, locale: string = 'en'): PlayerClubHistoryEntry {
  return {
    clubId: row.club_id ?? undefined,
    clubName: row.club_id ? getLocalizedClubName(row.club_id, row.club_name, locale) : row.club_name,
    startYear: row.start_year,
    endYear: row.end_year,
    periodLabel: row.start_year === row.end_year ? `${row.start_year}` : `${row.start_year}~${row.end_year}`,
    isFreeAgent: !row.club_id && row.club_name.toLowerCase().includes('without club'),
  };
}

function buildPlayerClubHistoryFromTransfers(
  rows: PlayerTransferRowDb[],
  locale: string,
  options: {
    fallbackEndYear?: number;
    isRetired?: boolean;
  }
) {
  if (rows.length === 0) {
    return [] as PlayerClubHistoryEntry[];
  }

  const chronologicalRows = [...rows]
    .filter((row) => row.moved_at)
    .sort((left, right) => (left.moved_at ?? '').localeCompare(right.moved_at ?? ''));

  if (chronologicalRows.length === 0) {
    return [] as PlayerClubHistoryEntry[];
  }

  const currentYear = new Date().getUTCFullYear();
  const terminalYear = options.isRetired ? (options.fallbackEndYear ?? currentYear) : currentYear;
  const periods: Array<{ clubId?: string; clubName: string; startYear: number; endYear: number }> = [];

  const getPeriodKey = (clubId: string | undefined, clubName: string | undefined) => {
    if (clubId) {
      return `id:${clubId}`;
    }

    const normalizedName = clubName
      ?.normalize('NFKD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-zA-Z0-9]+/g, ' ')
      .trim()
      .toLowerCase();

    return normalizedName ? `name:${normalizedName}` : undefined;
  };

  const pushOrExtendPeriod = (clubId: string | undefined, clubName: string | undefined, startYear: number, endYear: number) => {
    if (!clubName) {
      return;
    }

    const periodKey = getPeriodKey(clubId, clubName);
    if (!periodKey) {
      return;
    }

    const lastPeriod = periods[periods.length - 1];
    if (lastPeriod && getPeriodKey(lastPeriod.clubId, lastPeriod.clubName) === periodKey) {
      lastPeriod.startYear = Math.min(lastPeriod.startYear, startYear);
      lastPeriod.endYear = Math.max(lastPeriod.endYear, endYear);
      return;
    }

    periods.push({ clubId, clubName, startYear, endYear });
  };

  let activeClubId = chronologicalRows[0].from_team_id ?? chronologicalRows[0].to_team_id ?? undefined;
  let activeClubName = chronologicalRows[0].from_team_name ?? chronologicalRows[0].to_team_name ?? undefined;
  let activeStartYear = chronologicalRows[0].moved_at ? Number.parseInt(chronologicalRows[0].moved_at.slice(0, 4), 10) : terminalYear;

  for (const row of chronologicalRows) {
    const moveYear = row.moved_at ? Number.parseInt(row.moved_at.slice(0, 4), 10) : activeStartYear;
    if (!Number.isFinite(moveYear)) {
      continue;
    }

    const fromClubId = row.from_team_id ?? undefined;
    const fromClubName = row.from_team_name ?? undefined;
    const toClubId = row.to_team_id ?? undefined;
    const toClubName = row.to_team_name ?? undefined;

    if (!activeClubId && !activeClubName && (fromClubId || fromClubName)) {
      activeClubId = fromClubId;
      activeClubName = fromClubName;
      activeStartYear = moveYear;
    }

    if (activeClubId || activeClubName) {
      pushOrExtendPeriod(activeClubId, activeClubName, activeStartYear, moveYear);
    }

    activeClubId = toClubId;
    activeClubName = toClubName;
    activeStartYear = moveYear;
  }

  if (activeClubId || activeClubName) {
    pushOrExtendPeriod(activeClubId, activeClubName, activeStartYear, Math.max(activeStartYear, terminalYear));
  }

  return periods
    .sort((left, right) => {
      if (right.endYear !== left.endYear) {
        return right.endYear - left.endYear;
      }
      return right.startYear - left.startYear;
    })
    .map((row) => mapPlayerClubHistoryEntry({
      club_id: row.clubId ?? null,
      club_name: row.clubName,
      end_year: row.endYear,
      start_year: row.startYear,
    }, locale));
}

function mapPlayerMarketValueEntry(row: PlayerMarketValueRowDb, locale: string = 'en'): PlayerMarketValueEntry {
  return {
    age: row.age ?? undefined,
    clubId: row.club_id ?? undefined,
    clubName: row.club_id && row.club_name ? getLocalizedClubName(row.club_id, row.club_name, locale) : (row.club_name ?? undefined),
    currencyCode: row.currency_code ?? 'EUR',
    marketValue: row.market_value_eur,
    observedAt: row.observed_at,
    seasonLabel: row.season_label ?? undefined,
    sourceUrl: row.source_url ?? undefined,
  };
}

function mapPlayerTransferEntry(row: PlayerTransferRowDb, locale: string = 'en'): PlayerTransferEntry {
  return {
    age: row.age ?? undefined,
    contractUntilDate: row.contract_until_date ?? undefined,
    currencyCode: row.currency_code ?? undefined,
    fee: row.fee_eur ?? undefined,
    feeDisplay: row.fee_display ?? undefined,
    fromClubId: row.from_team_id ?? undefined,
    fromClubName: row.from_team_id && row.from_team_name ? getLocalizedClubName(row.from_team_id, row.from_team_name, locale) : (row.from_team_name ?? undefined),
    id: row.external_transfer_id,
    marketValue: row.market_value_eur ?? undefined,
    movedAt: row.moved_at ?? undefined,
    seasonLabel: row.season_label ?? undefined,
    sourceUrl: row.source_url ?? undefined,
    toClubId: row.to_team_id ?? undefined,
    toClubName: row.to_team_id && row.to_team_name ? getLocalizedClubName(row.to_team_id, row.to_team_name, locale) : (row.to_team_name ?? undefined),
    transferType: row.transfer_type ?? undefined,
    transferTypeLabel: row.transfer_type_label ?? undefined,
  };
}

function mapPlayerNationalTeamSummary(
  row: PlayerNationalTeamSummaryRowDb | undefined,
  recentMatches: Match[]
): PlayerNationalTeamSummary {
  return {
    caps: row?.caps ?? 0,
    goals: row?.goals ?? 0,
    recentMatches,
  };
}

function mapPlayerListItem(row: PlayerListRow, locale: string = 'en'): PlayerListItem {
  return {
    ...mapPlayer(row),
    clubName: getLocalizedClubName(row.club_id, row.club_name, locale),
    clubShortName: getLocalizedClubName(row.club_id, row.club_short_name, locale),
    clubLogo: clubLogoMap[row.club_id] ?? row.club_logo ?? undefined,
    nationName: row.nation_name,
    nationCode: row.nation_code,
    nationFlag: row.nation_flag ?? getNationFlagUrl(row.nation_code),
  };
}

function hasEmptySeasonStats(stats: Player['seasonStats']) {
  return stats.appearances === 0
    && stats.goals === 0
    && stats.assists === 0
    && stats.minutesPlayed === 0;
}

async function getFallbackPlayerSeasonStatsRowsDb(
  playerIds: string[],
  filters: { seasonId?: string; clubId?: string } = {}
): Promise<PlayerFallbackStatsRowDb[]> {
  if (playerIds.length === 0) {
    return [];
  }

  const sql = getDb();
  const { seasonId, clubId } = filters;
  const seasonFilter = seasonId ? sql`AND s.slug = ${seasonId}` : sql``;
  const clubFilter = clubId ? sql`AND team.slug = ${clubId}` : sql``;

  return sql<PlayerFallbackStatsRowDb[]>`
    WITH target_players AS (
      SELECT p.id, p.slug
      FROM players p
      JOIN (
        SELECT UNNEST(STRING_TO_ARRAY(${playerIds.join(',')}, ',')) AS slug
      ) target_slugs ON target_slugs.slug = p.slug
    ), appearance_summary AS (
      SELECT
        tp.slug AS player_id,
        s.slug AS season_id,
        team.slug AS club_id,
        s.start_date::TEXT AS start_date,
        s.end_date::TEXT AS end_date,
        COUNT(DISTINCT CASE
          WHEN ml.is_starter OR sub_in.minute IS NOT NULL THEN m.id
          ELSE NULL
        END)::INT AS appearances,
        SUM(CASE
          WHEN ml.is_starter THEN COALESCE(sub_out.minute, 90)
          WHEN sub_in.minute IS NOT NULL THEN GREATEST(90 - sub_in.minute, 0)
          ELSE 0
        END)::INT AS minutes_played
      FROM target_players tp
      JOIN match_lineups ml ON ml.player_id = tp.id
      JOIN matches m ON m.id = ml.match_id
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN seasons s ON s.id = cs.season_id
      JOIN teams team ON team.id = ml.team_id
      LEFT JOIN LATERAL (
        SELECT MIN(me.minute) AS minute
        FROM match_events me
        WHERE me.match_id = m.id
          AND me.event_type = 'substitution'
          AND me.secondary_player_id = ml.player_id
      ) sub_in ON TRUE
      LEFT JOIN LATERAL (
        SELECT MIN(me.minute) AS minute
        FROM match_events me
        WHERE me.match_id = m.id
          AND me.event_type = 'substitution'
          AND me.player_id = ml.player_id
      ) sub_out ON TRUE
      WHERE m.status <> 'scheduled'
        ${seasonFilter}
        ${clubFilter}
      GROUP BY tp.slug, s.slug, s.start_date, s.end_date, team.slug
      HAVING COUNT(DISTINCT CASE
        WHEN ml.is_starter OR sub_in.minute IS NOT NULL THEN m.id
        ELSE NULL
      END) > 0
    ), primary_event_summary AS (
      SELECT
        tp.slug AS player_id,
        s.slug AS season_id,
        team.slug AS club_id,
        SUM(CASE WHEN me.event_type IN ('goal', 'penalty_scored') THEN 1 ELSE 0 END)::INT AS goals,
        SUM(CASE WHEN me.event_type = 'yellow_card' THEN 1 ELSE 0 END)::INT AS yellow_cards,
        SUM(CASE WHEN me.event_type IN ('red_card', 'yellow_red_card') THEN 1 ELSE 0 END)::INT AS red_cards
      FROM target_players tp
      JOIN match_events me ON me.player_id = tp.id
      JOIN matches m ON m.id = me.match_id
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN seasons s ON s.id = cs.season_id
      JOIN teams team ON team.id = me.team_id
      WHERE m.status <> 'scheduled'
        ${seasonFilter}
        ${clubFilter}
      GROUP BY tp.slug, s.slug, team.slug
    ), assist_event_summary AS (
      SELECT
        tp.slug AS player_id,
        s.slug AS season_id,
        team.slug AS club_id,
        COUNT(*)::INT AS assists
      FROM target_players tp
      JOIN match_events me ON me.secondary_player_id = tp.id
      JOIN matches m ON m.id = me.match_id
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN seasons s ON s.id = cs.season_id
      JOIN teams team ON team.id = me.team_id
      WHERE m.status <> 'scheduled'
        AND me.event_type IN ('goal', 'own_goal', 'penalty_scored')
        ${seasonFilter}
        ${clubFilter}
      GROUP BY tp.slug, s.slug, team.slug
    )
    SELECT
      appearance_summary.player_id,
      appearance_summary.season_id,
      appearance_summary.club_id,
      appearance_summary.start_date,
      appearance_summary.end_date,
      appearance_summary.appearances,
      COALESCE(primary_event_summary.goals, 0)::INT AS goals,
      COALESCE(assist_event_summary.assists, 0)::INT AS assists,
      appearance_summary.minutes_played,
      COALESCE(primary_event_summary.yellow_cards, 0)::INT AS yellow_cards,
      COALESCE(primary_event_summary.red_cards, 0)::INT AS red_cards,
      NULL::INT AS clean_sheets
    FROM appearance_summary
    LEFT JOIN primary_event_summary
      ON primary_event_summary.player_id = appearance_summary.player_id
     AND primary_event_summary.season_id = appearance_summary.season_id
     AND primary_event_summary.club_id = appearance_summary.club_id
    LEFT JOIN assist_event_summary
      ON assist_event_summary.player_id = appearance_summary.player_id
     AND assist_event_summary.season_id = appearance_summary.season_id
     AND assist_event_summary.club_id = appearance_summary.club_id
    ORDER BY appearance_summary.player_id ASC, appearance_summary.end_date DESC NULLS LAST, appearance_summary.start_date DESC NULLS LAST, appearance_summary.club_id ASC
  `;
}

function applyFallbackStatsToPlayers<T extends Player | PlayerListItem>(
  players: T[],
  fallbackRows: PlayerFallbackStatsRowDb[],
  options: { seasonId?: string } = {}
): T[] {
  if (players.length === 0 || fallbackRows.length === 0) {
    return players;
  }

  const fallbackMap = new Map<string, PlayerFallbackStatsRowDb[]>();

  for (const row of fallbackRows) {
    const existing = fallbackMap.get(row.player_id) ?? [];
    existing.push(row);
    fallbackMap.set(row.player_id, existing);
  }

  return players.map((player) => {
    if (!hasEmptySeasonStats(player.seasonStats)) {
      return player;
    }

    const candidates = fallbackMap.get(player.id) ?? [];
    const fallback = candidates.find((candidate) => (
      candidate.club_id === player.clubId
      && (options.seasonId ? candidate.season_id === options.seasonId : true)
    )) ?? candidates[0];

    if (!fallback) {
      return player;
    }

    return {
      ...player,
      clubId: player.clubId || fallback.club_id,
      seasonStats: {
        appearances: fallback.appearances ?? 0,
        goals: fallback.goals ?? 0,
        assists: fallback.assists ?? 0,
        minutesPlayed: fallback.minutes_played ?? 0,
        yellowCards: fallback.yellow_cards ?? 0,
        redCards: fallback.red_cards ?? 0,
        cleanSheets: fallback.clean_sheets ?? undefined,
      },
    };
  });
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

function getLocalizedNationRows(rows: NationRow[]) {
  return rows;
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

function mapMatch(row: MatchRow, locale: string = 'en'): Match {
  const homeTeamName = row.team_type === 'club'
    ? getLocalizedClubName(row.home_team_id, row.home_team_name, locale)
    : row.home_team_name;
  const awayTeamName = row.team_type === 'club'
    ? getLocalizedClubName(row.away_team_id, row.away_team_name, locale)
    : row.away_team_name;

  return {
    id: row.id,
    homeTeamId: row.home_team_id,
    awayTeamId: row.away_team_id,
    homeTeamName,
    awayTeamName,
    homeTeamCode: row.home_team_code,
    awayTeamCode: row.away_team_code,
    homeTeamLogo: clubLogoMap[row.home_team_id] ?? row.home_team_logo ?? undefined,
    awayTeamLogo: clubLogoMap[row.away_team_id] ?? row.away_team_logo ?? undefined,
    homeScore: row.home_score,
    awayScore: row.away_score,
    date: row.date,
    time: row.time ?? '00:00',
    venue: row.venue,
    attendance: row.attendance ?? undefined,
    referee: row.referee ?? undefined,
    homeFormation: row.home_formation ?? undefined,
    awayFormation: row.away_formation ?? undefined,
    leagueId: row.league_id,
    matchWeek: row.match_week ?? undefined,
    stage: row.stage ?? undefined,
    groupName: row.group_name ?? undefined,
    competitionName: row.competition_name,
    teamType: row.team_type,
    status: normalizeMatchStatus(row.status),
  };
}

function mapStanding(row: StandingRowDb, locale: string = 'en'): StandingRow {
  return {
    position: row.position,
    clubId: row.club_id,
    clubName: getLocalizedClubName(row.club_id, row.club_name, locale),
    clubShortName: getLocalizedClubName(row.club_id, row.club_short_name, locale),
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
    gridPosition: row.grid_position ?? undefined,
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
      policySlug: 'master.competitions',
      loader: async () => {
        const rows = await sql<LeagueRow[]>`
          WITH latest_competition_seasons AS (
            SELECT DISTINCT ON (cs.competition_id)
              cs.id,
              cs.competition_id,
              cs.season_id,
              COUNT(DISTINCT ts.team_id)::INT AS participant_count,
              COUNT(DISTINCT m.id)::INT AS match_count
            FROM competition_seasons cs
            JOIN seasons s ON s.id = cs.season_id
            LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
            LEFT JOIN matches m ON m.competition_season_id = cs.id
            GROUP BY cs.id, cs.competition_id, cs.season_id, s.end_date, s.start_date, s.id
            ORDER BY
              cs.competition_id,
              CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
              s.end_date DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              s.id DESC
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
            c.comp_type::TEXT AS comp_type,
            c.emblem_url,
            lcs.participant_count AS number_of_clubs
          FROM competitions c
          LEFT JOIN countries country ON country.id = c.country_id
          JOIN latest_competition_seasons lcs ON lcs.competition_id = c.id
          JOIN seasons s ON s.id = lcs.season_id
          GROUP BY c.id, country.id, s.id, lcs.participant_count
          ORDER BY name ASC
        `;

        return rows.map(mapLeague);
      },
    });
  }, () => []);
}

export async function getLeagueCountDb(): Promise<number> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'league-count' });

    return readThroughCache({
      key,
      tier: 'master',
      policySlug: 'master.competitions',
      loader: async () => {
        const rows = await sql<{ total_count: number }[]>`
          SELECT COUNT(*)::INT AS total_count
          FROM competitions
        `;

        return rows[0]?.total_count ?? 0;
      },
    });
  }, () => 0);
}

export async function getLeaguesByIdsDb(leagueIds: string[], locale: string = 'en'): Promise<League[]> {
  const normalizedIds = Array.from(new Set(leagueIds.filter(Boolean)));

  if (normalizedIds.length === 0) {
    return [];
  }

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'leagues-by-ids', locale, params: { ids: normalizedIds.join(',') } });

    return readThroughCache({
      key,
      tier: 'master',
      policySlug: 'master.competitions',
      loader: async () => {
        const rows = await sql<LeagueRow[]>`
          WITH target_competitions AS (
            SELECT c.id, c.slug
            FROM competitions c
            WHERE c.slug = ANY(${normalizedIds})
          ), latest_competition_seasons AS (
            SELECT DISTINCT ON (cs.competition_id)
              cs.id,
              cs.competition_id,
              cs.season_id,
              COUNT(DISTINCT ts.team_id)::INT AS participant_count,
              COUNT(DISTINCT m.id)::INT AS match_count
            FROM competition_seasons cs
            JOIN target_competitions tc ON tc.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
            LEFT JOIN matches m ON m.competition_season_id = cs.id
            GROUP BY cs.id, cs.competition_id, cs.season_id, s.end_date, s.start_date, s.id
            ORDER BY
              cs.competition_id,
              CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
              s.end_date DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              s.id DESC
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
            lcs.participant_count AS number_of_clubs
          FROM target_competitions tc
          JOIN competitions c ON c.id = tc.id
          LEFT JOIN countries country ON country.id = c.country_id
          JOIN latest_competition_seasons lcs ON lcs.competition_id = c.id
          JOIN seasons s ON s.id = lcs.season_id
          GROUP BY c.id, country.id, s.id, lcs.participant_count
          ORDER BY name ASC
        `;

        return rows.map(mapLeague);
      },
    });
  }, () => []);
}

export async function getLeagueFilterOptionsDb(
  locale: string = 'en',
  gender: 'male' | 'female' = 'male',
): Promise<LeagueFilterOption[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'league-filter-options', locale, params: { gender } });

    return readThroughCache({
      key,
      tier: 'master',
      policySlug: 'master.competitions',
      loader: async () => {
        const rows = await sql<{ id: string; name: string }[]>`
          SELECT
            c.slug AS id,
            COALESCE(
              ct_locale.name,
              ct_en.name,
              c.slug
            ) AS name
          FROM competitions c
          LEFT JOIN competition_translations ct_locale
            ON ct_locale.competition_id = c.id
            AND ct_locale.locale = ${locale}
          LEFT JOIN competition_translations ct_en
            ON ct_en.competition_id = c.id
            AND ct_en.locale = 'en'
          WHERE c.gender = ${gender}
          ORDER BY name ASC
        `;

        return rows.map((row) => ({ id: row.id, name: row.name }));
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
      policySlug: 'master.competitions',
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
                cs.season_id,
                COUNT(DISTINCT ts.team_id)::INT AS participant_count,
                COUNT(DISTINCT m.id)::INT AS match_count
              FROM competition_seasons cs
              JOIN seasons s ON s.id = cs.season_id
              LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
              LEFT JOIN matches m ON m.competition_season_id = cs.id
              GROUP BY cs.id, cs.competition_id, cs.season_id, s.end_date, s.start_date, s.id
              ORDER BY
                cs.competition_id,
                CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
                s.end_date DESC NULLS LAST,
                s.start_date DESC NULLS LAST,
                s.id DESC
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
              c.comp_type::TEXT AS comp_type,
              c.emblem_url,
              lcs.participant_count AS number_of_clubs
            FROM competitions c
            LEFT JOIN countries country ON country.id = c.country_id
            JOIN latest_competition_seasons lcs ON lcs.competition_id = c.id
            JOIN seasons s ON s.id = lcs.season_id
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
            GROUP BY c.id, country.id, s.id, lcs.participant_count
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
      policySlug: 'master.competitions',
      loader: async () => {
        const rows = await sql<LeagueRow[]>`
          WITH latest_competition_season AS (
            SELECT
              cs.id,
              cs.competition_id,
              cs.season_id,
              COUNT(DISTINCT ts.team_id)::INT AS participant_count,
              COUNT(DISTINCT m.id)::INT AS match_count
            FROM competition_seasons cs
            JOIN seasons s ON s.id = cs.season_id
            LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
            LEFT JOIN matches m ON m.competition_season_id = cs.id
            WHERE cs.competition_id = (SELECT c.id FROM competitions c WHERE c.slug = ${id} LIMIT 1)
            GROUP BY cs.id, cs.competition_id, cs.season_id, s.end_date, s.start_date, s.id
            ORDER BY
              CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
              s.end_date DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              s.id DESC
            LIMIT 1
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
              c.comp_type::TEXT AS comp_type,
              c.emblem_url,
              lcs.participant_count AS number_of_clubs
          FROM competitions c
          LEFT JOIN countries country ON country.id = c.country_id
          JOIN latest_competition_season lcs ON lcs.competition_id = c.id
          JOIN seasons s ON s.id = lcs.season_id
          WHERE c.slug = ${id}
          GROUP BY c.id, country.id, s.id, lcs.participant_count
          LIMIT 1
        `;

        return rows[0] ? mapLeague(rows[0]) : undefined;
      },
    });
  }, () => undefined);
}

export async function getClubsDb(locale: string = 'en'): Promise<Club[]> {
  return withFallback(async () => {
    const key = buildCacheKey({ namespace: 'clubs-v4', locale });

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
    const key = buildCacheKey({ namespace: 'clubs-paginated-v4', locale, params: { page: currentPage, pageSize, q: trimmedQuery, gender } });

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
    const key = buildCacheKey({ namespace: 'club-by-id-v5', locale, id });

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
          (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
          (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
          t.slug
        ) AS name,
        (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'ko') AS korean_name,
        COALESCE(
          (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
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

function buildClubLookupKeys(value: string | null | undefined) {
  const normalized = value
    ?.normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .toLowerCase() ?? '';

  if (!normalized) {
    return [] as string[];
  }

  const compact = normalized
    .replace(/\b(fc|cf|afc|cfc|sc|ac|club|football|futbol|clube|club de futebol)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  const squashed = normalized.replace(/\s+/g, '');
  const compactSquashed = compact.replace(/\s+/g, '');

  return [...new Set([normalized, compact, squashed, compactSquashed].filter(Boolean))];
}

function getClubSearchIdentityKey(row: Pick<ClubRepresentativeRow, 'id' | 'name' | 'short_name' | 'country'>) {
  const clubKey = buildClubLookupKeys(row.name)[0]
    ?? buildClubLookupKeys(row.short_name)[0]
    ?? buildClubLookupKeys(row.id)[0]
    ?? row.name.trim().toLowerCase();
  const countryKey = buildClubLookupKeys(row.country)[0] ?? row.country.trim().toLowerCase();

  return [clubKey, countryKey].join('::');
}

function getClubSearchDisplayName(row: Pick<ClubRepresentativeRow, 'name' | 'korean_name'>, locale: string) {
  if (locale === 'ko') {
    return row.korean_name || row.name;
  }

  return row.name;
}

async function deduplicateClubSearchResults(results: SearchResult[], locale: string) {
  const clubResults = results
    .filter((result) => result.type === 'club')
    .map((result) => ({
      ...result,
      id: CANONICAL_SEARCH_CLUB_ID_MAP[result.id] ?? result.id,
    }));

  if (clubResults.length < 2) {
    return results;
  }

  const representativeRows = await Promise.all(
    Array.from(new Set(clubResults.map((result) => result.id))).map(async (id) => {
      const row = await getClubRepresentativeRowBySlugDb(id, 'en');
      return row ? [id, row] as const : null;
    })
  );

  const representativeRowById = new Map(
    representativeRows.filter((entry): entry is readonly [string, ClubRepresentativeRow] => entry !== null)
  );

  const grouped = new Map<string, {
    chosenId: string;
    chosenRepresentativeRow: ClubRepresentativeRow;
    firstResult: SearchResult;
    order: number;
  }>();

  clubResults.forEach((result, index) => {
    const representativeRow = representativeRowById.get(result.id);

    if (!representativeRow) {
      grouped.set(`id::${result.id}`, {
        chosenId: result.id,
        chosenRepresentativeRow: {
          id: result.id,
          name: result.name,
          korean_name: result.name,
          short_name: result.shortName ?? result.name,
          country: '',
          gender: result.gender,
          founded: 0,
          stadium: '',
          stadium_capacity: 0,
          league_id: '',
          league_comp_type: 'league',
          league_name: '',
          crest_url: result.imageUrl ?? null,
          season_start_date: null,
          season_end_date: null,
        },
        firstResult: result,
        order: index,
      });
      return;
    }

    const key = getClubSearchIdentityKey(representativeRow);
    const existing = grouped.get(key);

    if (!existing) {
      grouped.set(key, {
        chosenId: result.id,
        chosenRepresentativeRow: representativeRow,
        firstResult: result,
        order: index,
      });
      return;
    }

    if (compareClubRepresentativeRows(representativeRow, existing.chosenRepresentativeRow) < 0) {
      existing.chosenId = result.id;
      existing.chosenRepresentativeRow = representativeRow;
    }
  });

  if (grouped.size === clubResults.length) {
    return results;
  }

  const localizedRepresentativeRows = await Promise.all(
    Array.from(grouped.values()).map(async (entry) => {
      const row = await getClubRepresentativeRowBySlugDb(entry.chosenId, locale);
      return [entry.chosenId, row] as const;
    })
  );

  const localizedRepresentativeRowById = new Map(
    localizedRepresentativeRows.filter((entry): entry is readonly [string, ClubRepresentativeRow] => Boolean(entry[1]))
  );

  const deduplicatedClubResults = Array.from(grouped.values())
    .sort((left, right) => left.order - right.order)
    .map((entry) => {
      const localizedRow = localizedRepresentativeRowById.get(entry.chosenId);

      if (!localizedRow) {
        return entry.firstResult;
      }

      return {
        ...entry.firstResult,
        id: entry.chosenId,
        name: getClubSearchDisplayName(localizedRow, locale),
        shortName: localizedRow.short_name,
        imageUrl: clubLogoMap[entry.chosenId] ?? localizedRow.crest_url ?? entry.firstResult.imageUrl,
        gender: localizedRow.gender ?? entry.firstResult.gender,
      } satisfies SearchResult;
    });

  const deduplicatedResults: SearchResult[] = [];
  let insertedClubResults = false;

  for (const result of results) {
    if (result.type !== 'club') {
      deduplicatedResults.push(result);
      continue;
    }

    if (!insertedClubResults) {
      deduplicatedResults.push(...deduplicatedClubResults);
      insertedClubResults = true;
    }
  }

  return deduplicatedResults;
}

export async function getClubLinksByNamesDb(names: string[], locale: string = 'en'): Promise<Array<Pick<Club, 'id' | 'name' | 'shortName'>>> {
  const normalizedNames = Array.from(new Set(names.map((name) => name.trim()).filter(Boolean)));
  if (normalizedNames.length === 0) {
    return [];
  }

  return withFallback(async () => {
    const sql = getDb();
    const rows = await sql<Array<Pick<ClubRow, 'id' | 'name' | 'short_name'>>>`
      SELECT
        t.slug AS id,
        COALESCE(
          (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
          (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
          t.slug
        ) AS name,
        COALESCE(
          (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
          (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
          t.slug
        ) AS short_name
      FROM teams t
      WHERE t.is_national = FALSE
    `;

    const requestedKeys = new Set(normalizedNames.flatMap((name) => buildClubLookupKeys(name)));
    return rows
      .filter((row) => [row.name, row.short_name, row.id].some((value) => buildClubLookupKeys(value).some((key) => requestedKeys.has(key))))
      .map((row) => ({ id: row.id, name: row.name, shortName: row.short_name }));
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
    const key = buildCacheKey({ namespace: 'clubs-by-league-v3', locale, id: leagueId });

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
            LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
            LEFT JOIN matches m ON m.competition_season_id = cs.id
            WHERE c.slug = ${leagueId}
            GROUP BY cs.id, s.end_date, s.start_date, s.id
            ORDER BY
              CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
              s.end_date DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              s.id DESC
            LIMIT 1
          ), has_regular_season_matches AS (
            SELECT EXISTS (
              SELECT 1
              FROM matches m
              JOIN latest_competition_season lcs ON lcs.id = m.competition_season_id
              WHERE m.stage = 'REGULAR_SEASON'
            ) AS has_regular_season
          ), season_participants AS (
            SELECT DISTINCT participant.team_id
            FROM has_regular_season_matches hrsm
            JOIN LATERAL (
              SELECT m.home_team_id AS team_id
              FROM matches m
              JOIN latest_competition_season lcs ON lcs.id = m.competition_season_id
              WHERE hrsm.has_regular_season = TRUE
                AND m.stage = 'REGULAR_SEASON'

              UNION

              SELECT m.away_team_id AS team_id
              FROM matches m
              JOIN latest_competition_season lcs ON lcs.id = m.competition_season_id
              WHERE hrsm.has_regular_season = TRUE
                AND m.stage = 'REGULAR_SEASON'

              UNION

              SELECT ts.team_id
              FROM team_seasons ts
              JOIN latest_competition_season lcs ON lcs.id = ts.competition_season_id
              WHERE hrsm.has_regular_season = FALSE
            ) participant ON TRUE
          ), ranked_clubs AS (
            SELECT
              t.slug AS id,
              COALESCE(
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
                t.slug
              ) AS name,
              COALESCE(
                (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
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
              t.crest_url,
              ROW_NUMBER() OVER (
                PARTITION BY LOWER(
                  COALESCE(
                    (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
                    (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
                    t.slug
                  )
                )
                ORDER BY CASE WHEN t.slug LIKE '%-germany' THEN 0 ELSE 1 END, t.slug ASC
              ) AS duplicate_rank
            FROM season_participants sp
            JOIN teams t ON t.id = sp.team_id
            JOIN countries country ON country.id = t.country_id
            LEFT JOIN venues v ON v.id = t.venue_id
            JOIN latest_competition_season lcs ON TRUE
            JOIN competition_seasons cs ON cs.id = lcs.id
            JOIN competitions c ON c.id = cs.competition_id
            WHERE t.is_national = FALSE
          )
          SELECT
            id,
            name,
            short_name,
            country,
            gender,
            founded,
            stadium,
            stadium_capacity,
            league_id,
            crest_url
          FROM ranked_clubs
          WHERE duplicate_rank = 1
          ORDER BY name ASC
        `;

        return rows.map(mapClub);
      },
    });
  }, () => []);
}

export async function getClubNameDb(id: string, locale: string = 'en'): Promise<string> {
  const club = await getClubByIdDb(id, locale);
  return club ? getLocalizedClubName(club.id, club.name, locale) : 'Unknown';
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
            JOIN teams current_team ON current_team.id = pc.team_id
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE current_team.is_national = FALSE
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
            team.gender,
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
            season_meta.season_end_date::TEXT AS latest_season_end_date,
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
  status: 'active' | 'retired' = 'active',
  options: PaginationOptions = {}
): Promise<PaginatedResult<PlayerListItem>> {
  const { currentPage, pageSize, offset } = normalizePagination(options);
  const trimmedQuery = query.trim();
  const searchPattern = `%${trimmedQuery}%`;

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'players-paginated-v3', locale, params: { page: currentPage, pageSize, q: trimmedQuery, gender, status } });
    const statusFilter = status === 'retired'
      ? sql`s.end_date < CURRENT_DATE - INTERVAL '3 years'`
      : sql`s.end_date >= CURRENT_DATE - INTERVAL '3 years'`;

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
            JOIN teams current_team ON current_team.id = pc.team_id
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE current_team.is_national = FALSE
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
            JOIN competition_seasons cs ON cs.id = latest_player_contracts.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            LEFT JOIN team_translations tt_locale ON tt_locale.team_id = team.id AND tt_locale.locale = ${locale}
            LEFT JOIN team_translations tt_en ON tt_en.team_id = team.id AND tt_en.locale = 'en'
            WHERE team.gender = ${gender}
              AND ${statusFilter}
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
              s.end_date::TEXT AS latest_season_end_date,
              pss.appearances,
              pss.goals,
              pss.assists,
              pss.minutes_played,
              pss.yellow_cards,
              pss.red_cards,
              pss.clean_sheets,
              COALESCE(
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
                team.slug
              ) AS club_name,
              COALESCE(
                (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
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
            JOIN seasons s ON s.id = cs.season_id
            JOIN teams team ON team.id = lpc.team_id
            LEFT JOIN player_season_stats pss ON pss.player_id = p.id AND pss.competition_season_id = lpc.competition_season_id
            WHERE team.gender = ${gender}
              AND ${statusFilter}
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
                  (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
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

        const items = rows.map((row) => mapPlayerListItem(row, locale));
        const fallbackRows = await getFallbackPlayerSeasonStatsRowsDb(
          items.filter((player) => hasEmptySeasonStats(player.seasonStats)).map((player) => player.id)
        );

        return createPaginatedResult(
          applyFallbackStatsToPlayers(items, fallbackRows),
          countRows[0]?.total_count ?? 0,
          currentPage,
          pageSize,
        );
      },
    });
  }, () => createPaginatedResult([], 0, currentPage, pageSize), 'getPaginatedPlayersDb');
}

export async function getPlayerByIdDb(id: string, locale: string = 'en'): Promise<Player | undefined> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'player-by-id-v8', locale, id });

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
            JOIN teams current_team ON current_team.id = pc.team_id
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE current_team.is_national = FALSE
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
            team.gender,
            COALESCE(team.slug, '') AS club_id,
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
            season_meta.season_end_date::TEXT AS latest_season_end_date,
            pss.appearances,
            pss.goals,
            pss.assists,
            pss.minutes_played,
            pss.yellow_cards,
            pss.red_cards,
            pss.clean_sheets
          FROM players p
          LEFT JOIN latest_player_contracts lpc ON lpc.player_id = p.id
          LEFT JOIN countries country ON country.id = p.country_id
          LEFT JOIN teams team ON team.id = lpc.team_id
          LEFT JOIN LATERAL (
            SELECT s.end_date AS season_end_date
            FROM competition_seasons cs
            JOIN seasons s ON s.id = cs.season_id
            WHERE cs.id = lpc.competition_season_id
            LIMIT 1
          ) season_meta ON TRUE
          LEFT JOIN player_season_stats pss ON pss.player_id = p.id AND pss.competition_season_id = lpc.competition_season_id
          WHERE p.slug = ${id}
          LIMIT 1
        `;

        if (!rows[0]) {
          return undefined;
        }

        let seasonHistoryRows: PlayerSeasonHistoryRowDb[] = await sql<PlayerSeasonHistoryRowDb[]>`
          SELECT
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
            team.slug AS club_id,
            COALESCE(
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_name,
            SUM(COALESCE(pss.appearances, 0))::INT AS appearances,
            SUM(COALESCE(pss.goals, 0))::INT AS goals,
            SUM(COALESCE(pss.assists, 0))::INT AS assists,
            SUM(COALESCE(pss.minutes_played, 0))::INT AS minutes_played,
            SUM(COALESCE(pss.yellow_cards, 0))::INT AS yellow_cards,
            SUM(COALESCE(pss.red_cards, 0))::INT AS red_cards,
            SUM(COALESCE(pss.clean_sheets, 0))::INT AS clean_sheets
          FROM players p
          JOIN player_contracts pc ON pc.player_id = p.id
          JOIN teams team ON team.id = pc.team_id
          JOIN competition_seasons cs ON cs.id = pc.competition_season_id
          JOIN seasons s ON s.id = cs.season_id
          LEFT JOIN player_season_stats pss ON pss.player_id = p.id AND pss.competition_season_id = pc.competition_season_id
          WHERE p.slug = ${id}
            AND team.is_national = FALSE
          GROUP BY p.id, s.id, s.slug, s.start_date, s.end_date, team.id, team.slug
          ORDER BY s.start_date DESC NULLS LAST, s.end_date DESC NULLS LAST, team.slug ASC
        `;

        let clubHistoryRows: PlayerClubHistoryRowDb[] = await sql<PlayerClubHistoryRowDb[]>`
          SELECT
            team.slug AS club_id,
            COALESCE(
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_name,
            MIN(EXTRACT(YEAR FROM s.start_date))::INT AS start_year,
            MAX(EXTRACT(YEAR FROM s.end_date))::INT AS end_year
          FROM players p
          JOIN player_contracts pc ON pc.player_id = p.id
          JOIN teams team ON team.id = pc.team_id
          JOIN competition_seasons cs ON cs.id = pc.competition_season_id
          JOIN seasons s ON s.id = cs.season_id
          WHERE p.slug = ${id}
            AND team.is_national = FALSE
          GROUP BY p.id, team.id, team.slug
          ORDER BY MIN(s.start_date) ASC NULLS LAST, team.slug ASC
        `;

        const [marketValueRows, transferRows, nationalTeamSummaryRows, nationalTeamRecentMatchRows] = await Promise.all([
          sql<PlayerMarketValueRowDb[]>`
            SELECT
              COALESCE(pmv.season_label, seasons.slug) AS season_label,
              pmv.observed_at::TEXT AS observed_at,
              pmv.age,
              team.slug AS club_id,
              COALESCE(
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
                pmv.club_name,
                team.slug
              ) AS club_name,
              pmv.market_value_eur,
              pmv.currency_code,
              pmv.source_url
            FROM players p
            JOIN player_market_values pmv ON pmv.player_id = p.id
            LEFT JOIN seasons ON seasons.id = pmv.season_id
            LEFT JOIN teams team ON team.id = pmv.club_id
            WHERE p.slug = ${id}
            ORDER BY pmv.observed_at DESC, pmv.id DESC
          `,
          sql<PlayerTransferRowDb[]>`
            SELECT
              pt.external_transfer_id,
              COALESCE(pt.season_label, seasons.slug) AS season_label,
              pt.moved_at::TEXT AS moved_at,
              pt.age,
              from_team.slug AS from_team_id,
              COALESCE(
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = from_team.id AND tt.locale = ${locale}),
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = from_team.id AND tt.locale = 'en'),
                pt.from_team_name,
                from_team.slug
              ) AS from_team_name,
              to_team.slug AS to_team_id,
              COALESCE(
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = to_team.id AND tt.locale = ${locale}),
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = to_team.id AND tt.locale = 'en'),
                pt.to_team_name,
                to_team.slug
              ) AS to_team_name,
              pt.market_value_eur,
              pt.fee_eur,
              pt.fee_display,
              pt.currency_code,
              pt.transfer_type,
              pt.transfer_type_label,
              pt.contract_until_date::TEXT AS contract_until_date,
              pt.source_url
            FROM players p
            JOIN player_transfers pt ON pt.player_id = p.id
            LEFT JOIN seasons ON seasons.id = pt.season_id
            LEFT JOIN teams from_team ON from_team.id = pt.from_team_id
            LEFT JOIN teams to_team ON to_team.id = pt.to_team_id
            WHERE p.slug = ${id}
            ORDER BY pt.moved_at DESC NULLS LAST, pt.id DESC
          `,
          sql<PlayerNationalTeamSummaryRowDb[]>`
            WITH player_record AS (
              SELECT id
              FROM players
              WHERE slug = ${id}
              LIMIT 1
            ), national_caps AS (
              SELECT COUNT(DISTINCT m.id)::INT AS caps
              FROM player_record pr
              JOIN match_lineups ml ON ml.player_id = pr.id
              JOIN matches m ON m.id = ml.match_id
              JOIN teams t ON t.id = ml.team_id
              LEFT JOIN LATERAL (
                SELECT 1 AS participated
                FROM match_events me
                WHERE me.match_id = m.id
                  AND me.event_type = 'substitution'
                  AND me.secondary_player_id = ml.player_id
                LIMIT 1
              ) sub_in ON TRUE
              WHERE t.is_national = TRUE
                AND m.status <> 'scheduled'
                AND (ml.is_starter = TRUE OR sub_in.participated = 1)
            ), national_goals AS (
              SELECT COUNT(*)::INT AS goals
              FROM player_record pr
              JOIN match_events me ON me.player_id = pr.id
              JOIN matches m ON m.id = me.match_id
              JOIN teams t ON t.id = me.team_id
              WHERE t.is_national = TRUE
                AND m.status <> 'scheduled'
                AND me.event_type IN ('goal', 'penalty_scored')
            )
            SELECT nc.caps, ng.goals
            FROM national_caps nc
            CROSS JOIN national_goals ng
          `,
          sql<MatchRow[]>`
            WITH player_record AS (
              SELECT id
              FROM players
              WHERE slug = ${id}
              LIMIT 1
            )
            SELECT
              m.id::TEXT AS id,
              home.slug AS home_team_id,
              away.slug AS away_team_id,
              COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
              COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = home.id AND tt.locale = 'ko') AS home_team_korean_name,
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = away.id AND tt.locale = 'ko') AS away_team_korean_name,
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
            FROM player_record pr
            JOIN match_lineups ml ON ml.player_id = pr.id
            JOIN matches m ON m.id = ml.match_id
            JOIN teams team ON team.id = ml.team_id
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
            LEFT JOIN LATERAL (
              SELECT 1 AS participated
              FROM match_events me
              WHERE me.match_id = m.id
                AND me.event_type = 'substitution'
                AND me.secondary_player_id = ml.player_id
              LIMIT 1
            ) sub_in ON TRUE
            WHERE team.is_national = TRUE
              AND m.status IN ('finished', 'finished_aet', 'finished_pen')
              AND (ml.is_starter = TRUE OR sub_in.participated = 1)
            ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
            LIMIT 8
          `,
        ]);

        if (seasonHistoryRows.length === 0) {
          const fallbackSeasonHistoryRows = await sql<PlayerFallbackSeasonHistoryRowDb[]>`
            WITH player_record AS (
              SELECT p.id
              FROM players p
              WHERE p.slug = ${id}
              LIMIT 1
            ), appearance_summary AS (
              SELECT
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
                team.slug AS club_id,
                COALESCE(
                  (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
                  (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
                  team.slug
                ) AS club_name,
                EXTRACT(YEAR FROM s.start_date)::INT AS start_year,
                EXTRACT(YEAR FROM s.end_date)::INT AS end_year,
                s.start_date,
                s.end_date,
                COUNT(DISTINCT CASE
                  WHEN ml.is_starter OR sub_in.minute IS NOT NULL THEN m.id
                  ELSE NULL
                END)::INT AS appearances,
                SUM(CASE
                  WHEN ml.is_starter THEN COALESCE(sub_out.minute, 90)
                  WHEN sub_in.minute IS NOT NULL THEN GREATEST(90 - sub_in.minute, 0)
                  ELSE 0
                END)::INT AS minutes_played
              FROM player_record pr
              JOIN match_lineups ml ON ml.player_id = pr.id
              JOIN matches m ON m.id = ml.match_id
              JOIN competition_seasons cs ON cs.id = m.competition_season_id
              JOIN seasons s ON s.id = cs.season_id
              JOIN teams team ON team.id = ml.team_id
              LEFT JOIN LATERAL (
                SELECT MIN(me.minute) AS minute
                FROM match_events me
                WHERE me.match_id = m.id
                  AND me.event_type = 'substitution'
                  AND me.secondary_player_id = ml.player_id
              ) sub_in ON TRUE
              LEFT JOIN LATERAL (
                SELECT MIN(me.minute) AS minute
                FROM match_events me
                WHERE me.match_id = m.id
                  AND me.event_type = 'substitution'
                  AND me.player_id = ml.player_id
              ) sub_out ON TRUE
              WHERE m.status <> 'scheduled'
              GROUP BY s.slug, s.start_date, s.end_date, team.id, team.slug
              HAVING COUNT(DISTINCT CASE
                WHEN ml.is_starter OR sub_in.minute IS NOT NULL THEN m.id
                ELSE NULL
              END) > 0
            ), primary_event_summary AS (
              SELECT
                m.competition_season_id,
                me.team_id,
                SUM(CASE WHEN me.event_type IN ('goal', 'penalty_scored') THEN 1 ELSE 0 END)::INT AS goals,
                SUM(CASE WHEN me.event_type = 'yellow_card' THEN 1 ELSE 0 END)::INT AS yellow_cards,
                SUM(CASE WHEN me.event_type IN ('red_card', 'yellow_red_card') THEN 1 ELSE 0 END)::INT AS red_cards
              FROM player_record pr
              JOIN match_events me ON me.player_id = pr.id
              JOIN matches m ON m.id = me.match_id
              WHERE m.status <> 'scheduled'
              GROUP BY m.competition_season_id, me.team_id
            ), assist_event_summary AS (
              SELECT
                m.competition_season_id,
                me.team_id,
                COUNT(*)::INT AS assists
              FROM player_record pr
              JOIN match_events me ON me.secondary_player_id = pr.id
              JOIN matches m ON m.id = me.match_id
              WHERE m.status <> 'scheduled'
                AND me.event_type IN ('goal', 'own_goal', 'penalty_scored')
              GROUP BY m.competition_season_id, me.team_id
            ), event_summary AS (
              SELECT
                s.slug AS season_id,
                team.slug AS club_id,
                SUM(primary_event_summary.goals)::INT AS goals,
                COALESCE(SUM(assist_event_summary.assists), 0)::INT AS assists,
                SUM(primary_event_summary.yellow_cards)::INT AS yellow_cards,
                SUM(primary_event_summary.red_cards)::INT AS red_cards
              FROM primary_event_summary
              JOIN competition_seasons cs ON cs.id = primary_event_summary.competition_season_id
              JOIN seasons s ON s.id = cs.season_id
              JOIN teams team ON team.id = primary_event_summary.team_id
              LEFT JOIN assist_event_summary
                ON assist_event_summary.competition_season_id = primary_event_summary.competition_season_id
               AND assist_event_summary.team_id = primary_event_summary.team_id
              GROUP BY s.slug, team.slug
            )
            SELECT
              appearance_summary.season_id,
              appearance_summary.season_label,
              appearance_summary.club_id,
              appearance_summary.club_name,
              appearance_summary.appearances,
              COALESCE(event_summary.goals, 0)::INT AS goals,
              COALESCE(event_summary.assists, 0)::INT AS assists,
              appearance_summary.minutes_played,
              COALESCE(event_summary.yellow_cards, 0)::INT AS yellow_cards,
              COALESCE(event_summary.red_cards, 0)::INT AS red_cards,
              NULL::INT AS clean_sheets,
              appearance_summary.start_year,
              appearance_summary.end_year
            FROM appearance_summary
            LEFT JOIN event_summary
              ON event_summary.season_id = appearance_summary.season_id
             AND event_summary.club_id = appearance_summary.club_id
            ORDER BY appearance_summary.end_date DESC NULLS LAST, appearance_summary.start_date DESC NULLS LAST, appearance_summary.club_id ASC
          `;

          seasonHistoryRows = fallbackSeasonHistoryRows.map(({ start_year: _startYear, end_year: _endYear, ...row }) => row);
          if (clubHistoryRows.length === 0) {
            const fallbackClubHistoryMap = new Map<string, { clubId: string; clubName: string; startYear: number; endYear: number }>();

            for (const row of fallbackSeasonHistoryRows) {
              const startYear = row.start_year ?? row.end_year;
              const endYear = row.end_year ?? row.start_year;

              if (!startYear || !endYear) {
                continue;
              }

              const existing = fallbackClubHistoryMap.get(row.club_id);
              if (!existing) {
                fallbackClubHistoryMap.set(row.club_id, {
                  clubId: row.club_id,
                  clubName: row.club_name,
                  startYear,
                  endYear,
                });
                continue;
              }

              existing.startYear = Math.min(existing.startYear, startYear);
              existing.endYear = Math.max(existing.endYear, endYear);
            }

            clubHistoryRows = [...fallbackClubHistoryMap.values()].map((row) => ({
              club_id: row.clubId,
              club_name: row.clubName,
              start_year: row.startYear,
              end_year: row.endYear,
            }));
          }
        }

        const player = mapPlayer(rows[0]);
        const latestSeason = seasonHistoryRows[0];
        const shouldUseFallbackSeasonStats = hasEmptySeasonStats(player.seasonStats);
        const fallbackEndYear = rows[0].latest_season_end_date ? new Date(rows[0].latest_season_end_date).getUTCFullYear() : undefined;
        const transferFirstClubHistory = buildPlayerClubHistoryFromTransfers(transferRows, locale, {
          fallbackEndYear,
          isRetired: player.isRetired,
        });

        if (latestSeason && shouldUseFallbackSeasonStats) {
          player.seasonStats = {
            appearances: latestSeason.appearances ?? 0,
            goals: latestSeason.goals ?? 0,
            assists: latestSeason.assists ?? 0,
            minutesPlayed: latestSeason.minutes_played ?? 0,
            yellowCards: latestSeason.yellow_cards ?? 0,
            redCards: latestSeason.red_cards ?? 0,
            cleanSheets: latestSeason.clean_sheets ?? undefined,
          };
        }

        if (latestSeason && !player.clubId) {
          player.clubId = latestSeason.club_id;
        }

        return {
          ...player,
          marketValueHistory: marketValueRows.map((row) => mapPlayerMarketValueEntry(row, locale)),
          nationalTeam: mapPlayerNationalTeamSummary(
            nationalTeamSummaryRows[0],
            nationalTeamRecentMatchRows.map((row) => mapMatch(row, locale))
          ),
          seasonHistory: seasonHistoryRows.map((row) => mapPlayerSeasonHistoryEntry(row, locale)),
          transferHistory: transferRows.map((row) => mapPlayerTransferEntry(row, locale)),
          clubHistory: transferFirstClubHistory.length > 0
            ? transferFirstClubHistory
            : clubHistoryRows.map((row) => mapPlayerClubHistoryEntry(row, locale)),
        };
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
            JOIN teams current_team ON current_team.id = pc.team_id
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE current_team.is_national = FALSE
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
            COALESCE(team.slug, '') AS club_id,
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
            season_meta.season_end_date::TEXT AS latest_season_end_date,
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
          LEFT JOIN LATERAL (
            SELECT s.end_date AS season_end_date
            FROM competition_seasons cs
            JOIN seasons s ON s.id = cs.season_id
            WHERE cs.id = lpc.competition_season_id
            LIMIT 1
          ) season_meta ON TRUE
          LEFT JOIN player_season_stats pss ON pss.player_id = p.id AND pss.competition_season_id = lpc.competition_season_id
          WHERE team.slug = ${clubId}
          ORDER BY name ASC
        `;

        const players = rows.map(mapPlayer);
        const fallbackRows = await getFallbackPlayerSeasonStatsRowsDb(
          players.filter((player) => hasEmptySeasonStats(player.seasonStats)).map((player) => player.id),
          { clubId }
        );

        return applyFallbackStatsToPlayers(players, fallbackRows);
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

        const players = rows.map(mapPlayer);
        const fallbackRows = await getFallbackPlayerSeasonStatsRowsDb(
          players.filter((player) => hasEmptySeasonStats(player.seasonStats)).map((player) => player.id),
          { seasonId, clubId }
        );

        return applyFallbackStatsToPlayers(players, fallbackRows, { seasonId });
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
            JOIN teams current_team ON current_team.id = pc.team_id
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE current_team.is_national = FALSE
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

        const players = rows.map(mapPlayer);
        const fallbackRows = await getFallbackPlayerSeasonStatsRowsDb(
          players.filter((player) => hasEmptySeasonStats(player.seasonStats)).map((player) => player.id)
        );

        return applyFallbackStatsToPlayers(players, fallbackRows);
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
      policySlug: 'master.countries',
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
          getLocalizedNationRows(rows).map((row) => mapNation(row, rankingCategory)),
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
      policySlug: 'master.countries',
      loader: async () => {
        const nationBase = sql`
          WITH latest_player_contracts AS (
            SELECT DISTINCT ON (pc.player_id)
              pc.player_id
            FROM player_contracts pc
            JOIN teams current_team ON current_team.id = pc.team_id
            JOIN competition_seasons cs ON cs.id = pc.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            WHERE current_team.is_national = FALSE
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

        const localizedRows = getLocalizedNationRows(rows) as NationListRow[];

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
      policySlug: 'master.countries',
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
          const localizedRow = getLocalizedNationRows(rows)[0] ?? rows[0];
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
    const key = buildCacheKey({ namespace: 'matches-v2', locale });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      policySlug: 'match.read_model',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(
              home_name.name,
              home_name_en.name,
              home.slug
            ) AS home_team_name,
            COALESCE(
              away_name.name,
              away_name_en.name,
              away.slug
            ) AS away_team_name,
            COALESCE(
              home_name.short_name,
              home_name_en.short_name,
              home.slug
            ) AS home_team_code,
            COALESCE(
              away_name.short_name,
              away_name_en.short_name,
              away.slug
            ) AS away_team_code,
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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST
        `;

        return mergeMatches(rows.map((row) => mapMatch(row, locale)), tournament.matches);
      },
    });
  }, () => mergeMatches([], tournament.matches));

  return localizeNationMatchNames(matches, locale);
}

export async function getMatchByIdDb(id: string, locale: string = 'en'): Promise<Match | undefined> {
  if (!/^\d+$/.test(id)) {
    const tournament = await loadWorldCup2026Source();
    const fallbackMatch = tournament.matches.find((match) => match.id === id);
    if (!fallbackMatch) {
      return undefined;
    }

    return (await localizeNationMatchNames([fallbackMatch], locale))[0];
  }

  const numericId = Number(id);

  return withFallback(async () => {
    const sql = getDb();
    const rows = await sql<MatchRow[]>`
      WITH target_match AS (
        SELECT *
        FROM matches
        WHERE id = ${numericId}
        LIMIT 1
      )
      SELECT
        m.id::TEXT AS id,
        home.slug AS home_team_id,
        away.slug AS away_team_id,
        COALESCE(
          home_name.name,
          home_name_en.name,
          home.slug
        ) AS home_team_name,
        COALESCE(
          away_name.name,
          away_name_en.name,
          away.slug
        ) AS away_team_name,
        COALESCE(
          home_name.short_name,
          home_name_en.short_name,
          home.slug
        ) AS home_team_code,
        COALESCE(
          away_name.short_name,
          away_name_en.short_name,
          away.slug
        ) AS away_team_code,
        home.crest_url AS home_team_logo,
        away.crest_url AS away_team_logo,
        m.home_score,
        m.away_score,
        m.match_date::TEXT AS date,
        TO_CHAR(m.kickoff_at AT TIME ZONE 'UTC', 'HH24:MI') AS time,
        COALESCE(vt.name, vt_en.name, v.slug, '') AS venue,
        m.attendance,
        m.referee,
        m.home_formation,
        m.away_formation,
        c.slug AS league_id,
        m.matchday AS match_week,
        m.stage,
        m.group_name,
        COALESCE(ct.name, ct_en.name, c.slug) AS competition_name,
        'club'::TEXT AS team_type,
        m.status::TEXT AS status
      FROM target_match m
      JOIN teams home ON home.id = m.home_team_id
      JOIN teams away ON away.id = m.away_team_id
      LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
      LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
      LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
      LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      LEFT JOIN competition_translations ct ON ct.competition_id = c.id AND ct.locale = ${locale}
      LEFT JOIN competition_translations ct_en ON ct_en.competition_id = c.id AND ct_en.locale = 'en'
      LEFT JOIN venues v ON v.id = m.venue_id
      LEFT JOIN venue_translations vt ON vt.venue_id = v.id AND vt.locale = ${locale}
      LEFT JOIN venue_translations vt_en ON vt_en.venue_id = v.id AND vt_en.locale = 'en'
      LIMIT 1
    `;

      const row = rows[0];
      if (!row) {
        return undefined;
      }

    const match = mapMatch(row, locale);
    const enrichedMatch: Match = match;

    if (enrichedMatch.teamType !== 'nation') {
      return enrichedMatch;
    }

    return (await localizeNationMatchNames([enrichedMatch], locale))[0];
  }, async () => {
    const matches = await getMatchesDb(locale);
    return matches.find((match) => match.id === id);
  });
}

export async function getMatchTimelineDb(id: string, locale: string = 'en'): Promise<MatchEvent[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'match-timeline', locale, id });
    const numericId = Number(id);

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      policySlug: 'match.read_model',
      loader: async () => {
        const rows = await sql<MatchEventRowDb[]>`
          SELECT
            me.source_event_id::TEXT AS source_event_id,
            me.minute,
            me.event_type,
            player.slug AS player_id,
            COALESCE(player_name_local.known_as, player_name_en.known_as, player.slug) AS player_name,
            CASE WHEN me.event_type = 'substitution' THEN secondary_player.slug ELSE NULL::TEXT END AS secondary_player_id,
            CASE WHEN me.event_type = 'substitution' THEN COALESCE(secondary_player_name_local.known_as, secondary_player_name_en.known_as, secondary_player.slug) ELSE NULL::TEXT END AS secondary_player_name,
            CASE WHEN me.event_type IN ('goal', 'own_goal', 'penalty_scored') THEN secondary_player.slug ELSE NULL::TEXT END AS assist_player_id,
            CASE WHEN me.event_type IN ('goal', 'own_goal', 'penalty_scored') THEN COALESCE(secondary_player_name_local.known_as, secondary_player_name_en.known_as, secondary_player.slug) ELSE NULL::TEXT END AS assist_player_name,
            team.slug AS team_id,
            me.detail,
            me.source_details
          FROM match_events me
          JOIN teams team ON team.id = me.team_id
          LEFT JOIN players player ON player.id = me.player_id
          LEFT JOIN player_translations player_name_local ON player_name_local.player_id = player.id AND player_name_local.locale = ${locale}
          LEFT JOIN player_translations player_name_en ON player_name_en.player_id = player.id AND player_name_en.locale = 'en'
          LEFT JOIN players secondary_player ON secondary_player.id = me.secondary_player_id
          LEFT JOIN player_translations secondary_player_name_local ON secondary_player_name_local.player_id = secondary_player.id AND secondary_player_name_local.locale = ${locale}
          LEFT JOIN player_translations secondary_player_name_en ON secondary_player_name_en.player_id = secondary_player.id AND secondary_player_name_en.locale = 'en'
          WHERE me.match_id = ${numericId}
            AND me.is_notable = TRUE
          ORDER BY me.minute ASC, me.event_index ASC
        `;

        return rows.filter((event) => event.player_id).map(mapTimelineMatchEvent);
      },
    });
  }, () => []);
}

export async function getMatchStatsDb(id: string): Promise<MatchStats | undefined> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'match-stats', id });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      policySlug: 'match.read_model',
      loader: async () => {
        const rows = await sql<MatchStatsRowDb[]>`
          SELECT
            team.slug AS team_id,
            ms.possession,
            ms.expected_goals::FLOAT8 AS expected_goals,
            ms.total_passes,
            ms.accurate_passes,
            ms.pass_accuracy,
            ms.total_shots,
            ms.shots_on_target,
            ms.corner_kicks,
            ms.fouls,
            ms.offsides,
            ms.gk_saves
          FROM match_stats ms
          JOIN teams team ON team.id = ms.team_id
          WHERE ms.match_id = ${Number(id)}
        `;

        if (rows.length < 2) {
          return undefined;
        }

        return {
          possession: [rows[0].possession ?? 0, rows[1].possession ?? 0],
          expectedGoals: [rows[0].expected_goals ?? 0, rows[1].expected_goals ?? 0],
          totalPasses: [rows[0].total_passes ?? 0, rows[1].total_passes ?? 0],
          accuratePasses: [rows[0].accurate_passes ?? 0, rows[1].accurate_passes ?? 0],
          passAccuracy: [rows[0].pass_accuracy ?? 0, rows[1].pass_accuracy ?? 0],
          shots: [rows[0].total_shots, rows[1].total_shots],
          shotsOnTarget: [rows[0].shots_on_target, rows[1].shots_on_target],
          corners: [rows[0].corner_kicks ?? 0, rows[1].corner_kicks ?? 0],
          fouls: [rows[0].fouls ?? 0, rows[1].fouls ?? 0],
          offsides: [rows[0].offsides ?? 0, rows[1].offsides ?? 0],
          saves: [rows[0].gk_saves ?? 0, rows[1].gk_saves ?? 0],
        };
      },
    });
  }, () => undefined);
}

export async function getMatchLineupsDb(id: string, locale: string = 'en'): Promise<MatchLineup[]> {
  if (!/^\d+$/.test(id)) {
    return [];
  }

  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'match-lineups-v2', locale, id });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      policySlug: 'match.read_model',
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
            ml.grid_position,
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
    const key = buildCacheKey({ namespace: 'match-analysis', id });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      policySlug: 'match.read_model',
      loader: async () => {
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
      },
    });
  }, () => ({ events: [] }));
}

export async function getMatchesByLeagueDb(leagueId: string, locale: string = 'en'): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'matches-by-league-v3', locale, id: leagueId });

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
            LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
            LEFT JOIN matches m ON m.competition_season_id = cs.id
            WHERE c.slug = ${leagueId}
            GROUP BY cs.id, s.end_date, s.start_date, s.id
            ORDER BY
              CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
              s.end_date DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              s.id DESC
            LIMIT 1
          )
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(
              home_name.name,
              home_name_en.name,
              home.slug
            ) AS home_team_name,
            COALESCE(
              away_name.name,
              away_name_en.name,
              away.slug
            ) AS away_team_name,
            COALESCE(
              home_name.short_name,
              home_name_en.short_name,
              home.slug
            ) AS home_team_code,
            COALESCE(
              away_name.short_name,
              away_name_en.short_name,
              away.slug
            ) AS away_team_code,
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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST
        `;

        return rows.map((row) => mapMatch(row, locale));
      },
    });
  }, () => []);
}

export async function getMatchesByClubDb(clubId: string, locale: string = 'en'): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'matches-by-club-v3', locale, id: clubId });

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
            (SELECT tt.name FROM team_translations tt WHERE tt.team_id = home.id AND tt.locale = 'ko') AS home_team_korean_name,
            (SELECT tt.name FROM team_translations tt WHERE tt.team_id = away.id AND tt.locale = 'ko') AS away_team_korean_name,
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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
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

        return rows.map((row) => mapMatch(row, locale));
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
    const key = buildCacheKey({ namespace: 'recent-finished-matches-by-club-v2', locale, id: clubId, params: { limit } });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      policySlug: 'match.read_model',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          SELECT
            m.id::TEXT AS id,
            home.slug AS home_team_id,
            away.slug AS away_team_id,
            COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
            COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
            (SELECT tt.name FROM team_translations tt WHERE tt.team_id = home.id AND tt.locale = 'ko') AS home_team_korean_name,
            (SELECT tt.name FROM team_translations tt WHERE tt.team_id = away.id AND tt.locale = 'ko') AS away_team_korean_name,
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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
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

        return rows.map((row) => mapMatch(row, locale));
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
    const key = buildCacheKey({ namespace: 'upcoming-scheduled-matches-by-league-ids-v2', locale, params: { ids: normalizedIds.join(','), limit } });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      policySlug: 'match.read_model',
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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          WHERE c.slug = ANY(${normalizedIds})
            AND m.status IN ('scheduled', 'timed')
          ORDER BY m.match_date ASC, m.kickoff_at ASC NULLS LAST, m.id ASC
          LIMIT ${limit}
        `;

        return rows.map((row) => mapMatch(row, locale));
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
    const key = buildCacheKey({ namespace: 'recent-finished-matches-by-league-ids-v2', locale, params: { ids: normalizedIds.join(','), limit } });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      policySlug: 'match.read_model',
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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
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

        return rows.map((row) => mapMatch(row, locale));
      },
    });
  }, () => []);
}

export async function getMatchesByClubAndSeasonDb(
  clubId: string,
  seasonId: string,
  competitionId: string | null = null,
  locale: string = 'en',
): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'matches-by-club-season-v4', locale, id: clubId, params: { season: seasonId, competition: competitionId ?? undefined } });

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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          WHERE s.slug = ${seasonId}
            AND (${competitionId}::TEXT IS NULL OR c.slug = ${competitionId})
            AND (home.slug = ${clubId} OR away.slug = ${clubId})
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
        `;

        return rows.map((row) => mapMatch(row, locale));
      },
    });
  }, () => []);
}

export async function getRecentFinishedMatchesByClubAndSeasonDb(
  clubId: string,
  seasonId: string,
  competitionId: string | null = null,
  locale: string = 'en',
  limit: number = 10,
): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'recent-finished-matches-by-club-season-v3', locale, id: clubId, params: { season: seasonId, competition: competitionId ?? undefined, limit } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          WITH target_competition_seasons AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN seasons s ON s.id = cs.season_id
            JOIN competitions c ON c.id = cs.competition_id
            WHERE s.slug = ${seasonId}
              AND (${competitionId}::TEXT IS NULL OR c.slug = ${competitionId})
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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
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

        return rows.map((row) => mapMatch(row, locale));
      },
    });
  }, () => []);
}

export async function getUpcomingScheduledMatchesByClubAndSeasonDb(
  clubId: string,
  seasonId: string,
  competitionId: string | null = null,
  locale: string = 'en',
  limit: number = 10,
): Promise<Match[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'upcoming-scheduled-matches-by-club-season-v3', locale, id: clubId, params: { season: seasonId, competition: competitionId ?? undefined, limit } });

    return readThroughCache({
      key,
      tier: 'season-finished',
      loader: async () => {
        const rows = await sql<MatchRow[]>`
          WITH target_competition_seasons AS (
            SELECT cs.id
            FROM competition_seasons cs
            JOIN seasons s ON s.id = cs.season_id
            JOIN competitions c ON c.id = cs.competition_id
            WHERE s.slug = ${seasonId}
              AND (${competitionId}::TEXT IS NULL OR c.slug = ${competitionId})
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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
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

        return rows.map((row) => mapMatch(row, locale));
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
      policySlug: 'match.read_model',
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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
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
          rows.map((row) => mapMatch(row, locale)),
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
      policySlug: 'match.read_model',
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
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = home.id AND tt.locale = 'ko') AS home_team_korean_name,
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = away.id AND tt.locale = 'ko') AS away_team_korean_name,
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
            ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
            LIMIT ${pageSize}
            OFFSET ${offset}
          `,
        ]);

        return createPaginatedResult(rows.map((row) => mapMatch(row, locale)), countRows[0]?.total_count ?? 0, currentPage, pageSize);
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
      LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
      LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
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

    return rows.map((row) => mapMatch(row, locale));
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
      LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
      LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
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

    return rows.map((row) => mapMatch(row, locale));
  }, () => []);
}

export async function getStandingsByLeagueDb(leagueId: string, locale: string = 'en'): Promise<StandingRow[]> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'standings-v4', locale, id: leagueId });

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
            LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
            LEFT JOIN matches m ON m.competition_season_id = cs.id
            WHERE c.slug = ${leagueId}
            GROUP BY cs.id, s.end_date, s.start_date, s.id
            ORDER BY
              CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
              s.end_date DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              s.id DESC
            LIMIT 1
          )
          SELECT
            standings.position,
            team.slug AS club_id,
            COALESCE(
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_name,
            (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'ko') AS club_korean_name,
            COALESCE(
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
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

        return rows.map((row) => mapStanding(row, locale));
      },
    });
  }, () => []);
}

export async function getDashboardTournamentSummaryDb(
  leagueId: string,
  locale: string = 'en',
): Promise<DashboardTournamentSummary> {
  return withFallback(async () => {
    const sql = getDb();
    const key = buildCacheKey({ namespace: 'dashboard-tournament-summary-v3', locale, id: leagueId });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      policySlug: 'match.read_model',
      loader: async () => {
        const [recentRows, upcomingRows, stageRows] = await Promise.all([
          sql<MatchRow[]>`
            WITH latest_competition_season AS (
              SELECT cs.id
              FROM competition_seasons cs
              JOIN competitions c ON c.id = cs.competition_id
              JOIN seasons s ON s.id = cs.season_id
              LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
              LEFT JOIN matches m ON m.competition_season_id = cs.id
              WHERE c.slug = ${leagueId}
              GROUP BY cs.id, s.end_date, s.start_date, s.id
              ORDER BY
                CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
                s.end_date DESC NULLS LAST,
                s.start_date DESC NULLS LAST,
                s.id DESC
              LIMIT 1
            )
            SELECT
              m.id::TEXT AS id,
              home.slug AS home_team_id,
              away.slug AS away_team_id,
              COALESCE(home_name.name, home_name_en.name, home.slug) AS home_team_name,
              COALESCE(away_name.name, away_name_en.name, away.slug) AS away_team_name,
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = home.id AND tt.locale = 'ko') AS home_team_korean_name,
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = away.id AND tt.locale = 'ko') AS away_team_korean_name,
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
            FROM latest_competition_season lcs
            JOIN matches m ON m.competition_season_id = lcs.id
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
            ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST, m.id DESC
            LIMIT 2
          `,
          sql<MatchRow[]>`
            WITH latest_competition_season AS (
              SELECT cs.id
              FROM competition_seasons cs
              JOIN competitions c ON c.id = cs.competition_id
              JOIN seasons s ON s.id = cs.season_id
              LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
              LEFT JOIN matches m ON m.competition_season_id = cs.id
              WHERE c.slug = ${leagueId}
              GROUP BY cs.id, s.end_date, s.start_date, s.id
              ORDER BY
                CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
                s.end_date DESC NULLS LAST,
                s.start_date DESC NULLS LAST,
                s.id DESC
              LIMIT 1
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
            FROM latest_competition_season lcs
            JOIN matches m ON m.competition_season_id = lcs.id
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
            WHERE m.status IN ('scheduled', 'timed')
            ORDER BY m.match_date ASC, m.kickoff_at ASC NULLS LAST, m.id ASC
            LIMIT 2
          `,
          sql<TournamentStageTrailRowDb[]>`
            WITH latest_competition_season AS (
              SELECT cs.id
              FROM competition_seasons cs
              JOIN competitions c ON c.id = cs.competition_id
              JOIN seasons s ON s.id = cs.season_id
              LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
              LEFT JOIN matches m ON m.competition_season_id = cs.id
              WHERE c.slug = ${leagueId}
              GROUP BY cs.id, s.end_date, s.start_date, s.id
              ORDER BY
                CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
                s.end_date DESC NULLS LAST,
                s.start_date DESC NULLS LAST,
                s.id DESC
              LIMIT 1
            )
            SELECT ranked.stage
            FROM (
              SELECT
                m.stage,
                MAX(COALESCE(m.kickoff_at, m.match_date::timestamp)) AS latest_at
              FROM latest_competition_season lcs
              JOIN matches m ON m.competition_season_id = lcs.id
              WHERE m.stage IS NOT NULL
                AND BTRIM(m.stage) <> ''
              GROUP BY m.stage
              ORDER BY latest_at DESC
              LIMIT 4
            ) ranked
            ORDER BY ranked.latest_at ASC
          `,
        ]);

        return {
          recentResults: recentRows.map((row) => mapMatch(row, locale)),
          upcomingFixtures: upcomingRows.map((row) => mapMatch(row, locale)),
          stageTrail: stageRows.map((row) => row.stage),
        };
      },
    });
  }, () => ({ recentResults: [], upcomingFixtures: [], stageTrail: [] }));
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
    const key = buildCacheKey({ namespace: 'standings-by-league-ids-v3', locale, params: { ids: normalizedIds.join(',') } });

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
              tc.slug AS league_id,
              COUNT(DISTINCT ts.team_id)::INT AS participant_count,
              COUNT(DISTINCT m.id)::INT AS match_count
            FROM competition_seasons cs
            JOIN target_competitions tc ON tc.id = cs.competition_id
            JOIN seasons s ON s.id = cs.season_id
            LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
            LEFT JOIN matches m ON m.competition_season_id = cs.id
            GROUP BY cs.id, cs.competition_id, tc.slug, s.end_date, s.start_date, s.id
            ORDER BY
              cs.competition_id,
              CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
              s.end_date DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              s.id DESC
          )
          SELECT
            lcs.league_id,
            standings.position,
            team.slug AS club_id,
            COALESCE(
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_name,
            COALESCE(
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
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
    const key = buildCacheKey({ namespace: 'standings-by-season-v3', locale, id: leagueId, params: { season: seasonId } });

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
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = 'en'),
              team.slug
            ) AS club_name,
            COALESCE(
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
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

        return rows.map((row) => mapStanding(row, locale));
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
            LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
            LEFT JOIN matches m ON m.competition_season_id = cs.id
            WHERE c.slug = ${leagueId}
            GROUP BY cs.id, s.end_date, s.start_date, s.id
            ORDER BY
              CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
              s.end_date DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              s.id DESC
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
    const key = buildCacheKey({ namespace: 'top-scorer-rows-v2', locale, id: leagueId, params: { limit } });

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
            LEFT JOIN team_seasons ts ON ts.competition_season_id = cs.id
            LEFT JOIN matches m ON m.competition_season_id = cs.id
            WHERE c.slug = ${leagueId}
            GROUP BY cs.id, s.end_date, s.start_date, s.id
            ORDER BY
              CASE WHEN COUNT(DISTINCT ts.team_id) > 0 OR COUNT(DISTINCT m.id) > 0 THEN 0 ELSE 1 END,
              s.end_date DESC NULLS LAST,
              s.start_date DESC NULLS LAST,
              s.id DESC
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
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
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
    clubShortName: getLocalizedClubName(row.club_id, row.club_short_name, locale),
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
    const key = buildCacheKey({ namespace: 'top-scorer-rows-by-season-v2', locale, id: leagueId, params: { season: seasonId, limit } });

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
              (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = team.id AND tt.locale = ${locale}),
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
          clubShortName: getLocalizedClubName(row.club_id, row.club_short_name, locale),
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
    const key = buildCacheKey({ namespace: 'matches-by-league-season-v3', locale, id: leagueId, params: { season: seasonId } });

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
            COALESCE(
              home_name.name,
              home_name_en.name,
              home.slug
            ) AS home_team_name,
            COALESCE(
              away_name.name,
              away_name_en.name,
              away.slug
            ) AS away_team_name,
            COALESCE(
              home_name.short_name,
              home_name_en.short_name,
              home.slug
            ) AS home_team_code,
            COALESCE(
              away_name.short_name,
              away_name_en.short_name,
              away.slug
            ) AS away_team_code,
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
          LEFT JOIN team_translations home_name ON home_name.team_id = home.id AND home_name.locale = ${locale}
          LEFT JOIN team_translations away_name ON away_name.team_id = away.id AND away_name.locale = ${locale}
          LEFT JOIN team_translations home_name_en ON home_name_en.team_id = home.id AND home_name_en.locale = 'en'
          LEFT JOIN team_translations away_name_en ON away_name_en.team_id = away.id AND away_name_en.locale = 'en'
          JOIN competition_seasons cs ON cs.id = m.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations comp_name ON comp_name.competition_id = c.id AND comp_name.locale = ${locale}
          LEFT JOIN competition_translations comp_name_en ON comp_name_en.competition_id = c.id AND comp_name_en.locale = 'en'
          LEFT JOIN venues v ON v.id = m.venue_id
          ORDER BY m.match_date DESC, m.kickoff_at DESC NULLS LAST
        `;

        return rows.map((row) => mapMatch(row, locale));
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
    const key = buildCacheKey({ namespace: 'clubs-by-league-season-v3', locale, id: leagueId, params: { season: seasonId } });

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
            ORDER BY cs.id DESC
            LIMIT 1
          ), has_regular_season_matches AS (
            SELECT EXISTS (
              SELECT 1
              FROM matches m
              JOIN target_competition_season tcs ON tcs.id = m.competition_season_id
              WHERE m.stage = 'REGULAR_SEASON'
            ) AS has_regular_season
          ), season_participants AS (
            SELECT DISTINCT participant.team_id
            FROM has_regular_season_matches hrsm
            JOIN LATERAL (
              SELECT m.home_team_id AS team_id
              FROM matches m
              JOIN target_competition_season tcs ON tcs.id = m.competition_season_id
              WHERE hrsm.has_regular_season = TRUE
                AND m.stage = 'REGULAR_SEASON'

              UNION

              SELECT m.away_team_id AS team_id
              FROM matches m
              JOIN target_competition_season tcs ON tcs.id = m.competition_season_id
              WHERE hrsm.has_regular_season = TRUE
                AND m.stage = 'REGULAR_SEASON'

              UNION

              SELECT ts.team_id
              FROM team_seasons ts
              JOIN target_competition_season tcs ON tcs.id = ts.competition_season_id
              WHERE hrsm.has_regular_season = FALSE
            ) participant ON TRUE
          ), ranked_clubs AS (
            SELECT
              t.slug AS id,
              COALESCE(
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
                t.slug
              ) AS name,
              (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'ko') AS korean_name,
              COALESCE(
                (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
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
              t.crest_url,
              ROW_NUMBER() OVER (
                PARTITION BY LOWER(
                  COALESCE(
                    (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = ${locale}),
                    (SELECT tt.name FROM team_translations tt WHERE tt.team_id = t.id AND tt.locale = 'en'),
                    t.slug
                  )
                )
                ORDER BY CASE WHEN t.slug LIKE '%-germany' THEN 0 ELSE 1 END, t.slug ASC
              ) AS duplicate_rank
            FROM season_participants sp
            JOIN teams t ON t.id = sp.team_id
            JOIN countries country ON country.id = t.country_id
            LEFT JOIN venues v ON v.id = t.venue_id
            JOIN target_competition_season tcs ON TRUE
            JOIN competition_seasons cs ON cs.id = tcs.id
            JOIN competitions c ON c.id = cs.competition_id
            WHERE t.is_national = FALSE
          )
          SELECT
            id,
            name,
            korean_name,
            short_name,
            country,
            gender,
            founded,
            stadium,
            stadium_capacity,
            league_id,
            crest_url
          FROM ranked_clubs
          WHERE duplicate_rank = 1
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
    const key = buildCacheKey({ namespace: 'search-v4', locale, params: { q: normalizedQuery, gender: gender ?? 'all' } });

    return readThroughCache({
      key,
      tier: 'matchday-warm',
      policySlug: 'search.read_model',
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
                (SELECT tt.name FROM team_translations tt WHERE tt.team_id = matched.entity_id AND tt.locale = ${locale}),
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
            END AS gender,
            CASE matched.entity_type
              WHEN 'competition' THEN (SELECT c.emblem_url FROM competitions c WHERE c.id = matched.entity_id)
              WHEN 'team' THEN (SELECT t.crest_url FROM teams t WHERE t.id = matched.entity_id)
              WHEN 'player' THEN (SELECT p.photo_url FROM players p WHERE p.id = matched.entity_id)
              WHEN 'country' THEN (SELECT c.flag_url FROM countries c WHERE c.id = matched.entity_id)
              ELSE NULL
            END AS image_url,
            CASE matched.entity_type
              WHEN 'team' THEN COALESCE(
                (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = matched.entity_id AND tt.locale = ${locale}),
                (SELECT tt.short_name FROM team_translations tt WHERE tt.team_id = matched.entity_id AND tt.locale = 'en'),
                matched.matched_alias
              )
              ELSE NULL
            END AS short_name,
            CASE matched.entity_type
              WHEN 'country' THEN (SELECT c.code_alpha3::TEXT FROM countries c WHERE c.id = matched.entity_id)
              ELSE NULL
            END AS nation_code,
            CASE matched.entity_type
              WHEN 'player' THEN (SELECT p.position::TEXT FROM players p WHERE p.id = matched.entity_id)
              ELSE NULL
            END AS player_position
          FROM matched
        `;

        const filteredResults = rows
          .map((row) => ({
            type: row.result_type,
            id: row.result_id,
            name: row.result_type === 'club'
              ? getLocalizedClubName(row.result_id, row.result_name, locale)
              : row.result_name,
            subtitle: row.subtitle,
            gender: row.gender ?? undefined,
            imageUrl: row.result_type === 'club'
              ? clubLogoMap[row.result_id] ?? row.image_url ?? undefined
              : row.result_type === 'league'
                ? leagueLogoMap[row.result_id] ?? row.image_url ?? undefined
                : row.result_type === 'nation'
                  ? getNationFlagUrl(row.nation_code ?? row.result_id, row.image_url ?? undefined)
                  : row.image_url ?? undefined,
            shortName: row.short_name ?? undefined,
            nationCode: row.nation_code ?? undefined,
            playerPosition: row.player_position ?? undefined,
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

        return deduplicateClubSearchResults(filteredResults, locale);
      },
    });
  }, () => []);
}
