export const STATSBOMB_OPEN_DATA_BASE_URL = 'https://raw.githubusercontent.com/statsbomb/open-data/master';

export interface StatsBombCompetitionEntry {
  competition_id: number;
  season_id: number;
  country_name: string;
  competition_name: string;
  competition_gender: 'male' | 'female';
  competition_youth: boolean;
  competition_international: boolean;
  season_name: string;
  match_updated: string | null;
  match_updated_360: string | null;
  match_available: string | null;
  match_available_360: string | null;
}

export interface StatsBombCompetitionSeasonManifest {
  sourceCompetitionId: string;
  sourceSeasonId: string;
  competitionSlug: string;
  seasonSlug: string;
  countryCode: string;
  competitionName: string;
  seasonName: string;
  competitionGender: 'male' | 'female';
  isYouth: boolean;
  isInternational: boolean;
  matchUpdatedAt: string | null;
  matchAvailableAt: string | null;
  matchUpdated360At: string | null;
  matchAvailable360At: string | null;
}

export interface StatsBombTeamReference {
  team_id?: number;
  team_name?: string;
  team_gender?: 'male' | 'female';
  home_team_id?: number;
  home_team_name?: string;
  home_team_gender?: 'male' | 'female';
  home_team_group?: string | null;
  away_team_id?: number;
  away_team_name?: string;
  away_team_gender?: 'male' | 'female';
  away_team_group?: string | null;
}

export interface StatsBombCountryReference {
  id?: number;
  name?: string;
}

export interface StatsBombMatchEntry {
  match_id: number;
  match_date: string;
  kick_off: string;
  last_updated: string | null;
  last_updated_360: string | null;
  home_score: number | null;
  away_score: number | null;
  match_week: number | null;
  match_status?: string | null;
  match_status_360?: string | null;
  competition: {
    competition_id: number;
    country_name: string;
    competition_name: string;
  };
  season: {
    season_id: number;
    season_name: string;
  };
  home_team: StatsBombTeamReference & {
    home_team_id: number;
    home_team_name: string;
    country?: StatsBombCountryReference;
  };
  away_team: StatsBombTeamReference & {
    away_team_id: number;
    away_team_name: string;
    country?: StatsBombCountryReference;
  };
  metadata?: {
    data_version?: string;
    shot_fidelity_version?: string;
    xy_fidelity_version?: string;
  };
  competition_stage?: {
    id?: number;
    name?: string;
  };
  stadium?: {
    id?: number;
    name?: string;
    country?: StatsBombCountryReference;
  };
  referee?: {
    id?: number;
    name?: string;
    country?: StatsBombCountryReference;
  };
}

export interface StatsBombMatchManifest {
  sourceMatchId: string;
  sourceCompetitionId: string;
  sourceSeasonId: string;
  sourceHomeTeamId: string;
  sourceAwayTeamId: string;
  competitionSlug: string;
  seasonSlug: string;
  homeTeamSlug: string;
  awayTeamSlug: string;
  matchDate: string;
  kickOff: string;
  matchWeek: number | null;
  lastUpdatedAt: string | null;
  lastUpdated360At: string | null;
  dataVersion: string | null;
  shotFidelityVersion: string | null;
  xyFidelityVersion: string | null;
}

export interface StatsBombLineupPosition {
  position_id: number;
  position: string;
  from: string;
  to: string | null;
  from_period: number;
  to_period: number | null;
  start_reason: string;
  end_reason: string | null;
}

export interface StatsBombLineupCard {
  time: string;
  card_type: string;
  reason: string;
  period: number;
}

export interface StatsBombLineupPlayer {
  player_id: number;
  player_name: string;
  player_nickname: string | null;
  jersey_number: number | null;
  country?: StatsBombCountryReference;
  cards: StatsBombLineupCard[];
  positions: StatsBombLineupPosition[];
}

export interface StatsBombLineupEntry {
  team_id: number;
  team_name: string;
  lineup: StatsBombLineupPlayer[];
}

export interface StatsBombEventActor {
  id: number;
  name: string;
}

export interface StatsBombEventLocationCarrier {
  end_location?: number[];
  type?: StatsBombEventTypeRef;
}

export interface StatsBombEventTypeRef {
  id?: number;
  name?: string;
}

export interface StatsBombEventFreezeFrame {
  location?: number[];
  player?: StatsBombEventActor;
  position?: StatsBombEventTypeRef;
  teammate?: boolean;
  keeper?: boolean;
}

export interface StatsBombEventEntry {
  id: string;
  index: number;
  period: number;
  timestamp: string;
  minute: number;
  second: number;
  duration?: number;
  under_pressure?: boolean;
  type: StatsBombEventTypeRef;
  team?: StatsBombEventActor;
  player?: StatsBombEventActor;
  possession?: number;
  possession_team?: StatsBombEventActor;
  location?: number[];
  related_events?: string[];
  pass?: {
    recipient?: StatsBombEventActor;
    end_location?: number[];
    type?: StatsBombEventTypeRef;
    outcome?: StatsBombEventTypeRef;
    assisted_shot_id?: string;
    shot_assist?: boolean;
  };
  carry?: StatsBombEventLocationCarrier;
  goalkeeper?: StatsBombEventLocationCarrier;
  shot?: {
    statsbomb_xg?: number;
    end_location?: number[];
    type?: StatsBombEventTypeRef;
    outcome?: StatsBombEventTypeRef;
    key_pass_id?: string;
    freeze_frame?: StatsBombEventFreezeFrame[];
  };
  substitution?: {
    replacement?: StatsBombEventActor;
  };
  bad_behaviour?: {
    card?: StatsBombEventTypeRef;
  };
  foul_committed?: {
    card?: StatsBombEventTypeRef;
  };
}

export interface StatsBombThreeSixtyFreezeFrame {
  teammate?: boolean;
  actor?: boolean;
  keeper?: boolean;
  location?: number[];
}

export interface StatsBombThreeSixtyEntry {
  event_uuid: string;
  visible_area?: number[];
  freeze_frame?: StatsBombThreeSixtyFreezeFrame[];
}

export interface TranslationDraft {
  locale: 'en';
  name: string;
  shortName?: string;
}

export function buildStatsBombRawUrl(path: string) {
  return `${STATSBOMB_OPEN_DATA_BASE_URL}/${path.replace(/^\/+/, '')}`;
}

export async function fetchStatsBombJson<T>(path: string): Promise<T> {
  const response = await fetch(buildStatsBombRawUrl(path), {
    headers: {
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch StatsBomb data: ${path} (${response.status})`);
  }

  return (await response.json()) as T;
}

export function createStatsBombSlug(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/-{2,}/g, '-');
}

export function createCountryCode(value: string) {
  const normalized = createStatsBombSlug(value).replace(/-/g, '');
  return normalized.slice(0, 3).toUpperCase().padEnd(3, 'X');
}

function getCanonicalCompetitionSlug(name: string) {
  const normalizedName = name.trim().toLowerCase();
  const canonicalSlugMap: Record<string, string> = {
    'uefa champions league': 'champions-league',
    'uefa europa league': 'europa-league',
  };
  return canonicalSlugMap[normalizedName];
}

export function createCompetitionSlug(entry: Pick<StatsBombCompetitionEntry, 'competition_name' | 'competition_gender' | 'competition_youth' | 'competition_international'>) {
  const canonicalSlug = getCanonicalCompetitionSlug(entry.competition_name);
  if (canonicalSlug && entry.competition_gender !== 'female' && !entry.competition_youth) {
    return canonicalSlug;
  }

  const parts = [entry.competition_name];

  if (entry.competition_international) {
    parts.push('international');
  }

  if (entry.competition_youth) {
    parts.push('youth');
  }

  if (entry.competition_gender === 'female') {
    parts.push('women');
  }

  return createStatsBombSlug(parts.join(' '));
}

export function createSeasonSlug(seasonName: string, seasonId?: number | string) {
  const base = seasonName.trim().replace(/\s+/g, '').replace(/-/g, '/');
  return seasonId === undefined ? base : `${base}-${seasonId}`;
}

export function createTeamSlug(teamName: string, countryName?: string) {
  const base = createStatsBombSlug(teamName);

  if (!countryName) {
    return base;
  }

  const normalizedCountry = createStatsBombSlug(countryName);

  if (!normalizedCountry || normalizedCountry === base) {
    return base;
  }

  return createStatsBombSlug(`${teamName} ${countryName}`);
}

export function createTranslationDraft(name: string, shortName?: string): TranslationDraft {
  return {
    locale: 'en',
    name,
    shortName,
  };
}

export function buildCompetitionSeasonManifest(entry: StatsBombCompetitionEntry): StatsBombCompetitionSeasonManifest {
  return {
    sourceCompetitionId: String(entry.competition_id),
    sourceSeasonId: String(entry.season_id),
    competitionSlug: createCompetitionSlug(entry),
    seasonSlug: createSeasonSlug(entry.season_name, entry.season_id),
    countryCode: createCountryCode(entry.country_name),
    competitionName: entry.competition_name,
    seasonName: entry.season_name,
    competitionGender: entry.competition_gender,
    isYouth: entry.competition_youth,
    isInternational: entry.competition_international,
    matchUpdatedAt: entry.match_updated,
    matchAvailableAt: entry.match_available,
    matchUpdated360At: entry.match_updated_360,
    matchAvailable360At: entry.match_available_360,
  };
}

export function buildMatchManifest(entry: StatsBombMatchEntry): StatsBombMatchManifest {
  return {
    sourceMatchId: String(entry.match_id),
    sourceCompetitionId: String(entry.competition.competition_id),
    sourceSeasonId: String(entry.season.season_id),
    sourceHomeTeamId: String(entry.home_team.home_team_id),
    sourceAwayTeamId: String(entry.away_team.away_team_id),
    competitionSlug: getCanonicalCompetitionSlug(entry.competition.competition_name)
      ?? createStatsBombSlug(entry.competition.competition_name),
    seasonSlug: createSeasonSlug(entry.season.season_name, entry.season.season_id),
    homeTeamSlug: createTeamSlug(entry.home_team.home_team_name, entry.home_team.country?.name),
    awayTeamSlug: createTeamSlug(entry.away_team.away_team_name, entry.away_team.country?.name),
    matchDate: entry.match_date,
    kickOff: entry.kick_off,
    matchWeek: entry.match_week,
    lastUpdatedAt: entry.last_updated,
    lastUpdated360At: entry.last_updated_360,
    dataVersion: entry.metadata?.data_version ?? null,
    shotFidelityVersion: entry.metadata?.shot_fidelity_version ?? null,
    xyFidelityVersion: entry.metadata?.xy_fidelity_version ?? null,
  };
}

export async function listCompetitionSeasons() {
  const entries = await fetchStatsBombJson<StatsBombCompetitionEntry[]>('data/competitions.json');
  return entries.map(buildCompetitionSeasonManifest);
}

export async function listMatches(competitionId: number | string, seasonId: number | string) {
  const entries = await fetchStatsBombJson<StatsBombMatchEntry[]>(`data/matches/${competitionId}/${seasonId}.json`);
  return entries.map(buildMatchManifest);
}

export async function getMatchLineups(matchId: number | string) {
  return fetchStatsBombJson<StatsBombLineupEntry[]>(`data/lineups/${matchId}.json`);
}

export async function getMatchEvents(matchId: number | string) {
  return fetchStatsBombJson<StatsBombEventEntry[]>(`data/events/${matchId}.json`);
}

export async function getMatchThreeSixty(matchId: number | string) {
  return fetchStatsBombJson<StatsBombThreeSixtyEntry[]>(`data/three-sixty/${matchId}.json`);
}
