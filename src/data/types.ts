// MatchIndex — Domain Models
// All interfaces are API-ready: replace mock data with fetch calls without changing consumers.

export interface League {
  id: string;
  name: string;
  country: string;
  season: string;
  gender?: 'male' | 'female' | 'mixed';
  logo?: string;
  numberOfClubs: number;
  competitionType: 'league' | 'tournament';
}

export interface Club {
  id: string;
  name: string;
  shortName: string;
  country: string;
  gender?: 'male' | 'female' | 'mixed';
  founded: number;
  stadium: string;
  stadiumCapacity: number;
  leagueId: string;
  logo?: string;
}

export interface ClubListItem extends Club {
  leagueName?: string;
}

export interface ClubSeasonHistoryEntry {
  seasonId: string;
  seasonLabel: string;
  leagueId: string;
  leagueName: string;
  position?: number;
  played: number;
  won: number;
  drawn: number;
  lost: number;
  goalsFor: number;
  goalsAgainst: number;
  goalDifference: number;
  points: number;
  form: ('W' | 'D' | 'L')[];
}

export interface Player {
  id: string;
  name: string;
  firstName: string;
  lastName: string;
  dateOfBirth: string;
  age: number;
  nationality: string;
  nationId: string;
  gender?: 'male' | 'female' | 'mixed';
  clubId: string;
  position: 'GK' | 'DEF' | 'MID' | 'FWD';
  photoUrl?: string;
  shirtNumber: number;
  height: number; // cm
  preferredFoot: 'Left' | 'Right' | 'Both';
  isRetired?: boolean;
  contract?: PlayerContract;
  scoutingReport?: PlayerScoutingReport;
  seasonStats: PlayerSeasonStats;
  seasonHistory?: PlayerSeasonHistoryEntry[];
  clubHistory?: PlayerClubHistoryEntry[];
}

export interface PlayerListItem extends Player {
  clubName?: string;
  clubShortName?: string;
  clubLogo?: string;
  nationName?: string;
  nationCode?: string;
  nationFlag?: string;
}

export interface PlayerContract {
  startDate?: string;
  endDate?: string;
  annualSalary?: number;
  weeklyWage?: number;
  currencyCode?: string;
  source?: string;
  sourceUrl?: string;
  isEstimated?: boolean;
  marketValue?: {
    min: number;
    max: number;
  };
}

export interface PlayerScoutingReport {
  role: string;
  summary: string;
  strengths: string[];
  weaknesses: string[];
}

export interface PlayerSeasonStats {
  appearances: number;
  goals: number;
  assists: number;
  minutesPlayed: number;
  yellowCards: number;
  redCards: number;
  cleanSheets?: number; // goalkeepers only
}

export interface PlayerSeasonHistoryEntry {
  seasonId: string;
  seasonLabel: string;
  clubId: string;
  clubName: string;
  appearances: number;
  goals: number;
  assists: number;
  minutesPlayed: number;
  yellowCards: number;
  redCards: number;
  cleanSheets?: number;
}

export interface PlayerClubHistoryEntry {
  clubId: string;
  clubName: string;
  startYear: number;
  endYear: number;
  periodLabel: string;
}

export type PhotoSyncProvider = 'api_football' | 'sofascore' | 'wikimedia';

export type PlayerPhotoSourceStatus = 'active' | 'broken' | 'pending' | 'skipped';

export interface PlayerPhotoSource {
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
  failureCount: number;
  lastError?: string;
}

export interface PlayerPhotoSyncTarget {
  id: string;
  name: string;
  firstName: string;
  lastName: string;
  dateOfBirth: string;
  nationality: string;
  photoUrl?: string;
}

export interface Nation {
  id: string;
  name: string;
  code: string; // ISO 3166-1 alpha-3
  confederation: string;
  rankingCategory?: 'men' | 'women';
  crest?: string;
  previousFifaRanking?: number;
  rankingChange?: number;
  fifaRanking: number;
  flag?: string;
  recentTournaments?: NationTournamentRecord[];
}

export interface NationListItem extends Nation {
  playerCount: number;
}

export interface PaginatedResult<T> {
  items: T[];
  totalCount: number;
  currentPage: number;
  pageSize: number;
  totalPages: number;
}

export interface NationTournamentRecord {
  competition: string;
  year: string;
  result: string;
}

export interface WorldCupGroupStanding {
  position: number;
  nationId: string;
  nationName?: string;
  nationCode?: string;
  participant?: ResolvedParticipant;
  played: number;
  won: number;
  drawn: number;
  lost: number;
  goalsFor: number;
  goalsAgainst: number;
  goalDifference: number;
  points: number;
  form: ('W' | 'D' | 'L')[];
}

export interface WorldCupGroup {
  id: string;
  name: string;
  standings: WorldCupGroupStanding[];
}

export interface WorldCupSpotlight {
  nationId: string;
  playerId: string;
  note: string;
}

export interface WorldCupStage {
  name: string;
  matchIds: string[];
}

export interface TournamentSlotCandidate {
  nationId?: string;
  name: string;
  note?: string;
}

export type TournamentSlotSource =
  | {
    kind: 'manual';
    resolvedParticipant?: {
      entityId: string;
      displayName?: string;
      displayCode?: string;
    };
  }
  | {
    kind: 'groupPlacement';
    groupId: string;
    position: number;
  }
  | {
    kind: 'groupPoolPlacement';
    groupIds: string[];
    position: number;
  }
  | {
    kind: 'matchOutcome';
    matchId: string;
    outcome: 'winner' | 'loser';
  };

export interface TournamentSlot {
  id: string;
  label: string;
  entityType?: 'nation' | 'club';
  source: TournamentSlotSource;
  confederation?: string;
  resolvedOn?: string;
  description?: string;
  candidates: TournamentSlotCandidate[];
}

export type WorldCupPlaceholderCandidate = TournamentSlotCandidate;

export type WorldCupPlaceholder = TournamentSlot;

export interface ResolvedParticipant {
  sourceId: string;
  entityType: 'nation' | 'club';
  status: 'resolved' | 'unresolved';
  entityId?: string;
  displayName: string;
  displayCode?: string;
  slot?: TournamentSlot;
}

export interface WorldCupTournament {
  year: string;
  host: string;
  subtitle: string;
  groups: WorldCupGroup[];
  stages: WorldCupStage[];
  spotlights: WorldCupSpotlight[];
  matches: Match[];
  slots?: TournamentSlot[];
  placeholders?: WorldCupPlaceholder[];
}

export interface Match {
  id: string;
  homeTeamId: string;
  awayTeamId: string;
  homeTeamName?: string;
  awayTeamName?: string;
  homeTeamCode?: string;
  awayTeamCode?: string;
  homeTeamLogo?: string;
  awayTeamLogo?: string;
  homeParticipant?: ResolvedParticipant;
  awayParticipant?: ResolvedParticipant;
  homeScore: number | null;
  awayScore: number | null;
  date: string; // ISO date
  time: string; // HH:mm
  venue: string;
  leagueId: string;
  matchWeek?: number;
  stage?: string;
  groupName?: string;
  competitionName?: string;
  teamType?: 'club' | 'nation';
  status: 'scheduled' | 'live' | 'finished';
  events?: MatchEvent[];
  stats?: MatchStats;
}

export type MatchAnalysisEventType =
  | 'pass'
  | 'shot'
  | 'carry'
  | 'pressure'
  | 'ball_receipt'
  | 'clearance'
  | 'interception'
  | 'block'
  | 'ball_recovery'
  | 'foul_won'
  | 'foul_committed'
  | 'duel'
  | 'miscontrol'
  | 'goalkeeper'
  | 'offside'
  | 'dribble'
  | 'dispossessed'
  | 'goal'
  | 'own_goal'
  | 'penalty_scored'
  | 'penalty_missed'
  | 'yellow_card'
  | 'red_card'
  | 'yellow_red_card'
  | 'substitution'
  | 'var_decision';

export interface MatchEvent {
  sourceEventId?: string;
  minute: number;
  type: 'goal' | 'yellow_card' | 'red_card' | 'substitution';
  rawType?: MatchAnalysisEventType;
  playerId: string;
  playerName?: string;
  teamId: string;
  secondaryPlayerId?: string;
  secondaryPlayerName?: string;
  assistPlayerId?: string;
  assistPlayerName?: string;
  detail?: string;
}

export interface MatchAnalysisEvent {
  id: string;
  minute: number;
  second: number | null;
  type: MatchAnalysisEventType;
  teamId: string;
  playerId?: string;
  playerName?: string;
  secondaryPlayerId?: string;
  secondaryPlayerName?: string;
  locationX?: number;
  locationY?: number;
  endLocationX?: number;
  endLocationY?: number;
  endLocationZ?: number;
  underPressure: boolean;
  statsbombXg?: number;
  outcome?: string;
  detail?: string;
}

export interface MatchAnalysisData {
  events: MatchAnalysisEvent[];
}

export interface MatchStats {
  possession: [number, number]; // [home, away]
  shots: [number, number];
  shotsOnTarget: [number, number];
  corners: [number, number];
  fouls: [number, number];
}

export interface MatchLineup {
  teamId: string;
  playerId: string;
  playerName: string;
  shirtNumber?: number;
  position?: string;
  isStarter: boolean;
}

export interface StandingRow {
  position: number;
  clubId: string;
   clubName?: string;
   clubShortName?: string;
   clubLogo?: string;
  played: number;
  won: number;
  drawn: number;
  lost: number;
  goalsFor: number;
  goalsAgainst: number;
  goalDifference: number;
  points: number;
  form: ('W' | 'D' | 'L')[];
}

export interface StatLeader {
  playerId: string;
  clubId: string;
  leagueId: string;
  goals: number;
  assists: number;
}

export interface LeagueSeasonEntry {
  seasonId: string;
  seasonLabel: string;
  isCurrent: boolean;
}

// Utility types for data access
export type EntityType = 'player' | 'club' | 'league' | 'nation';

export interface SearchResult {
  type: EntityType;
  id: string;
  name: string;
  subtitle: string; // e.g., "Manchester City · FWD" or "Premier League · England"
  gender?: 'male' | 'female' | 'mixed';
}
