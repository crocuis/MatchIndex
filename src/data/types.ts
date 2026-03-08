// MatchIndex — Domain Models
// All interfaces are API-ready: replace mock data with fetch calls without changing consumers.

export interface League {
  id: string;
  name: string;
  country: string;
  season: string;
  logo?: string;
  numberOfClubs: number;
}

export interface Club {
  id: string;
  name: string;
  shortName: string;
  country: string;
  founded: number;
  stadium: string;
  stadiumCapacity: number;
  leagueId: string;
  logo?: string;
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
  clubId: string;
  position: 'GK' | 'DEF' | 'MID' | 'FWD';
  shirtNumber: number;
  height: number; // cm
  preferredFoot: 'Left' | 'Right' | 'Both';
  seasonStats: PlayerSeasonStats;
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

export interface Nation {
  id: string;
  name: string;
  code: string; // ISO 3166-1 alpha-3
  confederation: string;
  fifaRanking: number;
  flag?: string;
}

export interface Match {
  id: string;
  homeTeamId: string;
  awayTeamId: string;
  homeScore: number | null;
  awayScore: number | null;
  date: string; // ISO date
  time: string; // HH:mm
  venue: string;
  leagueId: string;
  status: 'scheduled' | 'live' | 'finished';
  events?: MatchEvent[];
  stats?: MatchStats;
}

export interface MatchEvent {
  minute: number;
  type: 'goal' | 'yellow_card' | 'red_card' | 'substitution';
  playerId: string;
  teamId: string;
  detail?: string;
}

export interface MatchStats {
  possession: [number, number]; // [home, away]
  shots: [number, number];
  shotsOnTarget: [number, number];
  corners: [number, number];
  fouls: [number, number];
}

export interface StandingRow {
  position: number;
  clubId: string;
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

// Utility types for data access
export type EntityType = 'player' | 'club' | 'league' | 'nation';

export interface SearchResult {
  type: EntityType;
  id: string;
  name: string;
  subtitle: string; // e.g., "Manchester City · FWD" or "Premier League · England"
}
