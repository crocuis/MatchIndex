import { leagueLogoMap } from './entityImages.generated.ts';
import { deriveCompetitionType } from './competitionTypes';
import type { League } from './types';

export const baseLeagues: League[] = [
  {
    id: 'pl',
    name: 'Premier League',
    country: 'England',
    season: '2025/26',
    logo: 'https://assets.football-logos.cc/logos/england/256x256/english-premier-league.d2af2256.png',
    numberOfClubs: 20,
    competitionType: deriveCompetitionType('pl', 'Premier League'),
  },
  {
    id: 'laliga',
    name: 'La Liga',
    country: 'Spain',
    season: '2025/26',
    logo: 'https://assets.football-logos.cc/logos/spain/256x256/la-liga.f431b6b0.png',
    numberOfClubs: 20,
    competitionType: deriveCompetitionType('laliga', 'La Liga'),
  },
  {
    id: 'bundesliga',
    name: 'Bundesliga',
    country: 'Germany',
    season: '2025/26',
    numberOfClubs: 4,
    competitionType: deriveCompetitionType('bundesliga', 'Bundesliga'),
  },
  {
    id: 'seriea',
    name: 'Serie A',
    country: 'Italy',
    season: '2025/26',
    numberOfClubs: 4,
    competitionType: deriveCompetitionType('seriea', 'Serie A'),
  },
  {
    id: 'ligue1',
    name: 'Ligue 1',
    country: 'France',
    season: '2025/26',
    numberOfClubs: 4,
    competitionType: deriveCompetitionType('ligue1', 'Ligue 1'),
  },
  {
    id: 'championship',
    name: 'EFL Championship',
    country: 'England',
    season: '2025/26',
    numberOfClubs: 4,
    competitionType: deriveCompetitionType('championship', 'EFL Championship'),
  },
  {
    id: 'eredivisie',
    name: 'Eredivisie',
    country: 'Netherlands',
    season: '2025/26',
    numberOfClubs: 4,
    competitionType: deriveCompetitionType('eredivisie', 'Eredivisie'),
  },
  {
    id: 'ucl',
    name: 'UEFA Champions League',
    country: 'Europe',
    season: '2025/26',
    numberOfClubs: 4,
    competitionType: deriveCompetitionType('ucl', 'UEFA Champions League'),
  },
  {
    id: 'uel',
    name: 'UEFA Europa League',
    country: 'Europe',
    season: '2025/26',
    numberOfClubs: 4,
    competitionType: deriveCompetitionType('uel', 'UEFA Europa League'),
  },
  {
    id: 'cwc',
    name: 'FIFA Club World Cup',
    country: 'World',
    season: '2025',
    numberOfClubs: 4,
    competitionType: deriveCompetitionType('cwc', 'FIFA Club World Cup'),
  },
];

export const leagues: League[] = baseLeagues.map((league) => ({
  ...league,
  logo: leagueLogoMap[league.id] ?? league.logo,
}));
