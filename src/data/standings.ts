import type { StandingRow } from './types';

export const standings: Record<string, StandingRow[]> = {
  pl: [
    { position: 1, clubId: 'liverpool', played: 30, won: 22, drawn: 5, lost: 3, goalsFor: 68, goalsAgainst: 25, goalDifference: 43, points: 71, form: ['W', 'D', 'W', 'W', 'D'] },
    { position: 2, clubId: 'arsenal', played: 30, won: 21, drawn: 5, lost: 4, goalsFor: 62, goalsAgainst: 22, goalDifference: 40, points: 68, form: ['W', 'W', 'L', 'W', 'D'] },
    { position: 3, clubId: 'mancity', played: 30, won: 18, drawn: 6, lost: 6, goalsFor: 58, goalsAgainst: 30, goalDifference: 28, points: 60, form: ['L', 'W', 'D', 'W', 'L'] },
    { position: 4, clubId: 'chelsea', played: 30, won: 16, drawn: 7, lost: 7, goalsFor: 52, goalsAgainst: 35, goalDifference: 17, points: 55, form: ['D', 'L', 'W', 'W', 'D'] },
  ],
  laliga: [
    { position: 1, clubId: 'barcelona', played: 30, won: 24, drawn: 3, lost: 3, goalsFor: 75, goalsAgainst: 20, goalDifference: 55, points: 75, form: ['W', 'W', 'W', 'W', 'W'] },
    { position: 2, clubId: 'realmadrid', played: 30, won: 21, drawn: 5, lost: 4, goalsFor: 65, goalsAgainst: 28, goalDifference: 37, points: 68, form: ['W', 'D', 'W', 'L', 'W'] },
    { position: 3, clubId: 'atletico', played: 30, won: 19, drawn: 6, lost: 5, goalsFor: 50, goalsAgainst: 24, goalDifference: 26, points: 63, form: ['W', 'L', 'D', 'W', 'W'] },
    { position: 4, clubId: 'sevilla', played: 30, won: 10, drawn: 8, lost: 12, goalsFor: 35, goalsAgainst: 42, goalDifference: -7, points: 38, form: ['L', 'L', 'D', 'W', 'L'] },
  ],
};
