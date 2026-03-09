import { nationFlagMap } from './entityImages.generated.ts';
import { getNationBadgeUrl } from './nationVisuals';
import type { Nation } from './types';

export const baseNations: Nation[] = [
  {
    id: 'eng',
    name: 'England',
    code: 'ENG',
    confederation: 'UEFA',
    fifaRanking: 0,
    flag: 'https://flagcdn.com/gb-eng.svg',
    recentTournaments: [
      { competition: 'Euro', year: '2024', result: 'Runner-up' },
      { competition: 'World Cup', year: '2022', result: 'Quarter-finals' },
      { competition: 'Nations League', year: '2022/23', result: 'Group Stage' },
    ],
  },
  {
    id: 'esp',
    name: 'Spain',
    code: 'ESP',
    confederation: 'UEFA',
    fifaRanking: 0,
    flag: 'https://flagcdn.com/es.svg',
    recentTournaments: [
      { competition: 'Euro', year: '2024', result: 'Champions' },
      { competition: 'World Cup', year: '2022', result: 'Round of 16' },
      { competition: 'Nations League', year: '2022/23', result: 'Champions' },
    ],
  },
  {
    id: 'fra',
    name: 'France',
    code: 'FRA',
    confederation: 'UEFA',
    fifaRanking: 0,
    flag: 'https://flagcdn.com/fr.svg',
    recentTournaments: [
      { competition: 'Euro', year: '2024', result: 'Semi-finals' },
      { competition: 'World Cup', year: '2022', result: 'Runner-up' },
      { competition: 'Nations League', year: '2022/23', result: 'Quarter-finals' },
    ],
  },
  {
    id: 'bra',
    name: 'Brazil',
    code: 'BRA',
    confederation: 'CONMEBOL',
    fifaRanking: 0,
    flag: 'https://flagcdn.com/br.svg',
    recentTournaments: [
      { competition: 'Copa America', year: '2024', result: 'Quarter-finals' },
      { competition: 'World Cup', year: '2022', result: 'Quarter-finals' },
      { competition: 'Copa America', year: '2021', result: 'Runner-up' },
    ],
  },
  {
    id: 'ger',
    name: 'Germany',
    code: 'GER',
    confederation: 'UEFA',
    fifaRanking: 0,
    flag: 'https://flagcdn.com/de.svg',
  },
  {
    id: 'ita',
    name: 'Italy',
    code: 'ITA',
    confederation: 'UEFA',
    fifaRanking: 0,
    flag: 'https://flagcdn.com/it.svg',
  },
  {
    id: 'ned',
    name: 'Netherlands',
    code: 'NED',
    confederation: 'UEFA',
    fifaRanking: 0,
    flag: 'https://flagcdn.com/nl.svg',
  },
  {
    id: 'por',
    name: 'Portugal',
    code: 'POR',
    confederation: 'UEFA',
    fifaRanking: 0,
    flag: 'https://flagcdn.com/pt.svg',
  },
  {
    id: 'sau',
    name: 'Saudi Arabia',
    code: 'KSA',
    confederation: 'AFC',
    fifaRanking: 0,
    flag: 'https://flagcdn.com/sa.svg',
  },
];

export const nations: Nation[] = baseNations.map((nation) => ({
  ...nation,
  flag: nation.flag ?? nationFlagMap[nation.id],
  crest: getNationBadgeUrl(nation.code),
}));
