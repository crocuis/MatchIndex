import type { Match, WorldCupTournament } from './types';

export const worldCup2026: WorldCupTournament = {
  year: '2026',
  host: 'United States · Canada · Mexico',
  subtitle: 'Expanded finals mock overview with group tables, knockout schedule, and nation spotlights.',
  placeholders: [
    {
      id: 'uefa-path-a-winner',
      label: 'UEFA Path A winner',
      source: { kind: 'manual' },
      confederation: 'UEFA',
      resolvedOn: '2026-03-31',
      description: 'UEFA playoff path A winner. Single-leg semi-finals on March 26, then the path final on March 31.',
      candidates: [{ name: 'Italy' }, { name: 'Northern Ireland' }, { name: 'Wales' }, { name: 'Bosnia-Herzegovina' }],
    },
    {
      id: 'uefa-path-b-winner',
      label: 'UEFA Path B winner',
      source: { kind: 'manual' },
      confederation: 'UEFA',
      resolvedOn: '2026-03-31',
      description: 'UEFA playoff path B winner. Single-leg semi-finals on March 26, then the path final on March 31.',
      candidates: [{ name: 'Ukraine' }, { name: 'Sweden' }, { name: 'Poland' }, { name: 'Albania' }],
    },
    {
      id: 'uefa-path-c-winner',
      label: 'UEFA Path C winner',
      source: { kind: 'manual' },
      confederation: 'UEFA',
      resolvedOn: '2026-03-31',
      description: 'UEFA playoff path C winner. Single-leg semi-finals on March 26, then the path final on March 31.',
      candidates: [{ name: 'Turkey' }, { name: 'Romania' }, { name: 'Slovakia' }, { name: 'Kosovo' }],
    },
    {
      id: 'uefa-path-d-winner',
      label: 'UEFA Path D winner',
      source: { kind: 'manual' },
      confederation: 'UEFA',
      resolvedOn: '2026-03-31',
      description: 'UEFA playoff path D winner. Single-leg semi-finals on March 26, then the path final on March 31.',
      candidates: [{ name: 'Denmark' }, { name: 'North Macedonia' }, { name: 'Czechia' }, { name: 'Republic of Ireland' }],
    },
    {
      id: 'ic-path-1-winner',
      label: 'IC Path 1 winner',
      source: { kind: 'manual' },
      confederation: 'Intercontinental Playoff',
      resolvedOn: '2026-03-31',
      description: 'Intercontinental playoff path 1 winner. Seeded final entrant plus one semifinal winner qualify path champion on March 31.',
      candidates: [{ name: 'DR Congo' }, { name: 'Jamaica' }, { name: 'New Caledonia' }],
    },
    {
      id: 'ic-path-2-winner',
      label: 'IC Path 2 winner',
      source: { kind: 'manual' },
      confederation: 'Intercontinental Playoff',
      resolvedOn: '2026-03-31',
      description: 'Intercontinental playoff path 2 winner. Seeded final entrant plus one semifinal winner qualify path champion on March 31.',
      candidates: [{ name: 'Iraq' }, { name: 'Bolivia' }, { name: 'Suriname' }],
    },
  ],
  matches: [
    {
      id: 'm-wc26-01',
      homeTeamId: 'eng',
      awayTeamId: 'esp',
      homeScore: 2,
      awayScore: 1,
      date: '2026-07-02',
      time: '19:00',
      venue: 'MetLife Stadium',
      leagueId: 'worldcup2026',
      competitionName: '2026 FIFA World Cup',
      teamType: 'nation',
      status: 'finished',
      events: [
        { minute: 14, type: 'goal', playerId: 'p-bay-1', teamId: 'eng', detail: 'Near-post finish' },
        { minute: 44, type: 'goal', playerId: 'p-bar-4', teamId: 'esp', detail: 'Left-foot curler' },
        { minute: 81, type: 'goal', playerId: 'p-bay-1', teamId: 'eng', detail: 'Penalty' },
      ],
      stats: { possession: [46, 54], shots: [11, 14], shotsOnTarget: [5, 4], corners: [4, 7], fouls: [12, 10] },
    },
    {
      id: 'm-wc26-02',
      homeTeamId: 'fra',
      awayTeamId: 'ned',
      homeScore: 3,
      awayScore: 1,
      date: '2026-07-03',
      time: '21:00',
      venue: 'AT&T Stadium',
      leagueId: 'worldcup2026',
      competitionName: '2026 FIFA World Cup',
      teamType: 'nation',
      status: 'finished',
      events: [
        { minute: 22, type: 'goal', playerId: 'p-rma-5', teamId: 'fra', detail: 'Counter attack' },
        { minute: 51, type: 'goal', playerId: 'p-lee-1', teamId: 'ned', detail: 'Cut-back finish' },
        { minute: 64, type: 'goal', playerId: 'p-rma-5', teamId: 'fra', detail: 'Breakaway' },
        { minute: 88, type: 'goal', playerId: 'p-rma-5', teamId: 'fra', detail: 'Open-net tap in' },
      ],
      stats: { possession: [52, 48], shots: [15, 9], shotsOnTarget: [7, 3], corners: [6, 4], fouls: [11, 13] },
    },
    {
      id: 'm-wc26-03',
      homeTeamId: 'ger',
      awayTeamId: 'ita',
      homeScore: null,
      awayScore: null,
      date: '2026-07-06',
      time: '20:00',
      venue: 'SoFi Stadium',
      leagueId: 'worldcup2026',
      competitionName: '2026 FIFA World Cup',
      teamType: 'nation',
      status: 'scheduled',
    },
    {
      id: 'm-wc26-04',
      homeTeamId: 'bra',
      awayTeamId: 'por',
      homeScore: null,
      awayScore: null,
      date: '2026-07-07',
      time: '18:00',
      venue: 'Estadio Azteca',
      leagueId: 'worldcup2026',
      competitionName: '2026 FIFA World Cup',
      teamType: 'nation',
      status: 'scheduled',
    },
    {
      id: 'm-wc26-05',
      homeTeamId: 'eng',
      awayTeamId: 'fra',
      homeScore: null,
      awayScore: null,
      date: '2026-07-10',
      time: '20:00',
      venue: 'Mercedes-Benz Stadium',
      leagueId: 'worldcup2026',
      competitionName: '2026 FIFA World Cup',
      teamType: 'nation',
      status: 'scheduled',
    },
    {
      id: 'm-wc26-06',
      homeTeamId: 'eng',
      awayTeamId: 'ger',
      homeScore: null,
      awayScore: null,
      date: '2026-07-13',
      time: '20:00',
      venue: 'MetLife Stadium',
      leagueId: 'worldcup2026',
      competitionName: '2026 FIFA World Cup',
      teamType: 'nation',
      status: 'scheduled',
    },
  ],
  groups: [
    {
      id: 'group-a',
      name: 'Group A',
      standings: [
        { position: 1, nationId: 'eng', played: 3, won: 2, drawn: 1, lost: 0, goalsFor: 6, goalsAgainst: 2, goalDifference: 4, points: 7, form: ['W', 'D', 'W'] },
        { position: 2, nationId: 'ned', played: 3, won: 2, drawn: 0, lost: 1, goalsFor: 5, goalsAgainst: 3, goalDifference: 2, points: 6, form: ['W', 'L', 'W'] },
        { position: 3, nationId: 'ger', played: 3, won: 1, drawn: 1, lost: 1, goalsFor: 4, goalsAgainst: 4, goalDifference: 0, points: 4, form: ['D', 'W', 'L'] },
        { position: 4, nationId: 'bra', played: 3, won: 0, drawn: 0, lost: 3, goalsFor: 2, goalsAgainst: 8, goalDifference: -6, points: 0, form: ['L', 'L', 'L'] },
      ],
    },
    {
      id: 'group-b',
      name: 'Group B',
      standings: [
        { position: 1, nationId: 'fra', played: 3, won: 2, drawn: 1, lost: 0, goalsFor: 7, goalsAgainst: 2, goalDifference: 5, points: 7, form: ['W', 'W', 'D'] },
        { position: 2, nationId: 'esp', played: 3, won: 2, drawn: 0, lost: 1, goalsFor: 6, goalsAgainst: 3, goalDifference: 3, points: 6, form: ['W', 'L', 'W'] },
        { position: 3, nationId: 'ita', played: 3, won: 1, drawn: 1, lost: 1, goalsFor: 3, goalsAgainst: 4, goalDifference: -1, points: 4, form: ['L', 'D', 'W'] },
        { position: 4, nationId: 'por', played: 3, won: 0, drawn: 0, lost: 3, goalsFor: 1, goalsAgainst: 8, goalDifference: -7, points: 0, form: ['L', 'L', 'L'] },
      ],
    },
  ],
  stages: [
    {
      name: 'Round of 16',
      matchIds: ['m-wc26-01', 'm-wc26-02'],
    },
    {
      name: 'Quarter-finals',
      matchIds: ['m-wc26-03', 'm-wc26-04'],
    },
    {
      name: 'Semi-finals',
      matchIds: ['m-wc26-05'],
    },
    {
      name: 'Final',
      matchIds: ['m-wc26-06'],
    },
  ],
  spotlights: [
    { nationId: 'eng', playerId: 'p-bay-1', note: 'Transitions through central zones and remains the most reliable box finisher.' },
    { nationId: 'ned', playerId: 'p-lee-1', note: 'Gives direct running behind the back line and set-piece threat.' },
    { nationId: 'ger', playerId: 'p-b04-1', note: 'Primary creative outlet between the lines and the tempo setter in possession.' },
    { nationId: 'bra', playerId: 'p-rma-cwc-1', note: 'Best 1v1 carrier and the quickest route to territory gains on the left.' },
    { nationId: 'fra', playerId: 'p-rma-5', note: 'Still the highest-value final-third runner and the reference point in transition.' },
    { nationId: 'esp', playerId: 'p-bar-4', note: 'Breaks settled blocks with wide isolation and early-final-ball creation.' },
    { nationId: 'ita', playerId: 'p-int-1', note: 'Key penalty-box presence and the side most likely to turn low shot volume into goals.' },
    { nationId: 'por', playerId: 'p-mun-uel-1', note: 'Controls rhythm, chance volume, and dead-ball delivery from midfield.' },
  ],
};

function isFormValue(value: unknown): value is 'W' | 'D' | 'L' {
  return value === 'W' || value === 'D' || value === 'L';
}

function isStringRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isTournamentSlotSource(value: unknown) {
  if (!isStringRecord(value) || typeof value.kind !== 'string') {
    return false;
  }

  switch (value.kind) {
    case 'manual':
      return value.resolvedParticipant === undefined || (
        isStringRecord(value.resolvedParticipant)
        && typeof value.resolvedParticipant.entityId === 'string'
        && (typeof value.resolvedParticipant.displayName === 'string' || value.resolvedParticipant.displayName === undefined)
        && (typeof value.resolvedParticipant.displayCode === 'string' || value.resolvedParticipant.displayCode === undefined)
      );
    case 'groupPlacement':
      return typeof value.groupId === 'string' && typeof value.position === 'number';
    case 'groupPoolPlacement':
      return Array.isArray(value.groupIds)
        && value.groupIds.every((groupId) => typeof groupId === 'string')
        && typeof value.position === 'number';
    case 'matchOutcome':
      return typeof value.matchId === 'string' && (value.outcome === 'winner' || value.outcome === 'loser');
    default:
      return false;
  }
}

function isTournamentSlot(value: unknown) {
  return isStringRecord(value)
    && typeof value.id === 'string'
    && typeof value.label === 'string'
    && isTournamentSlotSource(value.source)
    && (value.entityType === 'nation' || value.entityType === 'club' || value.entityType === undefined)
    && (typeof value.confederation === 'string' || value.confederation === undefined)
    && (typeof value.resolvedOn === 'string' || value.resolvedOn === undefined)
    && (typeof value.description === 'string' || value.description === undefined)
    && Array.isArray(value.candidates)
    && value.candidates.every((candidate) =>
      isStringRecord(candidate)
        && typeof candidate.name === 'string'
        && (typeof candidate.nationId === 'string' || candidate.nationId === undefined)
        && (typeof candidate.note === 'string' || candidate.note === undefined)
    );
}

function isMatch(value: unknown): value is Match {
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

export function isWorldCupTournament(value: unknown): value is WorldCupTournament {
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
    && (value.slots === undefined || (Array.isArray(value.slots) && value.slots.every(isTournamentSlot)))
    && (value.placeholders === undefined || (Array.isArray(value.placeholders) && value.placeholders.every(isTournamentSlot)))
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
    && value.matches.every(isMatch);
}

export function normalizeWorldCupTournament(value: unknown): WorldCupTournament | null {
  if (!isStringRecord(value)) return null;

  const tournament = isStringRecord(value.tournament) ? { ...value.tournament, matches: value.matches } : value;

  if (!isWorldCupTournament(tournament)) return null;

  return tournament;
}
