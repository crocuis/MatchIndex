import type { Club, Match, StandingRow } from '@/data/types';
import { getMatchTimelineDb } from '@/data/server';

export interface TournamentGroupView {
  id: string;
  name: string;
  standings: StandingRow[];
}

export interface KnockoutStageView {
  name: string;
  matches: Match[];
}

export interface TournamentMatchStageView {
  id: string;
  name: string;
  matches: Match[];
}

export type ChampionsLeagueFormat = 'legacy' | 'league-phase';

interface GroupStandingAccumulator {
  clubId: string;
  clubName: string;
  clubShortName: string;
  clubLogo?: string;
  played: number;
  won: number;
  drawn: number;
  lost: number;
  goalsFor: number;
  goalsAgainst: number;
  points: number;
  form: Array<'W' | 'D' | 'L'>;
}

const KNOCKOUT_STAGE_ORDER = ['knockout play-offs', 'round of 16', 'quarter-finals', 'semi-finals', 'third place', 'final'];

function normalizeGroupName(groupName: string) {
  const trimmed = groupName.trim();
  if (/^[a-z]$/i.test(trimmed)) {
    return `Group ${trimmed.toUpperCase()}`;
  }

  return trimmed;
}

function normalizeStageName(stageName: string) {
  const normalized = stageName.replace(/_/g, ' ').trim().toLowerCase();

  switch (normalized) {
    case 'group stage':
      return 'Group Stage';
    case 'league stage':
      return 'League Phase';
    case 'playoffs':
      return 'Knockout Play-offs';
    case 'last 16':
      return 'Round of 16';
    case 'quarter finals':
      return 'Quarter-finals';
    case 'semi finals':
      return 'Semi-finals';
    case 'third place':
      return 'Third Place';
    case 'final':
      return 'Final';
    case 'qualification':
      return 'Qualifying';
    case 'play off round':
      return 'Play-off Round';
    default:
      return stageName.replace(/_/g, ' ').trim();
  }
}

function isGroupStageMatch(match: Match) {
  return normalizeStageName(match.stage ?? '').toLowerCase() === 'group stage';
}

function sortMatchesAscending(matches: Match[]) {
  return [...matches].sort((left, right) => {
    const leftValue = `${left.date}T${left.time || '00:00'}`;
    const rightValue = `${right.date}T${right.time || '00:00'}`;
    return leftValue.localeCompare(rightValue);
  });
}

export function buildLeaguePhaseStandings(matches: Match[], clubs: Club[]): StandingRow[] {
  const clubMap = new Map(clubs.map((club) => [club.id, club]));
  const rows = new Map<string, GroupStandingAccumulator>();

  for (const match of matches) {
    if (normalizeStageName(match.stage ?? '') !== 'League Phase') {
      continue;
    }

    for (const clubId of [match.homeTeamId, match.awayTeamId]) {
      if (!rows.has(clubId)) {
        const club = clubMap.get(clubId);
        rows.set(clubId, {
          clubId,
          clubName: club?.name ?? clubId,
          clubShortName: club?.shortName ?? club?.name ?? clubId,
          clubLogo: club?.logo,
          played: 0,
          won: 0,
          drawn: 0,
          lost: 0,
          goalsFor: 0,
          goalsAgainst: 0,
          points: 0,
          form: [],
        });
      }
    }

    if (match.status !== 'finished' || match.homeScore === null || match.awayScore === null) {
      continue;
    }

    const home = rows.get(match.homeTeamId);
    const away = rows.get(match.awayTeamId);
    if (!home || !away) {
      continue;
    }

    home.played += 1;
    away.played += 1;
    home.goalsFor += match.homeScore;
    home.goalsAgainst += match.awayScore;
    away.goalsFor += match.awayScore;
    away.goalsAgainst += match.homeScore;

    if (match.homeScore > match.awayScore) {
      home.won += 1;
      away.lost += 1;
      home.points += 3;
      home.form.push('W');
      away.form.push('L');
    } else if (match.homeScore < match.awayScore) {
      away.won += 1;
      home.lost += 1;
      away.points += 3;
      home.form.push('L');
      away.form.push('W');
    } else {
      home.drawn += 1;
      away.drawn += 1;
      home.points += 1;
      away.points += 1;
      home.form.push('D');
      away.form.push('D');
    }
  }

  return Array.from(rows.values())
    .sort((left, right) => {
      if (right.points !== left.points) return right.points - left.points;
      const goalDifferenceDelta = (right.goalsFor - right.goalsAgainst) - (left.goalsFor - left.goalsAgainst);
      if (goalDifferenceDelta !== 0) return goalDifferenceDelta;
      if (right.goalsFor !== left.goalsFor) return right.goalsFor - left.goalsFor;
      return left.clubName.localeCompare(right.clubName);
    })
    .map((row, index) => ({
      position: index + 1,
      clubId: row.clubId,
      clubName: row.clubName,
      clubShortName: row.clubShortName,
      clubLogo: row.clubLogo,
      played: row.played,
      won: row.won,
      drawn: row.drawn,
      lost: row.lost,
      goalsFor: row.goalsFor,
      goalsAgainst: row.goalsAgainst,
      goalDifference: row.goalsFor - row.goalsAgainst,
      points: row.points,
      form: row.form.slice(-5),
    }));
}

export function buildTournamentGroups(matches: Match[], clubs: Club[]): TournamentGroupView[] {
  const clubMap = new Map(clubs.map((club) => [club.id, club]));
  const groups = new Map<string, Map<string, GroupStandingAccumulator>>();
  const inferredGroupLinks = new Map<string, Set<string>>();

  for (const match of matches) {
    if (!match.groupName && !isGroupStageMatch(match)) {
      continue;
    }

    const normalizedGroupName = match.groupName ? normalizeGroupName(match.groupName) : undefined;
    if (!normalizedGroupName) {
      const homeLinks = inferredGroupLinks.get(match.homeTeamId) ?? new Set<string>();
      homeLinks.add(match.awayTeamId);
      inferredGroupLinks.set(match.homeTeamId, homeLinks);

      const awayLinks = inferredGroupLinks.get(match.awayTeamId) ?? new Set<string>();
      awayLinks.add(match.homeTeamId);
      inferredGroupLinks.set(match.awayTeamId, awayLinks);
      continue;
    }

    const group = groups.get(normalizedGroupName) ?? new Map<string, GroupStandingAccumulator>();
    groups.set(normalizedGroupName, group);

    for (const clubId of [match.homeTeamId, match.awayTeamId]) {
      if (!group.has(clubId)) {
        const club = clubMap.get(clubId);
        group.set(clubId, {
          clubId,
          clubName: club?.name ?? clubId,
          clubShortName: club?.shortName ?? club?.name ?? clubId,
          clubLogo: club?.logo,
          played: 0,
          won: 0,
          drawn: 0,
          lost: 0,
          goalsFor: 0,
          goalsAgainst: 0,
          points: 0,
          form: [],
        });
      }
    }

    if (match.status !== 'finished' || match.homeScore === null || match.awayScore === null) {
      continue;
    }

    const home = group.get(match.homeTeamId);
    const away = group.get(match.awayTeamId);
    if (!home || !away) {
      continue;
    }

    home.played += 1;
    away.played += 1;
    home.goalsFor += match.homeScore;
    home.goalsAgainst += match.awayScore;
    away.goalsFor += match.awayScore;
    away.goalsAgainst += match.homeScore;

    if (match.homeScore > match.awayScore) {
      home.won += 1;
      away.lost += 1;
      home.points += 3;
      home.form.push('W');
      away.form.push('L');
    } else if (match.homeScore < match.awayScore) {
      away.won += 1;
      home.lost += 1;
      away.points += 3;
      home.form.push('L');
      away.form.push('W');
    } else {
      home.drawn += 1;
      away.drawn += 1;
      home.points += 1;
      away.points += 1;
      home.form.push('D');
      away.form.push('D');
    }
  }

  return Array.from(groups.entries())
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([groupName, rows]) => ({
      id: groupName.toLowerCase().replace(/\s+/g, '-'),
      name: groupName,
      standings: Array.from(rows.values())
        .sort((left, right) => {
          if (right.points !== left.points) return right.points - left.points;
          const goalDifferenceDelta = (right.goalsFor - right.goalsAgainst) - (left.goalsFor - left.goalsAgainst);
          if (goalDifferenceDelta !== 0) return goalDifferenceDelta;
          if (right.goalsFor !== left.goalsFor) return right.goalsFor - left.goalsFor;
          return left.clubName.localeCompare(right.clubName);
        })
        .map((row, index) => ({
          position: index + 1,
          clubId: row.clubId,
          clubName: row.clubName,
          clubShortName: row.clubShortName,
          clubLogo: row.clubLogo,
          played: row.played,
          won: row.won,
          drawn: row.drawn,
          lost: row.lost,
          goalsFor: row.goalsFor,
          goalsAgainst: row.goalsAgainst,
          goalDifference: row.goalsFor - row.goalsAgainst,
          points: row.points,
          form: row.form.slice(-5),
        })),
    }));
}

export function getChampionsLeagueFormat(seasonLabel?: string): ChampionsLeagueFormat {
  const match = seasonLabel?.match(/^(\d{4})/);
  const startYear = match ? Number.parseInt(match[1], 10) : Number.NaN;
  return Number.isFinite(startYear) && startYear >= 2024 ? 'league-phase' : 'legacy';
}

export function buildLeaguePhaseMatchdays(matches: Match[]): TournamentMatchStageView[] {
  const stages = new Map<number, Match[]>();

  for (const match of matches) {
    if (normalizeStageName(match.stage ?? '') !== 'League Phase') {
      continue;
    }

    const matchWeek = match.matchWeek;
    if (!matchWeek) {
      continue;
    }

    const stageMatches = stages.get(matchWeek) ?? [];
    stageMatches.push(match);
    stages.set(matchWeek, stageMatches);
  }

  return Array.from(stages.entries())
    .sort(([left], [right]) => left - right)
    .map(([matchWeek, stageMatches]) => ({
      id: `matchday-${matchWeek}`,
      name: `Matchday ${matchWeek}`,
      matches: sortMatchesAscending(stageMatches),
    }));
}

export function buildQualifyingStages(matches: Match[]): TournamentMatchStageView[] {
  const stages = new Map<string, Match[]>();

  for (const match of matches) {
    const normalizedStageName = normalizeStageName(match.stage ?? '');
    const lowered = normalizedStageName.toLowerCase();

    if (!lowered.includes('qualif') && !lowered.includes('play-off')) {
      continue;
    }

    const stageMatches = stages.get(normalizedStageName) ?? [];
    stageMatches.push(match);
    stages.set(normalizedStageName, stageMatches);
  }

  return Array.from(stages.entries())
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([name, stageMatches]) => ({
      id: name.toLowerCase().replace(/\s+/g, '-'),
      name,
      matches: sortMatchesAscending(stageMatches),
    }));
}

export function buildKnockoutStages(matches: Match[]): KnockoutStageView[] {
  const stages = new Map<string, Match[]>();

  for (const match of matches) {
    if (match.groupName || isGroupStageMatch(match) || normalizeStageName(match.stage ?? '') === 'League Phase') {
      continue;
    }

    const normalizedStageName = normalizeStageName(match.stage ?? '');
    if (!normalizedStageName || normalizedStageName.toLowerCase() === 'regular season') {
      continue;
    }

    const stageMatches = stages.get(normalizedStageName) ?? [];
    stageMatches.push(match);
    stages.set(normalizedStageName, stageMatches);
  }

  return Array.from(stages.entries())
    .sort(([left], [right]) => {
      const leftIndex = KNOCKOUT_STAGE_ORDER.indexOf(left.toLowerCase());
      const rightIndex = KNOCKOUT_STAGE_ORDER.indexOf(right.toLowerCase());
      if (leftIndex !== -1 || rightIndex !== -1) {
        if (leftIndex === -1) return -1;
        if (rightIndex === -1) return 1;
        return leftIndex - rightIndex;
      }

      return left.localeCompare(right);
    })
    .map(([name, stageMatches]) => ({
      name,
      matches: sortMatchesAscending(stageMatches),
    }));
}

export async function getTournamentChampion(stages: KnockoutStageView[], locale: string) {
  const finalStage = stages.find((stage) => stage.name.toLowerCase() === 'final');
  const finalMatch = finalStage?.matches.at(-1);
  if (!finalMatch || finalMatch.homeScore === null || finalMatch.awayScore === null) {
    return undefined;
  }

  if (finalMatch.homeScore > finalMatch.awayScore) return finalMatch.homeTeamName;
  if (finalMatch.awayScore > finalMatch.homeScore) return finalMatch.awayTeamName;

  const timelineEvents = await getMatchTimelineDb(finalMatch.id, locale);
  const penaltyEvents = timelineEvents.filter((event) => event.rawType === 'penalty_scored' || event.rawType === 'penalty_missed');
  const homePenaltyScore = penaltyEvents.filter((event) => event.teamId === finalMatch.homeTeamId && event.rawType === 'penalty_scored').length;
  const awayPenaltyScore = penaltyEvents.filter((event) => event.teamId === finalMatch.awayTeamId && event.rawType === 'penalty_scored').length;

  if (homePenaltyScore > awayPenaltyScore) return finalMatch.homeTeamName;
  if (awayPenaltyScore > homePenaltyScore) return finalMatch.awayTeamName;
  return undefined;
}
