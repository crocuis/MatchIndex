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

export interface TournamentAnalysisView {
  groups: TournamentGroupView[];
  qualifyingStages: TournamentMatchStageView[];
  leaguePhaseStages: TournamentMatchStageView[];
  legacyGroupStageMatches?: TournamentMatchStageView;
  knockoutStages: KnockoutStageView[];
}

export type TournamentFormat = 'legacy' | 'league-phase';

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
const QUALIFYING_STAGE_ORDER = ['play-offs', 'third qualifying round', 'second qualifying round', 'first qualifying round', 'preliminary round'];
const STAGE_DISPLAY_BY_SLUG: Record<string, string> = {
  'group-stage': 'Group Stage',
  'league-phase': 'League Phase',
  'knockout-play-offs': 'Knockout Play-offs',
  'round-of-16': 'Round of 16',
  'quarter-finals': 'Quarter-finals',
  'semi-finals': 'Semi-finals',
  'third-place': 'Third Place',
  final: 'Final',
  'preliminary-round': 'Preliminary Round',
  'first-qualifying-round': 'First Qualifying Round',
  'second-qualifying-round': 'Second Qualifying Round',
  'third-qualifying-round': 'Third Qualifying Round',
  qualifying: 'Qualifying',
  'play-offs': 'Play-offs',
};

function slugifyStageName(stageName: string) {
  return stageName
    .trim()
    .toLowerCase()
    .replace(/[_/]+/g, ' ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function getCanonicalStageSlug(stageName: string) {
  const slug = slugifyStageName(stageName);

  if (slug === 'group-stage') return 'group-stage';
  if (/^group-[a-z0-9]+$/i.test(slug)) return 'group-stage';
  if (/^[0-9]+(st|nd|rd|th)-group-stage$/i.test(slug)) return 'group-stage';
  if (slug === 'league-stage' || slug === 'league-phase') return 'league-phase';
  if (slug === 'playoffs' || slug === 'knockout-playoffs' || slug === 'knockout-play-offs' || slug === 'knockout-play-offs-round') return 'knockout-play-offs';
  if (slug === 'player-offs' || slug === 'playeroffs' || slug === 'knockout-player-offs') return 'knockout-play-offs';
  if (slug === 'last-16' || slug === 'round-of-16') return 'round-of-16';
  if (slug === 'quarter-finals' || slug === 'quarterfinals' || slug === 'quarter-finals-round' || slug === 'quater-finals' || slug === 'quaterfinals') return 'quarter-finals';
  if (slug === 'semi-finals' || slug === 'semifinals') return 'semi-finals';
  if (slug === 'third-place') return 'third-place';
  if (slug === 'final') return 'final';
  if (slug === 'preliminary-round' || slug === 'preliminary') return 'preliminary-round';
  if (slug === '1st-qualifying-round' || slug === 'first-qualifying-round') return 'first-qualifying-round';
  if (slug === '2nd-qualifying-round' || slug === 'second-qualifying-round') return 'second-qualifying-round';
  if (slug === '3rd-qualifying-round' || slug === 'third-qualifying-round') return 'third-qualifying-round';
  if (slug === 'qualification' || slug === 'qualifying') return 'qualifying';
  if (slug === 'play-off-round' || slug === 'playoff-round' || slug === 'play-offs' || slug === 'playoffs-round') return 'play-offs';

  return slug;
}

function normalizeGroupName(groupName: string) {
  const trimmed = groupName.trim();
  if (/^[a-z]$/i.test(trimmed)) {
    return `Group ${trimmed.toUpperCase()}`;
  }

  return trimmed;
}

function normalizeStageName(stageName: string) {
  const canonicalSlug = getCanonicalStageSlug(stageName);
  return STAGE_DISPLAY_BY_SLUG[canonicalSlug] ?? stageName.replace(/_/g, ' ').trim();
}

function getMatchDateTimeValue(match: Match) {
  return `${match.date}T${match.time || '00:00'}`;
}

function shouldUpgradePlayoffsToKnockout(match: Match, matches: Match[]) {
  if (getCanonicalStageSlug(match.stage ?? '') !== 'play-offs') {
    return false;
  }

  const leaguePhaseMatches = matches.filter((candidate) => isLeaguePhaseMatch(candidate));
  const roundOf16Matches = matches.filter((candidate) => getCanonicalStageSlug(candidate.stage ?? '') === 'round-of-16');
  if (leaguePhaseMatches.length === 0 || roundOf16Matches.length === 0) {
    return false;
  }

  const lastLeaguePhaseDate = leaguePhaseMatches
    .map((candidate) => getMatchDateTimeValue(candidate))
    .sort((left, right) => right.localeCompare(left))[0];
  const firstRoundOf16Date = roundOf16Matches
    .map((candidate) => getMatchDateTimeValue(candidate))
    .sort((left, right) => left.localeCompare(right))[0];
  const targetDate = getMatchDateTimeValue(match);

  if (!lastLeaguePhaseDate || !firstRoundOf16Date) {
    return false;
  }

  return targetDate > lastLeaguePhaseDate && targetDate < firstRoundOf16Date;
}

function getResolvedStageSlug(match: Match, matches: Match[]) {
  return shouldUpgradePlayoffsToKnockout(match, matches)
    ? 'knockout-play-offs'
    : getCanonicalStageSlug(match.stage ?? '');
}

function getResolvedStageName(match: Match, matches: Match[]) {
  const resolvedSlug = getResolvedStageSlug(match, matches);
  return STAGE_DISPLAY_BY_SLUG[resolvedSlug] ?? normalizeStageName(match.stage ?? '');
}

function getLeaguePhaseMatchWeek(match: Match) {
  if (match.matchWeek) {
    return match.matchWeek;
  }

  const stageValue = match.stage?.replace(/_/g, ' ').trim();
  const trailingNumber = stageValue?.match(/(?:league stage|league phase)\s*-?\s*(\d+)$/i)?.[1];
  if (!trailingNumber) {
    return undefined;
  }

  const parsed = Number.parseInt(trailingNumber, 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function isGroupStageMatch(match: Match) {
  return normalizeStageName(match.stage ?? '').toLowerCase() === 'group stage';
}

export function isLegacyGroupStageMatch(match: Match) {
  return Boolean(match.groupName) || isGroupStageMatch(match);
}

function isLeaguePhaseMatch(match: Match) {
  const canonicalSlug = getCanonicalStageSlug(match.stage ?? '');
  return canonicalSlug === 'league-phase' || /^league (stage|phase)\s*-?\s*\d+$/i.test(match.stage ?? '');
}

function isKnockoutPlayoffStage(stageName: string) {
  return getCanonicalStageSlug(stageName) === 'knockout-play-offs';
}

function sortMatchesAscending(matches: Match[]) {
  return [...matches].sort((left, right) => {
    const leftValue = `${left.date}T${left.time || '00:00'}`;
    const rightValue = `${right.date}T${right.time || '00:00'}`;
    return leftValue.localeCompare(rightValue);
  });
}

function getGroupDisplayName(index: number, existingNames: Set<string>) {
  const letterName = index < 26 ? `Group ${String.fromCharCode(65 + index)}` : `Group ${index + 1}`;
  if (!existingNames.has(letterName)) {
    return letterName;
  }

  return `Group ${index + 1}`;
}

function getConnectedTeamGroups(links: Map<string, Set<string>>) {
  const visited = new Set<string>();
  const groups: string[][] = [];

  for (const teamId of links.keys()) {
    if (visited.has(teamId)) {
      continue;
    }

    const queue = [teamId];
    const component: string[] = [];
    visited.add(teamId);

    while (queue.length > 0) {
      const current = queue.shift();
      if (!current) {
        continue;
      }

      component.push(current);
      for (const linkedTeamId of links.get(current) ?? []) {
        if (visited.has(linkedTeamId)) {
          continue;
        }

        visited.add(linkedTeamId);
        queue.push(linkedTeamId);
      }
    }

    groups.push(component);
  }

  return groups;
}

function createGroupStandingAccumulator(clubMap: Map<string, Club>, clubId: string): GroupStandingAccumulator {
  const club = clubMap.get(clubId);
  return {
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
  };
}

function applyGroupMatch(
  group: Map<string, GroupStandingAccumulator>,
  clubMap: Map<string, Club>,
  match: Match,
) {
  for (const clubId of [match.homeTeamId, match.awayTeamId]) {
    if (!group.has(clubId)) {
      group.set(clubId, createGroupStandingAccumulator(clubMap, clubId));
    }
  }

  if (match.status !== 'finished' || match.homeScore === null || match.awayScore === null) {
    return;
  }

  const home = group.get(match.homeTeamId);
  const away = group.get(match.awayTeamId);
  if (!home || !away) {
    return;
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
    return;
  }

  if (match.homeScore < match.awayScore) {
    away.won += 1;
    home.lost += 1;
    away.points += 3;
    home.form.push('L');
    away.form.push('W');
    return;
  }

  home.drawn += 1;
  away.drawn += 1;
  home.points += 1;
  away.points += 1;
  home.form.push('D');
  away.form.push('D');
}

function getStageEarliestMatchValue(matches: Match[]) {
  return sortMatchesAscending(matches)[0]
    ? `${sortMatchesAscending(matches)[0].date}T${sortMatchesAscending(matches)[0].time || '00:00'}`
    : '';
}

function compareStageEntries(
  [leftName, leftMatches]: [string, Match[]],
  [rightName, rightMatches]: [string, Match[]],
  stageOrder: string[],
) {
  const leftEarliest = getStageEarliestMatchValue(leftMatches);
  const rightEarliest = getStageEarliestMatchValue(rightMatches);
  if (leftEarliest !== rightEarliest) {
    return leftEarliest.localeCompare(rightEarliest);
  }

  const leftIndex = stageOrder.indexOf(leftName.toLowerCase());
  const rightIndex = stageOrder.indexOf(rightName.toLowerCase());
  if (leftIndex !== -1 || rightIndex !== -1) {
    if (leftIndex === -1) return -1;
    if (rightIndex === -1) return 1;
    return leftIndex - rightIndex;
  }

  return leftName.localeCompare(rightName);
}

export function buildLeaguePhaseStandings(matches: Match[], clubs: Club[]): StandingRow[] {
  const clubMap = new Map(clubs.map((club) => [club.id, club]));
  const rows = new Map<string, GroupStandingAccumulator>();

  for (const match of matches) {
    if (!isLeaguePhaseMatch(match)) {
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
  const inferredGroupMatches: Match[] = [];

  for (const match of matches) {
    if (!isLegacyGroupStageMatch(match)) {
      continue;
    }

    const normalizedGroupName = match.groupName ? normalizeGroupName(match.groupName) : undefined;
    if (!normalizedGroupName) {
      inferredGroupMatches.push(match);
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
    applyGroupMatch(group, clubMap, match);
  }

  if (inferredGroupMatches.length > 0) {
    const existingNames = new Set(groups.keys());
    const inferredGroups = getConnectedTeamGroups(inferredGroupLinks)
      .map((teamIds) => {
        const teamIdSet = new Set(teamIds);
        const groupMatches = inferredGroupMatches.filter((match) => teamIdSet.has(match.homeTeamId) && teamIdSet.has(match.awayTeamId));
        return { teamIds, groupMatches };
      })
      .filter((entry) => entry.groupMatches.length > 0)
      .sort((left, right) => {
        const leftEarliest = getStageEarliestMatchValue(left.groupMatches);
        const rightEarliest = getStageEarliestMatchValue(right.groupMatches);
        if (leftEarliest !== rightEarliest) {
          return leftEarliest.localeCompare(rightEarliest);
        }

        const leftKey = left.teamIds.map((teamId) => clubMap.get(teamId)?.name ?? teamId).sort()[0] ?? '';
        const rightKey = right.teamIds.map((teamId) => clubMap.get(teamId)?.name ?? teamId).sort()[0] ?? '';
        return leftKey.localeCompare(rightKey);
      });

    inferredGroups.forEach((entry, index) => {
      const groupName = getGroupDisplayName(index, existingNames);
      existingNames.add(groupName);
      const group = new Map<string, GroupStandingAccumulator>();

      entry.groupMatches.forEach((match) => applyGroupMatch(group, clubMap, match));
      groups.set(groupName, group);
    });
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

export function getEuropeanCompetitionFormat(leagueId: string, seasonLabel?: string): TournamentFormat | undefined {
  if (leagueId !== 'champions-league' && leagueId !== 'europa-league') {
    return undefined;
  }

  const match = seasonLabel?.match(/^(\d{4})/);
  const startYear = match ? Number.parseInt(match[1], 10) : Number.NaN;
  return Number.isFinite(startYear) && startYear >= 2024 ? 'league-phase' : 'legacy';
}

export function getChampionsLeagueFormat(seasonLabel?: string): TournamentFormat {
  return getEuropeanCompetitionFormat('champions-league', seasonLabel) ?? 'legacy';
}

export function buildLeaguePhaseMatchdays(matches: Match[]): TournamentMatchStageView[] {
  const stages = new Map<number, Match[]>();

  for (const match of matches) {
    if (!isLeaguePhaseMatch(match)) {
      continue;
    }

    const matchWeek = getLeaguePhaseMatchWeek(match);
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

export function buildGroupStageMatches(matches: Match[]): TournamentMatchStageView | undefined {
  const groupStageMatches = matches.filter((match) => isLegacyGroupStageMatch(match));

  if (groupStageMatches.length === 0) {
    return undefined;
  }

  return {
    id: 'group-stage',
    name: 'Group Stage',
    matches: sortMatchesAscending(groupStageMatches),
  };
}

export function buildQualifyingStages(matches: Match[]): TournamentMatchStageView[] {
  const stages = new Map<string, Match[]>();

  for (const match of matches) {
    const normalizedStageName = getResolvedStageName(match, matches);
    const lowered = normalizedStageName.toLowerCase();

    if (isKnockoutPlayoffStage(normalizedStageName)) {
      continue;
    }

    if (!lowered.includes('qualif') && !lowered.includes('play-off') && !lowered.includes('playoff')) {
      continue;
    }

    const stageMatches = stages.get(normalizedStageName) ?? [];
    stageMatches.push(match);
    stages.set(normalizedStageName, stageMatches);
  }

  return Array.from(stages.entries())
    .sort((left, right) => compareStageEntries(left, right, QUALIFYING_STAGE_ORDER))
    .map(([name, stageMatches]) => ({
      id: name.toLowerCase().replace(/\s+/g, '-'),
      name,
      matches: sortMatchesAscending(stageMatches),
    }));
}

export function buildKnockoutStages(matches: Match[]): KnockoutStageView[] {
  const stages = new Map<string, Match[]>();

  for (const match of matches) {
    const normalizedStageName = getResolvedStageName(match, matches);
    const lowered = normalizedStageName.toLowerCase();
    if (
      match.groupName
      || isGroupStageMatch(match)
      || isLeaguePhaseMatch(match)
      || lowered.includes('qualif')
      || ((lowered.includes('play-off') || lowered.includes('playoff')) && !isKnockoutPlayoffStage(normalizedStageName))
    ) {
      continue;
    }
    if (!normalizedStageName || normalizedStageName.toLowerCase() === 'regular season') {
      continue;
    }

    const stageMatches = stages.get(normalizedStageName) ?? [];
    stageMatches.push(match);
    stages.set(normalizedStageName, stageMatches);
  }

  return Array.from(stages.entries())
    .sort((left, right) => compareStageEntries(left, right, KNOCKOUT_STAGE_ORDER))
    .map(([name, stageMatches]) => ({
      name,
      matches: sortMatchesAscending(stageMatches),
    }));
}

export function analyzeTournamentMatches(matches: Match[], clubs: Club[]): TournamentAnalysisView {
  const clubMap = new Map(clubs.map((club) => [club.id, club]));
  const groups = new Map<string, Map<string, GroupStandingAccumulator>>();
  const leaguePhaseMatchdays = new Map<number, Match[]>();
  const qualifyingMatches = new Map<string, Match[]>();
  const knockoutMatches = new Map<string, Match[]>();
  const groupStageMatches: Match[] = [];

  for (const match of matches) {
    const normalizedGroupName = match.groupName ? normalizeGroupName(match.groupName) : undefined;
    const isGroupStage = isLegacyGroupStageMatch(match);
    const isLeaguePhase = isLeaguePhaseMatch(match);
    const resolvedStageName = getResolvedStageName(match, matches);
    const loweredStageName = resolvedStageName.toLowerCase();
    const isKnockoutPlayoff = isKnockoutPlayoffStage(resolvedStageName);
    const isQualifying = !isKnockoutPlayoff && (loweredStageName.includes('qualif') || loweredStageName.includes('play-off') || loweredStageName.includes('playoff'));
    const isKnockout = !isGroupStage
      && !isLeaguePhase
      && !isQualifying
      && resolvedStageName
      && loweredStageName !== 'regular season';

    if (isGroupStage) {
      groupStageMatches.push(match);
    }

    if (normalizedGroupName) {
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

      if (match.status === 'finished' && match.homeScore !== null && match.awayScore !== null) {
        const home = group.get(match.homeTeamId);
        const away = group.get(match.awayTeamId);

        if (home && away) {
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
      }
    }

    if (isLeaguePhase) {
      const matchWeek = getLeaguePhaseMatchWeek(match);
      if (matchWeek) {
        const stageMatches = leaguePhaseMatchdays.get(matchWeek) ?? [];
        stageMatches.push(match);
        leaguePhaseMatchdays.set(matchWeek, stageMatches);
      }
    }

    if (isQualifying) {
      const stageMatches = qualifyingMatches.get(resolvedStageName) ?? [];
      stageMatches.push(match);
      qualifyingMatches.set(resolvedStageName, stageMatches);
    }

    if (isKnockout) {
      const stageMatches = knockoutMatches.get(resolvedStageName) ?? [];
      stageMatches.push(match);
      knockoutMatches.set(resolvedStageName, stageMatches);
    }
  }

  return {
    groups: Array.from(groups.entries())
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
      })),
    qualifyingStages: Array.from(qualifyingMatches.entries())
      .sort((left, right) => compareStageEntries(left, right, QUALIFYING_STAGE_ORDER))
      .map(([name, stageMatches]) => ({
        id: name.toLowerCase().replace(/\s+/g, '-'),
        name,
        matches: sortMatchesAscending(stageMatches),
      })),
    leaguePhaseStages: Array.from(leaguePhaseMatchdays.entries())
      .sort(([left], [right]) => left - right)
      .map(([matchWeek, stageMatches]) => ({
        id: `matchday-${matchWeek}`,
        name: `Matchday ${matchWeek}`,
        matches: sortMatchesAscending(stageMatches),
      })),
    legacyGroupStageMatches: groupStageMatches.length > 0
      ? {
          id: 'group-stage',
          name: 'Group Stage',
          matches: sortMatchesAscending(groupStageMatches),
        }
      : undefined,
    knockoutStages: Array.from(knockoutMatches.entries())
      .sort((left, right) => compareStageEntries(left, right, KNOCKOUT_STAGE_ORDER))
      .map(([name, stageMatches]) => ({
        name,
        matches: sortMatchesAscending(stageMatches),
      })),
  };
}

export async function getTournamentChampion(stages: KnockoutStageView[], locale: string) {
  const finalStage = stages.find((stage) => stage.name.toLowerCase() === 'final');
  const finalMatch = finalStage?.matches.at(-1);
  if (!finalMatch || finalMatch.homeScore === null || finalMatch.awayScore === null) {
    return undefined;
  }

  if (finalMatch.homeScore > finalMatch.awayScore) return finalMatch.homeTeamName;
  if (finalMatch.awayScore > finalMatch.homeScore) return finalMatch.awayTeamName;

  const timelineEvents = await getMatchTimelineDb(finalMatch.id, locale, finalMatch.date);
  const penaltyEvents = timelineEvents.filter((event) => event.rawType === 'penalty_scored' || event.rawType === 'penalty_missed');
  const homePenaltyScore = penaltyEvents.filter((event) => event.teamId === finalMatch.homeTeamId && event.rawType === 'penalty_scored').length;
  const awayPenaltyScore = penaltyEvents.filter((event) => event.teamId === finalMatch.awayTeamId && event.rawType === 'penalty_scored').length;

  if (homePenaltyScore > awayPenaltyScore) return finalMatch.homeTeamName;
  if (awayPenaltyScore > homePenaltyScore) return finalMatch.awayTeamName;
  return undefined;
}
