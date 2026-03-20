export type CompetitionSeasonFormatType = 'regular_league' | 'league_phase' | 'group_knockout' | 'knockout';

interface CompetitionFormatInput {
  competitionSlug: string;
  compType: 'league' | 'international';
  seasonStartDate?: string;
}

const GROUP_KNOCKOUT_SLUG_KEYWORDS = [
  'world-cup',
  'euro',
  'copa-america',
  'african-cup-of-nations',
];

function getSeasonStartYear(seasonStartDate?: string) {
  const year = Number.parseInt(seasonStartDate?.slice(0, 4) ?? '', 10);
  return Number.isFinite(year) ? year : Number.NaN;
}

export function deriveCompetitionSeasonFormat({
  competitionSlug,
  compType,
  seasonStartDate,
}: CompetitionFormatInput): CompetitionSeasonFormatType {
  if (compType === 'league') {
    return 'regular_league';
  }

  const startYear = getSeasonStartYear(seasonStartDate);
  if (competitionSlug === 'champions-league' || competitionSlug === 'europa-league') {
    return Number.isFinite(startYear) && startYear >= 2024 ? 'league_phase' : 'group_knockout';
  }

  if (GROUP_KNOCKOUT_SLUG_KEYWORDS.some((keyword) => competitionSlug.includes(keyword))) {
    return 'group_knockout';
  }

  return 'knockout';
}
