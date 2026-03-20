export const PLAYER_SEASON_STATS_START_YEAR = 2015;
export const PLAYER_SEASON_STATS_END_YEAR = 2026;

export function getDefaultPlayerSeasonYears() {
  return Array.from(
    { length: PLAYER_SEASON_STATS_END_YEAR - PLAYER_SEASON_STATS_START_YEAR + 1 },
    (_, index) => PLAYER_SEASON_STATS_START_YEAR + index,
  );
}

export function normalizePlayerSeasonYears(input?: number[]) {
  if (input && input.length > 0) {
    return [...new Set(input)].sort((a, b) => a - b);
  }

  return getDefaultPlayerSeasonYears();
}
