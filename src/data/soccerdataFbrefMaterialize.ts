export interface MaterializeSoccerdataFbrefOptions {
  competitionCode?: string;
  dryRun?: boolean;
  season?: string;
  sourceSlug?: string;
}

export interface MaterializeSoccerdataFbrefSummary {
  competitionCode: string | null;
  dryRun: boolean;
  implemented: boolean;
  nextStep: string;
  season: string | null;
  sourceSlug: string;
}

export async function materializeSoccerdataFbref(
  options: MaterializeSoccerdataFbrefOptions = {},
): Promise<MaterializeSoccerdataFbrefSummary> {
  const summary: MaterializeSoccerdataFbrefSummary = {
    competitionCode: options.competitionCode?.trim().toUpperCase() || null,
    dryRun: options.dryRun ?? true,
    implemented: false,
    nextStep: 'Implement canonical mapping and materialization for competition_seasons, matches, and player_season_stats.',
    season: options.season?.trim() || null,
    sourceSlug: options.sourceSlug?.trim() || 'soccerdata_fbref',
  };

  if (!summary.dryRun) {
    throw new Error('soccerdata FBref materialize scaffold is not implemented yet. Use dry-run only for now.');
  }

  return summary;
}
