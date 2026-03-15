export interface ExportMatchAnalysisArtifactsSummary {
  dryRun: boolean;
  force: boolean;
  requestedMatchId: string | null;
  scannedMatches: number;
  exportedMatches: number;
  skippedMatches: number;
  totalEvents: number;
  totalBytes: number;
  generatedAt: string;
  artifacts: Array<{
    matchId: string;
    matchDate: string;
    artifactType: string;
    storageKey: string;
    rowCount: number;
    byteSize: number;
    sourceVendor: string | null;
  }>;
}

export interface ExportMatchAnalysisArtifactsOptions {
  dryRun?: boolean;
  force?: boolean;
  limit?: number;
  matchId?: string;
}

export async function exportMatchAnalysisArtifacts(
  options: ExportMatchAnalysisArtifactsOptions = {},
): Promise<ExportMatchAnalysisArtifactsSummary> {
  void options;

  throw new Error(
    'export-match-analysis-artifacts is deprecated. Match event artifacts are now generated directly by source pipelines (StatsBomb, SofaScore, API-Football, FBref) and no longer exported from match_events tables.',
  );
}
