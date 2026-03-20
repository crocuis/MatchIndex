'use server';

import { getTranslations } from 'next-intl/server';
import { MatchAnalysisTabs } from '@/components/data/MatchAnalysisTabs';
import { SectionCard } from '@/components/ui/SectionCard';
import {
  getMatchAnalysisArtifactsBundleDb,
} from '@/data/server';

interface MatchAnalysisSectionProps {
  matchId: string;
  matchDate: string;
  homeTeamId: string;
  awayTeamId: string;
}

export async function MatchAnalysisSection({ matchId, matchDate, homeTeamId, awayTeamId }: MatchAnalysisSectionProps) {
  const [tMatch, artifacts] = await Promise.all([
    getTranslations('match'),
    getMatchAnalysisArtifactsBundleDb(matchId, matchDate),
  ]);
  const { analysis, freezeFrames, visibleAreas } = artifacts;

  if (analysis.events.length === 0) {
    return (
      <SectionCard title={tMatch('analysis')}>
        <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('analysisEmpty')}</div>
      </SectionCard>
    );
  }

  return (
    <MatchAnalysisTabs
      analysis={analysis}
      freezeFrames={freezeFrames}
      visibleAreas={visibleAreas}
      homeTeamId={homeTeamId}
      awayTeamId={awayTeamId}
      artifactSources={artifacts.artifactSources}
    />
  );
}
