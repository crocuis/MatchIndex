'use server';

import { getTranslations } from 'next-intl/server';
import { MatchAnalysisTabs } from '@/components/data/MatchAnalysisTabs';
import { SectionCard } from '@/components/ui/SectionCard';
import { getMatchAnalysisDataDb } from '@/data/server';

interface MatchAnalysisSectionProps {
  matchId: string;
  homeTeamId: string;
  awayTeamId: string;
}

export async function MatchAnalysisSection({ matchId, homeTeamId, awayTeamId }: MatchAnalysisSectionProps) {
  const [tMatch, analysis] = await Promise.all([
    getTranslations('match'),
    getMatchAnalysisDataDb(matchId),
  ]);

  if (analysis.events.length === 0) {
    return (
      <SectionCard title={tMatch('analysis')}>
        <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('analysisEmpty')}</div>
      </SectionCard>
    );
  }

  return <MatchAnalysisTabs analysis={analysis} homeTeamId={homeTeamId} awayTeamId={awayTeamId} />;
}
