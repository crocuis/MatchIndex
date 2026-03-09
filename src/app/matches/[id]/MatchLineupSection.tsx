import { getTranslations } from 'next-intl/server';
import { MatchLineup } from '@/components/data/MatchLineup';
import { SectionCard } from '@/components/ui/SectionCard';
import { getMatchLineupsDb } from '@/data/server';

interface MatchLineupSectionProps {
  matchId: string;
  locale: string;
  homeTeamId: string;
  awayTeamId: string;
  homeTeamName: string;
  awayTeamName: string;
}

export async function MatchLineupSection({
  matchId,
  locale,
  homeTeamId,
  awayTeamId,
  homeTeamName,
  awayTeamName,
}: MatchLineupSectionProps) {
  const [tMatch, lineups] = await Promise.all([
    getTranslations('match'),
    getMatchLineupsDb(matchId, locale),
  ]);

  return (
    <SectionCard title={tMatch('lineups')}>
      <MatchLineup
        lineups={lineups}
        homeTeamId={homeTeamId}
        awayTeamId={awayTeamId}
        homeTeamName={homeTeamName}
        awayTeamName={awayTeamName}
        placeholder={tMatch('lineupsPlaceholder')}
      />
    </SectionCard>
  );
}
