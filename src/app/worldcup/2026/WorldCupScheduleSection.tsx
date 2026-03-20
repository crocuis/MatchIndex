import { getTranslations } from 'next-intl/server';
import { SectionCard } from '@/components/ui/SectionCard';
import { WorldCupScheduleTabs } from '@/app/worldcup/2026/WorldCupScheduleTabs';
import { getWorldCup2026Db } from '@/data/server';
import { getResolvedWorldCupMatches } from '@/app/worldcup/2026/worldCupPageData';

interface WorldCupScheduleSectionProps {
  locale: string;
}

export async function WorldCupScheduleSection({ locale }: WorldCupScheduleSectionProps) {
  const [tournament, tWorldCup] = await Promise.all([
    getWorldCup2026Db(),
    getTranslations('worldCup'),
  ]);
  const resolvedMatches = await getResolvedWorldCupMatches(tournament, locale);

  return (
    <SectionCard title={tWorldCup('schedule')}>
      <WorldCupScheduleTabs
        matches={resolvedMatches}
        stages={tournament.stages}
        groups={tournament.groups}
        placeholders={tournament.placeholders}
      />
    </SectionCard>
  );
}
