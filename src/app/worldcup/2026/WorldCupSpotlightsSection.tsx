import { getTranslations } from 'next-intl/server';
import { EntityLink } from '@/components/ui/EntityLink';
import { NationFlag } from '@/components/ui/NationFlag';
import { SectionCard } from '@/components/ui/SectionCard';
import { getNationsDb, getPlayerLinksByIdsDb, getWorldCup2026Db } from '@/data/server';
import { createWorldCupNationResolver } from '@/app/worldcup/2026/worldCupPageData';

interface WorldCupSpotlightsSectionProps {
  locale: string;
}

export async function WorldCupSpotlightsSection({ locale }: WorldCupSpotlightsSectionProps) {
  const [tournament, nations, tWorldCup] = await Promise.all([
    getWorldCup2026Db(),
    getNationsDb(locale),
    getTranslations('worldCup'),
  ]);
  const spotlightPlayers = await getPlayerLinksByIdsDb(tournament.spotlights.map((spotlight) => spotlight.playerId), locale);
  const resolveNation = createWorldCupNationResolver(nations, tournament);
  const spotlightPlayerMap = new Map(spotlightPlayers.map((player) => [player.id, player]));

  return (
    <SectionCard title={tWorldCup('spotlights')} noPadding>
      <table className="w-full">
        <thead>
          <tr className="border-b border-border">
            <th className="px-3 py-2 text-left text-[11px] text-text-muted">{tWorldCup('nation')}</th>
            <th className="px-3 py-2 text-left text-[11px] text-text-muted">{tWorldCup('player')}</th>
            <th className="px-3 py-2 text-left text-[11px] text-text-muted">{tWorldCup('note')}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border-subtle">
          {tournament.spotlights.map((spotlight) => {
            const nation = resolveNation(spotlight.nationId);
            const player = spotlightPlayerMap.get(spotlight.playerId);

            return (
              <tr key={`${spotlight.nationId}-${spotlight.playerId}`} className="hover:bg-surface-2 transition-colors">
                <td className="px-3 py-2 text-[13px]">
                  {nation ? (
                    <EntityLink type="nation" id={nation.id} className="flex items-center gap-2 font-medium text-text-primary">
                      <NationFlag nationId={nation.id} code={nation.code} size="sm" />
                      <span>{nation.name}</span>
                    </EntityLink>
                  ) : (
                    spotlight.nationId
                  )}
                </td>
                <td className="px-3 py-2 text-[13px]">
                  {player ? (
                    <EntityLink type="player" id={player.id} className="font-medium text-text-primary">
                      {player.name}
                    </EntityLink>
                  ) : (
                    '-'
                  )}
                </td>
                <td className="px-3 py-2 text-[12px] leading-5 text-text-secondary">{spotlight.note}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </SectionCard>
  );
}
