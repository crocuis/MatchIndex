import type { Metadata } from 'next';
import { getLocale, getTranslations } from 'next-intl/server';
import { StatPanel } from '@/components/data/StatPanel';
import { PageHeader } from '@/components/layout/PageHeader';
import { EntityLink } from '@/components/ui/EntityLink';
import { NationFlag } from '@/components/ui/NationFlag';
import { SectionCard } from '@/components/ui/SectionCard';
import { WorldCupPlaceholderLink } from '@/components/ui/WorldCupPlaceholderLink';
import { WorldCupScheduleTabs } from '@/app/worldcup/2026/WorldCupScheduleTabs';
import { cn } from '@/lib/utils';
import type { Match } from '@/data/types';
import {
  getMatchByIdDb,
  getNationsDb,
  getPlayerByIdDb,
  getWorldCup2026Db,
} from '@/data/server';

export const metadata: Metadata = {
  title: '2026 World Cup',
};

export default async function WorldCup2026Page() {
  const locale = await getLocale();
  const tournament = await getWorldCup2026Db();
  const tWorldCup = await getTranslations('worldCup');
  const tStandings = await getTranslations('standings');

  const worldCupNationRefs = new Map<string, string>();
  for (const group of tournament.groups) {
    for (const row of group.standings) {
      worldCupNationRefs.set(row.nationId, row.nationCode ?? '');
    }
  }
  for (const match of tournament.matches) {
    if (match.teamType !== 'nation') {
      continue;
    }
    worldCupNationRefs.set(match.homeTeamId, match.homeTeamCode ?? '');
    worldCupNationRefs.set(match.awayTeamId, match.awayTeamCode ?? '');
  }

  const nations = await getNationsDb(locale);
  const nationIdMap = new Map(nations.map((nation) => [nation.id, nation]));
  const nationCodeMap = new Map(nations.map((nation) => [nation.code.toUpperCase(), nation]));
  const resolveNation = (nationId: string) => nationIdMap.get(nationId) ?? nationCodeMap.get(worldCupNationRefs.get(nationId)?.toUpperCase() ?? '');

  const nationIds = Array.from(new Set(tournament.groups.flatMap((group) => group.standings.map((row) => row.nationId))));
  const placeholderMap = new Map((tournament.placeholders ?? []).map((placeholder) => [placeholder.id, placeholder]));

  const allMatchIds = new Set([
    ...tournament.matches.map((match) => match.id),
    ...tournament.stages.flatMap((stage) => stage.matchIds),
  ]);
  const resolvedMatches = (await Promise.all(
    Array.from(allMatchIds).map((id) => getMatchByIdDb(id, locale))
  )).filter((m): m is Match => m !== undefined);

  const spotlightRows = await Promise.all(
      tournament.spotlights.map(async (spotlight) => ({
        spotlight,
        nation: resolveNation(spotlight.nationId),
        player: await getPlayerByIdDb(spotlight.playerId),
      }))
  );

  return (
    <div>
      <PageHeader title={tWorldCup('title')} subtitle={tWorldCup('subtitle')} meta={tournament.year}>
        <StatPanel
          stats={[
            { label: tWorldCup('host'), value: tournament.host },
            { label: tWorldCup('nations'), value: nationIds.length },
            { label: tWorldCup('matches'), value: resolvedMatches.length },
          ]}
          columns={3}
          className="w-[32rem]"
        />
      </PageHeader>

      <div className="grid grid-cols-12 gap-4">
        <div className="col-span-8 space-y-4">
          <SectionCard title={tWorldCup('groups')}>
            <div className="grid grid-cols-2 gap-4">
              {tournament.groups.map((group) => (
                <div key={group.id} className="overflow-hidden rounded-lg border border-border-subtle bg-surface-1">
                  <div className="border-b border-border bg-surface-2/40 px-3 py-2 text-[12px] font-semibold uppercase tracking-wider text-text-secondary">
                    {group.name}
                  </div>
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-border-subtle">
                        <th className="px-2 py-2 text-center w-8 text-[11px] text-text-muted">{tStandings('pos')}</th>
                        <th className="px-3 py-2 text-left text-[11px] text-text-muted">{tWorldCup('nation')}</th>
                        <th className="px-2 py-2 text-center w-8 text-[11px] text-text-muted">{tStandings('played')}</th>
                        <th className="px-2 py-2 text-center w-8 text-[11px] text-text-muted">{tStandings('won')}</th>
                        <th className="px-2 py-2 text-center w-8 text-[11px] text-text-muted">{tStandings('drawn')}</th>
                        <th className="px-2 py-2 text-center w-8 text-[11px] text-text-muted">{tStandings('lost')}</th>
                        <th className="px-2 py-2 text-center w-10 text-[11px] text-text-muted">{tStandings('goalDifference')}</th>
                        <th className="px-2 py-2 text-center w-10 text-[11px] font-semibold text-text-muted">{tStandings('points')}</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border-subtle">
                      {group.standings.map((row) => {
                        const nation = resolveNation(row.nationId);
                        const nationName = nation?.name ?? row.nationName ?? row.nationId;
                        const placeholder = placeholderMap.get(row.nationId);

                        return (
                          <tr key={row.nationId} className="hover:bg-surface-2 transition-colors">
                            <td className="px-2 py-1.5 text-center text-[13px] tabular-nums text-text-muted">{row.position}</td>
                            <td className="px-3 py-1.5 text-[13px]">
                              {nation ? (
                                <EntityLink type="nation" id={nation.id} className="flex items-center gap-2 font-medium text-text-primary">
                                  <NationFlag nationId={nation.id} code={nation.code} size="sm" />
                                  <span>{nation.name}</span>
                                </EntityLink>
                              ) : placeholder ? (
                                <WorldCupPlaceholderLink placeholder={placeholder} label={nationName} className="font-medium" />
                              ) : (
                                <span className="font-medium text-text-primary">{nationName}</span>
                              )}
                            </td>
                            <td className="px-2 py-1.5 text-center text-[13px] tabular-nums">{row.played}</td>
                            <td className="px-2 py-1.5 text-center text-[13px] tabular-nums">{row.won}</td>
                            <td className="px-2 py-1.5 text-center text-[13px] tabular-nums">{row.drawn}</td>
                            <td className="px-2 py-1.5 text-center text-[13px] tabular-nums">{row.lost}</td>
                            <td className={cn(
                              'px-2 py-1.5 text-center text-[13px] tabular-nums font-medium',
                              row.goalDifference > 0 ? 'text-emerald-400' : row.goalDifference < 0 ? 'text-red-400' : 'text-text-secondary'
                            )}>
                              {row.goalDifference > 0 ? `+${row.goalDifference}` : row.goalDifference}
                            </td>
                            <td className="px-2 py-1.5 text-center text-[13px] tabular-nums font-bold text-text-primary">{row.points}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ))}
            </div>
          </SectionCard>
        </div>

        <div className="col-span-4 space-y-4">
          <SectionCard title={tWorldCup('schedule')}>
            <WorldCupScheduleTabs
              matches={resolvedMatches}
              stages={tournament.stages}
              groups={tournament.groups}
              placeholders={tournament.placeholders}
            />
          </SectionCard>

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
                {spotlightRows.map(({ spotlight, nation, player }) => (
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
                ))}
              </tbody>
            </table>
          </SectionCard>
        </div>
      </div>
    </div>
  );
}
