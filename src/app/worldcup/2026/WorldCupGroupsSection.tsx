import { getTranslations } from 'next-intl/server';
import { EntityLink } from '@/components/ui/EntityLink';
import { NationFlag } from '@/components/ui/NationFlag';
import { SectionCard } from '@/components/ui/SectionCard';
import { WorldCupPlaceholderLink } from '@/components/ui/WorldCupPlaceholderLink';
import { cn } from '@/lib/utils';
import { getNationsDb, getWorldCup2026Db } from '@/data/server';
import { createWorldCupNationResolver } from '@/app/worldcup/2026/worldCupPageData';

interface WorldCupGroupsSectionProps {
  locale: string;
}

export async function WorldCupGroupsSection({ locale }: WorldCupGroupsSectionProps) {
  const [tournament, nations, tWorldCup, tStandings] = await Promise.all([
    getWorldCup2026Db(),
    getNationsDb(locale),
    getTranslations('worldCup'),
    getTranslations('standings'),
  ]);
  const resolveNation = createWorldCupNationResolver(nations, tournament);
  const placeholderMap = new Map((tournament.placeholders ?? []).map((placeholder) => [placeholder.id, placeholder]));

  return (
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
                      <td className={cn('px-2 py-1.5 text-center text-[13px] tabular-nums font-medium', row.goalDifference > 0 ? 'text-emerald-400' : row.goalDifference < 0 ? 'text-red-400' : 'text-text-secondary')}>
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
  );
}
