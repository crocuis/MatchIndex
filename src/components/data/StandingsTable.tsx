import Link from 'next/link';
import { getTranslations } from 'next-intl/server';
import type { StandingRow } from '@/data/types';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { FormIndicator } from '@/components/ui/FormIndicator';
import { cn } from '@/lib/utils';

interface StandingsTableProps {
  standings: StandingRow[];
  compact?: boolean;
  className?: string;
  getClubHref?: (clubId: string) => string;
}

export async function StandingsTable({ standings, compact = false, className, getClubHref }: StandingsTableProps) {
  const tStandings = await getTranslations('standings');
  const buildClubHref = (clubId: string) => getClubHref?.(clubId) ?? `/clubs/${clubId}`;

  return (
    <div className={cn('overflow-x-auto', className)}>
      <table className="w-full">
        <thead>
          <tr className="border-b border-border">
            <th className="px-2 py-2 text-center w-8">{tStandings('pos')}</th>
            <th className="px-3 py-2 text-left">{tStandings('club')}</th>
            <th className="px-2 py-2 text-center w-8">{tStandings('played')}</th>
            {!compact && (
              <>
                <th className="px-2 py-2 text-center w-8">{tStandings('won')}</th>
                <th className="px-2 py-2 text-center w-8">{tStandings('drawn')}</th>
                <th className="px-2 py-2 text-center w-8">{tStandings('lost')}</th>
              </>
            )}
            <th className="px-2 py-2 text-center w-10">{tStandings('goalsFor')}</th>
            <th className="px-2 py-2 text-center w-10">{tStandings('goalsAgainst')}</th>
            <th className="px-2 py-2 text-center w-10">{tStandings('goalDifference')}</th>
            <th className="px-2 py-2 text-center w-10 font-semibold">{tStandings('points')}</th>
            {!compact && <th className="px-2 py-2 text-center w-28">{tStandings('form')}</th>}
          </tr>
        </thead>
        <tbody className="divide-y divide-border-subtle">
          {standings.map((row) => (
            <tr key={row.clubId} className="transition-colors hover:bg-surface-2">
              <td className="p-0 text-center text-[13px] tabular-nums text-text-muted">
                <Link href={buildClubHref(row.clubId)} className="block px-2 py-1.5">
                  {row.position}
                </Link>
              </td>
              <td className="p-0 text-[13px] font-medium text-text-primary">
                <Link href={buildClubHref(row.clubId)} className="block px-3 py-1.5 text-text-primary transition-colors hover:text-accent-emerald">
                  <div className="flex items-center gap-2">
                    <ClubBadge
                      shortName={row.clubShortName ?? row.clubName ?? row.clubId}
                      clubId={row.clubId}
                      logo={row.clubLogo}
                      size="sm"
                      showText={false}
                    />
                    <span>{compact ? (row.clubShortName ?? row.clubId) : (row.clubName ?? row.clubId)}</span>
                  </div>
                </Link>
              </td>
              <td className="p-0 text-center text-[13px] tabular-nums">
                <Link href={buildClubHref(row.clubId)} className="block px-2 py-1.5 text-inherit">
                  {row.played}
                </Link>
              </td>
              {!compact && (
                <>
                  <td className="p-0 text-center text-[13px] tabular-nums"><Link href={buildClubHref(row.clubId)} className="block px-2 py-1.5 text-inherit">{row.won}</Link></td>
                  <td className="p-0 text-center text-[13px] tabular-nums"><Link href={buildClubHref(row.clubId)} className="block px-2 py-1.5 text-inherit">{row.drawn}</Link></td>
                  <td className="p-0 text-center text-[13px] tabular-nums"><Link href={buildClubHref(row.clubId)} className="block px-2 py-1.5 text-inherit">{row.lost}</Link></td>
                </>
              )}
               <td className="p-0 text-center text-[13px] tabular-nums"><Link href={buildClubHref(row.clubId)} className="block px-2 py-1.5 text-inherit">{row.goalsFor}</Link></td>
               <td className="p-0 text-center text-[13px] tabular-nums"><Link href={buildClubHref(row.clubId)} className="block px-2 py-1.5 text-inherit">{row.goalsAgainst}</Link></td>
              <td className={cn(
                'p-0 text-center text-[13px] tabular-nums font-medium',
                row.goalDifference > 0 ? 'text-emerald-400' : row.goalDifference < 0 ? 'text-red-400' : 'text-text-secondary'
              )}>
                <Link href={buildClubHref(row.clubId)} className="block px-2 py-1.5 text-inherit">
                  {row.goalDifference > 0 ? `+${row.goalDifference}` : row.goalDifference}
                </Link>
              </td>
              <td className="p-0 text-center text-[13px] tabular-nums font-bold text-text-primary">
                <Link href={buildClubHref(row.clubId)} className="block px-2 py-1.5 text-inherit">
                  {row.points}
                </Link>
              </td>
              {!compact && (
                <td className="p-0">
                   <Link href={buildClubHref(row.clubId)} className="block px-2 py-1.5 text-inherit">
                    <FormIndicator form={row.form} />
                  </Link>
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
