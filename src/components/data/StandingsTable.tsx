'use client';

import { useRouter } from 'next/navigation';
import { useTranslations } from 'next-intl';
import type { StandingRow } from '@/data/types';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { FormIndicator } from '@/components/ui/FormIndicator';
import { cn } from '@/lib/utils';

interface StandingsTableProps {
  standings: StandingRow[];
  compact?: boolean;
  className?: string;
}

export function StandingsTable({ standings, compact = false, className }: StandingsTableProps) {
  const router = useRouter();
  const tStandings = useTranslations('standings');

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
            <tr
              key={row.clubId}
              className="hover:bg-surface-2 cursor-pointer transition-colors"
              onClick={() => router.push(`/clubs/${row.clubId}`)}
            >
              <td className="px-2 py-1.5 text-center text-[13px] tabular-nums text-text-muted">
                {row.position}
              </td>
              <td className="px-3 py-1.5 text-[13px] font-medium text-text-primary">
                <div className="flex items-center gap-2">
                  <ClubBadge
                    shortName={row.clubShortName ?? row.clubId.slice(0, 3).toUpperCase()}
                    clubId={row.clubId}
                    logo={row.clubLogo}
                    size="sm"
                    showText={false}
                  />
                  <span>{compact ? (row.clubShortName ?? row.clubId) : (row.clubName ?? row.clubId)}</span>
                </div>
              </td>
              <td className="px-2 py-1.5 text-center text-[13px] tabular-nums">{row.played}</td>
              {!compact && (
                <>
                  <td className="px-2 py-1.5 text-center text-[13px] tabular-nums">{row.won}</td>
                  <td className="px-2 py-1.5 text-center text-[13px] tabular-nums">{row.drawn}</td>
                  <td className="px-2 py-1.5 text-center text-[13px] tabular-nums">{row.lost}</td>
                </>
              )}
              <td className="px-2 py-1.5 text-center text-[13px] tabular-nums">{row.goalsFor}</td>
              <td className="px-2 py-1.5 text-center text-[13px] tabular-nums">{row.goalsAgainst}</td>
              <td className={cn(
                'px-2 py-1.5 text-center text-[13px] tabular-nums font-medium',
                row.goalDifference > 0 ? 'text-emerald-400' : row.goalDifference < 0 ? 'text-red-400' : 'text-text-secondary'
              )}>
                {row.goalDifference > 0 ? `+${row.goalDifference}` : row.goalDifference}
              </td>
              <td className="px-2 py-1.5 text-center text-[13px] tabular-nums font-bold text-text-primary">
                {row.points}
              </td>
              {!compact && (
                <td className="px-2 py-1.5">
                  <FormIndicator form={row.form} />
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
