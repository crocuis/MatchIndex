'use client';

import { useRouter } from 'next/navigation';
import { useTranslations } from 'next-intl';
import type { Match } from '@/data/types';
import { getClubById, getClubShortName } from '@/data';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { formatDateShort, cn } from '@/lib/utils';

interface MatchCardProps {
  match: Match;
  showDate?: boolean;
  showLeague?: boolean;
  leagueName?: string;
  className?: string;
}

export function MatchCard({ match, showDate = true, showLeague = false, leagueName, className }: MatchCardProps) {
  const router = useRouter();
  const tMatchStatus = useTranslations('matchStatus');
  const homeClub = getClubById(match.homeTeamId);
  const awayClub = getClubById(match.awayTeamId);
  const statusClassName = match.status === 'finished'
    ? 'text-zinc-400'
    : match.status === 'live'
      ? 'text-red-400 animate-pulse'
      : 'text-zinc-500';

  return (
    <div
      className={cn(
        'flex items-center gap-3 px-3 py-2 rounded border border-border-subtle bg-surface-2 hover:bg-surface-3 cursor-pointer transition-colors',
        className
      )}
      onClick={() => router.push(`/matches/${match.id}`)}
    >
      {showDate && (
        <div className="text-[11px] text-text-muted w-12 shrink-0">
          {formatDateShort(match.date)}
        </div>
      )}

      <div className="flex-1 flex items-center justify-between min-w-0">
        <div className="flex items-center gap-2 min-w-0 flex-1">
          <div className="flex items-center justify-end gap-1.5 w-20 shrink-0">
            {homeClub && <ClubBadge shortName={homeClub.shortName} clubId={homeClub.id} size="sm" />}
            <span className="text-[13px] font-medium text-text-primary truncate text-right">
              {getClubShortName(match.homeTeamId)}
            </span>
          </div>

          <div className="flex items-center gap-1.5 shrink-0">
            {match.status === 'finished' ? (
              <>
                <span className={cn(
                  'text-[13px] font-bold tabular-nums w-4 text-center',
                  match.homeScore !== null && match.awayScore !== null && match.homeScore > match.awayScore
                    ? 'text-text-primary'
                    : 'text-text-secondary'
                )}>
                  {match.homeScore}
                </span>
                <span className="text-[11px] text-text-muted">-</span>
                <span className={cn(
                  'text-[13px] font-bold tabular-nums w-4 text-center',
                  match.homeScore !== null && match.awayScore !== null && match.awayScore > match.homeScore
                    ? 'text-text-primary'
                    : 'text-text-secondary'
                )}>
                  {match.awayScore}
                </span>
              </>
            ) : (
              <span className="text-[11px] text-text-muted">{match.time}</span>
            )}
          </div>

          <div className="flex items-center gap-1.5 w-20 shrink-0">
            {awayClub && <ClubBadge shortName={awayClub.shortName} clubId={awayClub.id} size="sm" />}
            <span className="text-[13px] font-medium text-text-primary truncate">
              {getClubShortName(match.awayTeamId)}
            </span>
          </div>
        </div>
      </div>

      <span className={cn('text-[10px] font-medium shrink-0', statusClassName)}>
        {tMatchStatus(match.status)}
      </span>

      {showLeague && leagueName && (
        <span className="text-[10px] text-text-muted shrink-0 ml-1">{leagueName}</span>
      )}
    </div>
  );
}
