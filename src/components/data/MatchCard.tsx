'use client';

import { useRouter } from 'next/navigation';
import { useTranslations } from 'next-intl';
import type { Match, WorldCupPlaceholder } from '@/data/types';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { LocalizedMatchText } from '@/components/ui/LocalizedMatchText';
import { NationFlag } from '@/components/ui/NationFlag';
import { WorldCupPlaceholderLink } from '@/components/ui/WorldCupPlaceholderLink';
import { cn } from '@/lib/utils';

interface MatchCardProps {
  match: Match;
  placeholders?: WorldCupPlaceholder[];
  showDate?: boolean;
  showLeague?: boolean;
  leagueName?: string;
  className?: string;
}

function isPendingNationSlot(name?: string, code?: string) {
  const value = `${name ?? ''} ${code ?? ''}`.toLowerCase();
  return value.includes('winner')
    || value.includes('runners-up')
    || value.includes('third place')
    || value.includes('loser match')
    || value.includes('group ')
    || value.includes('path ')
    || value.includes('ic path');
}

export function MatchCard({ match, placeholders = [], showDate = true, showLeague = false, leagueName, className }: MatchCardProps) {
  const router = useRouter();
  const tMatchStatus = useTranslations('matchStatus');
  const isNationMatch = match.teamType === 'nation';
  const homeShortName = isNationMatch
    ? match.homeTeamCode ?? '???'
    : match.homeTeamCode ?? match.homeTeamName ?? '???';
  const awayShortName = isNationMatch
    ? match.awayTeamCode ?? '???'
    : match.awayTeamCode ?? match.awayTeamName ?? '???';
  const homeLabel = isNationMatch
    ? match.homeTeamName ?? homeShortName
    : match.homeTeamName ?? homeShortName;
  const awayLabel = isNationMatch
    ? match.awayTeamName ?? awayShortName
    : match.awayTeamName ?? awayShortName;
  const showHomeNationFlag = !isPendingNationSlot(homeLabel, homeShortName);
  const showAwayNationFlag = !isPendingNationSlot(awayLabel, awayShortName);
  const homePlaceholder = placeholders.find((placeholder) => placeholder.id === match.homeTeamId);
  const awayPlaceholder = placeholders.find((placeholder) => placeholder.id === match.awayTeamId);
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
          <LocalizedMatchText matchId={match.id} venue={match.venue} date={match.date} time={match.time} variant="dateShort" />
        </div>
      )}

      <div className="flex-1 flex items-center justify-between min-w-0">
        <div className="flex items-center gap-2 min-w-0 flex-1">
          <div className="flex items-center justify-end gap-1.5 min-w-0 flex-1">
              {isNationMatch
                ? showHomeNationFlag && (
                  <NationFlag nationId={match.homeTeamId} code={homeShortName} size="sm" />
                )
              : <ClubBadge shortName={homeShortName} clubId={match.homeTeamId} logo={match.homeTeamLogo} size="sm" showText={false} />}
            <div className="min-w-0 text-[13px] font-medium text-text-primary text-right">
              {homePlaceholder ? <WorldCupPlaceholderLink placeholder={homePlaceholder} label={homeLabel} className="items-end" /> : <span className="truncate block">{homeLabel}</span>}
            </div>
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
              <LocalizedMatchText matchId={match.id} venue={match.venue} date={match.date} time={match.time} variant="time" className="text-[11px] text-text-muted" />
            )}
          </div>

          <div className="flex items-center gap-1.5 min-w-0 flex-1 justify-start">
              {isNationMatch
                ? showAwayNationFlag && (
                  <NationFlag nationId={match.awayTeamId} code={awayShortName} size="sm" />
                )
              : <ClubBadge shortName={awayShortName} clubId={match.awayTeamId} logo={match.awayTeamLogo} size="sm" showText={false} />}
            <div className="min-w-0 text-[13px] font-medium text-text-primary">
              {awayPlaceholder ? <WorldCupPlaceholderLink placeholder={awayPlaceholder} label={awayLabel} /> : <span className="truncate block">{awayLabel}</span>}
            </div>
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
