'use client';

import { useRouter } from 'next/navigation';
import { useTranslations } from 'next-intl';
import type { Match, WorldCupPlaceholder } from '@/data/types';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { LocalizedMatchText } from '@/components/ui/LocalizedMatchText';
import { NationFlag } from '@/components/ui/NationFlag';
import { WorldCupPlaceholderLink } from '@/components/ui/WorldCupPlaceholderLink';
import { getMatchStatusClassName } from '@/lib/matchStatus';
import { cn } from '@/lib/utils';

interface FixtureCardProps {
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

export function FixtureCard({
  match,
  placeholders = [],
  showDate = true,
  showLeague = false,
  leagueName,
  className,
}: FixtureCardProps) {
  const router = useRouter();
  const tCommon = useTranslations('common');
  const tMatchStatus = useTranslations('matchStatus');
  const isNationMatch = match.teamType === 'nation';
  const homeNationCode = match.homeTeamCode ?? '???';
  const awayNationCode = match.awayTeamCode ?? '???';
  const homeNationName = match.homeTeamName ?? tCommon('home');
  const awayNationName = match.awayTeamName ?? tCommon('away');
  const showHomeNationFlag = !isPendingNationSlot(homeNationName, homeNationCode);
  const showAwayNationFlag = !isPendingNationSlot(awayNationName, awayNationCode);
  const homePlaceholder = placeholders.find((placeholder) => placeholder.id === match.homeTeamId);
  const awayPlaceholder = placeholders.find((placeholder) => placeholder.id === match.awayTeamId);
  const matchHref = `/matches/${match.id}`;

  return (
    <div
      className={cn(
        'flex items-center gap-3 rounded border border-border-subtle bg-surface-2 px-3 py-2 transition-colors hover:bg-surface-3 cursor-pointer',
        className
      )}
      onMouseEnter={() => router.prefetch(matchHref)}
      onClick={() => router.push(matchHref)}
    >
      {showDate ? (
        <div className="w-12 shrink-0 text-[11px] text-text-muted">
          <LocalizedMatchText matchId={match.id} venue={match.venue} date={match.date} time={match.time} variant="dateShort" />
        </div>
      ) : null}

      <div className="flex min-w-0 flex-1 items-center justify-between">
        <div className="flex min-w-0 flex-1 items-center gap-2">
          <div className="flex min-w-0 flex-1 items-center justify-end gap-1.5">
            <div className="min-w-0 text-right text-[13px] font-medium text-text-primary">
              {isNationMatch
                ? homePlaceholder
                  ? <WorldCupPlaceholderLink placeholder={homePlaceholder} label={homeNationName} className="items-end" />
                  : <span className="block truncate">{homeNationName}</span>
                : <span className="block truncate">{match.homeTeamName ?? tCommon('home')}</span>}
            </div>
            {isNationMatch
              ? showHomeNationFlag && (
                <NationFlag nationId={match.homeTeamId} code={homeNationCode} size="sm" />
              )
              : <ClubBadge shortName={match.homeTeamCode ?? match.homeTeamName ?? '???'} clubId={match.homeTeamId} logo={match.homeTeamLogo} size="sm" showText={false} />}
          </div>

          <div className="shrink-0 text-[11px] text-text-muted tabular-nums">
            <LocalizedMatchText matchId={match.id} venue={match.venue} date={match.date} time={match.time} variant="time" className="text-[11px] text-text-muted" />
          </div>

          <div className="flex min-w-0 flex-1 items-center justify-start gap-1.5">
            {isNationMatch
              ? showAwayNationFlag && (
                <NationFlag nationId={match.awayTeamId} code={awayNationCode} size="sm" />
              )
              : <ClubBadge shortName={match.awayTeamCode ?? match.awayTeamName ?? '???'} clubId={match.awayTeamId} logo={match.awayTeamLogo} size="sm" showText={false} />}
            <div className="min-w-0 text-[13px] font-medium text-text-primary">
              {isNationMatch
                ? awayPlaceholder
                  ? <WorldCupPlaceholderLink placeholder={awayPlaceholder} label={awayNationName} />
                  : <span className="block truncate">{awayNationName}</span>
                : <span className="block truncate">{match.awayTeamName ?? tCommon('away')}</span>}
            </div>
          </div>
        </div>
      </div>

      <span className={cn('shrink-0 text-[10px] font-medium', getMatchStatusClassName(match.status))}>
        {tMatchStatus(match.status)}
      </span>

      {showLeague && leagueName ? (
        <span className="ml-1 shrink-0 text-[10px] text-text-muted">{leagueName}</span>
      ) : null}
    </div>
  );
}
