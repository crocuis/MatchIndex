'use client';

import { useRouter } from 'next/navigation';
import { useTranslations } from 'next-intl';
import type { Match, WorldCupPlaceholder } from '@/data/types';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { LocalizedMatchText } from '@/components/ui/LocalizedMatchText';
import { NationFlag } from '@/components/ui/NationFlag';
import { WorldCupPlaceholderLink } from '@/components/ui/WorldCupPlaceholderLink';
import { cn } from '@/lib/utils';

interface FixtureCardProps {
  match: Match;
  placeholders?: WorldCupPlaceholder[];
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

export function FixtureCard({ match, placeholders = [], className }: FixtureCardProps) {
  const router = useRouter();
  const tCommon = useTranslations('common');
  const isNationMatch = match.teamType === 'nation';
  const homeNationCode = match.homeTeamCode ?? '???';
  const awayNationCode = match.awayTeamCode ?? '???';
  const homeNationName = match.homeTeamName ?? tCommon('home');
  const awayNationName = match.awayTeamName ?? tCommon('away');
  const showHomeNationFlag = !isPendingNationSlot(homeNationName, homeNationCode);
  const showAwayNationFlag = !isPendingNationSlot(awayNationName, awayNationCode);
  const homePlaceholder = placeholders.find((placeholder) => placeholder.id === match.homeTeamId);
  const awayPlaceholder = placeholders.find((placeholder) => placeholder.id === match.awayTeamId);

  return (
    <div
      className={cn(
        'flex items-center justify-between px-3 py-2 rounded border border-border-subtle bg-surface-2 hover:bg-surface-3 cursor-pointer transition-colors',
        className
      )}
      onClick={() => router.push(`/matches/${match.id}`)}
    >
      <div className="flex items-center gap-3 min-w-0 flex-1">
        <div className="flex items-center gap-1.5 min-w-0">
          <div className="min-w-0 text-[13px] font-medium text-text-primary">
            {isNationMatch
              ? homePlaceholder
                ? <WorldCupPlaceholderLink placeholder={homePlaceholder} label={homeNationName} />
                : homeNationName
              : match.homeTeamName ?? tCommon('home')}
          </div>
          {isNationMatch
            ? showHomeNationFlag && (
              <NationFlag nationId={match.homeTeamId} code={homeNationCode} size="sm" />
            )
            : <ClubBadge shortName={match.homeTeamCode ?? match.homeTeamName ?? '???'} clubId={match.homeTeamId} logo={match.homeTeamLogo} size="sm" showText={false} />}
        </div>
        <span className="text-[11px] text-text-muted shrink-0">{tCommon('vs')}</span>
        <div className="flex items-center gap-1.5 min-w-0">
          {isNationMatch
            ? showAwayNationFlag && (
              <NationFlag nationId={match.awayTeamId} code={awayNationCode} size="sm" />
            )
            : <ClubBadge shortName={match.awayTeamCode ?? match.awayTeamName ?? '???'} clubId={match.awayTeamId} logo={match.awayTeamLogo} size="sm" showText={false} />}
          <div className="min-w-0 text-[13px] font-medium text-text-primary">
            {isNationMatch
              ? awayPlaceholder
                ? <WorldCupPlaceholderLink placeholder={awayPlaceholder} label={awayNationName} />
                : awayNationName
              : match.awayTeamName ?? tCommon('away')}
          </div>
        </div>
      </div>
      <div className="flex items-center gap-2 shrink-0 ml-3">
        <LocalizedMatchText matchId={match.id} venue={match.venue} date={match.date} time={match.time} variant="date" className="text-[11px] text-text-secondary" />
        <LocalizedMatchText matchId={match.id} venue={match.venue} date={match.date} time={match.time} variant="time" className="text-[11px] text-text-muted" />
      </div>
    </div>
  );
}
