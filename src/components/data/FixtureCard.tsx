'use client';

import { useRouter } from 'next/navigation';
import { useTranslations } from 'next-intl';
import type { Match } from '@/data/types';
import { getClubById, getClubName } from '@/data';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { formatDate, cn } from '@/lib/utils';

interface FixtureCardProps {
  match: Match;
  className?: string;
}

export function FixtureCard({ match, className }: FixtureCardProps) {
  const router = useRouter();
  const tCommon = useTranslations('common');
  const homeClub = getClubById(match.homeTeamId);
  const awayClub = getClubById(match.awayTeamId);

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
          {homeClub && <ClubBadge shortName={homeClub.shortName} clubId={homeClub.id} size="sm" />}
          <span className="text-[13px] font-medium text-text-primary truncate">
            {getClubName(match.homeTeamId)}
          </span>
        </div>
        <span className="text-[11px] text-text-muted shrink-0">{tCommon('vs')}</span>
        <div className="flex items-center gap-1.5 min-w-0">
          {awayClub && <ClubBadge shortName={awayClub.shortName} clubId={awayClub.id} size="sm" />}
          <span className="text-[13px] font-medium text-text-primary truncate">
            {getClubName(match.awayTeamId)}
          </span>
        </div>
      </div>
      <div className="flex items-center gap-2 shrink-0 ml-3">
        <span className="text-[11px] text-text-secondary">{formatDate(match.date)}</span>
        <span className="text-[11px] text-text-muted">{match.time}</span>
      </div>
    </div>
  );
}
