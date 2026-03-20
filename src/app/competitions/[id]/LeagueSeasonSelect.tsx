'use client';

import { useRouter } from 'next/navigation';
import { cn, getCanonicalSeasonSlug } from '@/lib/utils';

interface LeagueSeasonSelectEntry {
  seasonId: string;
  seasonLabel: string;
}

interface LeagueSeasonSelectProps {
  leagueId: string;
  seasons: LeagueSeasonSelectEntry[];
  selectedSeasonId?: string;
  tab?: string;
  className?: string;
}

function buildLeagueSeasonHref(leagueId: string, seasonId: string, tab?: string) {
  const searchParams = new URLSearchParams({
    season: getCanonicalSeasonSlug(seasonId),
  });

  if (tab && tab !== 'overview') {
    searchParams.set('tab', tab);
  }

  return `/competitions/${leagueId}?${searchParams.toString()}`;
}

export function LeagueSeasonSelect({ leagueId, seasons, selectedSeasonId, tab, className }: LeagueSeasonSelectProps) {
  const router = useRouter();
  const selectedValue = selectedSeasonId
    ? buildLeagueSeasonHref(leagueId, selectedSeasonId, tab)
    : '';

  return (
    <select
      value={selectedValue}
      onChange={(event) => {
        const nextHref = event.target.value;
        if (nextHref) {
          router.push(nextHref, { scroll: false });
        }
      }}
      className={cn(
        'rounded border border-border bg-surface-2 px-2 py-1.5 text-[12px] text-text-primary transition-colors hover:border-border-subtle focus:border-accent-emerald focus:outline-none',
        className,
      )}
      aria-label="Select season"
    >
      {seasons.map((season) => (
        <option
          key={season.seasonId}
          value={buildLeagueSeasonHref(leagueId, season.seasonId, tab)}
        >
          {season.seasonLabel}
        </option>
      ))}
    </select>
  );
}
