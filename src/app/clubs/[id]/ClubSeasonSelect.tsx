'use client';

import { useRouter } from 'next/navigation';
import { cn, getCanonicalSeasonSlug } from '@/lib/utils';

interface ClubSeasonSelectEntry {
  seasonId: string;
  leagueId: string;
  leagueName: string;
}

interface ClubSeasonSelectGroup {
  seasonLabel: string;
  entries: ClubSeasonSelectEntry[];
}

interface ClubSeasonSelectProps {
  clubId: string;
  selectedValue?: string;
  groups: ClubSeasonSelectGroup[];
  className?: string;
  tab?: string;
}

function buildClubSeasonHref(clubId: string, seasonId: string, leagueId: string, tab?: string) {
  const searchParams = new URLSearchParams({
    season: getCanonicalSeasonSlug(seasonId),
    competition: leagueId,
  });

  if (tab) {
    searchParams.set('tab', tab);
  }

  return `/clubs/${clubId}?${searchParams.toString()}`;
}

export function ClubSeasonSelect({ clubId, selectedValue, groups, className, tab }: ClubSeasonSelectProps) {
  const router = useRouter();

  return (
    <select
      value={selectedValue ?? ''}
      onChange={(event) => {
        const nextHref = event.target.value;
        router.push(nextHref || `/clubs/${clubId}`);
      }}
      className={cn(
        'w-full rounded border border-border bg-surface-2 px-2 py-1.5 text-[12px] text-text-primary',
        className,
      )}
      aria-label="Select season"
    >
      {groups.map((group) => (
        <optgroup key={group.seasonLabel} label={group.seasonLabel}>
          {group.entries.map((entry) => (
            <option
              key={`${entry.seasonId}:${entry.leagueId}`}
              value={buildClubSeasonHref(clubId, entry.seasonId, entry.leagueId, tab)}
            >
              {entry.leagueName}
            </option>
          ))}
        </optgroup>
      ))}
    </select>
  );
}
