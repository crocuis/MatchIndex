'use client';

import { useRouter } from 'next/navigation';

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
}

export function ClubSeasonSelect({ clubId, selectedValue, groups }: ClubSeasonSelectProps) {
  const router = useRouter();

  return (
    <select
      value={selectedValue ?? ''}
      onChange={(event) => {
        const nextHref = event.target.value;
        router.push(nextHref || `/clubs/${clubId}`);
      }}
      className="w-full rounded border border-border bg-surface-2 px-2 py-1.5 text-[12px] text-text-primary"
      aria-label="Select season"
    >
      {groups.map((group) => (
        <optgroup key={group.seasonLabel} label={group.seasonLabel}>
          {group.entries.map((entry) => (
            <option
              key={`${entry.seasonId}:${entry.leagueId}`}
              value={`/clubs/${clubId}?season=${entry.seasonId}&competition=${entry.leagueId}`}
            >
              {entry.leagueName}
            </option>
          ))}
        </optgroup>
      ))}
    </select>
  );
}
