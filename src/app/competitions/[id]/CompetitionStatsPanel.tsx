'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { SectionCard } from '@/components/ui/SectionCard';
import { useStaticDetailTabActive } from '@/components/ui/StaticDetailTabs';
import type { Player } from '@/data/types';

interface TopScorerRow {
  playerId: string;
  playerName: string;
  photoUrl?: string;
  playerPosition?: Player['position'];
  clubId: string;
  clubShortName: string;
  clubLogo?: string;
  goals: number;
  assists: number;
}

interface CompetitionStatsPanelProps {
  competitionId: string;
  seasonId?: string;
  initialRows?: TopScorerRow[];
  isTournament: boolean;
  title: string;
  labels: {
    rank: string;
    player: string;
    club: string;
    goals: string;
    assists: string;
  };
}

function buildClubCompetitionContextHref(clubId: string, competitionId: string, seasonId?: string) {
  const searchParams = new URLSearchParams({ competition: competitionId });

  if (seasonId) {
    searchParams.set('season', seasonId);
  }

  return `/clubs/${clubId}?${searchParams.toString()}`;
}

export function CompetitionStatsPanel({
  competitionId,
  seasonId,
  initialRows,
  isTournament,
  title,
  labels,
}: CompetitionStatsPanelProps) {
  const isActive = useStaticDetailTabActive('stats');
  const [rows, setRows] = useState<TopScorerRow[] | null>(initialRows ?? null);

  useEffect(() => {
    if (!isActive || rows !== null) {
      return;
    }

    const controller = new AbortController();
    const params = new URLSearchParams();
    if (seasonId) {
      params.set('seasonId', seasonId);
    }

    fetch(`/api/competitions/${competitionId}/stats?${params.toString()}`, {
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) {
          throw new Error('Failed to load competition stats');
        }

        const data = (await response.json()) as { rows: TopScorerRow[] };
        setRows(data.rows);
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') {
          return;
        }

        setRows([]);
      });

    return () => controller.abort();
  }, [competitionId, isActive, rows, seasonId]);

  if (rows === null) {
    return (
      <SectionCard title={title}>
        <div className="space-y-2">
          {Array.from({ length: 6 }, (_, index) => (
            <div key={index} className="rounded border border-border-subtle bg-surface-2/60 px-3 py-3">
              <div className="h-3 w-1/4 animate-pulse rounded bg-surface-3/80" />
              <div className="mt-2 h-3 w-full animate-pulse rounded bg-surface-3/80" />
            </div>
          ))}
        </div>
      </SectionCard>
    );
  }

  return (
    <SectionCard title={title} noPadding>
      <table className="w-full">
        <thead>
          <tr className="border-b border-border bg-surface-2/35">
            <th className="px-3 py-1.5 text-left text-[10px] uppercase tracking-[0.18em] text-text-muted">{labels.rank}</th>
            <th className="px-3 py-1.5 text-left text-[10px] uppercase tracking-[0.18em] text-text-muted">{labels.player}</th>
            {!isTournament ? <th className="px-3 py-1.5 text-center text-[10px] uppercase tracking-[0.18em] text-text-muted">{labels.club}</th> : null}
            <th className="px-3 py-1.5 text-center text-[10px] uppercase tracking-[0.18em] text-text-muted">{labels.goals}</th>
            {!isTournament ? <th className="px-3 py-1.5 text-center text-[10px] uppercase tracking-[0.18em] text-text-muted">{labels.assists}</th> : null}
          </tr>
        </thead>
        <tbody className="divide-y divide-border-subtle">
          {rows.map((row, index) => (
            <tr key={row.playerId} className="transition-colors hover:bg-surface-2/75">
              <td className="px-3 py-1.5 text-[12px] font-medium text-text-muted tabular-nums">{index + 1}</td>
              <td className="px-3 py-1.5 text-[12px]">
                <div className="flex items-center gap-1.5">
                  <PlayerAvatar
                    name={row.playerName}
                    position={row.playerPosition ?? 'MID'}
                    imageUrl={row.photoUrl}
                    size="sm"
                    className="h-6 w-6 border-border/70"
                  />
                  <div className="flex min-w-0 flex-col gap-px leading-none">
                    <EntityLink type="player" id={row.playerId} className="truncate text-[12px] font-medium leading-tight text-text-primary">
                      {row.playerName}
                    </EntityLink>
                    {isTournament ? (
                      <Link
                        href={buildClubCompetitionContextHref(row.clubId, competitionId, seasonId)}
                        className="flex w-fit items-center gap-1 text-[10px] leading-none text-text-muted transition-colors hover:text-text-secondary"
                      >
                        <ClubBadge shortName={row.clubShortName} clubId={row.clubId} logo={row.clubLogo} size="sm" showText={false} className="h-4 w-4" />
                        <span className="truncate">{row.clubShortName}</span>
                      </Link>
                    ) : null}
                  </div>
                </div>
              </td>
              {!isTournament ? (
                <td className="px-3 py-1.5 text-[12px] text-center text-text-secondary">
                  <Link
                    href={buildClubCompetitionContextHref(row.clubId, competitionId, seasonId)}
                    className="inline-flex items-center justify-center gap-1.5 leading-none transition-colors hover:text-text-primary"
                  >
                    <ClubBadge shortName={row.clubShortName} clubId={row.clubId} logo={row.clubLogo} size="sm" showText={false} className="h-4 w-4" />
                    <span className="text-[11px] font-medium">{row.clubShortName}</span>
                  </Link>
                </td>
              ) : null}
              <td className="px-3 py-1.5 text-center text-[12px] font-semibold tabular-nums text-text-primary">{row.goals}</td>
              {!isTournament ? <td className="px-3 py-1.5 text-center text-[12px] tabular-nums text-text-secondary">{row.assists}</td> : null}
            </tr>
          ))}
        </tbody>
      </table>
    </SectionCard>
  );
}
