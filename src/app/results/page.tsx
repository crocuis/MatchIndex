'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { useTranslations } from 'next-intl';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { ClubBadge } from '@/components/ui/ClubBadge';
import {
  getFinishedMatches,
  getLeagues,
  getLeagueById,
  getClubById,
  getClubName,
} from '@/data';
import { cn } from '@/lib/utils';

export default function ResultsPage() {
  const router = useRouter();
  const tResults = useTranslations('results');
  const [selectedLeague, setSelectedLeague] = useState<string>('all');
  const leagues = getLeagues();
  const allResults = getFinishedMatches();

  const filteredResults = selectedLeague === 'all'
    ? allResults
    : allResults.filter((m) => m.leagueId === selectedLeague);

  return (
    <div>
      <PageHeader
        title={tResults('title')}
        subtitle={tResults('matchCount', { count: filteredResults.length })}
      />

      {/* League filter */}
      <div className="flex gap-1.5 mb-4">
        <button
          onClick={() => setSelectedLeague('all')}
          className={cn(
            'px-3 py-1.5 rounded text-[12px] font-medium transition-colors',
            selectedLeague === 'all'
              ? 'bg-surface-3 text-text-primary'
              : 'bg-surface-2 text-text-muted hover:text-text-secondary'
          )}
        >
          {tResults('allLeagues')}
        </button>
        {leagues.map((league) => (
          <button
            key={league.id}
            onClick={() => setSelectedLeague(league.id)}
            className={cn(
              'px-3 py-1.5 rounded text-[12px] font-medium transition-colors',
              selectedLeague === league.id
                ? 'bg-surface-3 text-text-primary'
                : 'bg-surface-2 text-text-muted hover:text-text-secondary'
            )}
          >
            {league.name}
          </button>
        ))}
      </div>

      <SectionCard title={tResults('matchResults')} noPadding>
        <table className="w-full">
          <thead>
            <tr className="border-b border-border">
               <th className="px-3 py-2 text-left w-20">{tResults('date')}</th>
               <th className="px-3 py-2 text-right">{tResults('home')}</th>
               <th className="px-3 py-2 text-center w-16">{tResults('score')}</th>
               <th className="px-3 py-2 text-left">{tResults('away')}</th>
               <th className="px-3 py-2 text-left">{tResults('league')}</th>
               <th className="px-3 py-2 text-left">{tResults('venue')}</th>
             </tr>
          </thead>
          <tbody className="divide-y divide-border-subtle">
            {filteredResults.map((match) => {
              const league = getLeagueById(match.leagueId);
              const homeClub = getClubById(match.homeTeamId);
              const awayClub = getClubById(match.awayTeamId);

              return (
                <tr
                  key={match.id}
                  className="hover:bg-surface-2 cursor-pointer"
                  onClick={() => router.push(`/matches/${match.id}`)}
                >
                  <td className="px-3 py-1.5 text-[13px] text-text-muted tabular-nums">
                    {new Date(match.date).toLocaleDateString('en-GB', { day: '2-digit', month: 'short' })}
                  </td>
                  <td className="px-3 py-1.5 text-[13px] text-right font-medium text-text-primary">
                    <div className="flex items-center justify-end gap-2">
                      {homeClub && <ClubBadge shortName={homeClub.shortName} clubId={homeClub.id} size="sm" />}
                      <span>{getClubName(match.homeTeamId)}</span>
                    </div>
                  </td>
                  <td className="px-3 py-1.5 text-[13px] text-center font-bold tabular-nums">
                    {match.homeScore} - {match.awayScore}
                  </td>
                  <td className="px-3 py-1.5 text-[13px] font-medium text-text-primary">
                    <div className="flex items-center gap-2">
                      {awayClub && <ClubBadge shortName={awayClub.shortName} clubId={awayClub.id} size="sm" />}
                      <span>{getClubName(match.awayTeamId)}</span>
                    </div>
                  </td>
                  <td className="px-3 py-1.5 text-[13px] text-text-secondary">
                    {league?.name ?? '-'}
                  </td>
                  <td className="px-3 py-1.5 text-[13px] text-text-muted">
                    {match.venue}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </SectionCard>
    </div>
  );
}
