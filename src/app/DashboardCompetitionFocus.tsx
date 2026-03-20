'use client';

import { useState, useCallback, type ReactNode } from 'react';
import { useTranslations } from 'next-intl';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import { cn } from '@/lib/utils';
import type { League } from '@/data/types';

interface DashboardCompetitionFocusProps {
  primaryLeagues: League[];
  secondaryLeagues: League[];
  initialLeagueId: string;
  defaultLeagueId: string;
  panels: Record<string, ReactNode>;
}

export function DashboardCompetitionFocus({
  primaryLeagues,
  secondaryLeagues,
  initialLeagueId,
  defaultLeagueId,
  panels,
}: DashboardCompetitionFocusProps) {
  const [selectedLeagueId, setSelectedLeagueId] = useState(initialLeagueId);
  const tDashboard = useTranslations('dashboard');

  const handleLeagueSelect = useCallback((leagueId: string) => {
    setSelectedLeagueId(leagueId);

    const url = new URL(window.location.href);
    if (leagueId === defaultLeagueId) {
      url.searchParams.delete('league');
    } else {
      url.searchParams.set('league', leagueId);
    }
    window.history.replaceState(null, '', url.toString());
  }, [defaultLeagueId]);

  return (
    <section className="space-y-4">
      <div>
        <div className="text-[10px] font-semibold uppercase tracking-[0.24em] text-text-muted">
          {tDashboard('competitionFocus')}
        </div>
      </div>

      <div>
        <div className="mb-4 rounded-lg border border-border bg-surface-1/70 p-2">
          <div className="space-y-1.5">
            {[primaryLeagues, secondaryLeagues].map((leagueRow, rowIndex) => (
              <div key={`dashboard-league-row-${rowIndex}`} className="grid gap-1.5 md:grid-cols-5">
                {leagueRow.map((league) => {
                  const isActive = league.id === selectedLeagueId;

                  return (
                    <button
                      key={league.id}
                      type="button"
                      onClick={() => handleLeagueSelect(league.id)}
                      className={cn(
                        'flex min-h-11 items-center gap-2 rounded-md border px-3 py-2 text-left text-[13px] font-medium transition-colors',
                        isActive
                          ? 'border-accent-emerald bg-accent-emerald/10 text-text-primary'
                          : 'border-border-subtle bg-surface-0/50 text-text-muted hover:border-border hover:bg-surface-2/40 hover:text-text-secondary'
                      )}
                    >
                      <LeagueLogo leagueId={league.id} name={league.name} competitionType={league.competitionType} logo={league.logo} size="sm" />
                      <span>{league.name}</span>
                    </button>
                  );
                })}
                {rowIndex === 1 && leagueRow.length < primaryLeagues.length
                  ? Array.from({ length: primaryLeagues.length - leagueRow.length }, (_, fillerIndex) => (
                    <div
                      key={`dashboard-league-filler-${fillerIndex}`}
                      className="hidden md:block"
                    />
                  ))
                  : null}
              </div>
            ))}
          </div>
        </div>

        {panels[selectedLeagueId]}
      </div>
    </section>
  );
}
