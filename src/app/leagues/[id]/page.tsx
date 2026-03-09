import { notFound } from 'next/navigation';
import Link from 'next/link';
import { Suspense } from 'react';
import { getLocale, getTranslations } from 'next-intl/server';
import { LeagueDetailSections } from '@/app/leagues/[id]/LeagueDetailSections';
import { SectionCard } from '@/components/ui/SectionCard';
import { PageHeader } from '@/components/layout/PageHeader';
import { StatPanel } from '@/components/data/StatPanel';
import { Badge } from '@/components/ui/Badge';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import { cn } from '@/lib/utils';
import { isTournamentCompetition } from '@/data/competitionTypes';
import {
  getLeagueByIdDb,
  getSeasonsByLeagueDb,
} from '@/data/server';

export default async function LeaguePage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ season?: string }>;
}) {
  const { id } = await params;
  const { season } = await searchParams;
  const locale = await getLocale();
  const league = await getLeagueByIdDb(id, locale);
  if (!league) notFound();

  const availableSeasons = await getSeasonsByLeagueDb(id);
  const currentSeason = availableSeasons.find((s) => s.isCurrent) ?? availableSeasons[0];
  const selectedSeason = season
    ? availableSeasons.find((s) => s.seasonId === season) ?? currentSeason
    : currentSeason;
  const isNonDefaultSeason = selectedSeason && selectedSeason.seasonId !== currentSeason?.seasonId;

  const [tLeague, tCommon] = await Promise.all([
    getTranslations('league'),
    getTranslations('common'),
  ]);
  const isTournament = isTournamentCompetition(league);
  const formatLabel = isTournament ? tLeague('formatTournament') : tLeague('formatLeague');
  const formatDetail = isTournament ? tLeague('formatTournamentDetail') : tLeague('formatLeagueDetail');

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <LeagueLogo leagueId={league.id} name={league.name} logo={league.logo} size="lg" />
            <span>{league.name}</span>
            <Badge variant={isTournament ? 'info' : 'default'}>{formatLabel}</Badge>
          </div>
        )}
        subtitle={`${league.country} · ${tLeague('season')} ${selectedSeason?.seasonLabel ?? league.season}`}
        meta={isTournament ? formatDetail : `${league.numberOfClubs} ${tLeague('clubs')}`}
      />

      <StatPanel
        stats={[
          { label: tLeague('country'), value: league.country },
          { label: tLeague('season'), value: selectedSeason?.seasonLabel ?? league.season },
          { label: tLeague('format'), value: formatLabel },
          { label: tLeague('clubs'), value: league.numberOfClubs },
        ]}
        columns={4}
        className="mb-4"
      />

      {availableSeasons.length > 1 ? (
        <SectionCard title={tLeague('seasonHistory')} className="mb-4">
          <div className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-text-muted">
            {tLeague('selectSeason')}
          </div>
          <div className="flex flex-wrap gap-2">
            {availableSeasons.map((entry) => {
              const isActive = entry.seasonId === selectedSeason?.seasonId;

              return (
                <Link
                  key={entry.seasonId}
                  href={`/leagues/${id}?season=${entry.seasonId}`}
                  className={cn(
                    'rounded border px-2.5 py-1 text-[11px] font-medium transition-colors',
                    isActive
                      ? 'border-accent-emerald bg-accent-emerald/10 text-accent-emerald'
                      : 'border-border-subtle bg-surface-2 text-text-secondary hover:border-border hover:text-text-primary'
                  )}
                >
                  {entry.seasonLabel}
                </Link>
              );
            })}
          </div>
        </SectionCard>
      ) : null}

      <Suspense
        fallback={
          <SectionCard title={tLeague('standings')}>
            <div className="py-8 text-center text-[13px] text-text-muted">{tCommon('loading')}</div>
          </SectionCard>
        }
      >
        <LeagueDetailSections
          league={league}
          locale={locale}
          selectedSeason={selectedSeason}
          isNonDefaultSeason={Boolean(isNonDefaultSeason)}
          isTournament={isTournament}
        />
      </Suspense>
    </div>
  );
}
