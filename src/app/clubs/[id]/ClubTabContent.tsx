import { Suspense } from 'react';
import { getTranslations } from 'next-intl/server';
import { ClubSeasonArchiveSections } from '@/app/clubs/[id]/ClubSeasonArchiveSections';
import { ClubSeasonSelect } from '@/app/clubs/[id]/ClubSeasonSelect';
import { FixtureCard } from '@/components/data/FixtureCard';
import { MatchCard } from '@/components/data/MatchCard';
import { StatPanel } from '@/components/data/StatPanel';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { MatchSectionTitle, renderMatchSectionDateLabel } from '@/components/ui/MatchSectionTitle';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { SectionCard } from '@/components/ui/SectionCard';
import type { Club, ClubSeasonHistoryEntry, League } from '@/data/types';
import { getClubOverviewStatsDb, getPlayersByClubDb, getPlayersByClubAndSeasonDb, getRecentFinishedMatchesByClubAndSeasonDb, getUpcomingScheduledMatchesByClubAndSeasonDb, getClubSeasonMetaDb } from '@/data/server';
import { cn, formatNumber, getClubDisplayName, getPositionColor } from '@/lib/utils';

interface SelectedSeasonValue {
  seasonId: string;
  seasonLabel: string;
  leagueId: string;
  leagueName: string;
  position?: number;
  points: number;
}

interface ClubTabContentProps {
  activeTab: 'overview' | 'squad' | 'archive';
  club: Club;
  league?: League;
  locale: string;
  seasonHistory: ClubSeasonHistoryEntry[];
  selectedArchiveSeasonHref?: string;
  selectedSeason?: SelectedSeasonValue;
}

export async function ClubTabContent({ activeTab, club, league, locale, seasonHistory, selectedArchiveSeasonHref, selectedSeason }: ClubTabContentProps) {
  const [tClub, tTable, tLeague, tStandings, overviewStats, squad, seasonMeta, recentMatches, upcomingFixtures] = await Promise.all([
    getTranslations('club'),
    getTranslations('table'),
    getTranslations('league'),
    getTranslations('standings'),
    getClubOverviewStatsDb(club.id, selectedSeason?.seasonId, selectedSeason?.leagueId),
    activeTab === 'squad'
      ? selectedSeason
        ? getPlayersByClubAndSeasonDb(club.id, selectedSeason.seasonId, selectedSeason.leagueId, locale)
        : getPlayersByClubDb(club.id, locale)
      : Promise.resolve([]),
    activeTab === 'overview' && selectedSeason ? getClubSeasonMetaDb(club.id, selectedSeason.seasonId, locale) : Promise.resolve({} as { coachName?: string }),
    activeTab === 'overview' && selectedSeason ? getRecentFinishedMatchesByClubAndSeasonDb(club.id, selectedSeason.seasonId, null, locale, 5) : Promise.resolve([]),
    activeTab === 'overview' && selectedSeason ? getUpcomingScheduledMatchesByClubAndSeasonDb(club.id, selectedSeason.seasonId, null, locale, 5) : Promise.resolve([]),
  ]);
  const clubDisplayName = getClubDisplayName(club, locale);
  const recentMatchesDateLabel = renderMatchSectionDateLabel(recentMatches[0], locale);
  const upcomingFixturesDateLabel = renderMatchSectionDateLabel(upcomingFixtures[0], locale);
  const seasonGroups = Array.from(
    seasonHistory.reduce((map, entry) => {
      const group = map.get(entry.seasonLabel) ?? [];
      group.push(entry);
      map.set(entry.seasonLabel, group);
      return map;
    }, new Map<string, ClubSeasonHistoryEntry[]>())
  );
  const visibleSeasonGroups = selectedSeason
    ? seasonGroups.filter(([seasonLabel]) => seasonLabel === selectedSeason.seasonLabel)
    : seasonGroups;
  const seasonSelectGroups = seasonGroups.map(([seasonLabel, entries]) => ({ seasonLabel, entries }));

  return (
    <>
      <StatPanel
        stats={[
          { label: tClub('leaguePos'), value: selectedSeason?.position ? `#${selectedSeason.position}` : '-', highlight: selectedSeason?.position === 1 },
          { label: tClub('points'), value: selectedSeason?.points ?? '-' },
          { label: tClub('goalsScored'), value: overviewStats.goals },
          { label: tClub('assists'), value: overviewStats.assists },
        ]}
        columns={4}
        className="mb-4"
      />

      <div className="grid grid-cols-12 gap-4">
        <div className="col-span-8 space-y-4">
          {activeTab === 'archive' ? (
            <SectionCard title={tClub('seasonHistory')} noPadding>
              <div className="border-b border-border-subtle px-3 py-2">
                <div className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-text-muted">{tClub('selectSeason')}</div>
                <ClubSeasonSelect clubId={club.id} selectedValue={selectedArchiveSeasonHref} groups={seasonSelectGroups} tab="archive" />
              </div>
              <div className="space-y-3 p-3">
                {visibleSeasonGroups.map(([seasonLabel, entries]) => (
                  <div key={`table-${seasonLabel}`} className="overflow-hidden rounded border border-border-subtle bg-surface-2">
                    <div className="border-b border-border-subtle px-3 py-2 text-[12px] font-semibold text-text-primary">{seasonLabel}</div>
                    <table className="w-full"><thead><tr className="border-b border-border-subtle"><th className="px-3 py-2 text-left">{tClub('league')}</th><th className="px-3 py-2 text-center">{tClub('leaguePos')}</th><th className="px-3 py-2 text-center">{tStandings('played')}</th><th className="px-3 py-2 text-center">{tStandings('goalDifference')}</th><th className="px-3 py-2 text-center">{tStandings('points')}</th><th className="px-3 py-2 text-center">{tStandings('form')}</th></tr></thead><tbody className="divide-y divide-border-subtle">{entries.map((entry) => { const isActive = entry.seasonId === selectedSeason?.seasonId && entry.leagueId === selectedSeason?.leagueId; return (<tr key={`${entry.seasonId}:${entry.leagueId}`} className={cn('hover:bg-surface-1', isActive && 'bg-surface-1')}><td className="px-3 py-2 text-[13px] text-text-secondary">{entry.leagueName}</td><td className="px-3 py-2 text-center text-[13px] tabular-nums font-semibold">{entry.position ? `#${entry.position}` : '-'}</td><td className="px-3 py-2 text-center text-[13px] tabular-nums">{entry.played}</td><td className={cn('px-3 py-2 text-center text-[13px] tabular-nums font-medium', entry.goalDifference > 0 ? 'text-emerald-400' : entry.goalDifference < 0 ? 'text-red-400' : 'text-text-secondary')}>{entry.goalDifference > 0 ? `+${entry.goalDifference}` : entry.goalDifference}</td><td className="px-3 py-2 text-center text-[13px] tabular-nums font-semibold">{entry.points}</td><td className="px-3 py-2"><div className="flex justify-center gap-1">{entry.form.map((value, index) => <span key={`${entry.seasonId}:${index}`} className={cn('inline-flex h-5 w-5 items-center justify-center rounded text-[10px] font-bold', value === 'W' && 'bg-emerald-500/15 text-emerald-400', value === 'D' && 'bg-amber-500/15 text-amber-300', value === 'L' && 'bg-red-500/15 text-red-400')}>{value}</span>)}</div></td></tr>); })}</tbody></table>
                  </div>
                ))}
              </div>
            </SectionCard>
          ) : null}

          {activeTab === 'archive' && selectedSeason ? (
            <Suspense fallback={<SectionCard title={`${tClub('seasonStandings')} · ${selectedSeason.seasonLabel}`}><div className="py-8 text-center text-[13px] text-text-muted">{tClub('selectSeason')}</div></SectionCard>}>
              <ClubSeasonArchiveSections clubId={club.id} leagueId={selectedSeason.leagueId} seasonId={selectedSeason.seasonId} seasonLabel={selectedSeason.seasonLabel} locale={locale} />
            </Suspense>
          ) : null}

          {activeTab === 'squad' ? (
            <SectionCard title={`${tClub('squad')} · ${selectedSeason?.seasonLabel ?? league?.season ?? '-'} (${squad.length})`} noPadding>
              <table className="w-full"><thead><tr className="border-b border-border"><th className="px-3 py-2 text-center w-8">{tTable('rank')}</th><th className="px-3 py-2 text-left">{tTable('player')}</th><th className="px-3 py-2 text-center w-16">{tTable('pos')}</th><th className="px-3 py-2 text-center w-10">{tTable('age')}</th><th className="px-3 py-2 text-left">{tTable('nationality')}</th><th className="px-3 py-2 text-center w-10">{tTable('app')}</th><th className="px-3 py-2 text-center w-10">{tTable('goals')}</th><th className="px-3 py-2 text-center w-10">{tTable('assists')}</th></tr></thead><tbody className="divide-y divide-border-subtle">{squad.slice().sort((a, b) => ({ GK: 0, DEF: 1, MID: 2, FWD: 3 }[a.position] - { GK: 0, DEF: 1, MID: 2, FWD: 3 }[b.position])).map((player) => <tr key={player.id} className="hover:bg-surface-2"><td className="px-3 py-1.5 text-[13px] text-center text-text-muted tabular-nums">{player.shirtNumber}</td><td className="px-3 py-1.5 text-[13px]"><EntityLink type="player" id={player.id} className="flex items-center gap-2"><PlayerAvatar name={player.name} position={player.position} imageUrl={player.photoUrl} size="sm" /><span>{player.name}</span></EntityLink></td><td className="px-3 py-1.5 text-center"><span className={cn('px-1.5 py-0.5 rounded text-[10px] font-bold', getPositionColor(player.position))}>{player.position}</span></td><td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">{player.age}</td><td className="px-3 py-1.5 text-[13px] text-text-secondary">{player.nationality}</td><td className="px-3 py-1.5 text-[13px] text-center tabular-nums">{player.seasonStats.appearances}</td><td className="px-3 py-1.5 text-[13px] text-center tabular-nums font-semibold">{player.seasonStats.goals}</td><td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">{player.seasonStats.assists}</td></tr>)}</tbody></table>
            </SectionCard>
          ) : null}

          {activeTab === 'overview' ? (
            <>
              <SectionCard title={<MatchSectionTitle title={tClub('recentMatches')} count={recentMatches.length} dateLabel={recentMatchesDateLabel} variant="results" />}><div className="space-y-1.5">{recentMatches.map((match) => <MatchCard key={match.id} match={match} />)}</div></SectionCard>
              <SectionCard title={<MatchSectionTitle title={tClub('upcomingFixtures')} count={upcomingFixtures.length} dateLabel={upcomingFixturesDateLabel} variant="fixtures" />}><div className="space-y-1.5">{upcomingFixtures.map((match) => <FixtureCard key={match.id} match={match} />)}</div></SectionCard>
            </>
          ) : null}
        </div>

        <div className="col-span-4 space-y-4">
          {activeTab === 'overview' ? (
            <SectionCard title={tClub('clubInfo')}>
              <div className="mb-3 flex items-center justify-center"><ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="lg" /></div>
              <dl className="space-y-2">{[[tLeague('season'), selectedSeason?.seasonLabel ?? league?.season ?? '-'], [tClub('fullName'), clubDisplayName], [tClub('shortName'), club.shortName], [tClub('founded'), String(club.founded)], [tClub('stadium'), club.stadium], [tClub('capacity'), formatNumber(club.stadiumCapacity)], [tClub('country'), club.country], [tClub('league'), selectedSeason?.leagueName ?? league?.name ?? '-'], ...(seasonMeta.coachName ? [[tClub('coach'), seasonMeta.coachName] as const] : [])].map(([label, value]) => <div key={label} className="flex justify-between"><dt className="text-[12px] text-text-muted">{label}</dt><dd className="text-[13px] text-text-primary font-medium">{value}</dd></div>)}</dl>
            </SectionCard>
          ) : null}
        </div>
      </div>
    </>
  );
}
