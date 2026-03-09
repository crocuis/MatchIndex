import { getTranslations } from 'next-intl/server';
import { FixtureCard } from '@/components/data/FixtureCard';
import { MatchCard } from '@/components/data/MatchCard';
import { StandingsTable } from '@/components/data/StandingsTable';
import { SectionCard } from '@/components/ui/SectionCard';
import { getMatchesByClubAndSeasonDb, getStandingsByLeagueAndSeasonDb } from '@/data/server';

interface ClubSeasonArchiveSectionsProps {
  clubId: string;
  leagueId: string;
  seasonId: string;
  seasonLabel: string;
  locale: string;
}

export async function ClubSeasonArchiveSections({
  clubId,
  leagueId,
  seasonId,
  seasonLabel,
  locale,
}: ClubSeasonArchiveSectionsProps) {
  const [tClub, standings, seasonMatches] = await Promise.all([
    getTranslations('club'),
    getStandingsByLeagueAndSeasonDb(leagueId, seasonId, locale),
    getMatchesByClubAndSeasonDb(clubId, seasonId, locale),
  ]);

  return (
    <>
      <SectionCard title={`${tClub('seasonStandings')} · ${seasonLabel}`} noPadding>
        <StandingsTable standings={standings} />
      </SectionCard>

      <SectionCard title={`${tClub('seasonMatchArchive')} · ${seasonLabel}`}>
        <div className="space-y-1.5">
          {seasonMatches.length > 0 ? seasonMatches.map((match) => (
            match.status === 'scheduled'
              ? <FixtureCard key={match.id} match={match} />
              : <MatchCard key={match.id} match={match} />
          )) : <div className="text-[13px] text-text-secondary">{tClub('seasonMatchesEmpty')}</div>}
        </div>
      </SectionCard>
    </>
  );
}
