import { FixtureCard } from '@/components/data/FixtureCard';
import { MatchCard } from '@/components/data/MatchCard';
import { CollapsibleList } from '@/components/ui/CollapsibleList';
import { MatchSectionTitle, renderMatchSectionDateLabel } from '@/components/ui/MatchSectionTitle';
import type { Match } from '@/data/types';
import { isUpcomingMatchStatus } from '@/lib/matchStatus';

interface MatchArchiveSplitListProps {
  matches: Match[];
  locale: string;
  recentResultsLabel: string;
  upcomingFixturesLabel: string;
  emptyLabel: string;
  limit?: number;
}

export function MatchArchiveSplitList({
  matches,
  locale,
  recentResultsLabel,
  upcomingFixturesLabel,
  emptyLabel,
  limit = 10,
}: MatchArchiveSplitListProps) {
  const completedMatches = matches.filter((match) => !isUpcomingMatchStatus(match.status));
  const upcomingMatches = matches.filter((match) => isUpcomingMatchStatus(match.status));
  const completedDateLabel = renderMatchSectionDateLabel(completedMatches[0], locale);
  const upcomingDateLabel = renderMatchSectionDateLabel(upcomingMatches[0], locale);

  if (matches.length === 0) {
    return <div className="text-[13px] text-text-secondary">{emptyLabel}</div>;
  }

  return (
    <div className="space-y-3">
      {completedMatches.length > 0 ? (
        <div>
          <div className="mb-2">
            <MatchSectionTitle
              title={recentResultsLabel}
              count={completedMatches.length}
              dateLabel={completedDateLabel}
              variant="results"
            />
          </div>
          <CollapsibleList limit={limit}>
            {completedMatches.map((match) => (
              <MatchCard key={match.id} match={match} />
            ))}
          </CollapsibleList>
        </div>
      ) : null}

      {upcomingMatches.length > 0 ? (
        <div>
          <div className="mb-2">
            <MatchSectionTitle
              title={upcomingFixturesLabel}
              count={upcomingMatches.length}
              dateLabel={upcomingDateLabel}
              variant="fixtures"
            />
          </div>
          <CollapsibleList limit={limit}>
            {upcomingMatches.map((match) => (
              <FixtureCard key={match.id} match={match} />
            ))}
          </CollapsibleList>
        </div>
      ) : null}
    </div>
  );
}
