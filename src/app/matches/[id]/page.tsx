import { notFound } from 'next/navigation';
import type { Metadata } from 'next';
import { getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { Badge } from '@/components/ui/Badge';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { formatDate, cn } from '@/lib/utils';
import {
  getMatchById,
  getMatches,
  getClubById,
  getLeagueById,
  getPlayerById,
  getPlayerName,
} from '@/data';

export async function generateStaticParams() {
  return getMatches().map((m) => ({ id: m.id }));
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ id: string }>;
}): Promise<Metadata> {
  const { id } = await params;
  const match = getMatchById(id);
  if (!match) return { title: 'Match' };
  const home = getClubById(match.homeTeamId);
  const away = getClubById(match.awayTeamId);
  return { title: `${home?.shortName ?? '?'} vs ${away?.shortName ?? '?'}` };
}

export default async function MatchPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const match = getMatchById(id);
  if (!match) notFound();

  const homeClub = getClubById(match.homeTeamId);
  const awayClub = getClubById(match.awayTeamId);
  const league = getLeagueById(match.leagueId);
  const tMatch = await getTranslations('match');
  const tCommon = await getTranslations('common');
  const tMatchStatus = await getTranslations('matchStatus');
  const statusText = tMatchStatus(match.status);

  return (
    <div>
      <PageHeader
        title={`${homeClub?.name ?? tCommon('home')} ${tCommon('vs')} ${awayClub?.name ?? tCommon('away')}`}
        subtitle={`${league?.name ?? ''} · ${formatDate(match.date)}`}
      />

      {/* Score Header */}
      <div className="rounded-lg border border-border bg-surface-1 p-6 mb-4">
        <div className="flex items-center justify-center gap-8">
          {/* Home */}
          <div className="text-right flex-1">
            <EntityLink type="club" id={match.homeTeamId} className="inline-flex items-center gap-2">
              {homeClub && <ClubBadge shortName={homeClub.shortName} clubId={homeClub.id} size="lg" />}
               <span className="text-lg font-bold text-text-primary">{homeClub?.name ?? tCommon('home')}</span>
             </EntityLink>
            <div className="text-[11px] text-text-muted mt-0.5">{homeClub?.shortName}</div>
          </div>

          {/* Score */}
          <div className="flex items-center gap-3">
            {match.status === 'finished' ? (
              <>
                <span className="text-3xl font-bold tabular-nums text-text-primary">{match.homeScore}</span>
                <span className="text-lg text-text-muted">-</span>
                <span className="text-3xl font-bold tabular-nums text-text-primary">{match.awayScore}</span>
              </>
            ) : (
              <span className="text-lg text-text-muted">{match.time}</span>
            )}
          </div>

          {/* Away */}
          <div className="text-left flex-1">
            <EntityLink type="club" id={match.awayTeamId} className="inline-flex items-center gap-2">
              {awayClub && <ClubBadge shortName={awayClub.shortName} clubId={awayClub.id} size="lg" />}
               <span className="text-lg font-bold text-text-primary">{awayClub?.name ?? tCommon('away')}</span>
             </EntityLink>
            <div className="text-[11px] text-text-muted mt-0.5">{awayClub?.shortName}</div>
          </div>
        </div>

        <div className="flex items-center justify-center gap-3 mt-3">
          <Badge variant={match.status === 'finished' ? 'default' : match.status === 'live' ? 'danger' : 'info'}>
            {statusText}
          </Badge>
          <span className="text-[11px] text-text-muted">{match.venue}</span>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-4">
        {/* Main */}
        <div className="col-span-8 space-y-4">
          {/* Match Events */}
          {match.events && match.events.length > 0 && (
            <SectionCard title={tMatch('matchEvents')}>
              <div className="space-y-1">
                {match.events.map((event, i) => {
                  const player = getPlayerById(event.playerId);

                  return (
                    <div
                      key={i}
                      className={cn(
                        'flex items-center gap-3 px-3 py-1.5 rounded',
                        event.teamId === match.homeTeamId ? 'bg-surface-2' : 'bg-surface-2/50'
                      )}
                    >
                      <span className="text-[13px] tabular-nums text-text-muted w-8">{event.minute}&apos;</span>
                      <span className={cn(
                        'text-[10px] font-bold uppercase w-16',
                        event.type === 'goal' ? 'text-emerald-400' :
                          event.type === 'yellow_card' ? 'text-amber-400' :
                            event.type === 'red_card' ? 'text-red-400' :
                              'text-text-muted'
                      )}>
                        {event.type.replace('_', ' ')}
                      </span>
                      <EntityLink type="player" id={event.playerId} className="flex items-center gap-2 text-[13px]">
                        {player && <PlayerAvatar name={player.name} position={player.position} size="sm" />}
                        <span>{getPlayerName(event.playerId)}</span>
                      </EntityLink>
                      {event.detail && (
                        <span className="text-[11px] text-text-muted">({event.detail})</span>
                      )}
                    </div>
                  );
                })}
              </div>
            </SectionCard>
          )}

          {/* Match Stats */}
          {match.stats && (
            <SectionCard title={tMatch('matchStats')}>
              <div className="space-y-3">
                {[
                  { label: tMatch('possession'), values: match.stats.possession, suffix: '%' },
                  { label: tMatch('shots'), values: match.stats.shots },
                  { label: tMatch('shotsOnTarget'), values: match.stats.shotsOnTarget },
                  { label: tMatch('corners'), values: match.stats.corners },
                  { label: tMatch('fouls'), values: match.stats.fouls },
                ].map((stat) => (
                  <div key={stat.label} className="flex items-center gap-3">
                    <span className={cn(
                      'text-[13px] tabular-nums w-10 text-right font-semibold',
                      stat.values[0] > stat.values[1] ? 'text-text-primary' : 'text-text-secondary'
                    )}>
                      {stat.values[0]}{stat.suffix ?? ''}
                    </span>
                    <div className="flex-1">
                      <div className="text-[11px] text-text-muted text-center mb-1">{stat.label}</div>
                      <div className="flex h-1.5 rounded-full overflow-hidden bg-surface-3">
                        <div
                          className="bg-accent-emerald rounded-l-full"
                          style={{ width: `${(stat.values[0] / (stat.values[0] + stat.values[1])) * 100}%` }}
                        />
                        <div
                          className="bg-accent-blue rounded-r-full"
                          style={{ width: `${(stat.values[1] / (stat.values[0] + stat.values[1])) * 100}%` }}
                        />
                      </div>
                    </div>
                    <span className={cn(
                      'text-[13px] tabular-nums w-10 font-semibold',
                      stat.values[1] > stat.values[0] ? 'text-text-primary' : 'text-text-secondary'
                    )}>
                      {stat.values[1]}{stat.suffix ?? ''}
                    </span>
                  </div>
                ))}
              </div>
            </SectionCard>
          )}

          {/* Lineups placeholder */}
          <SectionCard title={tMatch('lineups')}>
            <div className="text-[13px] text-text-muted text-center py-8">
              {tMatch('lineupsPlaceholder')}
            </div>
          </SectionCard>
        </div>

        {/* Sidebar */}
        <div className="col-span-4 space-y-4">
          <SectionCard title={tMatch('matchInfo')}>
            <dl className="space-y-2">
              {[
                [tMatch('competition'), league?.name ?? '-'],
                [tMatch('date'), formatDate(match.date)],
                [tMatch('kickoff'), match.time],
                [tMatch('venue'), match.venue],
                [tMatch('status'), statusText],
              ].map(([label, value]) => (
                <div key={label} className="flex justify-between">
                  <dt className="text-[12px] text-text-muted">{label}</dt>
                  <dd className="text-[13px] text-text-primary font-medium">{value}</dd>
                </div>
              ))}
            </dl>
          </SectionCard>

          <SectionCard title={tMatch('teams')}>
            <div className="space-y-3">
              {homeClub && (
                <EntityLink type="club" id={homeClub.id} className="flex items-center gap-3">
                  <ClubBadge shortName={homeClub.shortName} clubId={homeClub.id} size="md" />
                  <div>
                    <div className="text-[13px] font-medium">{homeClub.name}</div>
                    <div className="text-[10px] text-text-muted">{tCommon('home')}</div>
                  </div>
                </EntityLink>
              )}
              {awayClub && (
                <EntityLink type="club" id={awayClub.id} className="flex items-center gap-3">
                  <ClubBadge shortName={awayClub.shortName} clubId={awayClub.id} size="md" />
                  <div>
                    <div className="text-[13px] font-medium">{awayClub.name}</div>
                    <div className="text-[10px] text-text-muted">{tCommon('away')}</div>
                  </div>
                </EntityLink>
              )}
            </div>
          </SectionCard>
        </div>
      </div>
    </div>
  );
}
