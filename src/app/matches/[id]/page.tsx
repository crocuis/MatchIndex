import { notFound } from 'next/navigation';
import { Suspense } from 'react';
import { getLocale, getTranslations } from 'next-intl/server';
import { MatchAnalysisSection } from '@/app/matches/[id]/MatchAnalysisSection';
import { MatchLineupSection } from '@/app/matches/[id]/MatchLineupSection';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { Badge } from '@/components/ui/Badge';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { NationBadge } from '@/components/ui/NationBadge';
import { NationFlag } from '@/components/ui/NationFlag';
import { WorldCupPlaceholderLink } from '@/components/ui/WorldCupPlaceholderLink';
import {
  cn,
  formatMatchDateForTimeZone,
  formatMatchDateTimeForTimeZone,
  formatMatchTimeForTimeZone,
  getMatchSourceOffsetMinutes,
} from '@/lib/utils';
import {
  getMatchByIdDb,
} from '@/data/server';

export const dynamic = 'force-dynamic';

export default async function MatchPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const locale = await getLocale();
  const { id } = await params;
  const match = await getMatchByIdDb(id, locale);
  if (!match) notFound();
  const isNationMatch = match.teamType === 'nation';

  const tMatch = await getTranslations('match');
  const tCommon = await getTranslations('common');
  const tMatchStatus = await getTranslations('matchStatus');
  const statusText = tMatchStatus(match.status);
  const isWorldCupMatch = match.id.startsWith('m-wc26-');
  const timelineEvents = match.events ?? [];
  const matchStats = match.stats;
  const sourceOffsetMinutes = getMatchSourceOffsetMinutes({ id: match.id, venue: match.venue });
  const matchDateText = formatMatchDateForTimeZone(match.date, match.time, locale, 'UTC', sourceOffsetMinutes);
  const matchTimeText = formatMatchTimeForTimeZone(match.date, match.time, locale, 'UTC', sourceOffsetMinutes);
  const hostLocalKickoffText = formatMatchDateTimeForTimeZone(match.date, match.time, locale, 'UTC');
  const homeLabel = match.homeParticipant?.displayName ?? match.homeTeamName ?? match.homeTeamCode ?? tCommon('home');
  const awayLabel = match.awayParticipant?.displayName ?? match.awayTeamName ?? match.awayTeamCode ?? tCommon('away');
  const homeCode = match.homeParticipant?.displayCode ?? match.homeTeamCode;
  const awayCode = match.awayParticipant?.displayCode ?? match.awayTeamCode;
  const competitionLabel = match.competitionName ?? '-';
  const goalSummary = timelineEvents.filter((event) => event.type === 'goal');
  const homeGoals = goalSummary.filter((event) => event.teamId === match.homeTeamId);
  const awayGoals = goalSummary.filter((event) => event.teamId === match.awayTeamId);
  const homeResolvedNationId = isNationMatch
    ? (match.homeParticipant?.status === 'resolved' ? match.homeParticipant.entityId : match.homeTeamId)
    : undefined;
  const awayResolvedNationId = isNationMatch
    ? (match.awayParticipant?.status === 'resolved' ? match.awayParticipant.entityId : match.awayTeamId)
    : undefined;

  function getSpecialTag(rawType?: string) {
    if (rawType === 'penalty_scored') return tMatch('penalty');
    if (rawType === 'own_goal') return tMatch('ownGoal');
    return null;
  }

  function getEventTypeLabel(type: string) {
    if (type === 'goal') return tMatch('eventGoal');
    if (type === 'yellow_card') return tMatch('eventYellowCard');
    if (type === 'red_card') return tMatch('eventRedCard');
    if (type === 'substitution') return tMatch('eventSubstitution');
    return type.replace('_', ' ');
  }

  function getLocalizedEventDetail(detail?: string, eventType?: string) {
    if (!detail) return undefined;

    if (eventType === 'substitution') {
      return detail.replace(' OUT ', ` ${tMatch('eventPlayerOut')} `).replace(' IN', ` ${tMatch('eventPlayerIn')}`);
    }

    if (detail === 'Goal') return tMatch('eventGoal');
    if (detail === 'Bad Behaviour') return tMatch('eventBadBehaviour');
    if (detail === 'Foul Committed') return tMatch('eventFoulCommitted');
    if (detail === 'VAR Decision') return tMatch('eventVarDecision');

    return detail;
  }

  return (
    <div>
      <PageHeader
        title={`${homeLabel} ${tCommon('vs')} ${awayLabel}`}
        subtitle={<><span>{competitionLabel}</span>{' · '}<span>{matchDateText}</span></>}
      />

      {/* Score Header */}
      <div className="rounded-lg border border-border bg-surface-1 p-6 mb-4">
        <div className="flex items-center justify-center gap-8">
          {/* Home */}
            <div className="text-right flex-1">
            {isNationMatch ? (
              homeResolvedNationId ? (
                <EntityLink type="nation" id={homeResolvedNationId} className="inline-flex items-center gap-2">
                  {homeCode && <NationBadge nationId={homeResolvedNationId} code={homeCode} crest={match.homeTeamLogo} size="lg" />}
                  {homeCode && <NationFlag nationId={homeResolvedNationId} code={homeCode} size="lg" />}
                  <span className="text-lg font-bold text-text-primary">{homeLabel}</span>
                </EntityLink>
              ) : match.homeParticipant?.slot ? (
                <div className="inline-flex items-center gap-2">
                  <WorldCupPlaceholderLink placeholder={match.homeParticipant.slot} label={homeLabel} />
                </div>
              ) : (
                <span className="text-lg font-bold text-text-primary">{homeLabel}</span>
              )
            ) : (
              <EntityLink type="club" id={match.homeTeamId} className="inline-flex items-center gap-2">
                <ClubBadge shortName={homeCode ?? '???'} clubId={match.homeTeamId} logo={match.homeTeamLogo} size="lg" />
                <span className="text-lg font-bold text-text-primary">{homeLabel}</span>
              </EntityLink>
            )}
             <div className="text-[11px] text-text-muted mt-0.5">{homeCode}</div>
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
              <span className="text-lg text-text-muted">{matchTimeText}</span>
            )}
          </div>

          {/* Away */}
           <div className="text-left flex-1">
             {isNationMatch ? (
               awayResolvedNationId ? (
                  <EntityLink type="nation" id={awayResolvedNationId} className="inline-flex items-center gap-2">
                   {awayCode && <NationBadge nationId={awayResolvedNationId} code={awayCode} crest={match.awayTeamLogo} size="lg" />}
                   {awayCode && <NationFlag nationId={awayResolvedNationId} code={awayCode} size="lg" />}
                    <span className="text-lg font-bold text-text-primary">{awayLabel}</span>
                  </EntityLink>
               ) : match.awayParticipant?.slot ? (
                 <div className="inline-flex items-center gap-2">
                   <WorldCupPlaceholderLink placeholder={match.awayParticipant.slot} label={awayLabel} />
                 </div>
               ) : (
                 <span className="text-lg font-bold text-text-primary">{awayLabel}</span>
               )
              ) : (
                <EntityLink type="club" id={match.awayTeamId} className="inline-flex items-center gap-2">
                 <ClubBadge shortName={awayCode ?? '???'} clubId={match.awayTeamId} logo={match.awayTeamLogo} size="lg" />
                  <span className="text-lg font-bold text-text-primary">{awayLabel}</span>
                </EntityLink>
              )}
             <div className="text-[11px] text-text-muted mt-0.5">{awayCode}</div>
           </div>
        </div>

        <div className="flex items-center justify-center gap-3 mt-3">
          <Badge variant={match.status === 'finished' ? 'default' : match.status === 'live' ? 'danger' : 'info'}>
            {statusText}
          </Badge>
          <span className="text-[11px] text-text-muted">{match.venue}</span>
        </div>

        {goalSummary.length > 0 && (
          <div className="mt-4 border-t border-border pt-4">
            <div className="mb-2 text-center text-[11px] font-bold uppercase tracking-wider text-text-muted">
              {tMatch('goalSummary')}
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                {homeGoals.map((event, index) => {
                  const specialTag = getSpecialTag(event.rawType);

                  return (
                    <div
                      key={`${event.sourceEventId ?? event.playerId}-home-${index}`}
                      className="rounded border border-border bg-surface-2 px-3 py-2 text-right"
                    >
                      <div className="mb-1 flex items-center justify-end gap-2 text-[10px] uppercase tracking-wide text-text-muted">
                        <span>{homeLabel}</span>
                        {isNationMatch ? (
                          homeResolvedNationId && homeCode ? (
                            <>
                              <NationBadge nationId={homeResolvedNationId} code={homeCode} crest={match.homeTeamLogo} size="sm" />
                              <NationFlag nationId={homeResolvedNationId} code={homeCode} size="sm" />
                            </>
                          ) : null
                        ) : homeCode ? (
                          <ClubBadge shortName={homeCode} clubId={match.homeTeamId} logo={match.homeTeamLogo} size="sm" />
                        ) : null}
                      </div>
                      <div className="flex items-center justify-end gap-2 text-[13px] font-medium text-text-primary">
                        {specialTag && <span className="text-[10px] uppercase text-amber-400">{specialTag}</span>}
                        <EntityLink type="player" id={event.playerId} className="truncate">
                          <span>{event.playerName ?? tCommon('unknown')}</span>
                        </EntityLink>
                        <span className="tabular-nums text-text-muted">{event.minute}&apos;</span>
                      </div>
                      {event.assistPlayerId && event.assistPlayerName && (
                        <div className="mt-1 text-[11px] text-text-muted">
                          {tMatch('assistBy', { name: event.assistPlayerName })}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>

              <div className="space-y-2">
                {awayGoals.map((event, index) => {
                  const specialTag = getSpecialTag(event.rawType);

                  return (
                    <div
                      key={`${event.sourceEventId ?? event.playerId}-away-${index}`}
                      className="rounded border border-border bg-surface-2/50 px-3 py-2 text-left"
                    >
                      <div className="mb-1 flex items-center gap-2 text-[10px] uppercase tracking-wide text-text-muted">
                        {isNationMatch ? (
                          awayResolvedNationId && awayCode ? (
                            <>
                              <NationBadge nationId={awayResolvedNationId} code={awayCode} crest={match.awayTeamLogo} size="sm" />
                              <NationFlag nationId={awayResolvedNationId} code={awayCode} size="sm" />
                            </>
                          ) : null
                        ) : awayCode ? (
                          <ClubBadge shortName={awayCode} clubId={match.awayTeamId} logo={match.awayTeamLogo} size="sm" />
                        ) : null}
                        <span>{awayLabel}</span>
                      </div>
                      <div className="flex items-center gap-2 text-[13px] font-medium text-text-primary">
                        <span className="tabular-nums text-text-muted">{event.minute}&apos;</span>
                        <EntityLink type="player" id={event.playerId} className="truncate">
                          <span>{event.playerName ?? tCommon('unknown')}</span>
                        </EntityLink>
                        {specialTag && <span className="text-[10px] uppercase text-amber-400">{specialTag}</span>}
                      </div>
                      {event.assistPlayerId && event.assistPlayerName && (
                        <div className="mt-1 text-[11px] text-text-muted">
                          {tMatch('assistBy', { name: event.assistPlayerName })}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        )}
      </div>

      <div className="grid grid-cols-12 gap-4">
        {/* Main */}
        <div className="col-span-8 space-y-4">
          <Suspense
            fallback={
              <SectionCard title={tMatch('lineups')}>
                <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('loading')}</div>
              </SectionCard>
            }
          >
            <MatchLineupSection
              matchId={match.id}
              locale={locale}
              homeTeamId={match.homeTeamId}
              awayTeamId={match.awayTeamId}
              homeTeamName={homeLabel}
              awayTeamName={awayLabel}
            />
          </Suspense>

          {timelineEvents.length > 0 && (
            <SectionCard title={tMatch('matchEvents')}>
              <div className="space-y-1">
                {timelineEvents.map((event, i) => {
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
                        {getEventTypeLabel(event.type)}
                      </span>
                      <EntityLink type="player" id={event.playerId} className="text-[13px] font-medium">
                        <span>{event.playerName ?? tCommon('unknown')}</span>
                      </EntityLink>
                      {event.detail && (
                        <span className="text-[11px] text-text-muted">({getLocalizedEventDetail(event.detail, event.type)})</span>
                      )}
                    </div>
                  );
                })}
              </div>
            </SectionCard>
          )}

          {/* Match Stats */}
          {matchStats && (
            <SectionCard title={tMatch('matchStats')}>
              <div className="space-y-3">
                {[
                  { label: tMatch('possession'), values: matchStats.possession, suffix: '%' },
                  { label: tMatch('shots'), values: matchStats.shots },
                  { label: tMatch('shotsOnTarget'), values: matchStats.shotsOnTarget },
                  { label: tMatch('corners'), values: matchStats.corners },
                  { label: tMatch('fouls'), values: matchStats.fouls },
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

          <Suspense
            fallback={
              <SectionCard title={tMatch('analysis')}>
                <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('loading')}</div>
              </SectionCard>
            }
          >
            <MatchAnalysisSection
              matchId={match.id}
              homeTeamId={match.homeTeamId}
              awayTeamId={match.awayTeamId}
            />
          </Suspense>
        </div>

        {/* Sidebar */}
        <div className="col-span-4 space-y-4">
          <SectionCard title={tMatch('matchInfo')}>
            <dl className="space-y-2">
              {[
                { label: tMatch('competition'), value: competitionLabel },
                { label: tMatch('date'), value: matchDateText },
                { label: tMatch('kickoff'), value: matchTimeText },
                ...(isWorldCupMatch
                  ? [{
                      label: tMatch('hostLocalKickoff'),
                      value: hostLocalKickoffText,
                    }]
                  : []),
                { label: tMatch('venue'), value: match.venue },
                { label: tMatch('status'), value: statusText },
              ].map(({ label, value }) => (
                <div key={label} className="flex justify-between">
                  <dt className="text-[12px] text-text-muted">{label}</dt>
                  <dd className="text-[13px] text-text-primary font-medium">{value}</dd>
                </div>
              ))}
            </dl>
          </SectionCard>

          <SectionCard title={tMatch('teams')}>
            <div className="space-y-3">
              {isNationMatch ? (
                <>
                  {(homeResolvedNationId || match.homeParticipant?.slot) && (
                    homeResolvedNationId ? (
                      <EntityLink type="nation" id={homeResolvedNationId} className="flex items-center gap-3">
                        {homeCode && <NationBadge nationId={homeResolvedNationId} code={homeCode} crest={match.homeTeamLogo} size="md" />}
                        {homeCode && <NationFlag nationId={homeResolvedNationId} code={homeCode} size="md" />}
                        <div>
                          <div className="text-[13px] font-medium">{homeLabel}</div>
                          <div className="text-[10px] text-text-muted">{tCommon('home')}</div>
                        </div>
                      </EntityLink>
                    ) : (
                      <div className="flex items-center gap-3">
                        <div>
                          <WorldCupPlaceholderLink placeholder={match.homeParticipant!.slot!} label={homeLabel} />
                          <div className="text-[10px] text-text-muted">{tCommon('home')}</div>
                        </div>
                      </div>
                    )
                  )}
                  {(awayResolvedNationId || match.awayParticipant?.slot) && (
                    awayResolvedNationId ? (
                      <EntityLink type="nation" id={awayResolvedNationId} className="flex items-center gap-3">
                        {awayCode && <NationBadge nationId={awayResolvedNationId} code={awayCode} crest={match.awayTeamLogo} size="md" />}
                        {awayCode && <NationFlag nationId={awayResolvedNationId} code={awayCode} size="md" />}
                        <div>
                          <div className="text-[13px] font-medium">{awayLabel}</div>
                          <div className="text-[10px] text-text-muted">{tCommon('away')}</div>
                        </div>
                      </EntityLink>
                    ) : (
                      <div className="flex items-center gap-3">
                        <div>
                          <WorldCupPlaceholderLink placeholder={match.awayParticipant!.slot!} label={awayLabel} />
                          <div className="text-[10px] text-text-muted">{tCommon('away')}</div>
                        </div>
                      </div>
                    )
                  )}
                </>
              ) : (
                <>
                  {homeCode && (
                    <EntityLink type="club" id={match.homeTeamId} className="flex items-center gap-3">
                      <ClubBadge shortName={homeCode} clubId={match.homeTeamId} logo={match.homeTeamLogo} size="md" />
                      <div>
                        <div className="text-[13px] font-medium">{homeLabel}</div>
                        <div className="text-[10px] text-text-muted">{tCommon('home')}</div>
                      </div>
                    </EntityLink>
                  )}
                  {awayCode && (
                    <EntityLink type="club" id={match.awayTeamId} className="flex items-center gap-3">
                      <ClubBadge shortName={awayCode} clubId={match.awayTeamId} logo={match.awayTeamLogo} size="md" />
                      <div>
                        <div className="text-[13px] font-medium">{awayLabel}</div>
                        <div className="text-[10px] text-text-muted">{tCommon('away')}</div>
                      </div>
                    </EntityLink>
                  )}
                </>
              )}
            </div>
          </SectionCard>
        </div>
      </div>
    </div>
  );
}
