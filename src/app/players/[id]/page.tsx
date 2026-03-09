import { notFound } from 'next/navigation';
import { getLocale, getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { StatPanel } from '@/components/data/StatPanel';
import { MatchCard } from '@/components/data/MatchCard';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { NationFlag } from '@/components/ui/NationFlag';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { cn, formatDate, formatNumber, getPositionColor } from '@/lib/utils';
import { getClubByIdDb, getNationByIdDb, getPlayerByIdDb, getRecentFinishedMatchesByClubDb } from '@/data/server';

export default async function PlayerPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const player = await getPlayerByIdDb(id);
  if (!player) notFound();
  const locale = await getLocale();

  const [club, nation, recentMatches] = await Promise.all([
    getClubByIdDb(player.clubId, locale),
    getNationByIdDb(player.nationId, locale),
    getRecentFinishedMatchesByClubDb(player.clubId, locale, 10),
  ]);

  const stats = player.seasonStats;
  const contract = player.contract;
  const scoutingReport = player.scoutingReport;
  const tPlayer = await getTranslations('player');
  const tCommon = await getTranslations('common');

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <PlayerAvatar name={player.name} position={player.position} imageUrl={player.photoUrl} size="xl" />
            <span>{player.name}</span>
          </div>
        )}
        subtitle={`${club?.name ?? tCommon('freeAgent')} · ${player.position}`}
      >
        <span className={cn('px-2 py-1 rounded text-[11px] font-bold', getPositionColor(player.position))}>
          {player.position}
        </span>
      </PageHeader>

      <StatPanel
        stats={[
          { label: tPlayer('appearances'), value: stats.appearances },
          { label: tPlayer('goals'), value: stats.goals, highlight: stats.goals >= 10 },
          { label: tPlayer('assists'), value: stats.assists, highlight: stats.assists >= 10 },
          { label: tPlayer('minutes'), value: stats.minutesPlayed.toLocaleString() },
        ]}
        columns={4}
        className="mb-4"
      />

      <div className="grid grid-cols-12 gap-4">
        {/* Main */}
        <div className="col-span-8 space-y-4">
          {/* Season Stats Detail */}
          <SectionCard title={tPlayer('seasonStats')}>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                {[
                  [tPlayer('appearances'), stats.appearances],
                  [tPlayer('goals'), stats.goals],
                  [tPlayer('assists'), stats.assists],
                  [tPlayer('minutesPlayed'), stats.minutesPlayed.toLocaleString()],
                ].map(([label, value]) => (
                  <div key={label as string} className="flex justify-between items-center">
                    <span className="text-[13px] text-text-secondary">{label}</span>
                    <span className="text-[13px] text-text-primary font-semibold tabular-nums">{value}</span>
                  </div>
                ))}
              </div>
              <div className="space-y-2">
                {[
                  [tPlayer('yellowCards'), stats.yellowCards],
                  [tPlayer('redCards'), stats.redCards],
                  ...(stats.cleanSheets !== undefined
                    ? [[tPlayer('cleanSheets'), stats.cleanSheets] as const]
                    : []),
                  [tPlayer('goalsPer90'), stats.minutesPlayed > 0 ? ((stats.goals / stats.minutesPlayed) * 90).toFixed(2) : '0.00'],
                ].map(([label, value]) => (
                  <div key={label as string} className="flex justify-between items-center">
                    <span className="text-[13px] text-text-secondary">{label}</span>
                    <span className="text-[13px] text-text-primary font-semibold tabular-nums">{String(value)}</span>
                  </div>
                ))}
              </div>
            </div>
          </SectionCard>

          {scoutingReport ? (
            <SectionCard title={tPlayer('scoutingReport')}>
              <div className="space-y-4">
                <div className="grid gap-3 xl:grid-cols-[1.4fr_1fr]">
                  <div className="rounded border border-border bg-surface-2/40 p-3">
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <div>
                        <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">{tPlayer('primaryRole')}</div>
                        <div className="mt-1 text-[13px] font-semibold text-text-primary">{scoutingReport.role}</div>
                      </div>
                      <span className="rounded-full border border-border-subtle bg-surface-0/70 px-2 py-1 text-[10px] font-bold uppercase tracking-wide text-text-muted">
                        {player.position}
                      </span>
                    </div>
                    <p className="text-[13px] leading-6 text-text-secondary">{scoutingReport.summary}</p>
                  </div>

                  <div className="rounded border border-border bg-[linear-gradient(180deg,rgba(244,163,76,0.12),rgba(244,163,76,0.03))] p-3">
                    <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">{tPlayer('reportSummary')}</div>
                    <div className="mt-2 grid grid-cols-2 gap-2 text-[11px] text-text-secondary">
                      <div className="rounded border border-border-subtle bg-surface-0/60 px-2 py-2">
                        <div className="text-text-muted">{tPlayer('strengths')}</div>
                        <div className="mt-1 text-[16px] font-semibold text-text-primary tabular-nums">{scoutingReport.strengths.length}</div>
                      </div>
                      <div className="rounded border border-border-subtle bg-surface-0/60 px-2 py-2">
                        <div className="text-text-muted">{tPlayer('weaknesses')}</div>
                        <div className="mt-1 text-[16px] font-semibold text-text-primary tabular-nums">{scoutingReport.weaknesses.length}</div>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="grid gap-3 lg:grid-cols-2">
                  <div className="rounded border border-emerald-500/20 bg-emerald-500/5 p-3">
                    <div className="mb-2 text-[10px] font-bold uppercase tracking-[0.18em] text-emerald-300">{tPlayer('strengths')}</div>
                    <ul className="space-y-2">
                      {scoutingReport.strengths.map((item) => (
                        <li key={item} className="flex items-start gap-2 text-[13px] text-text-secondary">
                          <span className="mt-1 h-1.5 w-1.5 rounded-full bg-emerald-400" />
                          <span>{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>

                  <div className="rounded border border-amber-500/20 bg-amber-500/5 p-3">
                    <div className="mb-2 text-[10px] font-bold uppercase tracking-[0.18em] text-amber-300">{tPlayer('weaknesses')}</div>
                    <ul className="space-y-2">
                      {scoutingReport.weaknesses.map((item) => (
                        <li key={item} className="flex items-start gap-2 text-[13px] text-text-secondary">
                          <span className="mt-1 h-1.5 w-1.5 rounded-full bg-amber-400" />
                          <span>{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              </div>
            </SectionCard>
          ) : null}

          {/* Recent Matches */}
          <SectionCard title={tPlayer('recentMatches')}>
            <div className="space-y-1.5">
              {recentMatches.map((m) => (
                <MatchCard key={m.id} match={m} />
              ))}
            </div>
          </SectionCard>
        </div>

        {/* Sidebar — Bio */}
        <div className="col-span-4 space-y-4">
          <SectionCard title={player.name}>
            <div className="flex items-center justify-center">
              <PlayerAvatar
                name={player.name}
                position={player.position}
                imageUrl={player.photoUrl}
                size="xl"
              />
            </div>
          </SectionCard>

          <SectionCard title={tPlayer('playerInfo')}>
            <dl className="space-y-2">
              {[
                [tPlayer('fullName'), `${player.firstName} ${player.lastName}`],
                [tPlayer('dateOfBirth'), player.dateOfBirth],
                [tPlayer('age'), String(player.age)],
                [tPlayer('height'), `${player.height} cm`],
                [tPlayer('nationality'), nation?.name ?? player.nationality],
                [tPlayer('position'), player.position],
                [tPlayer('shirtNumber'), `#${player.shirtNumber}`],
                [tPlayer('preferredFoot'), player.preferredFoot],
              ].map(([label, value]) => (
                <div key={label} className="flex justify-between">
                  <dt className="text-[12px] text-text-muted">{label}</dt>
                  <dd className="text-[13px] text-text-primary font-medium">{value}</dd>
                </div>
              ))}
            </dl>
          </SectionCard>

          {contract ? (
            <SectionCard title={tPlayer('contract')}>
              <div className="space-y-3">
                {contract.startDate || contract.endDate ? (
                  <div className="rounded border border-border bg-surface-2/40 p-3">
                    <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">{tPlayer('contractPeriod')}</div>
                    <div className="mt-2 flex items-end justify-between gap-3">
                      <div>
                        <div className="text-[18px] font-semibold leading-none text-text-primary tabular-nums">
                          {contract.endDate ? formatDate(contract.endDate) : '-'}
                        </div>
                        <div className="mt-1 text-[11px] text-text-muted">{tPlayer('contractEnd')}</div>
                      </div>
                      {contract.startDate ? (
                        <div className="rounded border border-border-subtle bg-surface-0/70 px-2 py-1 text-right">
                          <div className="text-[10px] uppercase tracking-wide text-text-muted">{tPlayer('contractStart')}</div>
                          <div className="mt-1 text-[12px] font-medium text-text-primary tabular-nums">{formatDate(contract.startDate)}</div>
                        </div>
                      ) : null}
                    </div>
                  </div>
                ) : null}

                <dl className="space-y-2">
                  {contract.annualSalary !== undefined ? (
                    <div className="flex items-center justify-between gap-3">
                      <dt className="text-[12px] text-text-muted">{tPlayer('estimatedSalary')}</dt>
                      <dd className="text-[13px] font-semibold text-text-primary tabular-nums">
                        {contract.currencyCode ?? 'EUR'} {formatNumber(contract.annualSalary)}
                      </dd>
                    </div>
                  ) : null}
                  {contract.weeklyWage !== undefined ? (
                    <div className="flex items-center justify-between gap-3">
                      <dt className="text-[12px] text-text-muted">{tPlayer('weeklyWage')}</dt>
                      <dd className="text-[13px] font-semibold text-text-primary tabular-nums">
                        {contract.currencyCode ?? 'EUR'} {formatNumber(contract.weeklyWage)}
                      </dd>
                    </div>
                  ) : null}
                  {contract.source ? (
                    <div className="flex items-center justify-between gap-3">
                      <dt className="text-[12px] text-text-muted">{tPlayer('salarySource')}</dt>
                      <dd className="text-[12px] font-medium text-text-secondary">
                        {contract.sourceUrl ? (
                          <a href={contract.sourceUrl} target="_blank" rel="noreferrer" className="transition-colors hover:text-text-primary">
                            {contract.source}{contract.isEstimated ? ` (${tPlayer('estimated')})` : ''}
                          </a>
                        ) : (
                          `${contract.source}${contract.isEstimated ? ` (${tPlayer('estimated')})` : ''}`
                        )}
                      </dd>
                    </div>
                  ) : null}
                  {contract.marketValue ? (
                    <div className="flex items-center justify-between gap-3">
                      <dt className="text-[12px] text-text-muted">{tPlayer('marketValue')}</dt>
                      <dd className="text-[13px] font-semibold text-text-primary tabular-nums">
                        EUR {formatNumber(contract.marketValue.min)} - {formatNumber(contract.marketValue.max)}
                      </dd>
                    </div>
                  ) : null}
                </dl>
              </div>
            </SectionCard>
          ) : null}

          <SectionCard title={tPlayer('club')}>
            {club ? (
              <EntityLink type="club" id={club.id} className="flex items-center gap-3">
                <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="lg" />
                <div>
                  <div className="text-[13px] font-medium">{club.name}</div>
                  <div className="text-[11px] text-text-muted">{club.stadium}</div>
                </div>
              </EntityLink>
            ) : (
              <div className="text-[13px] text-text-muted">{tCommon('freeAgent')}</div>
            )}
          </SectionCard>

          <SectionCard title={tPlayer('nation')}>
            {nation ? (
              <EntityLink type="nation" id={nation.id} className="flex items-center gap-3">
                <NationFlag nationId={nation.id} code={nation.code} size="lg" />
                <div>
                  <div className="text-[13px] font-medium">{nation.name}</div>
                  <div className="text-[11px] text-text-muted">FIFA #{nation.fifaRanking} · {nation.confederation}</div>
                </div>
              </EntityLink>
            ) : (
              <div className="text-[13px] text-text-muted">{tCommon('unknown')}</div>
            )}
          </SectionCard>
        </div>
      </div>
    </div>
  );
}
