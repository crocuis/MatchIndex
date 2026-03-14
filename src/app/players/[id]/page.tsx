import { notFound } from 'next/navigation';
import { getLocale, getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { DetailTabNav } from '@/components/ui/DetailTabNav';
import { StatPanel } from '@/components/data/StatPanel';
import { MatchCard } from '@/components/data/MatchCard';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EntityLink } from '@/components/ui/EntityLink';
import { NationFlag } from '@/components/ui/NationFlag';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { cn, formatDate, formatNumber, getClubDisplayName, getPositionColor } from '@/lib/utils';
import { getClubByIdDb, getClubLinksByNamesDb, getClubsByIdsDb, getNationByIdDb, getPlayerByIdDb, getRecentFinishedMatchesByClubDb } from '@/data/server';

export default async function PlayerPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ tab?: string }>;
}) {
  const { id } = await params;
  const { tab } = await searchParams;
  const locale = await getLocale();
  const player = await getPlayerByIdDb(id, locale);
  if (!player) notFound();
  const currentYear = new Date().getUTCFullYear();
  const latestClubHistoryYear = player.clubHistory?.[player.clubHistory.length - 1]?.endYear;
  const isRetired = Boolean(player.isRetired || (latestClubHistoryYear !== undefined && latestClubHistoryYear < currentYear - 2));
  const clubHistoryIds = [...new Set((player.clubHistory ?? []).map((entry) => entry.clubId).filter(Boolean) as string[])];
  const stats = player.seasonStats;
  const latestSeason = player.seasonHistory?.[0];
  const contract = player.contract;
  const scoutingReport = player.scoutingReport;
  const marketValueHistory = player.marketValueHistory ?? [];
  const transferHistory = player.transferHistory ?? [];
  const clubLookupNames = [
    ...(player.clubHistory ?? []).map((entry) => entry.clubName),
    ...marketValueHistory.map((entry) => entry.clubName).filter(Boolean) as string[],
    ...transferHistory.flatMap((entry) => [entry.fromClubName, entry.toClubName]).filter(Boolean) as string[],
  ];

  const [club, nation, recentMatches, historyClubs, linkedClubs] = await Promise.all([
    isRetired ? Promise.resolve(undefined) : getClubByIdDb(player.clubId, locale),
    getNationByIdDb(player.nationId, locale),
    isRetired ? Promise.resolve([]) : getRecentFinishedMatchesByClubDb(player.clubId, locale, 10),
    clubHistoryIds.length > 0 ? getClubsByIdsDb(clubHistoryIds, locale) : Promise.resolve([]),
    clubLookupNames.length > 0 ? getClubLinksByNamesDb(clubLookupNames, locale) : Promise.resolve([]),
  ]);
  const historyClubMap = new Map(historyClubs.map((entry) => [entry.id, entry]));
  const normalizeClubKey = (value: string | undefined) => value?.normalize('NFKD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-zA-Z0-9]+/g, ' ').trim().toLowerCase() ?? '';
  const clubIdByName = new Map<string, string>();
  for (const entry of historyClubs) {
    for (const candidate of [entry.name, entry.shortName, entry.id]) {
      const key = normalizeClubKey(candidate);
      if (key && !clubIdByName.has(key)) {
        clubIdByName.set(key, entry.id);
      }
    }
  }
  if (club) {
    for (const candidate of [club.name, club.shortName, club.id]) {
      const key = normalizeClubKey(candidate);
      if (key && !clubIdByName.has(key)) {
        clubIdByName.set(key, club.id);
      }
    }
  }
  for (const entry of linkedClubs) {
    for (const candidate of [entry.name, entry.shortName, entry.id]) {
      const key = normalizeClubKey(candidate);
      if (key && !clubIdByName.has(key)) {
        clubIdByName.set(key, entry.id);
      }
    }
  }
  const resolveClubLinkId = (clubId?: string, clubName?: string) => clubId ?? clubIdByName.get(normalizeClubKey(clubName));
  const nationalTeam = player.nationalTeam;
  const recentNationMatches = nationalTeam?.recentMatches ?? [];
  const currentMarketValue = marketValueHistory[0];
  const highestMarketValue = marketValueHistory.reduce<typeof currentMarketValue | undefined>((best, entry) => {
    if (!best || entry.marketValue > best.marketValue) {
      return entry;
    }

    return best;
  }, undefined);
  const tPlayer = await getTranslations('player');
  const tCommon = await getTranslations('common');
  const detailTabs = [
    { key: 'profile', label: tCommon('tabProfile') },
    { key: 'stats', label: tCommon('tabStats') },
    { key: 'market-value', label: tPlayer('marketValueTab') },
    { key: 'transfers', label: tPlayer('transfersTab') },
    { key: 'national-team', label: tPlayer('nationalTeamTab') },
  ] as const;
  const activeTab = (tab && detailTabs.some((entry) => entry.key === tab) ? tab : 'profile') as 'profile' | 'stats' | 'market-value' | 'transfers' | 'national-team';

  return (
    <div>
      <PageHeader
        title={(
          <div className="flex items-center gap-2">
            <PlayerAvatar name={player.name} position={player.position} imageUrl={player.photoUrl} size="xl" />
            <span>{player.name}</span>
          </div>
        )}
        subtitle={`${isRetired ? tPlayer('retired') : club ? getClubDisplayName(club, locale) : tCommon('freeAgent')} · ${player.position}`}
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

      <DetailTabNav
        activeTab={activeTab}
        basePath={`/players/${id}`}
        className="mb-4"
        tabs={detailTabs.map((entry) => ({ ...entry }))}
      />

      <div className="grid grid-cols-12 gap-4">
        {/* Main */}
        <div className="col-span-8 space-y-4">
          {activeTab === 'stats' ? (
            <SectionCard title={tPlayer('seasonStats')}>
            <div className="mb-3 flex items-center justify-between gap-3">
              <div className="text-[11px] text-text-muted">
                {latestSeason ? `${tPlayer('latestSeason')} · ${latestSeason.seasonLabel}` : tCommon('unknown')}
              </div>
              {latestSeason ? (
                <EntityLink type="club" id={latestSeason.clubId} className="text-[11px] font-medium text-text-secondary transition-colors hover:text-text-primary">
                  {latestSeason.clubName}
                </EntityLink>
              ) : null}
            </div>
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
          ) : null}

          {activeTab === 'stats' && player.seasonHistory && player.seasonHistory.length > 0 ? (
            <SectionCard title={tPlayer('seasonHistory')}>
              <div className="overflow-x-auto">
                <table className="min-w-full text-left text-[12px]">
                  <thead className="border-b border-border-subtle text-text-muted">
                    <tr>
                      <th className="px-2 py-2 font-medium">{tPlayer('season')}</th>
                      <th className="px-2 py-2 font-medium">{tPlayer('club')}</th>
                      <th className="px-2 py-2 text-right font-medium">{tPlayer('appearances')}</th>
                      <th className="px-2 py-2 text-right font-medium">{tPlayer('goals')}</th>
                      <th className="px-2 py-2 text-right font-medium">{tPlayer('assists')}</th>
                      <th className="px-2 py-2 text-right font-medium">{tPlayer('minutes')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {player.seasonHistory.map((entry) => (
                      <tr key={`${entry.seasonId}-${entry.clubId}`} className="border-b border-border-subtle/60 last:border-b-0">
                        <td className="px-2 py-2 text-text-primary">{entry.seasonLabel}</td>
                        <td className="px-2 py-2 text-text-secondary">
                          <EntityLink type="club" id={entry.clubId} className="transition-colors hover:text-text-primary">
                            {entry.clubName}
                          </EntityLink>
                        </td>
                        <td className="px-2 py-2 text-right tabular-nums text-text-primary">{entry.appearances}</td>
                        <td className="px-2 py-2 text-right tabular-nums text-text-primary">{entry.goals}</td>
                        <td className="px-2 py-2 text-right tabular-nums text-text-primary">{entry.assists}</td>
                        <td className="px-2 py-2 text-right tabular-nums text-text-primary">{entry.minutesPlayed.toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </SectionCard>
          ) : null}

          {activeTab === 'profile' && scoutingReport ? (
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

          {activeTab === 'stats' && !isRetired ? (
            <SectionCard title={tPlayer('recentMatches')}>
              <div className="space-y-1.5">
                {recentMatches.map((m) => (
                  <MatchCard key={m.id} match={m} />
                ))}
              </div>
            </SectionCard>
          ) : null}

          {activeTab === 'profile' ? (
            <>
              <SectionCard title={tPlayer('playerInfo')}>
                <dl className="grid grid-cols-2 gap-x-6 gap-y-2">
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
                    <div key={label} className="flex items-center justify-between gap-4">
                      <dt className="text-[12px] text-text-muted">{label}</dt>
                      <dd className="text-[13px] font-medium text-text-primary">{value}</dd>
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

              <SectionCard title={tPlayer('marketValueSnapshot')}>
                {currentMarketValue || highestMarketValue ? (
                  <div className="grid gap-3 md:grid-cols-2">
                    <div className="rounded border border-border bg-surface-2/40 p-3">
                      <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">{tPlayer('currentMarketValue')}</div>
                      <div className="mt-2 text-[20px] font-semibold text-text-primary tabular-nums">
                        {currentMarketValue ? `${currentMarketValue.currencyCode} ${formatNumber(currentMarketValue.marketValue)}` : '-'}
                      </div>
                      <div className="mt-1 text-[11px] text-text-muted">
                        {currentMarketValue?.observedAt ? formatDate(currentMarketValue.observedAt) : tCommon('unknown')}
                      </div>
                    </div>
                    <div className="rounded border border-border bg-surface-2/40 p-3">
                      <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">{tPlayer('highestMarketValue')}</div>
                      <div className="mt-2 text-[20px] font-semibold text-text-primary tabular-nums">
                        {highestMarketValue ? `${highestMarketValue.currencyCode} ${formatNumber(highestMarketValue.marketValue)}` : '-'}
                      </div>
                      <div className="mt-1 text-[11px] text-text-muted">
                        {highestMarketValue?.observedAt ? formatDate(highestMarketValue.observedAt) : tCommon('unknown')}
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="text-[13px] text-text-muted">{tPlayer('marketValueEmpty')}</div>
                )}
              </SectionCard>
            </>
          ) : null}

          {activeTab === 'profile' && player.clubHistory && player.clubHistory.length > 0 ? (
            <SectionCard title={tPlayer('clubHistory')}>
              <div className="space-y-2">
                {player.clubHistory.filter((entry) => !entry.isFreeAgent).map((entry) => (
                  entry.clubId ? (
                    <EntityLink key={`${entry.clubId}-${entry.periodLabel}`} type="club" id={entry.clubId} className="flex items-center gap-3 rounded border border-border-subtle bg-surface-2/30 px-3 py-2 text-[13px] font-medium text-text-primary transition-colors hover:border-border hover:bg-surface-2/60">
                      <ClubBadge
                        shortName={historyClubMap.get(entry.clubId)?.shortName ?? entry.clubName}
                        clubId={entry.clubId}
                        logo={historyClubMap.get(entry.clubId)?.logo}
                        size="md"
                      />
                      <span>
                        {entry.clubName} <span className="text-[11px] text-text-muted tabular-nums">({entry.periodLabel})</span>
                      </span>
                    </EntityLink>
                  ) : (
                    <div key={`${entry.clubName}-${entry.periodLabel}`} className="flex items-center gap-3 rounded border border-border-subtle bg-surface-2/20 px-3 py-2 text-[13px] font-medium text-text-primary">
                      <div className="flex h-8 w-8 items-center justify-center rounded-full border border-dashed border-border-subtle bg-surface-0/50 text-[10px] uppercase tracking-wide text-text-muted">
                        {entry.isFreeAgent ? 'FA' : 'CL'}
                      </div>
                      <span>
                        {entry.clubName} <span className="text-[11px] text-text-muted tabular-nums">({entry.periodLabel})</span>
                      </span>
                    </div>
                  )
                ))}
              </div>
            </SectionCard>
          ) : null}

          {activeTab === 'market-value' ? (
            <SectionCard title={tPlayer('marketValueHistory')}>
              {marketValueHistory.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full text-left text-[12px]">
                    <thead className="border-b border-border-subtle text-text-muted">
                      <tr>
                        <th className="px-2 py-2 font-medium">{tPlayer('date')}</th>
                        <th className="px-2 py-2 font-medium">{tPlayer('season')}</th>
                        <th className="px-2 py-2 font-medium">{tPlayer('club')}</th>
                        <th className="px-2 py-2 text-right font-medium">{tPlayer('age')}</th>
                        <th className="px-2 py-2 text-right font-medium">{tPlayer('marketValue')}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {marketValueHistory.map((entry) => (
                        <tr key={`${entry.observedAt}-${entry.marketValue}`} className="border-b border-border-subtle/60 last:border-b-0">
                          <td className="px-2 py-2 text-text-primary tabular-nums">{formatDate(entry.observedAt)}</td>
                          <td className="px-2 py-2 text-text-secondary">{entry.seasonLabel ?? '-'}</td>
                          <td className="px-2 py-2 text-text-secondary">
                            {resolveClubLinkId(entry.clubId, entry.clubName) ? (
                              <EntityLink type="club" id={resolveClubLinkId(entry.clubId, entry.clubName)!} className="transition-colors hover:text-text-primary">
                                {entry.clubName ?? entry.clubId}
                              </EntityLink>
                            ) : (
                              entry.clubName ?? '-'
                            )}
                          </td>
                          <td className="px-2 py-2 text-right tabular-nums text-text-primary">{entry.age ?? '-'}</td>
                          <td className="px-2 py-2 text-right tabular-nums text-text-primary font-semibold">
                            {entry.currencyCode} {formatNumber(entry.marketValue)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="text-[13px] text-text-muted">{tPlayer('marketValueEmpty')}</div>
              )}
            </SectionCard>
          ) : null}

          {activeTab === 'transfers' ? (
            <SectionCard title={tPlayer('transferHistory')}>
              {transferHistory.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full text-left text-[12px]">
                    <thead className="border-b border-border-subtle text-text-muted">
                      <tr>
                        <th className="px-2 py-2 font-medium">{tPlayer('season')}</th>
                        <th className="px-2 py-2 font-medium">{tPlayer('date')}</th>
                        <th className="px-2 py-2 font-medium">{tPlayer('fromClub')}</th>
                        <th className="px-2 py-2 font-medium">{tPlayer('toClub')}</th>
                        <th className="px-2 py-2 text-right font-medium">{tPlayer('marketValue')}</th>
                        <th className="px-2 py-2 text-right font-medium">{tPlayer('transferFee')}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {transferHistory.map((entry) => (
                        <tr key={entry.id} className="border-b border-border-subtle/60 last:border-b-0 align-top">
                          <td className="px-2 py-2 text-text-secondary">{entry.seasonLabel ?? '-'}</td>
                          <td className="px-2 py-2 text-text-primary tabular-nums">{entry.movedAt ? formatDate(entry.movedAt) : '-'}</td>
                          <td className="px-2 py-2 text-text-secondary">
                            {resolveClubLinkId(entry.fromClubId, entry.fromClubName) ? (
                              <EntityLink type="club" id={resolveClubLinkId(entry.fromClubId, entry.fromClubName)!} className="transition-colors hover:text-text-primary">
                                {entry.fromClubName ?? entry.fromClubId}
                              </EntityLink>
                            ) : (
                              entry.fromClubName ?? '-'
                            )}
                          </td>
                          <td className="px-2 py-2 text-text-secondary">
                            {resolveClubLinkId(entry.toClubId, entry.toClubName) ? (
                              <EntityLink type="club" id={resolveClubLinkId(entry.toClubId, entry.toClubName)!} className="transition-colors hover:text-text-primary">
                                {entry.toClubName ?? entry.toClubId}
                              </EntityLink>
                            ) : (
                              entry.toClubName ?? '-'
                            )}
                          </td>
                          <td className="px-2 py-2 text-right tabular-nums text-text-primary">
                            {entry.marketValue !== undefined ? `${entry.currencyCode ?? 'EUR'} ${formatNumber(entry.marketValue)}` : '-'}
                          </td>
                          <td className="px-2 py-2 text-right tabular-nums text-text-primary">
                            {entry.feeDisplay ?? (entry.fee !== undefined ? `${entry.currencyCode ?? 'EUR'} ${formatNumber(entry.fee)}` : '-')}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="text-[13px] text-text-muted">{tPlayer('transferHistoryEmpty')}</div>
              )}
            </SectionCard>
          ) : null}

          {activeTab === 'national-team' ? (
            <>
              <SectionCard title={tPlayer('nationalTeamOverview')}>
                {nation ? (
                  <div className="grid gap-3 md:grid-cols-2">
                    <EntityLink type="nation" id={nation.id} className="flex items-center gap-3 rounded border border-border-subtle bg-surface-2/30 px-3 py-3">
                      <NationFlag nationId={nation.id} code={nation.code} size="lg" />
                      <div>
                        <div className="text-[13px] font-semibold text-text-primary">{nation.name}</div>
                        <div className="text-[11px] text-text-muted">{nation.confederation} · FIFA #{nation.fifaRanking}</div>
                      </div>
                    </EntityLink>
                    <div className="rounded border border-border bg-surface-2/40 p-3">
                      <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">{tPlayer('nationalTeamStatus')}</div>
                      <div className="mt-2 text-[13px] font-semibold text-text-primary">{tPlayer('currentInternational')}</div>
                      <div className="mt-1 text-[11px] text-text-muted">{nation.name}</div>
                    </div>
                  </div>
                ) : (
                  <div className="text-[13px] text-text-muted">{tCommon('unknown')}</div>
                )}
              </SectionCard>

              <SectionCard title={tPlayer('nationalTeamRecord')}>
                <div className="grid gap-3 md:grid-cols-2">
                  <div className="rounded border border-border bg-surface-2/40 p-3">
                    <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">{tPlayer('caps')}</div>
                    <div className="mt-2 text-[20px] font-semibold text-text-primary tabular-nums">{formatNumber(nationalTeam?.caps ?? 0)}</div>
                    <div className="mt-1 text-[11px] text-text-muted">{tPlayer('nationalTeamAppearances')}</div>
                  </div>
                  <div className="rounded border border-border bg-surface-2/40 p-3">
                    <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">{tPlayer('goals')}</div>
                    <div className="mt-2 text-[20px] font-semibold text-text-primary tabular-nums">{formatNumber(nationalTeam?.goals ?? 0)}</div>
                    <div className="mt-1 text-[11px] text-text-muted">{tPlayer('nationalTeamGoals')}</div>
                  </div>
                </div>
              </SectionCard>

              <SectionCard title={tPlayer('recentNationalMatches')}>
                {recentNationMatches.length > 0 ? (
                  <div className="space-y-1.5">
                    {recentNationMatches.map((match) => (
                      <MatchCard key={match.id} match={match} />
                    ))}
                  </div>
                ) : (
                  <div className="text-[13px] text-text-muted">{tPlayer('nationalTeamEmpty')}</div>
                )}
              </SectionCard>
            </>
          ) : null}
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

          {activeTab !== 'profile' && !isRetired ? (
            <SectionCard title={tPlayer('club')}>
              {club ? (
                <EntityLink type="club" id={club.id} className="flex items-center gap-3">
                  <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="lg" />
                  <div>
                    <div className="text-[13px] font-medium">{getClubDisplayName(club, locale)}</div>
                    <div className="text-[11px] text-text-muted">{club.stadium}</div>
                  </div>
                </EntityLink>
              ) : (
                <div className="text-[13px] text-text-muted">{tCommon('freeAgent')}</div>
              )}
            </SectionCard>
          ) : null}

          {activeTab !== 'profile' ? (
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
          ) : null}
        </div>
      </div>
    </div>
  );
}
