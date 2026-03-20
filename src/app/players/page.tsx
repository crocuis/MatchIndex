import Link from 'next/link';
import type { Metadata } from 'next';
import { getLocale, getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { EntityLink } from '@/components/ui/EntityLink';
import { Badge } from '@/components/ui/Badge';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { NationFlag } from '@/components/ui/NationFlag';
import { ListSearchForm } from '@/components/ui/ListSearchForm';
import { PaginationNav } from '@/components/ui/PaginationNav';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { getPaginatedPlayersDb } from '@/data/server';
import { getPositionColor } from '@/lib/utils';

const PAGE_SIZE = 50;

function parsePage(value?: string) {
  const page = Number.parseInt(value ?? '1', 10);
  return Number.isFinite(page) && page > 0 ? page : 1;
}

export const metadata: Metadata = {
  title: 'People',
};

export default async function PlayersPage({
  searchParams,
}: {
  searchParams: Promise<{ page?: string; q?: string; status?: string }>;
}) {
  const locale = await getLocale();
  const { page, q, status } = await searchParams;
  const currentPage = parsePage(page);
  const query = q?.trim() ?? '';
  const statusCategory = status === 'retired' ? 'retired' : 'active';
  const playersResult = await getPaginatedPlayersDb(locale, query, statusCategory, { page: currentPage, pageSize: PAGE_SIZE });
  const t = await getTranslations('playersList');
  const tTable = await getTranslations('table');
  const tCommon = await getTranslations('common');

  const hrefForPage = (nextPage: number) => {
    const params = new URLSearchParams();
    if (nextPage > 1) params.set('page', String(nextPage));
    if (query) params.set('q', query);
    params.set('status', statusCategory);
    const queryString = params.toString();
    return queryString ? `/players?${queryString}` : '/players';
  };

  return (
    <div>
      <PageHeader
        title={t('title')}
        subtitle={t('subtitle', { count: playersResult.totalCount })}
      />
      <div className="mb-3 flex items-center gap-2">
        <Link
          href={query ? `/players?status=active&q=${encodeURIComponent(query)}` : '/players?status=active'}
          className={statusCategory === 'active'
            ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
            : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
        >
          {t('active')}
        </Link>
        <Link
          href={query ? `/players?status=retired&q=${encodeURIComponent(query)}` : '/players?status=retired'}
          className={statusCategory === 'retired'
            ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
            : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
        >
          {t('retired')}
        </Link>
      </div>

      <ListSearchForm
        action="/players"
        query={query}
        placeholder={t('searchPlaceholder')}
        searchLabel={tCommon('search')}
        clearLabel={tCommon('clear')}
        hiddenValues={{ status: statusCategory }}
      />

      <SectionCard
        title={t('title')}
        noPadding
        action={(
          <PaginationNav
            currentPage={playersResult.currentPage}
            totalPages={playersResult.totalPages}
            hrefForPage={hrefForPage}
            previousLabel={tCommon('previous')}
            nextLabel={tCommon('next')}
            pageLabel={tCommon('pageOf', { page: playersResult.currentPage, totalPages: playersResult.totalPages })}
          />
        )}
      >
        <table className="w-full">
          <thead>
            <tr className="border-b border-border">
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tTable('player')}
              </th>
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tTable('pos')}
              </th>
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tTable('club')}
              </th>
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tTable('age')}
              </th>
              <th className="px-3 py-2 text-left text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tTable('nationality')}
              </th>
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tTable('app')}
              </th>
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tTable('goals')}
              </th>
              <th className="px-3 py-2 text-center text-[11px] font-medium uppercase tracking-wider text-text-muted">
                {tTable('assists')}
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border-subtle">
            {playersResult.items.map((player) => {
              const nationId = player.nationId.toLowerCase();
              const nationCode = player.nationCode ?? player.nationId.toUpperCase();
              const nationalityLabel = player.nationName ?? player.nationality;

              return (
                <tr key={player.id} className="hover:bg-surface-2 transition-colors">
                  <td className="px-3 py-1.5 text-[13px]">
                    <EntityLink type="player" id={player.id} className="flex items-center gap-2">
                      <PlayerAvatar name={player.name} position={player.position} imageUrl={player.photoUrl} size="sm" />
                      <span className="font-medium">{player.name}</span>
                    </EntityLink>
                  </td>
                  <td className="px-3 py-1.5 text-[13px] text-center">
                    <Badge className={getPositionColor(player.position)}>
                      {player.position}
                    </Badge>
                  </td>
                  <td className="px-3 py-1.5 text-[13px]">
                    {player.clubName && (
                      <EntityLink type="club" id={player.clubId} className="flex items-center gap-2">
                        <ClubBadge
                          shortName={player.clubShortName ?? player.clubName}
                          clubId={player.clubId}
                          logo={player.clubLogo}
                          size="sm"
                        />
                        <span>{player.clubName}</span>
                      </EntityLink>
                    )}
                  </td>
                  <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">
                    {player.age}
                  </td>
                  <td className="px-3 py-1.5 text-[13px] text-text-secondary">
                    {nationId ? (
                      <EntityLink type="nation" id={nationId} className="flex items-center gap-2">
                        <NationFlag nationId={nationId} code={nationCode} flag={player.nationFlag} size="sm" />
                        <span>{nationalityLabel}</span>
                      </EntityLink>
                    ) : (
                      <span>{nationalityLabel}</span>
                    )}
                  </td>
                  <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">
                    {player.seasonStats.appearances}
                  </td>
                  <td className="px-3 py-1.5 text-[13px] text-center tabular-nums font-semibold">
                    {player.seasonStats.goals}
                  </td>
                  <td className="px-3 py-1.5 text-[13px] text-center tabular-nums text-text-secondary">
                    {player.seasonStats.assists}
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
