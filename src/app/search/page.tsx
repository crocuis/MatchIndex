import { getLocale, getTranslations } from 'next-intl/server';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { EntityLink } from '@/components/ui/EntityLink';
import { Badge } from '@/components/ui/Badge';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EmptyState } from '@/components/ui/EmptyState';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import { NationFlag } from '@/components/ui/NationFlag';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import { ListSearchForm } from '@/components/ui/ListSearchForm';
import { searchAllDb } from '@/data/server';
import type { EntityType, SearchResult } from '@/data/types';

const typeBadgeVariants: Record<EntityType, 'default' | 'success' | 'info' | 'warning'> = {
  player: 'default',
  club: 'success',
  league: 'warning',
  nation: 'info',
};

function getResultImage(item: SearchResult) {
  if (item.type === 'player') {
    return (
      <PlayerAvatar
        name={item.name}
        position={item.playerPosition ?? 'MID'}
        imageUrl={item.imageUrl}
        size="sm"
      />
    );
  }

  if (item.type === 'club') {
    return <ClubBadge shortName={item.shortName ?? item.name} clubId={item.id} logo={item.imageUrl} size="sm" />;
  }

  if (item.type === 'league') {
    return <LeagueLogo leagueId={item.id} name={item.name} logo={item.imageUrl} size="sm" />;
  }

  return <NationFlag nationId={item.id} code={item.nationCode ?? item.id} flag={item.imageUrl} size="sm" />;
}

export default async function SearchPage({
  searchParams,
}: {
  searchParams: Promise<{ q?: string }>;
}) {
  const locale = await getLocale();
  const tSearch = await getTranslations('search');
  const tCommon = await getTranslations('common');
  const { q } = await searchParams;
  const query = q?.trim() ?? '';
  const results = query ? await searchAllDb(query, locale) : [];
  const typeLabels: Record<EntityType, string> = {
    player: tSearch('playersGroup'),
    club: tSearch('clubsGroup'),
    league: tSearch('leaguesGroup'),
    nation: tSearch('nationsGroup'),
  };
  const grouped = results.reduce<Record<EntityType, SearchResult[]>>((acc, result) => {
    if (!acc[result.type]) {
      acc[result.type] = [];
    }
    acc[result.type].push(result);
    return acc;
  }, {} as Record<EntityType, SearchResult[]>);

  const groupedEntries = (Object.entries(grouped) as [EntityType, SearchResult[]][]).map(([type, items]) => ({
    type,
    items: items.map((item) => ({
      item,
      image: getResultImage(item),
    })),
  }));

  return (
    <div>
      <PageHeader title={tSearch('title')} subtitle={tSearch('subtitle')} />

      <ListSearchForm
        action="/search"
        query={query}
        placeholder={tSearch('searchPlaceholder')}
        searchLabel={tCommon('search')}
        clearLabel={tCommon('clear')}
      />

      {!query ? (
        <EmptyState message={tSearch('emptyQuery')} />
      ) : results.length === 0 ? (
        <EmptyState message={tSearch('noResults', { query })} />
      ) : (
        <div className="space-y-4">
          {groupedEntries.map(({ type, items }) => (
            <SectionCard key={type} title={`${typeLabels[type]} (${items.length})`}>
              <div className="space-y-1">
                {items.map(({ item, image }) => (
                  <EntityLink
                    key={`${item.type}-${item.id}`}
                    type={item.type}
                    id={item.id}
                    className="flex items-center justify-between px-3 py-2 rounded hover:bg-surface-2 transition-colors"
                  >
                    <div className="flex items-center gap-2">
                      {image}
                      <div>
                         <div className="text-[13px] font-medium">{item.name}</div>
                        <div className="text-[11px] text-text-muted">{item.subtitle}</div>
                       </div>
                     </div>
                    <Badge variant={typeBadgeVariants[item.type]}>{typeLabels[item.type]}</Badge>
                  </EntityLink>
                ))}
              </div>
            </SectionCard>
          ))}
        </div>
      )}
    </div>
  );
}
