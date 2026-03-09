import Link from 'next/link';
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
import {
  getClubByIdDb,
  getLeagueByIdDb,
  getNationByIdDb,
  getPlayerByIdDb,
  searchAllDb,
} from '@/data/server';
import type { EntityType, SearchResult } from '@/data/types';

const typeBadgeVariants: Record<EntityType, 'default' | 'success' | 'info' | 'warning'> = {
  player: 'default',
  club: 'success',
  league: 'warning',
  nation: 'info',
};

async function getResultImage(item: SearchResult, locale: string) {
  if (item.type === 'player') {
    const player = await getPlayerByIdDb(item.id, locale);
    return (
      <PlayerAvatar
        name={player?.name ?? item.name}
        position={player?.position ?? 'MID'}
        imageUrl={player?.photoUrl}
        size="sm"
      />
    );
  }

  if (item.type === 'club') {
    const club = await getClubByIdDb(item.id, locale);
    if (!club) return null;
    return <ClubBadge shortName={club.shortName} clubId={club.id} logo={club.logo} size="sm" />;
  }

  if (item.type === 'league') {
    const league = await getLeagueByIdDb(item.id, locale);
    if (!league) return null;
    return <LeagueLogo leagueId={league.id} name={league.name} logo={league.logo} size="sm" />;
  }

  const nation = await getNationByIdDb(item.id, locale);
  if (!nation) return null;
  return <NationFlag nationId={nation.id} code={nation.code} flag={nation.flag} size="sm" />;
}

function formatSearchSubtitle(item: SearchResult, womenLabel: string) {
  if (item.gender !== 'female') {
    return item.subtitle;
  }

  return item.subtitle ? `${item.subtitle} · ${womenLabel}` : womenLabel;
}

export default async function SearchPage({
  searchParams,
}: {
  searchParams: Promise<{ q?: string; gender?: string }>;
}) {
  const locale = await getLocale();
  const tSearch = await getTranslations('search');
  const tCommon = await getTranslations('common');
  const { q, gender } = await searchParams;
  const query = q?.trim() ?? '';
  const genderCategory = gender === 'women' ? 'women' : 'men';
  const genderFilter = genderCategory === 'women' ? 'female' : 'male';
  const results = query ? await searchAllDb(query, locale, genderFilter) : [];
  const typeLabels: Record<EntityType, string> = {
    player: tSearch('playersGroup'),
    club: tSearch('clubsGroup'),
    league: tSearch('leaguesGroup'),
    nation: tSearch('nationsGroup'),
  };
  const womenLabel = tSearch('womenLabel');

  const grouped = results.reduce<Record<EntityType, SearchResult[]>>((acc, result) => {
    if (!acc[result.type]) {
      acc[result.type] = [];
    }
    acc[result.type].push(result);
    return acc;
  }, {} as Record<EntityType, SearchResult[]>);

  const groupedEntries = await Promise.all(
    (Object.entries(grouped) as [EntityType, SearchResult[]][]).map(async ([type, items]) => ({
      type,
        items: await Promise.all(items.map(async (item) => ({
          item,
          image: await getResultImage(item, locale),
        }))),
    }))
  );

  return (
    <div>
      <PageHeader title={tSearch('title')} subtitle={tSearch('subtitle')} />

      <div className="mb-3 flex items-center gap-2">
        <Link
          href={query ? `/search?gender=men&q=${encodeURIComponent(query)}` : '/search?gender=men'}
          className={genderCategory === 'men'
            ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
            : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
        >
          {tSearch('men')}
        </Link>
        <Link
          href={query ? `/search?gender=women&q=${encodeURIComponent(query)}` : '/search?gender=women'}
          className={genderCategory === 'women'
            ? 'rounded-md bg-surface-3 px-2.5 py-1 text-[11px] font-semibold text-text-primary'
            : 'rounded-md px-2.5 py-1 text-[11px] font-medium text-text-secondary hover:bg-surface-2'}
        >
          {tSearch('women')}
        </Link>
      </div>

      <ListSearchForm
        action="/search"
        query={query}
        placeholder={tSearch('searchPlaceholder')}
        searchLabel={tCommon('search')}
        clearLabel={tCommon('clear')}
        hiddenValues={{ gender: genderCategory }}
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
                        <div className="text-[11px] text-text-muted">{formatSearchSubtitle(item, womenLabel)}</div>
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
