'use client';

import { useSearchParams } from 'next/navigation';
import { Suspense } from 'react';
import { useTranslations } from 'next-intl';
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
import { EntityLink } from '@/components/ui/EntityLink';
import { Badge } from '@/components/ui/Badge';
import { ClubBadge } from '@/components/ui/ClubBadge';
import { EmptyState } from '@/components/ui/EmptyState';
import { LeagueLogo } from '@/components/ui/LeagueLogo';
import { NationFlag } from '@/components/ui/NationFlag';
import { PlayerAvatar } from '@/components/ui/PlayerAvatar';
import {
  searchAll,
  getPlayerById,
  getClubById,
  getLeagueById,
  getNationById,
} from '@/data';
import type { EntityType, SearchResult } from '@/data/types';

const typeBadgeVariants: Record<EntityType, 'default' | 'success' | 'info' | 'warning'> = {
  player: 'default',
  club: 'success',
  league: 'warning',
  nation: 'info',
};

function getResultImage(item: SearchResult) {
  if (item.type === 'player') {
    const player = getPlayerById(item.id);
    return (
      <PlayerAvatar
        name={player?.name ?? item.name}
        position={player?.position ?? 'MID'}
        size="sm"
      />
    );
  }

  if (item.type === 'club') {
    const club = getClubById(item.id);
    if (!club) return null;
    return <ClubBadge shortName={club.shortName} clubId={club.id} size="sm" />;
  }

  if (item.type === 'league') {
    const league = getLeagueById(item.id);
    if (!league) return null;
    return <LeagueLogo leagueId={league.id} name={league.name} size="sm" />;
  }

  const nation = getNationById(item.id);
  if (!nation) return null;
  return <NationFlag nationId={nation.id} code={nation.code} size="sm" />;
}

function SearchResults() {
  const tSearch = useTranslations('search');
  const searchParams = useSearchParams();
  const query = searchParams.get('q') ?? '';
  const results = searchAll(query);
  const typeLabels: Record<EntityType, string> = {
    player: tSearch('playersGroup'),
    club: tSearch('clubsGroup'),
    league: tSearch('leaguesGroup'),
    nation: tSearch('nationsGroup'),
  };

  // Group results by type
  const grouped = results.reduce<Record<EntityType, typeof results>>((acc, r) => {
    if (!acc[r.type]) acc[r.type] = [];
    acc[r.type].push(r);
    return acc;
  }, {} as Record<EntityType, typeof results>);

  if (!query) {
    return <EmptyState message={tSearch('emptyQuery')} />;
  }

  if (results.length === 0) {
    return <EmptyState message={tSearch('noResults', { query })} />;
  }

  return (
    <div className="space-y-4">
      {(Object.entries(grouped) as [EntityType, typeof results][]).map(([type, items]) => (
        <SectionCard key={type} title={`${typeLabels[type]} (${items.length})`}>
          <div className="space-y-1">
            {items.map((item) => (
              <EntityLink
                key={`${item.type}-${item.id}`}
                type={item.type}
                id={item.id}
                className="flex items-center justify-between px-3 py-2 rounded hover:bg-surface-2 transition-colors"
              >
                <div className="flex items-center gap-2">
                  {getResultImage(item)}
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
  );
}

export default function SearchPage() {
  const tSearch = useTranslations('search');
  const tCommon = useTranslations('common');

  return (
    <div>
      <PageHeader title={tSearch('title')} subtitle={tSearch('subtitle')} />
      <Suspense fallback={<EmptyState message={tCommon('loading')} />}>
        <SearchResults />
      </Suspense>
    </div>
  );
}
