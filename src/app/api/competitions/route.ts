import { NextRequest, NextResponse } from 'next/server';
import { getLocale } from 'next-intl/server';
import { getPaginatedLeaguesDb } from '@/data/server';
import type { League } from '@/data/types';

function parsePage(value: string | null) {
  const page = Number.parseInt(value ?? '1', 10);
  return Number.isFinite(page) && page > 0 ? page : 1;
}

function parseCompetitionListFilter(value: string | null): League['competitionType'] | undefined {
  if (value === 'league' || value === 'tournament') {
    return value;
  }

  return undefined;
}

export async function GET(request: NextRequest) {
  const locale = await getLocale();
  const searchParams = request.nextUrl.searchParams;
  const page = parsePage(searchParams.get('page'));
  const query = searchParams.get('q')?.trim() ?? '';
  const competitionType = parseCompetitionListFilter(searchParams.get('type'));
  const result = await getPaginatedLeaguesDb(locale, query, { page, pageSize: 50 }, competitionType);

  return NextResponse.json(result);
}
