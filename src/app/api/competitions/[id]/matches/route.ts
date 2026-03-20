import { NextRequest, NextResponse } from 'next/server';
import { getLocale } from 'next-intl/server';
import { getMatchesByLeagueAndSeasonDb, getMatchesByLeagueDb } from '@/data/server';

export async function GET(
  request: NextRequest,
  context: { params: Promise<unknown> },
) {
  const { id } = await context.params as { id: string };
  const { searchParams } = new URL(request.url);
  const seasonId = searchParams.get('seasonId') ?? undefined;
  const locale = await getLocale();

  const matches = seasonId
    ? await getMatchesByLeagueAndSeasonDb(id, seasonId, locale)
    : await getMatchesByLeagueDb(id, locale);

  return NextResponse.json({ matches });
}
