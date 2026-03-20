import { NextRequest, NextResponse } from 'next/server';
import { getLocale } from 'next-intl/server';
import { getTopScorerRowsBySeasonDb, getTopScorerRowsDb } from '@/data/server';

export async function GET(
  request: NextRequest,
  context: { params: Promise<unknown> },
) {
  const { id } = await context.params as { id: string };
  const { searchParams } = new URL(request.url);
  const seasonId = searchParams.get('seasonId') ?? undefined;
  const locale = await getLocale();

  const rows = seasonId
    ? await getTopScorerRowsBySeasonDb(id, seasonId, locale, 10)
    : await getTopScorerRowsDb(id, locale, 10);

  return NextResponse.json({ rows });
}
