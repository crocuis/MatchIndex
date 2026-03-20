import postgres from 'postgres';
import type { MatchAnalysisArtifactPayloadV2 } from '@/data/types';
import { persistMatchEventArtifacts } from '@/data/matchEventArtifactWriter';

interface MatchContextRow {
  away_team_id: number;
  home_team_id: number;
  match_date: string;
}

interface UnderstatShotRow {
  X?: string;
  Y?: string;
  h_a?: 'h' | 'a';
  home_away?: 'h' | 'a';
  id?: string;
  minute?: string;
  player?: string;
  result?: string;
  second?: string;
  shotType?: string;
  situation?: string;
  xG?: string;
}

export interface CollectUnderstatShotArtifactsOptions {
  dryRun?: boolean;
  matchId: string;
  understatUrl: string;
}

export interface CollectUnderstatShotArtifactsSummary {
  artifactWritten: boolean;
  dryRun: boolean;
  eventsPrepared: number;
  matchId: string;
  understatUrl: string;
}

function getDb() {
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    max: 1,
    idle_timeout: 20,
    prepare: false,
  });
}

function toNumber(value: string | undefined | null) {
  if (!value) {
    return null;
  }

  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function clampPitchX(value: number | null) {
  if (value === null) {
    return null;
  }

  return Math.max(0, Math.min(120, value * 120));
}

function clampPitchY(value: number | null) {
  if (value === null) {
    return null;
  }

  return Math.max(0, Math.min(80, value * 80));
}

function decodeUnderstatJson(encoded: string) {
  return encoded
    .replace(/\\x([0-9A-Fa-f]{2})/g, (_, hex: string) => String.fromCharCode(Number.parseInt(hex, 16)))
    .replace(/\\u([0-9A-Fa-f]{4})/g, (_, hex: string) => String.fromCharCode(Number.parseInt(hex, 16)))
    .replace(/\\\//g, '/')
    .replace(/\\"/g, '"')
    .replace(/\\'/g, "'");
}

function extractShotsFromHtml(html: string): UnderstatShotRow[] {
  const patterns = [
    /var\s+shotsData\s*=\s*JSON\.parse\('([\s\S]*?)'\)/,
    /var\s+shotsData\s*=\s*(\{[\s\S]*?\});/,
  ];

  for (const pattern of patterns) {
    const match = html.match(pattern);
    if (!match?.[1]) {
      continue;
    }

    const raw = match[1];
    const parsed = pattern.source.includes('JSON\\.parse')
      ? JSON.parse(decodeUnderstatJson(raw))
      : JSON.parse(raw);

    if (Array.isArray(parsed)) {
      return parsed as UnderstatShotRow[];
    }

    if (parsed && typeof parsed === 'object' && 'h' in parsed && 'a' in parsed) {
      const home = Array.isArray((parsed as { h: unknown }).h) ? (parsed as { h: UnderstatShotRow[] }).h : [];
      const away = Array.isArray((parsed as { a: unknown }).a) ? (parsed as { a: UnderstatShotRow[] }).a : [];
      return [...home, ...away];
    }
  }

  throw new Error('Unable to extract Understat shotsData from HTML');
}

function mapShotTypeToEventType(result: string | undefined) {
  return result === 'Goal' ? 'goal' : 'shot';
}

function buildDetail(row: UnderstatShotRow) {
  return [row.result, row.situation, row.shotType].filter(Boolean).join(' · ') || null;
}

async function loadMatchContext(sql: postgres.Sql, matchId: string) {
  const rows = await sql<MatchContextRow[]>`
    SELECT
      m.match_date::TEXT AS match_date,
      m.home_team_id,
      m.away_team_id
    FROM matches m
    WHERE m.id = ${Number(matchId)}
    LIMIT 1
  `;

  return rows[0] ?? null;
}

export async function collectUnderstatShotArtifacts(
  options: CollectUnderstatShotArtifactsOptions,
): Promise<CollectUnderstatShotArtifactsSummary> {
  const sql = getDb();

  try {
    const match = await loadMatchContext(sql, options.matchId);

    if (!match) {
      throw new Error(`Match not found: ${options.matchId}`);
    }

    const response = await fetch(options.understatUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
        Accept: 'text/html,application/xhtml+xml',
      },
    });

    if (!response.ok) {
      throw new Error(`Understat request failed: ${response.status}`);
    }

    const html = await response.text();
    const shots = extractShotsFromHtml(html);
    const payload: MatchAnalysisArtifactPayloadV2 = {
      version: 2,
      matchId: Number(options.matchId),
      artifactType: 'analysis_detail',
      sourceVendor: 'understat',
      generatedAt: new Date().toISOString(),
      coordinateSystem: 'pitch-0to1',
      normalizedCoordinateSystem: 'pitch-100x100',
      events: shots.map((shot, index) => ({
        sourceEventId: shot.id ?? `understat:${options.matchId}:${index}`,
        sourceType: 'shot',
        sourceSubtype: [shot.result, shot.shotType, shot.situation].filter(Boolean).join(' / ') || null,
        canonicalType: mapShotTypeToEventType(shot.result),
        eventIndex: index,
        period: null,
        minute: Math.trunc(toNumber(shot.minute) ?? 0),
        second: Math.trunc(toNumber(shot.second) ?? 0),
        stoppageMinute: null,
        matchSecond: null,
        type: mapShotTypeToEventType(shot.result),
        teamId: (shot.home_away ?? shot.h_a) === 'a' ? match.away_team_id : match.home_team_id,
        playerId: null,
        secondaryPlayerId: null,
        sourceLocationX: toNumber(shot.X),
        sourceLocationY: toNumber(shot.Y),
        sourceEndLocationX: null,
        sourceEndLocationY: null,
        sourceEndLocationZ: null,
        locationX: clampPitchX(toNumber(shot.X)),
        locationY: clampPitchY(toNumber(shot.Y)),
        endLocationX: null,
        endLocationY: null,
        endLocationZ: null,
        underPressure: false,
        statsbombXg: toNumber(shot.xG),
        metrics: {
          xg: toNumber(shot.xG),
        },
        detail: buildDetail(shot),
        outcome: shot.result ?? null,
        sourcePayload: {
          player: shot.player ?? null,
          result: shot.result ?? null,
          shotType: shot.shotType ?? null,
          situation: shot.situation ?? null,
        },
      })),
    };

    if (!options.dryRun) {
      await persistMatchEventArtifacts(sql, {
        matchDate: match.match_date,
        matchId: Number(options.matchId),
        sourceVendor: 'understat',
        payload,
      });
    }

    return {
      artifactWritten: !options.dryRun,
      dryRun: options.dryRun ?? true,
      eventsPrepared: payload.events.length,
      matchId: options.matchId,
      understatUrl: options.understatUrl,
    };
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
