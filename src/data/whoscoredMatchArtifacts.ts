import postgres from 'postgres';
import type { MatchAnalysisArtifactPayloadV2 } from '@/data/types';
import { persistMatchEventArtifacts } from '@/data/matchEventArtifactWriter';

interface MatchContextRow {
  away_team_id: number;
  home_team_id: number;
  match_date: string;
}

interface WhoScoredEvent {
  eventId?: number;
  id?: number;
  isGoal?: boolean;
  isShot?: boolean;
  minute?: number;
  second?: number;
  teamId?: number;
  x?: number;
  y?: number;
  endX?: number;
  endY?: number;
  type?: { displayName?: string };
  outcomeType?: { displayName?: string };
}

export interface CollectWhoScoredMatchArtifactsOptions {
  dryRun?: boolean;
  matchId: string;
  whoscoredUrl: string;
}

export interface CollectWhoScoredMatchArtifactsSummary {
  artifactWritten: boolean;
  dryRun: boolean;
  eventsPrepared: number;
  matchId: string;
  whoscoredUrl: string;
}

function getDb() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) throw new Error('DATABASE_URL is not set');
  return postgres(connectionString, { max: 1, idle_timeout: 20, prepare: false });
}

async function loadMatchContext(sql: postgres.Sql, matchId: string) {
  const rows = await sql<MatchContextRow[]>`
    SELECT m.match_date::TEXT AS match_date, m.home_team_id, m.away_team_id
    FROM matches m
    WHERE m.id = ${Number(matchId)}
    LIMIT 1
  `;
  return rows[0] ?? null;
}

function extractMatchCentreData(html: string) {
  const match = html.match(/matchCentreData\s*:\s*(\{[\s\S]*?\}),\s*matchCentreEventTypeJson/);
  if (!match?.[1]) {
    throw new Error('Unable to extract matchCentreData from WhoScored HTML');
  }

  return JSON.parse(match[1]) as { events?: WhoScoredEvent[] };
}

export async function collectWhoScoredMatchArtifacts(
  options: CollectWhoScoredMatchArtifactsOptions,
): Promise<CollectWhoScoredMatchArtifactsSummary> {
  const sql = getDb();

  try {
    const match = await loadMatchContext(sql, options.matchId);
    if (!match) throw new Error(`Match not found: ${options.matchId}`);

    const response = await fetch(options.whoscoredUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
        Accept: 'text/html,application/xhtml+xml',
      },
    });
    if (!response.ok) throw new Error(`WhoScored request failed: ${response.status}`);

    const html = await response.text();
    const matchCentreData = extractMatchCentreData(html);
    const payload: MatchAnalysisArtifactPayloadV2 = {
      version: 2,
      matchId: Number(options.matchId),
      artifactType: 'analysis_detail',
      sourceVendor: 'whoscored',
      generatedAt: new Date().toISOString(),
      coordinateSystem: 'pitch-100x100',
      normalizedCoordinateSystem: 'pitch-100x100',
      events: (matchCentreData.events ?? [])
        .filter((event) => event.isShot || event.isGoal || event.type?.displayName === 'Card' || event.type?.displayName === 'Substitution')
        .map((event, index) => ({
          sourceEventId: String(event.id ?? event.eventId ?? `whoscored:${options.matchId}:${index}`),
          sourceType: event.type?.displayName ?? null,
          sourceSubtype: event.outcomeType?.displayName ?? null,
          canonicalType: event.isGoal
            ? 'goal'
            : event.type?.displayName === 'Card'
              ? 'yellow_card'
              : event.type?.displayName === 'Substitution'
                ? 'substitution'
                : 'shot',
          eventIndex: index,
          period: null,
          minute: event.minute ?? 0,
          second: event.second ?? null,
          stoppageMinute: null,
          matchSecond: event.minute !== undefined && event.second !== undefined
            ? (event.minute * 60) + event.second
            : event.minute !== undefined
              ? event.minute * 60
              : null,
          type: event.isGoal ? 'goal' : event.type?.displayName === 'Card' ? 'yellow_card' : event.type?.displayName === 'Substitution' ? 'substitution' : 'shot',
          teamId: (event.teamId === 1 ? match.home_team_id : match.away_team_id),
          playerId: null,
          secondaryPlayerId: null,
          sourceLocationX: event.x ?? null,
          sourceLocationY: event.y ?? null,
          sourceEndLocationX: event.endX ?? null,
          sourceEndLocationY: event.endY ?? null,
          sourceEndLocationZ: null,
          locationX: event.x ?? null,
          locationY: event.y ?? null,
          endLocationX: event.endX ?? null,
          endLocationY: event.endY ?? null,
          endLocationZ: null,
          underPressure: false,
          statsbombXg: null,
          metrics: {},
          detail: event.type?.displayName ?? event.outcomeType?.displayName ?? null,
          outcome: event.outcomeType?.displayName ?? null,
          sourcePayload: {
            eventId: event.eventId ?? null,
            id: event.id ?? null,
            isGoal: event.isGoal ?? null,
            isShot: event.isShot ?? null,
            teamId: event.teamId ?? null,
          },
        })),
    };

    if (!options.dryRun) {
      await persistMatchEventArtifacts(sql, {
        matchDate: match.match_date,
        matchId: Number(options.matchId),
        sourceVendor: 'whoscored',
        payload,
      });
    }

    return {
      artifactWritten: !options.dryRun,
      dryRun: options.dryRun ?? true,
      eventsPrepared: payload.events.length,
      matchId: options.matchId,
      whoscoredUrl: options.whoscoredUrl,
    };
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
