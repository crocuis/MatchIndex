import postgres from 'postgres';
import type { MatchAnalysisArtifactPayloadV2 } from '@/data/types';
import { persistMatchEventArtifacts } from '@/data/matchEventArtifactWriter';

interface MatchContextRow {
  away_team_id: number;
  external_match_id: string;
  home_team_id: number;
  match_date: string;
}

interface RawPayloadRow {
  payload: unknown;
}

interface SofascoreIncidentPlayer {
  name?: string;
}

interface SofascoreIncident {
  addedTime?: number;
  assist1?: SofascoreIncidentPlayer;
  description?: string;
  id?: string | number;
  incidentClass?: string;
  incidentType?: string;
  isHome?: boolean;
  length?: number;
  player?: SofascoreIncidentPlayer;
  playerIn?: SofascoreIncidentPlayer;
  playerOut?: SofascoreIncidentPlayer;
  reason?: string;
  text?: string;
  time?: number;
}

interface SofascoreIncidentsPayload {
  incidents?: SofascoreIncident[];
}

export interface CollectSofascoreMatchArtifactsOptions {
  dryRun?: boolean;
  matchId: string;
  sourceSlug?: string;
}

export interface CollectSofascoreMatchArtifactsSummary {
  artifactWritten: boolean;
  dryRun: boolean;
  eventsPrepared: number;
  matchId: string;
  sourceSlug: string;
}

function normalizeIncidentClass(value?: string | null) {
  return (value ?? '').trim().toLowerCase();
}

function buildIncidentDetail(incident: SofascoreIncident) {
  if (incident.incidentType === 'period') {
    return incident.text ?? incident.reason ?? null;
  }

  if (incident.incidentType === 'injuryTime') {
    if (typeof incident.length === 'number' && Number.isFinite(incident.length) && incident.length > 0) {
      return `${incident.length} minutes`;
    }

    return incident.text ?? incident.reason ?? null;
  }

  if (incident.incidentType === 'substitution') {
    return incident.incidentClass ?? incident.reason ?? incident.text ?? null;
  }

  return incident.description ?? incident.incidentClass ?? incident.reason ?? incident.text ?? null;
}

function getDb() {
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, idle_timeout: 20, prepare: false });
}

function getIncidentEventType(incident: SofascoreIncident): MatchAnalysisArtifactPayloadV2['events'][number]['type'] | null {
  const klass = normalizeIncidentClass(incident.incidentClass);

  if (incident.incidentType === 'goal') {
    if (klass === 'owngoal') return 'own_goal';
    if (klass === 'penalty') return 'penalty_scored';
    return 'goal';
  }

  if (incident.incidentType === 'card') {
    if (klass === 'red') return 'red_card';
    if (klass === 'yellowred') return 'yellow_red_card';
    return 'yellow_card';
  }
  if (incident.incidentType === 'substitution') return 'substitution';
  if (incident.incidentType === 'inGamePenalty') return klass === 'missed' ? 'penalty_missed' : 'penalty_scored';
  if (incident.incidentType === 'penaltyShootout') return klass === 'missed' ? 'penalty_missed' : 'penalty_scored';
  if (incident.incidentType === 'varDecision') return 'var_decision';
  if (incident.incidentType === 'period') return 'period';
  if (incident.incidentType === 'injuryTime') return 'injury_time';
  return null;
}

async function loadMatchContext(sql: postgres.Sql, matchId: string) {
  const rows = await sql<MatchContextRow[]>`
    SELECT
      m.match_date::TEXT AS match_date,
      m.home_team_id,
      m.away_team_id,
      COALESCE(m.source_metadata->>'externalMatchId', m.id::TEXT) AS external_match_id
    FROM matches m
    WHERE m.id = ${Number(matchId)}
    LIMIT 1
  `;

  return rows[0] ?? null;
}

async function loadIncidentsPayload(sql: postgres.Sql, sourceSlug: string, season: string, externalMatchId: string) {
  const rows = await sql<RawPayloadRow[]>`
    SELECT rp.payload
    FROM raw_payloads rp
    JOIN data_sources ds ON ds.id = rp.source_id
    WHERE ds.slug = ${sourceSlug}
      AND rp.season_context = ${season}
      AND rp.external_id = ${externalMatchId}
      AND rp.endpoint LIKE ${'%/match_events'}
    ORDER BY rp.fetched_at DESC
    LIMIT 1
  `;

  return rows[0]?.payload as SofascoreIncidentsPayload | undefined;
}

export async function collectSofascoreMatchArtifacts(
  options: CollectSofascoreMatchArtifactsOptions,
): Promise<CollectSofascoreMatchArtifactsSummary> {
  const sourceSlug = options.sourceSlug?.trim() || 'soccerdata_sofascore';
  const sql = getDb();

  try {
    const match = await loadMatchContext(sql, options.matchId);
    if (!match) {
      throw new Error(`Match not found: ${options.matchId}`);
    }

    const season = match.match_date.slice(0, 4);
    const incidents = await loadIncidentsPayload(sql, sourceSlug, season, match.external_match_id);
    const events: MatchAnalysisArtifactPayloadV2['events'] = [];

    for (const [index, incident] of (incidents?.incidents ?? []).entries()) {
      const type = getIncidentEventType(incident);
      if (!type) {
        continue;
      }

      const playerName = incident.player?.name ?? incident.playerIn?.name ?? null;
      const secondaryPlayerName = type === 'substitution' ? incident.playerOut?.name ?? null : incident.assist1?.name ?? null;
      events.push({
        sourceEventId: String(incident.id ?? `sofascore:${options.matchId}:${index}`),
        sourceType: incident.incidentType ?? null,
        sourceSubtype: incident.incidentClass ?? incident.reason ?? null,
        canonicalType: type,
        eventIndex: index,
        period: null,
        minute: incident.time ?? 0,
        second: null,
        stoppageMinute: incident.addedTime ?? null,
        matchSecond: null,
        type,
        teamId: incident.isHome ? match.home_team_id : match.away_team_id,
        playerId: null,
        secondaryPlayerId: null,
        sourceLocationX: null,
        sourceLocationY: null,
        sourceEndLocationX: null,
        sourceEndLocationY: null,
        sourceEndLocationZ: null,
        locationX: null,
        locationY: null,
        endLocationX: null,
        endLocationY: null,
        endLocationZ: null,
        underPressure: false,
        statsbombXg: null,
        detail: buildIncidentDetail(incident),
        outcome: null,
        sourcePayload: {
          addedTime: incident.addedTime ?? null,
          description: incident.description ?? null,
          incidentClass: incident.incidentClass ?? null,
          incidentType: incident.incidentType ?? null,
          isHome: incident.isHome ?? null,
          length: incident.length ?? null,
          player: playerName,
          playerIn: incident.playerIn?.name ?? null,
          playerOut: incident.playerOut?.name ?? null,
          text: incident.text ?? null,
        },
      });
    }

    const payload: MatchAnalysisArtifactPayloadV2 = {
      version: 2,
      matchId: Number(options.matchId),
      artifactType: 'analysis_detail',
      sourceVendor: 'sofascore',
      generatedAt: new Date().toISOString(),
      coordinateSystem: 'pitch-100x100',
      normalizedCoordinateSystem: 'pitch-100x100',
      events,
    };

    if (!options.dryRun) {
      await persistMatchEventArtifacts(sql, {
        matchDate: match.match_date,
        matchId: Number(options.matchId),
        sourceVendor: 'sofascore',
        payload,
      });
    }

    return {
      artifactWritten: !options.dryRun,
      dryRun: options.dryRun ?? true,
      eventsPrepared: payload.events.length,
      matchId: options.matchId,
      sourceSlug,
    };
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
