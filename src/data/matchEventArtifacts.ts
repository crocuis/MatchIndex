import type {
  MatchAnalysisArtifactPayloadAny,
  MatchAnalysisData,
  MatchAnalysisEvent,
  MatchAnalysisSummary,
  MatchAnalysisEventType,
  MatchEvent,
} from '@/data/types';

interface ArtifactTeamReference {
  slug: string;
}

interface ArtifactPlayerReference {
  slug: string;
  name: string;
}

export interface MatchAnalysisArtifactReferences {
  teams: Map<number, ArtifactTeamReference>;
  players: Map<number, ArtifactPlayerReference>;
}

export function mapMatchAnalysisArtifactPayload(
  payload: MatchAnalysisArtifactPayloadAny,
  references: MatchAnalysisArtifactReferences,
): MatchAnalysisData {
  return {
    events: payload.events.map((event) => mapMatchAnalysisArtifactEvent(event, references)),
    summary: summarizeMatchAnalysisArtifactPayload(payload),
  };
}

export function createEmptyMatchAnalysisSummary(): MatchAnalysisSummary {
  return {
    totalEvents: 0,
    coordinateEvents: 0,
    trajectoryEvents: 0,
    playerLinkedEvents: 0,
    timelineEvents: 0,
    passMapEligibleEvents: 0,
    heatMapEligibleEvents: 0,
    shotMapEligibleEvents: 0,
    typeCounts: {},
  };
}

export function summarizeMatchAnalysisArtifactPayload(
  payload: MatchAnalysisArtifactPayloadAny,
): MatchAnalysisSummary {
  const summary = createEmptyMatchAnalysisSummary();

  for (const event of payload.events) {
    const eventType = event.canonicalType ?? event.type;
    const hasLocation = event.locationX !== null && event.locationY !== null;
    const hasEndLocation = event.endLocationX !== null && event.endLocationY !== null;
    const hasPlayer = event.playerId !== null && event.playerId !== undefined;

    summary.totalEvents += 1;
    summary.typeCounts[eventType] = (summary.typeCounts[eventType] ?? 0) + 1;

    if (hasLocation) {
      summary.coordinateEvents += 1;
    }

    if (hasEndLocation) {
      summary.trajectoryEvents += 1;
    }

    if (hasPlayer) {
      summary.playerLinkedEvents += 1;
    }

    if (TIMELINE_EVENT_TYPES.has(eventType)) {
      summary.timelineEvents += 1;
    }

    if (eventType === 'pass' && hasPlayer && hasLocation && hasEndLocation) {
      summary.passMapEligibleEvents += 1;
    }

    if (hasPlayer && hasLocation) {
      summary.heatMapEligibleEvents += 1;
    }

    if ((eventType === 'shot' || eventType === 'goal' || eventType === 'penalty_scored' || eventType === 'penalty_missed') && hasLocation) {
      summary.shotMapEligibleEvents += 1;
    }
  }

  return summary;
}

function mapMatchAnalysisArtifactEvent(
  event: MatchAnalysisArtifactPayloadAny['events'][number],
  references: MatchAnalysisArtifactReferences,
): MatchAnalysisEvent {
  const team = references.teams.get(event.teamId);
  const player = event.playerId === null ? undefined : references.players.get(event.playerId);
  const secondaryPlayer = event.secondaryPlayerId === null ? undefined : references.players.get(event.secondaryPlayerId);
  const fallbackPlayerId = event.playerId === null || event.playerId === undefined ? undefined : String(event.playerId);
  const fallbackSecondaryPlayerId = event.secondaryPlayerId === null || event.secondaryPlayerId === undefined ? undefined : String(event.secondaryPlayerId);

  return {
    id: event.sourceEventId ?? `${payloadEventId(event)}`,
    minute: event.minute,
    second: event.second,
    stoppageMinute: event.stoppageMinute ?? undefined,
    matchSecond: event.matchSecond ?? undefined,
    type: event.canonicalType ?? event.type,
    teamId: team?.slug ?? String(event.teamId),
    playerId: player?.slug ?? fallbackPlayerId,
    playerName: player?.name ?? fallbackPlayerId,
    secondaryPlayerId: secondaryPlayer?.slug ?? fallbackSecondaryPlayerId,
    secondaryPlayerName: secondaryPlayer?.name ?? fallbackSecondaryPlayerId,
    locationX: event.locationX ?? undefined,
    locationY: event.locationY ?? undefined,
    endLocationX: event.endLocationX ?? undefined,
    endLocationY: event.endLocationY ?? undefined,
    endLocationZ: event.endLocationZ ?? undefined,
    underPressure: event.underPressure,
    statsbombXg: event.metrics?.xg ?? event.statsbombXg ?? undefined,
    outcome: event.outcome ?? undefined,
    detail: event.detail ?? undefined,
    sourceSubtype: event.sourceSubtype ?? undefined,
  };
}

function payloadEventId(event: MatchAnalysisArtifactPayloadAny['events'][number]) {
  return [event.eventIndex, event.minute, event.second ?? 'na', event.teamId].join(':');
}

const TIMELINE_EVENT_TYPES: ReadonlySet<MatchAnalysisEventType> = new Set([
  'goal',
  'own_goal',
  'penalty_scored',
  'penalty_missed',
  'yellow_card',
  'red_card',
  'yellow_red_card',
  'substitution',
  'var_decision',
  'period',
  'injury_time',
]);

function mapTimelineEventType(type: MatchAnalysisEventType): MatchEvent['type'] {
  if (type === 'goal' || type === 'own_goal' || type === 'penalty_scored' || type === 'penalty_missed') {
    return 'goal';
  }

  if (type === 'yellow_card' || type === 'yellow_red_card') {
    return 'yellow_card';
  }

  if (type === 'red_card') {
    return 'red_card';
  }

  if (type === 'var_decision') {
    return 'var_decision';
  }

  if (type === 'period') {
    return 'period';
  }

  if (type === 'injury_time') {
    return 'injury_time';
  }

  return 'substitution';
}

function getSourcePayloadString(payload: Record<string, unknown> | null | undefined, key: string) {
  const value = payload?.[key];
  return typeof value === 'string' && value.trim().length > 0 ? value : undefined;
}

function buildTimelineDetail(
  type: MatchAnalysisEventType,
  detail: string | null,
  playerName: string | undefined,
  secondaryPlayerName: string | undefined,
) {
  if (type === 'substitution') {
    if (playerName && secondaryPlayerName) {
      return `${playerName} OUT · ${secondaryPlayerName} IN`;
    }

    return secondaryPlayerName ? `${secondaryPlayerName} IN` : detail ?? 'Substitution';
  }

  return detail ?? undefined;
}

export function mapMatchTimelineFromArtifact(
  payload: MatchAnalysisArtifactPayloadAny,
  references: MatchAnalysisArtifactReferences,
): MatchEvent[] {
  return payload.events
    .filter((event) => {
      const eventType = event.canonicalType ?? event.type;
      return TIMELINE_EVENT_TYPES.has(eventType);
    })
    .map((event) => {
      const eventType = event.canonicalType ?? event.type;
      const rawType = event.type as MatchAnalysisEventType;
      const team = references.teams.get(event.teamId);
      const player = event.playerId === null ? undefined : references.players.get(event.playerId);
      const secondaryPlayer = event.secondaryPlayerId === null ? undefined : references.players.get(event.secondaryPlayerId);
      const sourcePayload = event.sourcePayload ?? undefined;
      const fallbackPlayerName = getSourcePayloadString(sourcePayload, 'player') ?? getSourcePayloadString(sourcePayload, 'playerIn');
      const fallbackSecondaryPlayerName = getSourcePayloadString(sourcePayload, 'playerOut') ?? getSourcePayloadString(sourcePayload, 'assist1');

      const isGoalType = eventType === 'goal' || eventType === 'own_goal' || eventType === 'penalty_scored';
      const assistPlayerId = isGoalType ? secondaryPlayer?.slug : undefined;
      const assistPlayerName = isGoalType ? secondaryPlayer?.name ?? fallbackSecondaryPlayerName : undefined;

      return {
        sourceEventId: event.sourceEventId ?? payloadEventId(event),
        minute: event.minute,
        stoppageMinute: event.stoppageMinute ?? undefined,
        type: mapTimelineEventType(eventType),
        rawType,
        sourceSubtype: event.sourceSubtype ?? undefined,
        playerId: player?.slug,
        playerName: player?.name ?? player?.slug ?? fallbackPlayerName,
        teamId: team?.slug,
        secondaryPlayerId: secondaryPlayer?.slug,
        secondaryPlayerName: secondaryPlayer?.name ?? fallbackSecondaryPlayerName,
        assistPlayerId,
        assistPlayerName,
        detail: buildTimelineDetail(
          eventType,
          event.detail,
          player?.name ?? player?.slug ?? fallbackPlayerName,
          secondaryPlayer?.name ?? fallbackSecondaryPlayerName,
        ),
      };
    })
    .sort((left, right) => left.minute - right.minute);
}
