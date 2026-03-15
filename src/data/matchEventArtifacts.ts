import type {
  MatchAnalysisArtifactPayload,
  MatchAnalysisData,
  MatchAnalysisEvent,
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
  payload: MatchAnalysisArtifactPayload,
  references: MatchAnalysisArtifactReferences,
): MatchAnalysisData {
  return {
    events: payload.events.map((event) => mapMatchAnalysisArtifactEvent(event, references)),
  };
}

function mapMatchAnalysisArtifactEvent(
  event: MatchAnalysisArtifactPayload['events'][number],
  references: MatchAnalysisArtifactReferences,
): MatchAnalysisEvent {
  const team = references.teams.get(event.teamId);
  const player = event.playerId === null ? undefined : references.players.get(event.playerId);
  const secondaryPlayer = event.secondaryPlayerId === null ? undefined : references.players.get(event.secondaryPlayerId);

  return {
    id: event.sourceEventId ?? `${payloadEventId(event)}`,
    minute: event.minute,
    second: event.second,
    type: event.type,
    teamId: team?.slug ?? String(event.teamId),
    playerId: player?.slug,
    playerName: player?.name,
    secondaryPlayerId: secondaryPlayer?.slug,
    secondaryPlayerName: secondaryPlayer?.name,
    locationX: event.locationX ?? undefined,
    locationY: event.locationY ?? undefined,
    endLocationX: event.endLocationX ?? undefined,
    endLocationY: event.endLocationY ?? undefined,
    endLocationZ: event.endLocationZ ?? undefined,
    underPressure: event.underPressure,
    statsbombXg: event.statsbombXg ?? undefined,
    outcome: event.outcome ?? undefined,
    detail: event.detail ?? undefined,
  };
}

function payloadEventId(event: MatchAnalysisArtifactPayload['events'][number]) {
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
]);

function mapTimelineEventType(type: MatchAnalysisEventType): 'goal' | 'yellow_card' | 'red_card' | 'substitution' {
  if (type === 'goal' || type === 'own_goal' || type === 'penalty_scored' || type === 'penalty_missed') {
    return 'goal';
  }

  if (type === 'yellow_card' || type === 'yellow_red_card') {
    return 'yellow_card';
  }

  if (type === 'red_card') {
    return 'red_card';
  }

  return 'substitution';
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
  payload: MatchAnalysisArtifactPayload,
  references: MatchAnalysisArtifactReferences,
): MatchEvent[] {
  return payload.events
    .filter((event) => TIMELINE_EVENT_TYPES.has(event.type) && (event.playerId !== null || event.type === 'var_decision'))
    .map((event) => {
      const team = references.teams.get(event.teamId);
      const player = event.playerId === null ? undefined : references.players.get(event.playerId);
      const secondaryPlayer = event.secondaryPlayerId === null ? undefined : references.players.get(event.secondaryPlayerId);

      const isGoalType = event.type === 'goal' || event.type === 'own_goal' || event.type === 'penalty_scored';
      const assistPlayerId = isGoalType ? secondaryPlayer?.slug : undefined;
      const assistPlayerName = isGoalType ? secondaryPlayer?.name : undefined;

      return {
        sourceEventId: event.sourceEventId ?? payloadEventId(event),
        minute: event.minute,
        type: mapTimelineEventType(event.type),
        rawType: event.type,
        playerId: player?.slug ?? 'unknown',
        playerName: player?.name ?? player?.slug ?? 'Unknown',
        teamId: team?.slug ?? String(event.teamId),
        secondaryPlayerId: secondaryPlayer?.slug,
        secondaryPlayerName: secondaryPlayer?.name,
        assistPlayerId,
        assistPlayerName,
        detail: buildTimelineDetail(event.type, event.detail, player?.name, secondaryPlayer?.name),
      };
    })
    .sort((left, right) => left.minute - right.minute);
}
