import type {
  Match,
  ResolvedParticipant,
  TournamentSlot,
  WorldCupGroupStanding,
  WorldCupTournament,
} from './types';

interface SlotReference {
  label?: string;
  code?: string;
}

function buildMatchReferenceMap(tournament: WorldCupTournament) {
  const referencedNumbers = new Set<number>();

  for (const match of tournament.matches) {
    for (const ref of [match.homeTeamId, match.awayTeamId]) {
      const parsed = ref.match(/^(?:winner|loser)-match-(\d+)$/);
      if (parsed) {
        referencedNumbers.add(Number(parsed[1]));
      }
    }
  }

  if (referencedNumbers.size === 0) {
    return new Map<number, string>();
  }

  const startNumber = Math.min(...referencedNumbers);
  const knockoutMatches = tournament.matches
    .filter((match) => /-k\d+$/i.test(match.id))
    .slice()
    .sort((left, right) => left.id.localeCompare(right.id, undefined, { numeric: true }));

  const referenceMap = new Map<number, string>();
  knockoutMatches.forEach((match, index) => {
    referenceMap.set(startNumber + index, match.id);
  });

  return referenceMap;
}

function collectSlotReferences(tournament: WorldCupTournament) {
  const references = new Map<string, SlotReference>();

  for (const group of tournament.groups) {
    for (const row of group.standings) {
      references.set(row.nationId, {
        label: row.nationName,
        code: row.nationCode,
      });
    }
  }

  for (const match of tournament.matches) {
    references.set(match.homeTeamId, {
      label: match.homeTeamName,
      code: match.homeTeamCode,
    });
    references.set(match.awayTeamId, {
      label: match.awayTeamName,
      code: match.awayTeamCode,
    });
  }

  return references;
}

function deriveSlotFromId(id: string, reference: SlotReference | undefined, matchReferenceMap: Map<number, string>): TournamentSlot | null {
  const winnersMatch = id.match(/^winner-match-(\d+)$/);
  if (winnersMatch) {
    const matchId = matchReferenceMap.get(Number(winnersMatch[1]));
    if (!matchId) {
      return null;
    }

    return {
      id,
      label: reference?.label ?? `Winner match ${winnersMatch[1]}`,
      entityType: 'nation',
      description: `Resolves to the winner of match ${winnersMatch[1]}.`,
      source: {
        kind: 'matchOutcome',
        matchId,
        outcome: 'winner',
      },
      candidates: [],
    };
  }

  const losersMatch = id.match(/^loser-match-(\d+)$/);
  if (losersMatch) {
    const matchId = matchReferenceMap.get(Number(losersMatch[1]));
    if (!matchId) {
      return null;
    }

    return {
      id,
      label: reference?.label ?? `Loser match ${losersMatch[1]}`,
      entityType: 'nation',
      description: `Resolves to the loser of match ${losersMatch[1]}.`,
      source: {
        kind: 'matchOutcome',
        matchId,
        outcome: 'loser',
      },
      candidates: [],
    };
  }

  const groupPlacement = id.match(/^group-([a-z])-((?:winners)|(?:runners-up))$/);
  if (groupPlacement) {
    return {
      id,
      label: reference?.label ?? `Group ${groupPlacement[1].toUpperCase()} ${groupPlacement[2]}`,
      entityType: 'nation',
      description: `Resolves to the ${groupPlacement[2] === 'winners' ? 'winner' : 'runner-up'} of Group ${groupPlacement[1].toUpperCase()}.`,
      source: {
        kind: 'groupPlacement',
        groupId: `group-${groupPlacement[1]}`,
        position: groupPlacement[2] === 'winners' ? 1 : 2,
      },
      candidates: [],
    };
  }

  const thirdPlacePool = id.match(/^group-([a-z](?:-[a-z])*)-third-place$/);
  if (thirdPlacePool) {
    return {
      id,
      label: reference?.label ?? id,
      entityType: 'nation',
      description: `Resolves to one of the third-placed teams from ${thirdPlacePool[1].split('-').map((token) => `Group ${token.toUpperCase()}`).join(', ')} once the ranking pool is finalized.`,
      source: {
        kind: 'groupPoolPlacement',
        groupIds: thirdPlacePool[1].split('-').map((token) => `group-${token}`),
        position: 3,
      },
      candidates: [],
    };
  }

  return null;
}

function buildSlotRegistry(tournament: WorldCupTournament) {
  const references = collectSlotReferences(tournament);
  const matchReferenceMap = buildMatchReferenceMap(tournament);
  const registry = new Map<string, TournamentSlot>();

  for (const slot of tournament.slots ?? tournament.placeholders ?? []) {
    registry.set(slot.id, slot);
  }

  for (const [id, reference] of references) {
    if (registry.has(id)) {
      continue;
    }

    const derived = deriveSlotFromId(id, reference, matchReferenceMap);
    if (derived) {
      registry.set(id, derived);
    }
  }

  return registry;
}

function createResolvedParticipant(sourceId: string, entityId: string, displayName: string, displayCode?: string, slot?: TournamentSlot): ResolvedParticipant {
  return {
    sourceId,
    entityType: slot?.entityType ?? 'nation',
    status: 'resolved',
    entityId,
    displayName,
    displayCode,
    slot,
  };
}

function createUnresolvedParticipant(sourceId: string, displayName: string, displayCode?: string, slot?: TournamentSlot): ResolvedParticipant {
  return {
    sourceId,
    entityType: slot?.entityType ?? 'nation',
    status: 'unresolved',
    displayName,
    displayCode,
    slot,
  };
}

function resolveSlotParticipant(
  slot: TournamentSlot,
  tournament: WorldCupTournament,
  registry: Map<string, TournamentSlot>,
  cache: Map<string, ResolvedParticipant>,
  visited: Set<string>,
): ResolvedParticipant {
  const cached = cache.get(slot.id);
  if (cached) {
    return cached;
  }

  if (visited.has(slot.id)) {
    return createUnresolvedParticipant(slot.id, slot.label, undefined, slot);
  }

  const nextVisited = new Set(visited);
  nextVisited.add(slot.id);
  const source = slot.source;

  let participant: ResolvedParticipant;

  switch (source.kind) {
    case 'manual': {
      if (source.resolvedParticipant) {
        participant = createResolvedParticipant(
          slot.id,
          source.resolvedParticipant.entityId,
          source.resolvedParticipant.displayName ?? slot.label,
          source.resolvedParticipant.displayCode,
          slot,
        );
      } else {
        participant = createUnresolvedParticipant(slot.id, slot.label, undefined, slot);
      }
      break;
    }
    case 'groupPlacement': {
      const group = tournament.groups.find((item) => item.id === source.groupId);
      const row = group?.standings.find((item) => item.position === source.position);
      if (!row) {
        participant = createUnresolvedParticipant(slot.id, slot.label, undefined, slot);
        break;
      }

      const resolvedRow = resolveParticipantRef(row.nationId, row.nationName, row.nationCode, tournament, registry, cache, nextVisited);
      participant = resolvedRow.status === 'resolved'
        ? { ...resolvedRow, sourceId: slot.id, slot }
        : createUnresolvedParticipant(slot.id, row.nationName ?? slot.label, row.nationCode, slot);
      break;
    }
    case 'groupPoolPlacement': {
      participant = createUnresolvedParticipant(slot.id, slot.label, undefined, slot);
      break;
    }
    case 'matchOutcome': {
      const match = tournament.matches.find((item) => item.id === source.matchId);
      if (!match || match.homeScore === null || match.awayScore === null || match.homeScore === match.awayScore) {
        participant = createUnresolvedParticipant(slot.id, slot.label, undefined, slot);
        break;
      }

      const homeParticipant = resolveParticipantRef(match.homeTeamId, match.homeTeamName, match.homeTeamCode, tournament, registry, cache, nextVisited);
      const awayParticipant = resolveParticipantRef(match.awayTeamId, match.awayTeamName, match.awayTeamCode, tournament, registry, cache, nextVisited);
      const winningParticipant = match.homeScore > match.awayScore ? homeParticipant : awayParticipant;
      const losingParticipant = match.homeScore > match.awayScore ? awayParticipant : homeParticipant;
      const selectedParticipant = source.outcome === 'winner' ? winningParticipant : losingParticipant;

      participant = selectedParticipant.status === 'resolved'
        ? { ...selectedParticipant, sourceId: slot.id, slot }
        : createUnresolvedParticipant(slot.id, slot.label, undefined, slot);
      break;
    }
  }

  cache.set(slot.id, participant);
  return participant;
}

function resolveParticipantRef(
  refId: string,
  fallbackName: string | undefined,
  fallbackCode: string | undefined,
  tournament: WorldCupTournament,
  registry: Map<string, TournamentSlot>,
  cache: Map<string, ResolvedParticipant>,
  visited: Set<string>,
): ResolvedParticipant {
  const slot = registry.get(refId);
  if (!slot) {
    return createResolvedParticipant(refId, refId, fallbackName ?? refId, fallbackCode);
  }

  return resolveSlotParticipant(slot, tournament, registry, cache, visited);
}

function applyResolvedParticipantToStanding(row: WorldCupGroupStanding, participant: ResolvedParticipant): WorldCupGroupStanding {
  if (participant.status !== 'resolved' || !participant.entityId) {
    return {
      ...row,
      participant,
    };
  }

  return {
    ...row,
    nationId: participant.entityId,
    nationName: participant.displayName,
    nationCode: participant.displayCode ?? row.nationCode,
    participant,
  };
}

function applyResolvedParticipantToMatch(match: Match, side: 'home' | 'away', participant: ResolvedParticipant): Match {
  if (side === 'home') {
    return participant.status === 'resolved' && participant.entityId
      ? {
        ...match,
        homeTeamId: participant.entityId,
        homeTeamName: participant.displayName,
        homeTeamCode: participant.displayCode ?? match.homeTeamCode,
        homeParticipant: participant,
      }
      : {
        ...match,
        homeParticipant: participant,
      };
  }

  return participant.status === 'resolved' && participant.entityId
    ? {
      ...match,
      awayTeamId: participant.entityId,
      awayTeamName: participant.displayName,
      awayTeamCode: participant.displayCode ?? match.awayTeamCode,
      awayParticipant: participant,
    }
    : {
      ...match,
      awayParticipant: participant,
    };
}

export function resolveTournamentSlots(tournament: WorldCupTournament): WorldCupTournament {
  const registry = buildSlotRegistry(tournament);
  const cache = new Map<string, ResolvedParticipant>();
  const slots = Array.from(registry.values());

  const groups = tournament.groups.map((group) => ({
    ...group,
    standings: group.standings.map((row) => {
      const participant = resolveParticipantRef(row.nationId, row.nationName, row.nationCode, tournament, registry, cache, new Set());
      return applyResolvedParticipantToStanding(row, participant);
    }),
  }));

  const matches = tournament.matches.map((match) => {
    const homeParticipant = resolveParticipantRef(match.homeTeamId, match.homeTeamName, match.homeTeamCode, tournament, registry, cache, new Set());
    const awayParticipant = resolveParticipantRef(match.awayTeamId, match.awayTeamName, match.awayTeamCode, tournament, registry, cache, new Set());

    return applyResolvedParticipantToMatch(
      applyResolvedParticipantToMatch(match, 'home', homeParticipant),
      'away',
      awayParticipant,
    );
  });

  return {
    ...tournament,
    groups,
    matches,
    slots,
    placeholders: slots,
  };
}
