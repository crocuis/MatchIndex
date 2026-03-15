import type { JSONValue, Sql } from 'postgres';
import { open, readFile, rm } from 'node:fs/promises';
import type {
  MatchAnalysisArtifactPayload,
  MatchEventFreezeFramesArtifactPayload,
  MatchEventVisibleAreasArtifactPayload,
} from '@/data/types';
import { persistMatchEventArtifacts } from '@/data/matchEventArtifactWriter';
import { getSingleConnectionDb, resetDbClient } from '@/lib/db';
import type {
  StatsBombCompetitionEntry,
  StatsBombEventEntry,
  StatsBombLineupPlayer,
  StatsBombMatchEntry,
  StatsBombThreeSixtyEntry,
} from './statsbomb';

type PositionType = 'GK' | 'DEF' | 'MID' | 'FWD';
type MatchEventType =
  | 'pass'
  | 'shot'
  | 'carry'
  | 'pressure'
  | 'ball_receipt'
  | 'clearance'
  | 'interception'
  | 'block'
  | 'ball_recovery'
  | 'foul_won'
  | 'foul_committed'
  | 'duel'
  | 'miscontrol'
  | 'goalkeeper'
  | 'offside'
  | 'dribble'
  | 'dispossessed'
  | 'goal'
  | 'own_goal'
  | 'penalty_scored'
  | 'penalty_missed'
  | 'yellow_card'
  | 'red_card'
  | 'yellow_red_card'
  | 'substitution'
  | 'var_decision';

type AliasEntityType = 'player';

interface PlayerDraft {
  slug: string;
  name: string;
  knownAs: string;
  firstName: string;
  lastName: string;
  countryCode: string;
  position: PositionType | null;
}

interface PlayerContractDraft {
  playerSlug: string;
  teamSlug: string;
  competitionSlug: string;
  seasonSlug: string;
  shirtNumber: number | null;
}

interface PlayerSeasonStatsDraft {
  playerSlug: string;
  competitionSlug: string;
  seasonSlug: string;
  appearances: number;
  starts: number;
  minutesPlayed: number;
  goals: number;
  assists: number;
  penaltyGoals: number;
  ownGoals: number;
  yellowCards: number;
  redCards: number;
  yellowRedCards: number;
  cleanSheets: number;
  goalsConceded: number;
  saves: number;
}

interface MatchLineupDraft {
  matchId: number;
  matchDate: string;
  teamSlug: string;
  playerSlug: string;
  shirtNumber: number | null;
  position: string | null;
  isStarter: boolean;
  fromMinute: number | null;
  toMinute: number | null;
  startReason: string | null;
  endReason: string | null;
  minutesPlayed: number | null;
  sourceDetails: unknown;
}

interface MatchEventDraft {
  matchId: number;
  matchDate: string;
  sourceEventId: string;
  eventIndex: number;
  eventType: MatchEventType;
  period: number;
  eventTimestamp: string;
  minute: number;
  second: number;
  possession: number | null;
  possessionTeamSlug: string | null;
  teamSlug: string;
  playerSlug: string | null;
  secondaryPlayerSlug: string | null;
  locationX: number | null;
  locationY: number | null;
  endLocationX: number | null;
  endLocationY: number | null;
  endLocationZ: number | null;
  durationSeconds: number | null;
  underPressure: boolean;
  statsbombXg: number | null;
  isNotable: boolean;
  detail: string | null;
  sourceDetails: unknown;
}

interface MatchEventRelationDraft {
  sourceEventId: string;
  relatedSourceEventId: string;
  relationKind: string;
}

interface MatchEventFreezeFrameDraft {
  sourceEventId: string;
  playerSlug: string | null;
  teamSlug: string | null;
  isTeammate: boolean | null;
  isActor: boolean | null;
  isGoalkeeper: boolean | null;
  locationX: number;
  locationY: number;
}

interface MatchEventVisibleAreaDraft {
  sourceEventId: string;
  visibleArea: unknown;
}

interface MatchStatsDraft {
  matchId: number;
  matchDate: string;
  teamSlug: string;
  isHome: boolean;
  possession: number | null;
  totalPasses: number;
  accuratePasses: number;
  totalShots: number;
  shotsOnTarget: number;
  shotsOffTarget: number;
  blockedShots: number;
  fouls: number;
  offsides: number;
  gkSaves: number;
  expectedGoals: number;
}

export interface MaterializeStatsBombDetailsOptions {
  dryRun?: boolean;
  competitionLimit?: number;
  competitionOffset?: number;
  matchesPerSeasonLimit?: number;
}

export interface MaterializeStatsBombDetailsSummary {
  dryRun: boolean;
  players: number;
  contracts: number;
  playerSeasonStats: number;
  lineupRows: number;
  eventRows: number;
  relationRows: number;
  freezeFrameRows: number;
  visibleAreaRows: number;
  matchStatsRows: number;
}

const DETAILS_RUN_LOCK_PATH = '/tmp/matchindex-statsbomb-details.lock';

function getDetailsDb() {
  return getSingleConnectionDb('statsbomb-details');
}

async function resetDetailsDb() {
  try {
    await resetDbClient('statsbomb-details');
  } catch {
  }
}

function getErrorCode(error: unknown) {
  if (typeof error !== 'object' || error === null || !('code' in error)) {
    return undefined;
  }

  const { code } = error as { code?: unknown };
  return typeof code === 'string' ? code : undefined;
}

function isTransientDetailsDbError(error: unknown) {
  const code = getErrorCode(error);

  return code === 'ECONNRESET'
    || code === 'ECONNREFUSED'
    || code === '53300'
    || code === '57P01'
    || code === '57P03'
    || code === '08000'
    || code === '08003'
    || code === '08006'
    || code === '08001';
}

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function withDetailsDbRetry<T>(operation: (sql: Sql) => Promise<T>, label: string, maxAttempts: number = 5) {
  let attempt = 0;
  let lastError: unknown;

  while (attempt < maxAttempts) {
    attempt += 1;

    try {
      return await operation(getDetailsDb());
    } catch (error) {
      lastError = error;

      if (!isTransientDetailsDbError(error) || attempt >= maxAttempts) {
        throw error;
      }

      console.warn(`[statsbomb:details] retrying ${label} after transient DB error`, {
        attempt,
        maxAttempts,
        code: getErrorCode(error),
      });

      await resetDetailsDb();
      await sleep(Math.min(1000 * 2 ** (attempt - 1), 15000));
    }
  }

  throw lastError;
}

function isProcessAlive(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function acquireDetailsRunLock() {
  const lockPayload = JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() });

  try {
    const handle = await open(DETAILS_RUN_LOCK_PATH, 'wx');
    await handle.writeFile(lockPayload, 'utf8');
    await handle.close();
  } catch (error) {
    const code = getErrorCode(error);

    if (code !== 'EEXIST') {
      throw error;
    }

    const existing = await readFile(DETAILS_RUN_LOCK_PATH, 'utf8').catch(() => '');
    const parsed = existing ? JSON.parse(existing) as { pid?: number } : {};
    const existingPid = typeof parsed.pid === 'number' ? parsed.pid : null;

    if (existingPid && isProcessAlive(existingPid)) {
      throw new Error(`StatsBomb details materialization is already running (pid ${existingPid})`);
    }

    await rm(DETAILS_RUN_LOCK_PATH, { force: true });
    const handle = await open(DETAILS_RUN_LOCK_PATH, 'wx');
    await handle.writeFile(lockPayload, 'utf8');
    await handle.close();
  }

  return async () => {
    await rm(DETAILS_RUN_LOCK_PATH, { force: true });
  };
}

async function refreshDerivedViews(sql: Sql) {
  await sql`REFRESH MATERIALIZED VIEW mv_team_form`;
  await sql`REFRESH MATERIALIZED VIEW mv_standings`;
  await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;
}

async function upsertEntityAlias(sql: Sql, entityType: AliasEntityType, entityIdSql: ReturnType<Sql>, alias: string) {
  await sql`
    INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
    VALUES (${entityType}, (${entityIdSql}), ${alias}, 'en', 'common', TRUE, 'pending', 'imported', 'statsbomb_open_data')
    ON CONFLICT (entity_type, entity_id, alias_normalized)
    DO UPDATE SET
      locale = EXCLUDED.locale,
      alias_kind = EXCLUDED.alias_kind,
      is_primary = EXCLUDED.is_primary,
      status = EXCLUDED.status,
      source_type = EXCLUDED.source_type,
      source_ref = EXCLUDED.source_ref
    WHERE entity_aliases.status <> 'approved'
  `;
}

async function loadStatsBombModule(): Promise<typeof import('./statsbomb')> {
  return import(new URL('./statsbomb.ts', import.meta.url).href);
}

async function getMatchThreeSixtyEntries(
  helpers: typeof import('./statsbomb'),
  matchId: number,
  hasThreeSixty: boolean
): Promise<StatsBombThreeSixtyEntry[]> {
  if (!hasThreeSixty) {
    return [];
  }

  try {
    return await helpers.getMatchThreeSixty(matchId);
  } catch (error) {
    if (error instanceof Error && (error.message.includes('(404)') || error.name === 'SyntaxError')) {
      return [];
    }

    throw error;
  }
}

function parseNameParts(name: string) {
  const parts = name.trim().split(/\s+/);
  return {
    firstName: parts[0] ?? '',
    lastName: parts.slice(1).join(' '),
  };
}

function normalizePosition(position?: string | null): PositionType | null {
  if (!position) {
    return null;
  }

  const normalized = position.toLowerCase();

  if (normalized.includes('goalkeeper')) {
    return 'GK';
  }

  if (normalized.includes('back') || normalized.includes('center back') || normalized.includes('wing back')) {
    return 'DEF';
  }

  if (normalized.includes('midfield')) {
    return 'MID';
  }

  if (normalized.includes('wing') || normalized.includes('forward') || normalized.includes('striker')) {
    return 'FWD';
  }

  return null;
}

function parseClockToMinute(value?: string | null) {
  if (!value) {
    return null;
  }

  const [minuteValue] = value.split(':');
  const parsed = Number.parseInt(minuteValue, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseTimestampToInterval(value: string) {
  return value;
}

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = [];

  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }

  return chunks;
}

function toJsonValue(value: unknown): JSONValue {
  return JSON.parse(JSON.stringify(value)) as JSONValue;
}

function serializeMatchLineupDrafts(drafts: MatchLineupDraft[]) {
  return drafts.map((draft) => ({
    match_id: draft.matchId,
    match_date: draft.matchDate,
    team_slug: draft.teamSlug,
    player_slug: draft.playerSlug,
    shirt_number: draft.shirtNumber,
    position: draft.position,
    is_starter: draft.isStarter,
    from_minute: draft.fromMinute,
    to_minute: draft.toMinute,
    start_reason: draft.startReason,
    end_reason: draft.endReason,
    minutes_played: draft.minutesPlayed,
    source_details: draft.sourceDetails,
  }));
}

function serializeMatchEventDrafts(drafts: MatchEventDraft[]) {
  return drafts.map((draft) => ({
    match_id: draft.matchId,
    match_date: draft.matchDate,
    source_event_id: draft.sourceEventId,
    event_index: draft.eventIndex,
    event_type: draft.eventType,
    period: draft.period,
    event_timestamp: draft.eventTimestamp,
    minute: draft.minute,
    second: draft.second,
    possession: draft.possession,
    possession_team_slug: draft.possessionTeamSlug,
    team_slug: draft.teamSlug,
    player_slug: draft.playerSlug,
    secondary_player_slug: draft.secondaryPlayerSlug,
    location_x: draft.locationX,
    location_y: draft.locationY,
    end_location_x: draft.endLocationX,
    end_location_y: draft.endLocationY,
    end_location_z: draft.endLocationZ,
    duration_seconds: draft.durationSeconds,
    under_pressure: draft.underPressure,
    statsbomb_xg: draft.statsbombXg,
    is_notable: draft.isNotable,
    detail: draft.detail,
    source_details: draft.sourceDetails,
  }));
}

function serializeMatchEventRelationDrafts(drafts: MatchEventRelationDraft[]) {
  return drafts.map((draft) => ({
    source_event_id: draft.sourceEventId,
    related_source_event_id: draft.relatedSourceEventId,
    relation_kind: draft.relationKind,
  }));
}

function serializeMatchEventFreezeFrameDrafts(drafts: MatchEventFreezeFrameDraft[]) {
  return drafts.map((draft) => ({
    source_event_id: draft.sourceEventId,
    player_slug: draft.playerSlug,
    team_slug: draft.teamSlug,
    is_teammate: draft.isTeammate,
    is_actor: draft.isActor,
    is_goalkeeper: draft.isGoalkeeper,
    location_x: draft.locationX,
    location_y: draft.locationY,
  }));
}

function serializeMatchEventVisibleAreaDrafts(drafts: MatchEventVisibleAreaDraft[]) {
  return drafts.map((draft) => ({
    source_event_id: draft.sourceEventId,
    visible_area: draft.visibleArea,
  }));
}

function serializeMatchStatsDrafts(drafts: MatchStatsDraft[]) {
  return drafts.map((draft) => ({
    match_id: draft.matchId,
    match_date: draft.matchDate,
    team_slug: draft.teamSlug,
    is_home: draft.isHome,
    possession: draft.possession,
    total_passes: draft.totalPasses,
    accurate_passes: draft.accuratePasses,
    total_shots: draft.totalShots,
    shots_on_target: draft.shotsOnTarget,
    shots_off_target: draft.shotsOffTarget,
    blocked_shots: draft.blockedShots,
    fouls: draft.fouls,
    offsides: draft.offsides,
    gk_saves: draft.gkSaves,
    expected_goals: draft.expectedGoals,
  }));
}

function resolveMatchTeamSlug(
  name: string,
  match: StatsBombMatchEntry,
  helpers: typeof import('./statsbomb'),
  isInternational: boolean
) {
  if (name === match.home_team.home_team_name) {
    return helpers.createTeamSlug(match.home_team.home_team_name, isInternational ? undefined : match.home_team.country?.name);
  }

  if (name === match.away_team.away_team_name) {
    return helpers.createTeamSlug(match.away_team.away_team_name, isInternational ? undefined : match.away_team.country?.name);
  }

  return helpers.createTeamSlug(name, isInternational ? undefined : match.competition.country_name);
}

function createPlayerDraft(player: StatsBombLineupPlayer, countryCode: string, helpers: typeof import('./statsbomb')): PlayerDraft {
  const parsedName = parseNameParts(player.player_name);
  const knownAs = player.player_nickname ?? player.player_name;
  const primaryPosition = player.positions[0]?.position ?? null;

  return {
    slug: helpers.createStatsBombSlug(player.player_name),
    name: player.player_name,
    knownAs,
    firstName: parsedName.firstName,
    lastName: parsedName.lastName,
    countryCode,
    position: normalizePosition(primaryPosition),
  };
}

function createLineupDraft(
  player: StatsBombLineupPlayer,
  match: StatsBombMatchEntry,
  teamSlug: string,
  helpers: typeof import('./statsbomb')
): MatchLineupDraft {
  const primaryPosition = player.positions[0] ?? null;
  const lastPosition = player.positions.at(-1) ?? primaryPosition;
  const isStarter = primaryPosition?.start_reason === 'Starting XI';
  const fromMinute = parseClockToMinute(primaryPosition?.from ?? null);
  const toMinute = parseClockToMinute(lastPosition?.to ?? null);
  const minutesPlayed = fromMinute === null
    ? null
    : Math.max(0, (toMinute ?? 90) - fromMinute);

  return {
    matchId: match.match_id,
    matchDate: match.match_date,
    teamSlug,
    playerSlug: helpers.createStatsBombSlug(player.player_name),
    shirtNumber: player.jersey_number,
    position: primaryPosition?.position ?? null,
    isStarter,
    fromMinute,
    toMinute,
    startReason: primaryPosition?.start_reason ?? null,
    endReason: lastPosition?.end_reason ?? null,
    minutesPlayed,
    sourceDetails: player,
  };
}

function resolveEventType(event: StatsBombEventEntry): MatchEventType | null {
  const typeName = event.type.name ?? '';

  if (typeName === 'Pass') {
    return 'pass';
  }

  if (typeName === 'Carry') {
    return 'carry';
  }

  if (typeName === 'Pressure') {
    return 'pressure';
  }

  if (typeName === 'Ball Receipt*') {
    return 'ball_receipt';
  }

  if (typeName === 'Clearance') {
    return 'clearance';
  }

  if (typeName === 'Interception') {
    return 'interception';
  }

  if (typeName === 'Block') {
    return 'block';
  }

  if (typeName === 'Ball Recovery') {
    return 'ball_recovery';
  }

  if (typeName === 'Foul Won') {
    return 'foul_won';
  }

  if (typeName === 'Duel') {
    return 'duel';
  }

  if (typeName === 'Miscontrol') {
    return 'miscontrol';
  }

  if (typeName === 'Goal Keeper') {
    return 'goalkeeper';
  }

  if (typeName === 'Offside') {
    return 'offside';
  }

  if (typeName === 'Dribble') {
    return 'dribble';
  }

  if (typeName === 'Dispossessed') {
    return 'dispossessed';
  }

  if (typeName === 'Substitution') {
    return 'substitution';
  }

  if (typeName === 'Bad Behaviour') {
    const cardName = event.bad_behaviour?.card?.name ?? '';
    if (cardName === 'Yellow Card') return 'yellow_card';
    if (cardName === 'Red Card') return 'red_card';
    if (cardName === 'Second Yellow') return 'yellow_red_card';
  }

  if (typeName === 'Foul Committed') {
    const cardName = event.foul_committed?.card?.name ?? '';
    if (cardName === 'Yellow Card') return 'yellow_card';
    if (cardName === 'Red Card') return 'red_card';
    if (cardName === 'Second Yellow') return 'yellow_red_card';
    return 'foul_committed';
  }

  if (typeName === 'Own Goal Against') {
    return 'own_goal';
  }

  if (typeName === 'Shot') {
    const outcomeName = event.shot?.outcome?.name ?? '';
    const shotTypeName = event.shot?.type?.name ?? '';

    if (outcomeName === 'Goal' && shotTypeName === 'Penalty') {
      return 'penalty_scored';
    }

    if (outcomeName === 'Off T' && shotTypeName === 'Penalty') {
      return 'penalty_missed';
    }

    if (outcomeName === 'Goal') {
      return 'goal';
    }

    return 'shot';
  }

  return null;
}

function isNotableEventType(eventType: MatchEventType) {
  return eventType === 'goal'
    || eventType === 'own_goal'
    || eventType === 'penalty_scored'
    || eventType === 'penalty_missed'
    || eventType === 'yellow_card'
    || eventType === 'red_card'
    || eventType === 'yellow_red_card'
    || eventType === 'substitution'
    || eventType === 'var_decision';
}

function createEventDraft(
  event: StatsBombEventEntry,
  match: StatsBombMatchEntry,
  helpers: typeof import('./statsbomb'),
  isInternational: boolean
): MatchEventDraft | null {
  const eventType = resolveEventType(event);
  if (!eventType || !event.team?.name) {
    return null;
  }

  const endLocation = event.pass?.end_location ?? event.carry?.end_location ?? event.goalkeeper?.end_location ?? event.shot?.end_location;
  const detail = event.type.name ?? null;

  return {
    matchId: match.match_id,
    matchDate: match.match_date,
    sourceEventId: event.id,
    eventIndex: event.index,
    eventType,
    period: event.period,
    eventTimestamp: parseTimestampToInterval(event.timestamp),
    minute: event.minute,
    second: event.second,
    possession: event.possession ?? null,
    possessionTeamSlug: event.possession_team?.name ? resolveMatchTeamSlug(event.possession_team.name, match, helpers, isInternational) : null,
    teamSlug: resolveMatchTeamSlug(event.team.name, match, helpers, isInternational),
    playerSlug: event.player?.name ? helpers.createStatsBombSlug(event.player.name) : null,
    secondaryPlayerSlug: event.substitution?.replacement?.name
      ? helpers.createStatsBombSlug(event.substitution.replacement.name)
      : event.pass?.recipient?.name
        ? helpers.createStatsBombSlug(event.pass.recipient.name)
        : null,
    locationX: event.location?.[0] ?? null,
    locationY: event.location?.[1] ?? null,
    endLocationX: endLocation?.[0] ?? null,
    endLocationY: endLocation?.[1] ?? null,
    endLocationZ: endLocation?.[2] ?? null,
    durationSeconds: event.duration ?? null,
    underPressure: event.under_pressure ?? false,
    statsbombXg: event.shot?.statsbomb_xg ?? null,
    isNotable: isNotableEventType(eventType),
    detail,
    sourceDetails: event,
  };
}

function createEventRelationDrafts(event: StatsBombEventEntry): MatchEventRelationDraft[] {
  return (event.related_events ?? []).map((relatedSourceEventId) => ({
    sourceEventId: event.id,
    relatedSourceEventId,
    relationKind: 'related',
  }));
}

function createFreezeFrameDrafts(
  event: StatsBombEventEntry,
  eventDraft: MatchEventDraft,
  match: StatsBombMatchEntry,
  helpers: typeof import('./statsbomb'),
  isInternational: boolean
): MatchEventFreezeFrameDraft[] {
  const frames = event.shot?.freeze_frame ?? [];
  const homeTeamSlug = helpers.createTeamSlug(match.home_team.home_team_name, isInternational ? undefined : match.home_team.country?.name);
  const awayTeamSlug = helpers.createTeamSlug(match.away_team.away_team_name, isInternational ? undefined : match.away_team.country?.name);
  const opponentTeamSlug = eventDraft.teamSlug === homeTeamSlug ? awayTeamSlug : homeTeamSlug;

  return frames
    .filter((frame) => Array.isArray(frame.location) && frame.location.length >= 2)
    .map((frame) => ({
      sourceEventId: event.id,
      playerSlug: frame.player?.name ? helpers.createStatsBombSlug(frame.player.name) : null,
      teamSlug: frame.teammate === true ? eventDraft.teamSlug : frame.teammate === false ? opponentTeamSlug : null,
      isTeammate: frame.teammate ?? null,
      isActor: frame.player?.name ? helpers.createStatsBombSlug(frame.player.name) === eventDraft.playerSlug : null,
      isGoalkeeper: frame.keeper ?? (frame.position?.name === 'Goalkeeper'),
      locationX: frame.location![0]!,
      locationY: frame.location![1]!,
    }));
}

function createThreeSixtyFreezeFrameDrafts(
  entry: StatsBombThreeSixtyEntry,
  eventDraft: MatchEventDraft,
  match: StatsBombMatchEntry,
  helpers: typeof import('./statsbomb'),
  isInternational: boolean
): MatchEventFreezeFrameDraft[] {
  const homeTeamSlug = helpers.createTeamSlug(match.home_team.home_team_name, isInternational ? undefined : match.home_team.country?.name);
  const awayTeamSlug = helpers.createTeamSlug(match.away_team.away_team_name, isInternational ? undefined : match.away_team.country?.name);
  const opponentTeamSlug = eventDraft.teamSlug === homeTeamSlug ? awayTeamSlug : homeTeamSlug;

  return (entry.freeze_frame ?? [])
    .filter((frame) => Array.isArray(frame.location) && frame.location.length >= 2)
    .map((frame) => ({
      sourceEventId: entry.event_uuid,
      playerSlug: null,
      teamSlug: frame.teammate === true ? eventDraft.teamSlug : frame.teammate === false ? opponentTeamSlug : null,
      isTeammate: frame.teammate ?? null,
      isActor: frame.actor ?? null,
      isGoalkeeper: frame.keeper ?? null,
      locationX: frame.location![0]!,
      locationY: frame.location![1]!,
    }));
}

function createVisibleAreaDraft(entry: StatsBombThreeSixtyEntry): MatchEventVisibleAreaDraft | null {
  if (!entry.visible_area || entry.visible_area.length === 0) {
    return null;
  }

  return {
    sourceEventId: entry.event_uuid,
    visibleArea: entry.visible_area,
  };
}

function createEmptyMatchStatsDraft(match: StatsBombMatchEntry, teamSlug: string, isHome: boolean): MatchStatsDraft {
  return {
    matchId: match.match_id,
    matchDate: match.match_date,
    teamSlug,
    isHome,
    possession: null,
    totalPasses: 0,
    accuratePasses: 0,
    totalShots: 0,
    shotsOnTarget: 0,
    shotsOffTarget: 0,
    blockedShots: 0,
    fouls: 0,
    offsides: 0,
    gkSaves: 0,
    expectedGoals: 0,
  };
}

function buildMatchStatsDrafts(
  match: StatsBombMatchEntry,
  eventEntries: StatsBombEventEntry[],
  helpers: typeof import('./statsbomb'),
  isInternational: boolean
): MatchStatsDraft[] {
  const homeTeamSlug = helpers.createTeamSlug(match.home_team.home_team_name, isInternational ? undefined : match.home_team.country?.name);
  const awayTeamSlug = helpers.createTeamSlug(match.away_team.away_team_name, isInternational ? undefined : match.away_team.country?.name);
  const home = createEmptyMatchStatsDraft(match, homeTeamSlug, true);
  const away = createEmptyMatchStatsDraft(match, awayTeamSlug, false);
  const teamMap = new Map<string, MatchStatsDraft>([
    [homeTeamSlug, home],
    [awayTeamSlug, away],
  ]);

  for (const event of eventEntries) {
    if (!event.team?.name) {
      continue;
    }

    const teamSlug = resolveMatchTeamSlug(event.team.name, match, helpers, isInternational);
    const draft = teamMap.get(teamSlug);
    if (!draft) {
      continue;
    }

    const typeName = event.type.name ?? '';

    if (typeName === 'Pass') {
      draft.totalPasses += 1;
      if (!event.pass?.outcome?.name) {
        draft.accuratePasses += 1;
      }
    }

    if (typeName === 'Shot') {
      draft.totalShots += 1;
      draft.expectedGoals += event.shot?.statsbomb_xg ?? 0;
      const outcomeName = event.shot?.outcome?.name ?? '';

      if (outcomeName === 'Goal' || outcomeName === 'Saved' || outcomeName === 'Saved To Post') {
        draft.shotsOnTarget += 1;
      } else if (outcomeName === 'Blocked') {
        draft.blockedShots += 1;
      } else {
        draft.shotsOffTarget += 1;
      }
    }

    if (typeName === 'Foul Committed') {
      draft.fouls += 1;
    }

    if (typeName === 'Offside') {
      draft.offsides += 1;
    }

    if (typeName === 'Goal Keeper') {
      const outcomeName = event.goalkeeper?.type?.name ?? '';
      if (outcomeName.includes('Shot Saved')) {
        draft.gkSaves += 1;
      }
    }
  }

  const possessionTotals = eventEntries.reduce<Map<string, number>>((acc, event) => {
    if (!event.possession_team?.name) {
      return acc;
    }

    const teamSlug = resolveMatchTeamSlug(event.possession_team.name, match, helpers, isInternational);
    acc.set(teamSlug, (acc.get(teamSlug) ?? 0) + 1);
    return acc;
  }, new Map<string, number>());
  const totalPossessionEvents = Array.from(possessionTotals.values()).reduce((sum, value) => sum + value, 0);

  if (totalPossessionEvents > 0) {
    for (const [teamSlug, value] of possessionTotals.entries()) {
      const draft = teamMap.get(teamSlug);
      if (!draft) {
        continue;
      }

      draft.possession = Math.round((value / totalPossessionEvents) * 100);
    }
  }

  return [home, away];
}

function incrementStats(
  statsMap: Map<string, PlayerSeasonStatsDraft>,
  key: string,
  create: () => PlayerSeasonStatsDraft,
  mutate: (draft: PlayerSeasonStatsDraft) => void
) {
  const draft = statsMap.get(key) ?? create();
  mutate(draft);
  statsMap.set(key, draft);
}

function createEmptyPlayerSeasonStatsDraft(playerSlug: string, competitionSlug: string, seasonSlug: string): PlayerSeasonStatsDraft {
  return {
    playerSlug,
    competitionSlug,
    seasonSlug,
    appearances: 0,
    starts: 0,
    minutesPlayed: 0,
    goals: 0,
    assists: 0,
    penaltyGoals: 0,
    ownGoals: 0,
    yellowCards: 0,
    redCards: 0,
    yellowRedCards: 0,
    cleanSheets: 0,
    goalsConceded: 0,
    saves: 0,
  };
}

async function upsertPlayer(sql: Sql, draft: PlayerDraft) {
  await sql`
    INSERT INTO players (slug, country_id, position, is_active, updated_at)
    VALUES (
      ${draft.slug},
      (SELECT id FROM countries WHERE code_alpha3 = ${draft.countryCode}),
      ${draft.position},
      TRUE,
      NOW()
    )
    ON CONFLICT (slug)
    DO UPDATE SET
      country_id = COALESCE(EXCLUDED.country_id, players.country_id),
      position = COALESCE(EXCLUDED.position, players.position),
      is_active = TRUE,
      updated_at = NOW()
  `;

  await sql`
    INSERT INTO player_translations (player_id, locale, first_name, last_name, known_as)
    VALUES ((SELECT id FROM players WHERE slug = ${draft.slug}), 'en', ${draft.firstName}, ${draft.lastName}, ${draft.knownAs})
    ON CONFLICT (player_id, locale)
    DO UPDATE SET
      first_name = EXCLUDED.first_name,
      last_name = EXCLUDED.last_name,
      known_as = EXCLUDED.known_as
  `;

  await upsertEntityAlias(sql, 'player', sql`SELECT id FROM players WHERE slug = ${draft.slug}`, draft.knownAs);
}

async function upsertPlayerContract(sql: Sql, draft: PlayerContractDraft) {
  await sql`
    INSERT INTO player_contracts (player_id, team_id, competition_season_id, shirt_number, updated_at)
    VALUES (
      (SELECT id FROM players WHERE slug = ${draft.playerSlug}),
      (SELECT id FROM teams WHERE slug = ${draft.teamSlug}),
      (
        SELECT cs.id
        FROM competition_seasons cs
        JOIN competitions c ON c.id = cs.competition_id
        JOIN seasons s ON s.id = cs.season_id
        WHERE c.slug = ${draft.competitionSlug} AND s.slug = ${draft.seasonSlug}
      ),
      ${draft.shirtNumber},
      NOW()
    )
    ON CONFLICT (player_id, competition_season_id)
    DO UPDATE SET
      team_id = EXCLUDED.team_id,
      shirt_number = EXCLUDED.shirt_number,
      left_date = NULL,
      updated_at = NOW()
  `;
}

async function upsertPlayerSeasonStats(sql: Sql, draft: PlayerSeasonStatsDraft) {
  await sql`
    INSERT INTO player_season_stats (
      player_id,
      competition_season_id,
      appearances,
      starts,
      minutes_played,
      goals,
      assists,
      penalty_goals,
      own_goals,
      yellow_cards,
      red_cards,
      yellow_red_cards,
      clean_sheets,
      goals_conceded,
      saves,
      updated_at
    )
    VALUES (
      (SELECT id FROM players WHERE slug = ${draft.playerSlug}),
      (
        SELECT cs.id
        FROM competition_seasons cs
        JOIN competitions c ON c.id = cs.competition_id
        JOIN seasons s ON s.id = cs.season_id
        WHERE c.slug = ${draft.competitionSlug} AND s.slug = ${draft.seasonSlug}
      ),
      ${draft.appearances},
      ${draft.starts},
      ${draft.minutesPlayed},
      ${draft.goals},
      ${draft.assists},
      ${draft.penaltyGoals},
      ${draft.ownGoals},
      ${draft.yellowCards},
      ${draft.redCards},
      ${draft.yellowRedCards},
      ${draft.cleanSheets},
      ${draft.goalsConceded},
      ${draft.saves},
      NOW()
    )
    ON CONFLICT (player_id, competition_season_id)
    DO UPDATE SET
      appearances = EXCLUDED.appearances,
      starts = EXCLUDED.starts,
      minutes_played = EXCLUDED.minutes_played,
      goals = EXCLUDED.goals,
      assists = EXCLUDED.assists,
      penalty_goals = EXCLUDED.penalty_goals,
      own_goals = EXCLUDED.own_goals,
      yellow_cards = EXCLUDED.yellow_cards,
      red_cards = EXCLUDED.red_cards,
      yellow_red_cards = EXCLUDED.yellow_red_cards,
      clean_sheets = EXCLUDED.clean_sheets,
      goals_conceded = EXCLUDED.goals_conceded,
      saves = EXCLUDED.saves,
      updated_at = NOW()
  `;
}

async function upsertMatchLineup(sql: Sql, draft: MatchLineupDraft) {
  await sql`
    INSERT INTO match_lineups (
      match_id,
      match_date,
      team_id,
      player_id,
      shirt_number,
      position,
      is_starter,
      from_minute,
      to_minute,
      start_reason,
      end_reason,
      minutes_played,
      source_details
    )
    VALUES (
      ${draft.matchId},
      ${draft.matchDate},
      (SELECT id FROM teams WHERE slug = ${draft.teamSlug}),
      (SELECT id FROM players WHERE slug = ${draft.playerSlug}),
      ${draft.shirtNumber},
      ${draft.position},
      ${draft.isStarter},
      ${draft.fromMinute},
      ${draft.toMinute},
      ${draft.startReason},
      ${draft.endReason},
      ${draft.minutesPlayed},
      ${JSON.stringify(draft.sourceDetails)}::jsonb
    )
    ON CONFLICT (match_id, match_date, team_id, player_id)
    DO UPDATE SET
      shirt_number = EXCLUDED.shirt_number,
      position = EXCLUDED.position,
      is_starter = EXCLUDED.is_starter,
      from_minute = EXCLUDED.from_minute,
      to_minute = EXCLUDED.to_minute,
      start_reason = EXCLUDED.start_reason,
      end_reason = EXCLUDED.end_reason,
      minutes_played = EXCLUDED.minutes_played,
      source_details = EXCLUDED.source_details
  `;
}

async function upsertMatchLineupsBatch(sql: Sql, drafts: MatchLineupDraft[]) {
  if (drafts.length === 0) {
    return;
  }

  await sql`
    WITH input AS (
      SELECT *
      FROM jsonb_to_recordset(${sql.json(toJsonValue(serializeMatchLineupDrafts(drafts)))}::jsonb) AS item(
        match_id BIGINT,
        match_date DATE,
        team_slug TEXT,
        player_slug TEXT,
        shirt_number INTEGER,
        position TEXT,
        is_starter BOOLEAN,
        from_minute INTEGER,
        to_minute INTEGER,
        start_reason TEXT,
        end_reason TEXT,
        minutes_played INTEGER,
        source_details JSONB
      )
    )
    INSERT INTO match_lineups (
      match_id,
      match_date,
      team_id,
      player_id,
      shirt_number,
      position,
      is_starter,
      from_minute,
      to_minute,
      start_reason,
      end_reason,
      minutes_played,
      source_details
    )
    SELECT
      input.match_id,
      input.match_date,
      team.id,
      player.id,
      input.shirt_number,
      input.position,
      input.is_starter,
      input.from_minute,
      input.to_minute,
      input.start_reason,
      input.end_reason,
      input.minutes_played,
      input.source_details
    FROM input
    JOIN teams team ON team.slug = input.team_slug
    JOIN players player ON player.slug = input.player_slug
    ON CONFLICT (match_id, match_date, team_id, player_id)
    DO UPDATE SET
      shirt_number = EXCLUDED.shirt_number,
      position = EXCLUDED.position,
      is_starter = EXCLUDED.is_starter,
      from_minute = EXCLUDED.from_minute,
      to_minute = EXCLUDED.to_minute,
      start_reason = EXCLUDED.start_reason,
      end_reason = EXCLUDED.end_reason,
      minutes_played = EXCLUDED.minutes_played,
      source_details = EXCLUDED.source_details
  `;
}

async function upsertMatchStats(sql: Sql, draft: MatchStatsDraft) {
  await sql`
    INSERT INTO match_stats (
      match_id,
      match_date,
      team_id,
      is_home,
      possession,
      total_passes,
      accurate_passes,
      pass_accuracy,
      total_shots,
      shots_on_target,
      shots_off_target,
      blocked_shots,
      fouls,
      offsides,
      gk_saves,
      expected_goals
    )
    VALUES (
      ${draft.matchId},
      ${draft.matchDate},
      (SELECT id FROM teams WHERE slug = ${draft.teamSlug}),
      ${draft.isHome},
      ${draft.possession},
      ${draft.totalPasses},
      ${draft.accuratePasses},
      ${draft.totalPasses > 0 ? Math.round((draft.accuratePasses / draft.totalPasses) * 100) : null},
      ${draft.totalShots},
      ${draft.shotsOnTarget},
      ${draft.shotsOffTarget},
      ${draft.blockedShots},
      ${draft.fouls},
      ${draft.offsides},
      ${draft.gkSaves},
      ${draft.expectedGoals.toFixed(2)}
    )
    ON CONFLICT (match_id, match_date, team_id)
    DO UPDATE SET
      is_home = EXCLUDED.is_home,
      possession = EXCLUDED.possession,
      total_passes = EXCLUDED.total_passes,
      accurate_passes = EXCLUDED.accurate_passes,
      pass_accuracy = EXCLUDED.pass_accuracy,
      total_shots = EXCLUDED.total_shots,
      shots_on_target = EXCLUDED.shots_on_target,
      shots_off_target = EXCLUDED.shots_off_target,
      blocked_shots = EXCLUDED.blocked_shots,
      fouls = EXCLUDED.fouls,
      offsides = EXCLUDED.offsides,
      gk_saves = EXCLUDED.gk_saves,
      expected_goals = EXCLUDED.expected_goals
  `;
}

async function upsertMatchStatsBatch(sql: Sql, drafts: MatchStatsDraft[]) {
  if (drafts.length === 0) {
    return;
  }

  await sql`
    WITH input AS (
      SELECT *
      FROM jsonb_to_recordset(${sql.json(toJsonValue(serializeMatchStatsDrafts(drafts)))}::jsonb) AS item(
        match_id BIGINT,
        match_date DATE,
        team_slug TEXT,
        is_home BOOLEAN,
        possession INTEGER,
        total_passes INTEGER,
        accurate_passes INTEGER,
        total_shots INTEGER,
        shots_on_target INTEGER,
        shots_off_target INTEGER,
        blocked_shots INTEGER,
        fouls INTEGER,
        offsides INTEGER,
        gk_saves INTEGER,
        expected_goals DECIMAL
      )
    )
    INSERT INTO match_stats (
      match_id,
      match_date,
      team_id,
      is_home,
      possession,
      total_passes,
      accurate_passes,
      pass_accuracy,
      total_shots,
      shots_on_target,
      shots_off_target,
      blocked_shots,
      fouls,
      offsides,
      gk_saves,
      expected_goals
    )
    SELECT
      input.match_id,
      input.match_date,
      team.id,
      input.is_home,
      input.possession,
      input.total_passes,
      input.accurate_passes,
      CASE
        WHEN input.total_passes > 0 THEN ROUND((input.accurate_passes::numeric / input.total_passes::numeric) * 100)
        ELSE NULL
      END,
      input.total_shots,
      input.shots_on_target,
      input.shots_off_target,
      input.blocked_shots,
      input.fouls,
      input.offsides,
      input.gk_saves,
      input.expected_goals
    FROM input
    JOIN teams team ON team.slug = input.team_slug
    ON CONFLICT (match_id, match_date, team_id)
    DO UPDATE SET
      is_home = EXCLUDED.is_home,
      possession = EXCLUDED.possession,
      total_passes = EXCLUDED.total_passes,
      accurate_passes = EXCLUDED.accurate_passes,
      pass_accuracy = EXCLUDED.pass_accuracy,
      total_shots = EXCLUDED.total_shots,
      shots_on_target = EXCLUDED.shots_on_target,
      shots_off_target = EXCLUDED.shots_off_target,
      blocked_shots = EXCLUDED.blocked_shots,
      fouls = EXCLUDED.fouls,
      offsides = EXCLUDED.offsides,
      gk_saves = EXCLUDED.gk_saves,
      expected_goals = EXCLUDED.expected_goals
  `;
}

async function loadEntityIdBySlug(sql: Sql, tableName: 'players' | 'teams', slugs: string[]) {
  if (slugs.length === 0) {
    return new Map<string, number>();
  }

  const rows = tableName === 'players'
    ? await sql<{ id: number; slug: string }[]>`
        SELECT id, slug
        FROM players
        WHERE slug = ANY(${slugs})
      `
    : await sql<{ id: number; slug: string }[]>`
        SELECT id, slug
        FROM teams
        WHERE slug = ANY(${slugs})
      `;

  return new Map(rows.map((row) => [row.slug, row.id]));
}

async function persistStatsBombArtifacts(sql: Sql, params: {
  matchId: number;
  matchDate: string;
  events: MatchEventDraft[];
  freezeFrames: MatchEventFreezeFrameDraft[];
  visibleAreas: MatchEventVisibleAreaDraft[];
}) {
  const playerSlugs = Array.from(new Set([
    ...params.events.flatMap((draft) => [draft.playerSlug, draft.secondaryPlayerSlug].filter((value): value is string => Boolean(value))),
    ...params.freezeFrames.flatMap((draft) => [draft.playerSlug].filter((value): value is string => Boolean(value))),
  ]));
  const teamSlugs = Array.from(new Set([
    ...params.events.flatMap((draft) => [draft.teamSlug, draft.possessionTeamSlug].filter((value): value is string => Boolean(value))),
    ...params.freezeFrames.flatMap((draft) => [draft.teamSlug].filter((value): value is string => Boolean(value))),
  ]));
  const playerIdBySlug = await loadEntityIdBySlug(sql, 'players', playerSlugs);
  const teamIdBySlug = await loadEntityIdBySlug(sql, 'teams', teamSlugs);

  const analysisPayload: MatchAnalysisArtifactPayload = {
    version: 1,
    matchId: params.matchId,
    artifactType: 'analysis_detail',
    sourceVendor: 'statsbomb',
    generatedAt: new Date().toISOString(),
    events: params.events.map((draft) => ({
      sourceEventId: draft.sourceEventId,
      eventIndex: draft.eventIndex,
      minute: draft.minute,
      second: draft.second,
      type: draft.eventType,
      teamId: teamIdBySlug.get(draft.teamSlug) ?? 0,
      playerId: draft.playerSlug ? playerIdBySlug.get(draft.playerSlug) ?? null : null,
      secondaryPlayerId: draft.secondaryPlayerSlug ? playerIdBySlug.get(draft.secondaryPlayerSlug) ?? null : null,
      locationX: draft.locationX,
      locationY: draft.locationY,
      endLocationX: draft.endLocationX,
      endLocationY: draft.endLocationY,
      endLocationZ: draft.endLocationZ,
      underPressure: draft.underPressure,
      statsbombXg: draft.statsbombXg,
      detail: draft.detail,
      outcome: null,
    })),
  };

  await persistMatchEventArtifacts(sql, {
    matchId: params.matchId,
    matchDate: params.matchDate,
    sourceVendor: 'statsbomb',
    payload: analysisPayload,
  });

  const freezeFramesPayload: MatchEventFreezeFramesArtifactPayload = {
    version: 1,
    matchId: params.matchId,
    artifactType: 'freeze_frames',
    sourceVendor: 'statsbomb',
    generatedAt: new Date().toISOString(),
    freezeFrames: Array.from(params.freezeFrames.reduce((map, draft) => {
      const key = draft.sourceEventId;
      const current = map.get(key) ?? { sourceEventId: draft.sourceEventId, freezeFrames: [] as MatchEventFreezeFramesArtifactPayload['freezeFrames'][number]['freezeFrames'] };
      current.freezeFrames.push({
        sourceEventId: draft.sourceEventId,
        playerId: draft.playerSlug ? playerIdBySlug.get(draft.playerSlug) ?? null : null,
        teamId: draft.teamSlug ? teamIdBySlug.get(draft.teamSlug) ?? null : null,
        isTeammate: draft.isTeammate,
        isActor: draft.isActor,
        isGoalkeeper: draft.isGoalkeeper,
        locationX: draft.locationX,
        locationY: draft.locationY,
      });
      map.set(key, current);
      return map;
    }, new Map<string, MatchEventFreezeFramesArtifactPayload['freezeFrames'][number]>()).values()),
  };

  if (freezeFramesPayload.freezeFrames.length > 0) {
    await persistMatchEventArtifacts(sql, {
      matchId: params.matchId,
      matchDate: params.matchDate,
      sourceVendor: 'statsbomb',
      payload: freezeFramesPayload,
    });
  }

  const visibleAreasPayload: MatchEventVisibleAreasArtifactPayload = {
    version: 1,
    matchId: params.matchId,
    artifactType: 'visible_areas',
    sourceVendor: 'statsbomb',
    generatedAt: new Date().toISOString(),
    visibleAreas: params.visibleAreas.map((draft) => ({
      sourceEventId: draft.sourceEventId,
      visibleArea: Array.isArray(draft.visibleArea)
        ? draft.visibleArea.filter((value): value is number => typeof value === 'number')
        : [],
    })),
  };

  if (visibleAreasPayload.visibleAreas.length > 0) {
    await persistMatchEventArtifacts(sql, {
      matchId: params.matchId,
      matchDate: params.matchDate,
      sourceVendor: 'statsbomb',
      payload: visibleAreasPayload,
    });
  }
}

async function persistMatchDetails(sql: Sql, params: {
  players: Map<string, PlayerDraft>;
  lineups: MatchLineupDraft[];
  events: MatchEventDraft[];
  relations: MatchEventRelationDraft[];
  freezeFrames: MatchEventFreezeFrameDraft[];
  visibleAreas: MatchEventVisibleAreaDraft[];
  matchStats: MatchStatsDraft[];
}) {
  await sql`BEGIN`;

  try {
    for (const player of params.players.values()) {
      await upsertPlayer(sql, player);
    }

    for (const chunk of chunkArray(params.lineups, 500)) {
      await upsertMatchLineupsBatch(sql, chunk);
    }

    for (const chunk of chunkArray(params.matchStats, 500)) {
      await upsertMatchStatsBatch(sql, chunk);
    }

    if (params.events.length > 0) {
      await persistStatsBombArtifacts(sql, {
        matchId: params.events[0]!.matchId,
        matchDate: params.events[0]!.matchDate,
        events: params.events,
        freezeFrames: params.freezeFrames,
        visibleAreas: params.visibleAreas,
      });
    }

    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }
}

export async function materializeStatsBombDetails(
  options: MaterializeStatsBombDetailsOptions = {}
): Promise<MaterializeStatsBombDetailsSummary> {
  const releaseRunLock = options.dryRun ? null : await acquireDetailsRunLock();
  const helpers = await loadStatsBombModule();
  try {
    const competitionEntries = await helpers.fetchStatsBombJson<StatsBombCompetitionEntry[]>('data/competitions.json');
    const competitionOffset = options.competitionOffset ?? 0;
    const limitedCompetitionEntries = competitionEntries.slice(
      competitionOffset,
      competitionOffset + (options.competitionLimit ?? competitionEntries.length)
    );

    const players = new Map<string, PlayerDraft>();
    const persistedPlayerSlugs = new Set<string>();
    const contracts = new Map<string, PlayerContractDraft>();
    const stats = new Map<string, PlayerSeasonStatsDraft>();
    let lineupRows = 0;
    let eventRows = 0;
    let relationRows = 0;
    let freezeFrameRows = 0;
    let visibleAreaRows = 0;
    let matchStatsRows = 0;
    const dryRun = options.dryRun ?? false;
    const sql = dryRun ? null : getDetailsDb();

    for (const competitionEntry of limitedCompetitionEntries) {
      const seasonSlug = helpers.createSeasonSlug(competitionEntry.season_name, competitionEntry.season_id);
      const competitionSlug = helpers.createCompetitionSlug(competitionEntry);
      const matchEntries = await helpers.fetchStatsBombJson<StatsBombMatchEntry[]>(
        `data/matches/${competitionEntry.competition_id}/${competitionEntry.season_id}.json`
      );
    const limitedMatchEntries = matchEntries.slice(0, options.matchesPerSeasonLimit ?? matchEntries.length);

    for (const matchEntry of limitedMatchEntries) {
      const isInternational = competitionEntry.competition_international;
      const [lineupEntries, eventEntries, threeSixtyEntries] = await Promise.all([
        helpers.getMatchLineups(matchEntry.match_id),
        helpers.getMatchEvents(matchEntry.match_id),
        getMatchThreeSixtyEntries(helpers, matchEntry.match_id, Boolean(matchEntry.last_updated_360)),
      ]);
      const eventEntryById = new Map(eventEntries.map((eventEntry) => [eventEntry.id, eventEntry]));
      const currentMatchPlayers = new Map<string, PlayerDraft>();
      const currentMatchLineups: MatchLineupDraft[] = [];
      const currentMatchEvents: MatchEventDraft[] = [];
      const currentMatchEventBySourceId = new Map<string, MatchEventDraft>();
      const currentMatchRelations: MatchEventRelationDraft[] = [];
      const currentMatchFreezeFrames: MatchEventFreezeFrameDraft[] = [];
      const currentMatchVisibleAreas: MatchEventVisibleAreaDraft[] = [];

      for (const lineupEntry of lineupEntries) {
        const teamSlug = helpers.createTeamSlug(
          lineupEntry.team_name,
          competitionEntry.competition_international
            ? undefined
            : matchEntry.home_team.home_team_id === lineupEntry.team_id
              ? matchEntry.home_team.country?.name
              : matchEntry.away_team.country?.name
        );

        for (const player of lineupEntry.lineup) {
          const countryCode = helpers.createCountryCode(player.country?.name ?? lineupEntry.team_name);
          const playerDraft = createPlayerDraft(player, countryCode, helpers);
          players.set(playerDraft.slug, playerDraft);
          if (!persistedPlayerSlugs.has(playerDraft.slug)) {
            currentMatchPlayers.set(playerDraft.slug, playerDraft);
          }

          const contractKey = `${playerDraft.slug}:${competitionSlug}:${seasonSlug}`;
          contracts.set(contractKey, {
            playerSlug: playerDraft.slug,
            teamSlug,
            competitionSlug,
            seasonSlug,
            shirtNumber: player.jersey_number,
          });

          const lineupDraft = createLineupDraft(player, matchEntry, teamSlug, helpers);
          currentMatchLineups.push(lineupDraft);
          lineupRows += 1;

          const statsKey = `${playerDraft.slug}:${competitionSlug}:${seasonSlug}`;
          incrementStats(
            stats,
            statsKey,
            () => createEmptyPlayerSeasonStatsDraft(playerDraft.slug, competitionSlug, seasonSlug),
            (draft) => {
              draft.appearances += 1;
              draft.starts += lineupDraft.isStarter ? 1 : 0;
              draft.minutesPlayed += lineupDraft.minutesPlayed ?? 0;
            }
          );
        }
      }

      for (const eventEntry of eventEntries) {
        const eventDraft = createEventDraft(eventEntry, matchEntry, helpers, isInternational);
        if (!eventDraft) {
          continue;
        }

        currentMatchEvents.push(eventDraft);
        currentMatchEventBySourceId.set(eventDraft.sourceEventId, eventDraft);
        eventRows += 1;
        const eventFreezeFrames = createFreezeFrameDrafts(eventEntry, eventDraft, matchEntry, helpers, isInternational);
        currentMatchFreezeFrames.push(...eventFreezeFrames);
        freezeFrameRows += eventFreezeFrames.length;

        if (!eventDraft.playerSlug) {
          continue;
        }

        const statsKey = `${eventDraft.playerSlug}:${competitionSlug}:${seasonSlug}`;
        incrementStats(
          stats,
          statsKey,
          () => createEmptyPlayerSeasonStatsDraft(eventDraft.playerSlug!, competitionSlug, seasonSlug),
          (draft) => {
            if (eventDraft.eventType === 'goal') draft.goals += 1;
            if (eventDraft.eventType === 'penalty_scored') {
              draft.goals += 1;
              draft.penaltyGoals += 1;
            }
            if (eventDraft.eventType === 'own_goal') draft.ownGoals += 1;
            if (eventDraft.eventType === 'yellow_card') draft.yellowCards += 1;
            if (eventDraft.eventType === 'red_card') draft.redCards += 1;
            if (eventDraft.eventType === 'yellow_red_card') draft.yellowRedCards += 1;
          }
        );

        if ((eventDraft.eventType === 'goal' || eventDraft.eventType === 'penalty_scored') && eventEntry.shot?.key_pass_id) {
          const assistSourceEvent = eventEntryById.get(eventEntry.shot.key_pass_id);
          const assisterName = assistSourceEvent?.player?.name;

          if (assisterName) {
            const assisterSlug = helpers.createStatsBombSlug(assisterName);
            const assistStatsKey = `${assisterSlug}:${competitionSlug}:${seasonSlug}`;

            incrementStats(
              stats,
              assistStatsKey,
              () => createEmptyPlayerSeasonStatsDraft(assisterSlug, competitionSlug, seasonSlug),
              (draft) => {
                draft.assists += 1;
              }
            );
          }
        }

        const eventRelations = createEventRelationDrafts(eventEntry);
        currentMatchRelations.push(...eventRelations);
        relationRows += eventRelations.length;
      }

      const currentMatchStats = buildMatchStatsDrafts(matchEntry, eventEntries, helpers, isInternational);
      matchStatsRows += currentMatchStats.length;
      const homeTeamSlug = helpers.createTeamSlug(
        matchEntry.home_team.home_team_name,
        competitionEntry.competition_international ? undefined : matchEntry.home_team.country?.name
      );
      const awayTeamSlug = helpers.createTeamSlug(
        matchEntry.away_team.away_team_name,
        competitionEntry.competition_international ? undefined : matchEntry.away_team.country?.name
      );
      const homeGoalkeeper = currentMatchLineups
        .filter((lineup) => lineup.teamSlug === homeTeamSlug && lineup.position?.toLowerCase().includes('goalkeeper'))
        .sort((left, right) => (right.minutesPlayed ?? 0) - (left.minutesPlayed ?? 0))[0];
      const awayGoalkeeper = currentMatchLineups
        .filter((lineup) => lineup.teamSlug === awayTeamSlug && lineup.position?.toLowerCase().includes('goalkeeper'))
        .sort((left, right) => (right.minutesPlayed ?? 0) - (left.minutesPlayed ?? 0))[0];
      const homeMatchStats = currentMatchStats.find((row) => row.teamSlug === homeTeamSlug);
      const awayMatchStats = currentMatchStats.find((row) => row.teamSlug === awayTeamSlug);

      if (homeGoalkeeper && homeMatchStats) {
        incrementStats(
          stats,
          `${homeGoalkeeper.playerSlug}:${competitionSlug}:${seasonSlug}`,
          () => createEmptyPlayerSeasonStatsDraft(homeGoalkeeper.playerSlug, competitionSlug, seasonSlug),
          (draft) => {
            draft.saves += homeMatchStats.gkSaves;
            draft.goalsConceded += matchEntry.away_score ?? 0;
            draft.cleanSheets += (matchEntry.away_score ?? 0) === 0 ? 1 : 0;
          }
        );
      }

      if (awayGoalkeeper && awayMatchStats) {
        incrementStats(
          stats,
          `${awayGoalkeeper.playerSlug}:${competitionSlug}:${seasonSlug}`,
          () => createEmptyPlayerSeasonStatsDraft(awayGoalkeeper.playerSlug, competitionSlug, seasonSlug),
          (draft) => {
            draft.saves += awayMatchStats.gkSaves;
            draft.goalsConceded += matchEntry.home_score ?? 0;
            draft.cleanSheets += (matchEntry.home_score ?? 0) === 0 ? 1 : 0;
          }
        );
      }

      for (const threeSixtyEntry of threeSixtyEntries) {
        const parentEventDraft = currentMatchEventBySourceId.get(threeSixtyEntry.event_uuid);
        if (!parentEventDraft) {
          continue;
        }

        const visibleAreaDraft = createVisibleAreaDraft(threeSixtyEntry);
        if (visibleAreaDraft) {
          currentMatchVisibleAreas.push(visibleAreaDraft);
          visibleAreaRows += 1;
        }

        const threeSixtyFreezeFrames = createThreeSixtyFreezeFrameDrafts(threeSixtyEntry, parentEventDraft, matchEntry, helpers, isInternational);
        currentMatchFreezeFrames.push(...threeSixtyFreezeFrames);
        freezeFrameRows += threeSixtyFreezeFrames.length;
      }

      if (!dryRun && sql) {
        await withDetailsDbRetry(
          (detailsSql) => persistMatchDetails(detailsSql, {
            players: currentMatchPlayers,
            lineups: currentMatchLineups,
            events: currentMatchEvents,
            relations: currentMatchRelations,
            freezeFrames: currentMatchFreezeFrames,
            visibleAreas: currentMatchVisibleAreas,
            matchStats: currentMatchStats,
          }),
          `match ${matchEntry.match_id}`
        );

        for (const playerSlug of currentMatchPlayers.keys()) {
          persistedPlayerSlugs.add(playerSlug);
        }
      }
    }
    }

    const summary = {
      dryRun,
      players: players.size,
      contracts: contracts.size,
      playerSeasonStats: stats.size,
      lineupRows,
      eventRows,
      relationRows,
      freezeFrameRows,
      visibleAreaRows,
      matchStatsRows,
    } satisfies MaterializeStatsBombDetailsSummary;

    if (dryRun) {
      return summary;
    }

    await withDetailsDbRetry(async (detailsSql) => {
      for (const contract of contracts.values()) {
        await upsertPlayerContract(detailsSql, contract);
      }

      for (const playerSeasonStat of stats.values()) {
        await upsertPlayerSeasonStats(detailsSql, playerSeasonStat);
      }

      await refreshDerivedViews(detailsSql);
    }, 'final stats flush', 5);

    return summary;
  } finally {
    await releaseRunLock?.();
  }
}
