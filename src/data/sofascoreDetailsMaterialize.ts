import { createHash } from 'node:crypto';
import postgres from 'postgres';
import type { MatchAnalysisArtifactPayload } from './types.ts';
import { persistMatchEventArtifacts } from './matchEventArtifactWriter.ts';
import { isCompetitionSeasonWriteAllowed, loadCompetitionSeasonPolicies } from './sourceOwnership.ts';

const BATCH_SIZE = 500;

interface SourceRow {
  id: number;
}

interface ExistingPlayerMappingRow {
  external_id: string;
  slug: string;
}

interface TargetMatchRow {
  away_team_id: number;
  away_team_slug: string;
  competition_season_id: number;
  external_match_id: string;
  home_team_id: number;
  home_team_slug: string;
  match_date: string;
  match_id: number;
}

interface RawPayloadRow {
  external_id: string | null;
  payload: unknown;
}

interface PlayerDraft {
  countryCode: string;
  externalId: string;
  firstName: string;
  knownAs: string;
  lastName: string;
  position: 'GK' | 'DEF' | 'MID' | 'FWD' | null;
  slug: string;
}

interface MatchLineupDraft {
  isStarter: boolean;
  matchDate: string;
  matchId: number;
  minutesPlayed: number | null;
  playerSlug: string;
  position: string | null;
  rating: number | null;
  shirtNumber: number | null;
  sourceDetails: Record<string, unknown>;
  teamId: number;
}

interface PlayerContractDraft {
  competitionSeasonId: number;
  playerSlug: string;
  shirtNumber: number | null;
  teamId: number;
}

interface MatchEventDraft {
  detail: string | null;
  eventIndex: number;
  eventType: 'goal' | 'own_goal' | 'penalty_scored' | 'penalty_missed' | 'yellow_card' | 'red_card' | 'yellow_red_card' | 'substitution' | 'var_decision' | 'period' | 'injury_time';
  extraMinute: number | null;
  isNotable: boolean;
  matchDate: string;
  matchId: number;
  minute: number;
  playerSlug: string | null;
  secondaryPlayerSlug: string | null;
  sourceDetails: Record<string, unknown>;
  sourceEventId: string;
  teamId: number;
}

interface MatchStatsDraft {
  accuratePasses: number | null;
  bigChances: number | null;
  bigChancesMissed: number | null;
  blockedShots: number | null;
  cornerKicks: number | null;
  expectedGoals: number | null;
  fouls: number | null;
  freeKicks: number | null;
  gkSaves: number | null;
  isHome: boolean;
  matchDate: string;
  matchId: number;
  offsides: number | null;
  passAccuracy: number | null;
  possession: number | null;
  shotsOffTarget: number | null;
  shotsOnTarget: number | null;
  teamId: number;
  throwIns: number | null;
  totalPasses: number | null;
  totalShots: number | null;
}

interface SofascoreLineupPlayer {
  player?: {
    country?: { alpha3?: string | null };
    id?: number | string;
    name?: string;
    position?: string | null;
    shortName?: string | null;
  };
  position?: string | null;
  shirtNumber?: number | null;
  statistics?: {
    minutesPlayed?: number | null;
    rating?: number | null;
  };
  substitute?: boolean;
}

interface SofascoreLineupsPayload {
  away?: { players?: SofascoreLineupPlayer[] };
  home?: { players?: SofascoreLineupPlayer[] };
}

interface SofascoreIncidentPlayer {
  country?: { alpha3?: string | null };
  id?: number | string;
  name?: string;
  position?: string | null;
  shortName?: string | null;
}

interface SofascoreIncident {
  addedTime?: number | null;
  assist1?: SofascoreIncidentPlayer;
  description?: string | null;
  id?: number | string;
  incidentClass?: string | null;
  incidentType?: string | null;
  isHome?: boolean;
  length?: number | null;
  player?: SofascoreIncidentPlayer;
  playerIn?: SofascoreIncidentPlayer;
  playerOut?: SofascoreIncidentPlayer;
  reason?: string | null;
  text?: string | null;
  time?: number | null;
}

interface SofascoreIncidentsPayload {
  incidents?: SofascoreIncident[];
}

interface SofascoreStatisticsItem {
  awayValue?: number | null;
  homeValue?: number | null;
  key?: string;
}

interface SofascoreStatisticsGroup {
  statisticsItems?: SofascoreStatisticsItem[];
}

interface SofascoreStatisticsPeriod {
  groups?: SofascoreStatisticsGroup[];
  period?: string;
}

interface SofascoreOverviewPayload {
  event?: { id?: number | string };
  statistics?: SofascoreStatisticsPeriod[];
}

export interface MaterializeSofascoreDetailsOptions {
  competitionCodes?: string[];
  dryRun?: boolean;
  seasonLabel: string;
  sourceSlug?: string;
}

export interface MaterializeSofascoreDetailsSummary {
  contractRows: number;
  dryRun: boolean;
  eventRows: number;
  lineupRows: number;
  matchStatsRows: number;
  matchedCanonicalMatches: number;
  players: number;
  seasonLabel: string;
  sourceSlug: string;
}

const COMPETITION_SLUGS: Record<string, string> = {
  BL1: '1-bundesliga',
  FL1: 'ligue-1',
  PD: 'la-liga',
  PL: 'premier-league',
  SA: 'serie-a',
  UEL: 'europa-league',
  UCL: 'champions-league',
};

const COMPETITION_LEAGUES: Record<string, string> = {
  BL1: 'GER-Bundesliga',
  FL1: 'FRA-Ligue 1',
  PD: 'ESP-La Liga',
  PL: 'ENG-Premier League',
  SA: 'ITA-Serie A',
  UEL: 'INT-UEFA Europa League',
  UCL: 'INT-UEFA Champions League',
};

function getDetailsDb() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    idle_timeout: 20,
    max: 1,
    prepare: false,
  });
}

function slugify(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/['’.]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-')
    .toLowerCase();
}

function toUuid(value: string) {
  const hash = createHash('sha1').update(value).digest('hex').slice(0, 32);
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-${hash.slice(12, 16)}-${hash.slice(16, 20)}-${hash.slice(20, 32)}`;
}

function parseNameParts(name: string) {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  return {
    firstName: parts[0] ?? name,
    lastName: parts.slice(1).join(' '),
  };
}

function normalizePosition(position?: string | null): PlayerDraft['position'] {
  const normalized = (position ?? '').trim().toUpperCase();
  if (normalized === 'G' || normalized.includes('GK')) return 'GK';
  if (normalized === 'D' || normalized.includes('DEF')) return 'DEF';
  if (normalized === 'M' || normalized.includes('MID')) return 'MID';
  if (normalized === 'F' || normalized.includes('ATT') || normalized.includes('FW')) return 'FWD';
  return null;
}

function createPlayerSlug(externalId: string, knownAs: string) {
  return slugify(`sofascore-${knownAs}-${externalId}`).slice(0, 150);
}

function normalizeSeasonSlug(seasonLabel: string) {
  const match = seasonLabel.match(/^(\d{4})[-/](\d{2,4})$/);
  if (!match) {
    return seasonLabel;
  }

  const startYear = match[1];
  const rawEnd = match[2];
  const endYear = rawEnd.length === 2 ? rawEnd : rawEnd.slice(-2);
  return `${startYear}/${endYear}`;
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

function getIncidentEventType(incident: SofascoreIncident): MatchEventDraft['eventType'] | null {
  const type = incident.incidentType;
  const klass = normalizeIncidentClass(incident.incidentClass);
  if (type === 'goal') {
    if (klass === 'owngoal') return 'own_goal';
    if (klass === 'penalty') return 'penalty_scored';
    return 'goal';
  }
  if (type === 'card') {
    if (klass === 'red') return 'red_card';
    if (klass === 'yellowred') return 'yellow_red_card';
    return 'yellow_card';
  }
  if (type === 'substitution') return 'substitution';
  if (type === 'inGamePenalty') return klass === 'missed' ? 'penalty_missed' : 'penalty_scored';
  if (type === 'penaltyShootout') return klass === 'missed' ? 'penalty_missed' : 'penalty_scored';
  if (type === 'varDecision') return 'var_decision';
  if (type === 'period') return 'period';
  if (type === 'injuryTime') return 'injury_time';
  return null;
}

function getStatsValue(map: Map<string, SofascoreStatisticsItem>, key: string, side: 'home' | 'away') {
  const item = map.get(key);
  const raw = side === 'home' ? item?.homeValue : item?.awayValue;
  return typeof raw === 'number' && Number.isFinite(raw) ? raw : null;
}

async function ensureSource(sql: ReturnType<typeof getDetailsDb>, slug: string) {
  const rows = await sql<SourceRow[]>`
    SELECT id FROM data_sources WHERE slug = ${slug} LIMIT 1
  `;
  if (!rows[0]) {
    throw new Error(`${slug} source is not registered`);
  }
  return rows[0].id;
}

async function ensureCountry(sql: ReturnType<typeof getDetailsDb>, codeAlpha3: string, name: string) {
  await sql`
    INSERT INTO countries (code_alpha3, is_active, updated_at)
    VALUES (${codeAlpha3}, TRUE, NOW())
    ON CONFLICT (code_alpha3)
    DO UPDATE SET is_active = TRUE, updated_at = NOW()
  `;
  await sql`
    INSERT INTO country_translations (country_id, locale, name)
    VALUES ((SELECT id FROM countries WHERE code_alpha3 = ${codeAlpha3}), 'en', ${name})
    ON CONFLICT (country_id, locale)
    DO UPDATE SET name = EXCLUDED.name
  `;
}

async function upsertPlayer(sql: ReturnType<typeof getDetailsDb>, draft: PlayerDraft, sourceId: number) {
  await ensureCountry(sql, draft.countryCode, draft.countryCode === 'ZZZ' ? 'Unknown' : draft.countryCode);
  await sql`
    INSERT INTO players (slug, country_id, position, is_active, updated_at)
    VALUES (${draft.slug}, (SELECT id FROM countries WHERE code_alpha3 = ${draft.countryCode}), ${draft.position}, TRUE, NOW())
    ON CONFLICT (slug)
    DO UPDATE SET country_id = COALESCE(EXCLUDED.country_id, players.country_id), position = COALESCE(EXCLUDED.position, players.position), is_active = TRUE, updated_at = NOW()
  `;
  await sql`
    INSERT INTO player_translations (player_id, locale, first_name, last_name, known_as)
    VALUES ((SELECT id FROM players WHERE slug = ${draft.slug}), 'en', ${draft.firstName}, ${draft.lastName}, ${draft.knownAs})
    ON CONFLICT (player_id, locale)
    DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, known_as = EXCLUDED.known_as
  `;
  await sql`
    INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata, updated_at)
    VALUES ('player', (SELECT id FROM players WHERE slug = ${draft.slug}), ${sourceId}, ${draft.externalId}, ${JSON.stringify({ source: 'sofascore' })}::jsonb, NOW())
    ON CONFLICT (entity_type, source_id, external_id)
    DO UPDATE SET entity_id = EXCLUDED.entity_id, metadata = EXCLUDED.metadata, updated_at = NOW()
  `;
}

async function loadExistingPlayerMappings(sql: ReturnType<typeof getDetailsDb>, sourceId: number) {
  const rows = await sql<ExistingPlayerMappingRow[]>`
    SELECT sem.external_id, p.slug
    FROM source_entity_mapping sem
    JOIN players p ON p.id = sem.entity_id
    WHERE sem.source_id = ${sourceId}
      AND sem.entity_type = 'player'
  `;

  return new Map(rows.map((row) => [row.external_id, row.slug]));
}

async function loadTargetMatches(sql: ReturnType<typeof getDetailsDb>, competitionSlug: string, seasonLabel: string) {
  return sql<TargetMatchRow[]>`
    SELECT
      m.id AS match_id,
      m.match_date::text AS match_date,
      m.competition_season_id,
      COALESCE(m.source_metadata->>'externalMatchId', m.id::text) AS external_match_id,
      home.id AS home_team_id,
      home.slug AS home_team_slug,
      away.id AS away_team_id,
      away.slug AS away_team_slug
    FROM matches m
    JOIN competition_seasons cs ON cs.id = m.competition_season_id
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    JOIN teams home ON home.id = m.home_team_id
    JOIN teams away ON away.id = m.away_team_id
    WHERE c.slug = ${competitionSlug}
      AND s.slug = ${seasonLabel}
  `;
}

async function loadDetailRawPayloads(
  sql: ReturnType<typeof getDetailsDb>,
  sourceId: number,
  seasonLabel: string,
  endpointPrefix: string,
  endpointSuffix: string,
) {
  return sql<RawPayloadRow[]>`
    WITH target_run AS (
      SELECT MAX(sync_run_id) AS sync_run_id
      FROM raw_payloads
      WHERE source_id = ${sourceId}
        AND season_context = ${seasonLabel}
        AND endpoint LIKE ${`${endpointPrefix}%${endpointSuffix}`}
    )
    SELECT external_id, payload
    FROM raw_payloads
    WHERE source_id = ${sourceId}
      AND season_context = ${seasonLabel}
      AND endpoint LIKE ${`${endpointPrefix}%${endpointSuffix}`}
      AND sync_run_id = (SELECT sync_run_id FROM target_run)
  `;
}

async function deleteMatchDetails(sql: ReturnType<typeof getDetailsDb>, match: TargetMatchRow) {
  await sql`
    DELETE FROM match_lineups
    WHERE match_id = ${match.match_id}
      AND match_date = ${match.match_date}
      AND COALESCE(source_details->>'source', '') = 'sofascore'
  `;
}

async function deleteMatchDetailsBatch(sql: ReturnType<typeof getDetailsDb>, matches: TargetMatchRow[]) {
  if (matches.length === 0) {
    return;
  }

  for (let i = 0; i < matches.length; i += BATCH_SIZE) {
    const chunk = matches.slice(i, i + BATCH_SIZE);
    await sql`
      DELETE FROM match_lineups ml
      USING UNNEST(
        ${sql.array(chunk.map((match) => match.match_id))}::int[],
        ${sql.array(chunk.map((match) => match.match_date))}::text[]
      ) AS t(match_id, match_date)
      WHERE ml.match_id = t.match_id
        AND ml.match_date = t.match_date::date
        AND COALESCE(ml.source_details->>'source', '') = 'sofascore'
    `;
  }
}

async function loadPlayerIdBySlug(sql: ReturnType<typeof getDetailsDb>, slugs: string[]) {
  if (slugs.length === 0) {
    return new Map<string, number>();
  }

  const rows = await sql<{ id: number; slug: string }[]>`
    SELECT id, slug
    FROM players
    WHERE slug = ANY(${slugs})
  `;

  return new Map(rows.map((row) => [row.slug, row.id]));
}

async function persistSofascoreEventArtifacts(
  sql: ReturnType<typeof getDetailsDb>,
  match: TargetMatchRow,
  eventDrafts: MatchEventDraft[],
) {
  const playerSlugs = Array.from(new Set(eventDrafts.flatMap((draft) => {
    const values: string[] = [];

    if (draft.playerSlug) {
      values.push(draft.playerSlug);
    }

    if (draft.secondaryPlayerSlug) {
      values.push(draft.secondaryPlayerSlug);
    }

    return values;
  })));
  const playerIdBySlug = await loadPlayerIdBySlug(sql, playerSlugs);
  const payload: MatchAnalysisArtifactPayload = {
    version: 1,
    matchId: match.match_id,
    artifactType: 'analysis_detail',
    sourceVendor: 'sofascore',
    generatedAt: new Date().toISOString(),
    events: eventDrafts.map((draft) => ({
      sourceEventId: draft.sourceEventId,
      eventIndex: draft.eventIndex,
      minute: draft.minute,
      second: null,
      type: draft.eventType,
      teamId: draft.teamId,
      playerId: draft.playerSlug ? playerIdBySlug.get(draft.playerSlug) ?? null : null,
      secondaryPlayerId: draft.secondaryPlayerSlug ? playerIdBySlug.get(draft.secondaryPlayerSlug) ?? null : null,
      locationX: null,
      locationY: null,
      endLocationX: null,
      endLocationY: null,
      endLocationZ: null,
      underPressure: false,
      statsbombXg: null,
      detail: draft.detail,
      outcome: null,
    })),
  };

  await persistMatchEventArtifacts(sql, {
    matchId: match.match_id,
    matchDate: match.match_date,
    sourceVendor: 'sofascore',
    payload,
  });
}

async function upsertMatchLineup(sql: ReturnType<typeof getDetailsDb>, draft: MatchLineupDraft) {
  await sql`
    INSERT INTO match_lineups (
      match_id, match_date, team_id, player_id, shirt_number, position, is_starter,
      minutes_played, rating, source_details
    )
    VALUES (
      ${draft.matchId}, ${draft.matchDate}, ${draft.teamId},
      (SELECT id FROM players WHERE slug = ${draft.playerSlug}),
      ${draft.shirtNumber}, ${draft.position}, ${draft.isStarter},
      ${draft.minutesPlayed}, ${draft.rating}, ${JSON.stringify(draft.sourceDetails)}::jsonb
    )
    ON CONFLICT (match_id, match_date, team_id, player_id)
    DO UPDATE SET
      shirt_number = EXCLUDED.shirt_number,
      position = EXCLUDED.position,
      is_starter = EXCLUDED.is_starter,
      minutes_played = EXCLUDED.minutes_played,
      rating = EXCLUDED.rating,
      source_details = EXCLUDED.source_details
  `;
}

async function upsertPlayerContract(sql: ReturnType<typeof getDetailsDb>, draft: PlayerContractDraft) {
  await sql`
    INSERT INTO player_contracts (player_id, team_id, competition_season_id, shirt_number, updated_at)
    VALUES (
      (SELECT id FROM players WHERE slug = ${draft.playerSlug}),
      ${draft.teamId},
      ${draft.competitionSeasonId},
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

  await sql`
    INSERT INTO team_seasons (team_id, competition_season_id, updated_at)
    VALUES (${draft.teamId}, ${draft.competitionSeasonId}, NOW())
    ON CONFLICT (team_id, competition_season_id)
    DO UPDATE SET updated_at = NOW()
  `;
}

async function upsertMatchStat(sql: ReturnType<typeof getDetailsDb>, draft: MatchStatsDraft) {
  await sql`
    INSERT INTO match_stats (
      match_id, match_date, team_id, is_home, possession, total_passes, accurate_passes,
      pass_accuracy, total_shots, shots_on_target, shots_off_target, blocked_shots,
      corner_kicks, free_kicks, throw_ins, fouls, offsides, gk_saves, expected_goals,
      big_chances, big_chances_missed
    )
    VALUES (
      ${draft.matchId}, ${draft.matchDate}, ${draft.teamId}, ${draft.isHome}, ${draft.possession}, ${draft.totalPasses}, ${draft.accuratePasses},
      ${draft.passAccuracy}, ${draft.totalShots}, ${draft.shotsOnTarget}, ${draft.shotsOffTarget}, ${draft.blockedShots},
      ${draft.cornerKicks}, ${draft.freeKicks}, ${draft.throwIns}, ${draft.fouls}, ${draft.offsides}, ${draft.gkSaves}, ${draft.expectedGoals},
      ${draft.bigChances}, ${draft.bigChancesMissed}
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
      corner_kicks = EXCLUDED.corner_kicks,
      free_kicks = EXCLUDED.free_kicks,
      throw_ins = EXCLUDED.throw_ins,
      fouls = EXCLUDED.fouls,
      offsides = EXCLUDED.offsides,
      gk_saves = EXCLUDED.gk_saves,
      expected_goals = EXCLUDED.expected_goals,
      big_chances = EXCLUDED.big_chances,
      big_chances_missed = EXCLUDED.big_chances_missed
  `;
}

function ensurePlayerDraft(
  player: SofascoreIncidentPlayer | SofascoreLineupPlayer['player'] | undefined,
  drafts: Map<string, PlayerDraft>,
  existingMappings: Map<string, string>,
) {
  const externalId = player?.id ? String(player.id) : null;
  const knownAs = player?.name?.trim() || player?.shortName?.trim() || null;
  if (!externalId || !knownAs) {
    return null;
  }
  const mappedSlug = existingMappings.get(externalId);
  if (mappedSlug) {
    return mappedSlug;
  }
  const existing = drafts.get(externalId);
  if (existing) {
    return existing.slug;
  }
  const { firstName, lastName } = parseNameParts(knownAs);
  const draft: PlayerDraft = {
    countryCode: player?.country?.alpha3 ?? 'ZZZ',
    externalId,
    firstName,
    knownAs,
    lastName,
    position: normalizePosition(player?.position),
    slug: createPlayerSlug(externalId, knownAs),
  };
  drafts.set(externalId, draft);
  return draft.slug;
}

function buildMatchStats(match: TargetMatchRow, payload: SofascoreOverviewPayload): MatchStatsDraft[] {
  const allPeriod = payload.statistics?.find((entry) => entry.period === 'ALL') ?? payload.statistics?.[0];
  const itemMap = new Map<string, SofascoreStatisticsItem>();
  for (const group of allPeriod?.groups ?? []) {
    for (const item of group.statisticsItems ?? []) {
      if (item.key) itemMap.set(item.key, item);
    }
  }
  const build = (isHome: boolean): MatchStatsDraft => {
    const side = isHome ? 'home' : 'away';
    const totalPasses = getStatsValue(itemMap, 'passes', side);
    const accuratePasses = getStatsValue(itemMap, 'accuratePasses', side);
    return {
      accuratePasses,
      bigChances: getStatsValue(itemMap, 'bigChance', side),
      bigChancesMissed: getStatsValue(itemMap, 'bigChanceMissed', side),
      blockedShots: getStatsValue(itemMap, 'blockedScoringAttempt', side),
      cornerKicks: getStatsValue(itemMap, 'cornerKicks', side),
      expectedGoals: getStatsValue(itemMap, 'expectedGoals', side),
      fouls: getStatsValue(itemMap, 'fouls', side),
      freeKicks: getStatsValue(itemMap, 'freeKicks', side),
      gkSaves: getStatsValue(itemMap, 'goalkeeperSaves', side),
      isHome,
      matchDate: match.match_date,
      matchId: match.match_id,
      offsides: getStatsValue(itemMap, 'offsides', side),
      passAccuracy: totalPasses && accuratePasses !== null ? Math.round((accuratePasses / totalPasses) * 100) : null,
      possession: getStatsValue(itemMap, 'ballPossession', side),
      shotsOffTarget: getStatsValue(itemMap, 'shotsOffGoal', side),
      shotsOnTarget: getStatsValue(itemMap, 'shotsOnGoal', side),
      teamId: isHome ? match.home_team_id : match.away_team_id,
      throwIns: getStatsValue(itemMap, 'throwIns', side),
      totalPasses,
      totalShots: getStatsValue(itemMap, 'totalShotsOnGoal', side),
    };
  };
  return [build(true), build(false)];
}

function buildLineups(
  match: TargetMatchRow,
  payload: SofascoreLineupsPayload,
  playerDrafts: Map<string, PlayerDraft>,
  existingMappings: Map<string, string>,
) {
  const lineups: MatchLineupDraft[] = [];
  const pushSide = (players: SofascoreLineupPlayer[] | undefined, teamId: number) => {
    for (const entry of players ?? []) {
      const playerSlug = ensurePlayerDraft(entry.player, playerDrafts, existingMappings);
      if (!playerSlug) continue;
      lineups.push({
        isStarter: !entry.substitute,
        matchDate: match.match_date,
        matchId: match.match_id,
        minutesPlayed: entry.statistics?.minutesPlayed ?? null,
        playerSlug,
        position: entry.position ?? entry.player?.position ?? null,
        rating: entry.statistics?.rating ?? null,
        shirtNumber: entry.shirtNumber ?? (entry.player?.id ? null : null),
        sourceDetails: { source: 'sofascore' },
        teamId,
      });
    }
  };
  pushSide(payload.home?.players, match.home_team_id);
  pushSide(payload.away?.players, match.away_team_id);
  return lineups;
}

function buildEvents(
  match: TargetMatchRow,
  payload: SofascoreIncidentsPayload,
  playerDrafts: Map<string, PlayerDraft>,
  existingMappings: Map<string, string>,
) {
  const events: MatchEventDraft[] = [];
  let eventIndex = 0;
  for (const incident of payload.incidents ?? []) {
    const eventType = getIncidentEventType(incident);
    if (!eventType) continue;
    const teamId = incident.isHome ? match.home_team_id : match.away_team_id;
    const playerSlug = ensurePlayerDraft(incident.player ?? incident.playerIn, playerDrafts, existingMappings);
    const secondaryPlayerSlug = ensurePlayerDraft(
      eventType === 'substitution' ? incident.playerOut : (incident.assist1 ?? undefined),
      playerDrafts,
      existingMappings,
    );
    const sourceEventId = toUuid(`${match.match_id}:${incident.id ?? eventIndex}:${eventType}`);
    events.push({
      detail: buildIncidentDetail(incident),
      eventIndex,
      eventType,
      extraMinute: incident.addedTime ?? null,
      isNotable: true,
      matchDate: match.match_date,
      matchId: match.match_id,
      minute: incident.time ?? 0,
      playerSlug,
      secondaryPlayerSlug,
      sourceDetails: { source: 'sofascore', incidentId: incident.id ?? null, incidentType: incident.incidentType ?? null },
      sourceEventId,
      teamId,
    });
    eventIndex += 1;
  }
  return events;
}

function groupEventDraftsByMatch(eventDrafts: MatchEventDraft[]) {
  const draftsByMatch = new Map<string, MatchEventDraft[]>();

  for (const draft of eventDrafts) {
    const key = `${draft.matchId}:${draft.matchDate}`;
    const current = draftsByMatch.get(key) ?? [];
    current.push(draft);
    draftsByMatch.set(key, current);
  }

  return draftsByMatch;
}

function dedupeLineupDrafts(drafts: MatchLineupDraft[]) {
  return Array.from(new Map(drafts.map((draft) => [`${draft.matchId}:${draft.matchDate}:${draft.teamId}:${draft.playerSlug}`, draft])).values());
}

function dedupeMatchStatsDrafts(drafts: MatchStatsDraft[]) {
  return Array.from(new Map(drafts.map((draft) => [`${draft.matchId}:${draft.matchDate}:${draft.teamId}`, draft])).values());
}

export async function materializeSofascoreDetails(options: MaterializeSofascoreDetailsOptions): Promise<MaterializeSofascoreDetailsSummary> {
  const sql = getDetailsDb();
  const sourceSlug = options.sourceSlug?.trim() || 'soccerdata_sofascore';
  const competitionCode = options.competitionCodes?.[0]?.toUpperCase() || 'UEL';
  const competitionSlug = COMPETITION_SLUGS[competitionCode];
  const competitionLeague = COMPETITION_LEAGUES[competitionCode];
  if (!competitionSlug || !competitionLeague) {
    throw new Error(`Unsupported Sofascore detail competition code: ${competitionCode}`);
  }

  const sourceId = await ensureSource(sql, sourceSlug);
  const endpointPrefix = `sofascore://${competitionLeague}/${options.seasonLabel}/`;
  const existingPlayerMappings = await loadExistingPlayerMappings(sql, sourceId);
  const seasonSlug = normalizeSeasonSlug(options.seasonLabel);
  const matches = await loadTargetMatches(sql, competitionSlug, seasonSlug);
  const overviews = new Map((await loadDetailRawPayloads(sql, sourceId, options.seasonLabel, endpointPrefix, 'match_overview')).map((row) => [row.external_id ?? '', row.payload as SofascoreOverviewPayload]));
  const lineups = new Map((await loadDetailRawPayloads(sql, sourceId, options.seasonLabel, endpointPrefix, 'match_lineups')).map((row) => [row.external_id ?? '', row.payload as SofascoreLineupsPayload]));
  const incidents = new Map((await loadDetailRawPayloads(sql, sourceId, options.seasonLabel, endpointPrefix, 'match_events')).map((row) => [row.external_id ?? '', row.payload as SofascoreIncidentsPayload]));

  const playerDrafts = new Map<string, PlayerDraft>();
  const contractDrafts = new Map<string, PlayerContractDraft>();
  const lineupDrafts: MatchLineupDraft[] = [];
  const eventDrafts: MatchEventDraft[] = [];
  const statsDrafts: MatchStatsDraft[] = [];
  const matched = matches.filter((match) => overviews.has(match.external_match_id));
  const policies = await loadCompetitionSeasonPolicies(sql, Array.from(new Set(matched.map((match) => match.competition_season_id))));

  for (const match of matched) {
    const overview = overviews.get(match.external_match_id);
    const lineupPayload = lineups.get(match.external_match_id);
    const incidentsPayload = incidents.get(match.external_match_id);
    if (overview) {
      if (isCompetitionSeasonWriteAllowed(policies.get(match.competition_season_id), 'matchStats', 'sofascore', 'sync')) {
        statsDrafts.push(...buildMatchStats(match, overview));
      }
    }
    if (lineupPayload) {
      const currentLineups = buildLineups(match, lineupPayload, playerDrafts, existingPlayerMappings);
      lineupDrafts.push(...currentLineups);
      if (isCompetitionSeasonWriteAllowed(policies.get(match.competition_season_id), 'playerContracts', 'sofascore', 'sync')) {
        for (const draft of currentLineups) {
          contractDrafts.set(`${draft.playerSlug}:${match.competition_season_id}`, {
            competitionSeasonId: match.competition_season_id,
            playerSlug: draft.playerSlug,
            shirtNumber: draft.shirtNumber,
            teamId: draft.teamId,
          });
        }
      }
    }
    if (incidentsPayload) {
      if (isCompetitionSeasonWriteAllowed(policies.get(match.competition_season_id), 'matchArtifacts', 'sofascore', 'sync')) {
        eventDrafts.push(...buildEvents(match, incidentsPayload, playerDrafts, existingPlayerMappings));
      }
    }
  }

  const uniqueLineupDrafts = dedupeLineupDrafts(lineupDrafts);
  const uniqueStatsDrafts = dedupeMatchStatsDrafts(statsDrafts);

  const summary: MaterializeSofascoreDetailsSummary = {
    contractRows: contractDrafts.size,
    dryRun: options.dryRun ?? true,
    eventRows: eventDrafts.length,
    lineupRows: uniqueLineupDrafts.length,
    matchStatsRows: uniqueStatsDrafts.length,
    matchedCanonicalMatches: matched.length,
    players: playerDrafts.size,
    seasonLabel: options.seasonLabel,
    sourceSlug,
  };

  if (summary.dryRun) {
    return summary;
  }

  const playerDraftList = Array.from(playerDrafts.values());
  const uniqueCountryCodes = Array.from(new Set(playerDraftList.map((d) => d.countryCode)));

  await sql`BEGIN`;
  try {
    for (let i = 0; i < uniqueCountryCodes.length; i += BATCH_SIZE) {
      const chunk = uniqueCountryCodes.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO countries (code_alpha3, is_active, updated_at)
        SELECT t.code_alpha3, TRUE, NOW()
        FROM UNNEST(${sql.array(chunk)}::text[]) AS t(code_alpha3)
        ON CONFLICT (code_alpha3)
        DO UPDATE SET is_active = TRUE, updated_at = NOW()
      `;
      await sql`
        INSERT INTO country_translations (country_id, locale, name)
        SELECT c.id, 'en', t.code_alpha3
        FROM UNNEST(${sql.array(chunk)}::text[]) AS t(code_alpha3)
        JOIN countries c ON c.code_alpha3 = t.code_alpha3
        ON CONFLICT (country_id, locale)
        DO UPDATE SET name = EXCLUDED.name
      `;
    }
    for (let i = 0; i < playerDraftList.length; i += BATCH_SIZE) {
      const chunk = playerDraftList.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO players (slug, country_id, position, is_active, updated_at)
        SELECT t.slug, c.id, t.position::position_type, TRUE, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.slug))}::text[],
          ${sql.array(chunk.map((r) => r.countryCode))}::text[],
          ${sql.array(chunk.map((r) => r.position))}::text[]
        ) AS t(slug, country_code, position)
        LEFT JOIN countries c ON c.code_alpha3 = t.country_code
        ON CONFLICT (slug)
        DO UPDATE SET
          country_id = COALESCE(EXCLUDED.country_id, players.country_id),
          position = COALESCE(EXCLUDED.position, players.position),
          is_active = TRUE,
          updated_at = NOW()
      `;
      await sql`
        INSERT INTO player_translations (player_id, locale, first_name, last_name, known_as)
        SELECT p.id, 'en', t.first_name, t.last_name, t.known_as
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.slug))}::text[],
          ${sql.array(chunk.map((r) => r.firstName))}::text[],
          ${sql.array(chunk.map((r) => r.lastName))}::text[],
          ${sql.array(chunk.map((r) => r.knownAs))}::text[]
        ) AS t(slug, first_name, last_name, known_as)
        JOIN players p ON p.slug = t.slug
        ON CONFLICT (player_id, locale)
        DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, known_as = EXCLUDED.known_as
      `;
      await sql`
        INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata, updated_at)
        SELECT 'player', p.id, ${sourceId}, t.external_id, '{"source":"sofascore"}'::jsonb, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.slug))}::text[],
          ${sql.array(chunk.map((r) => r.externalId))}::text[]
        ) AS t(slug, external_id)
        JOIN players p ON p.slug = t.slug
        ON CONFLICT (entity_type, source_id, external_id)
        DO UPDATE SET entity_id = EXCLUDED.entity_id, metadata = EXCLUDED.metadata, updated_at = NOW()
      `;
    }
    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }

  const contractDraftList = Array.from(contractDrafts.values());

  await sql`BEGIN`;
  try {
    for (let i = 0; i < contractDraftList.length; i += BATCH_SIZE) {
      const chunk = contractDraftList.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO player_contracts (player_id, team_id, competition_season_id, shirt_number, updated_at)
        SELECT p.id, t.team_id, t.competition_season_id, t.shirt_number, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.playerSlug))}::text[],
          ${sql.array(chunk.map((r) => r.teamId))}::int[],
          ${sql.array(chunk.map((r) => r.competitionSeasonId))}::int[],
          ${sql.array(chunk.map((r) => r.shirtNumber))}::int[]
        ) AS t(player_slug, team_id, competition_season_id, shirt_number)
        JOIN players p ON p.slug = t.player_slug
        ON CONFLICT (player_id, competition_season_id)
        DO UPDATE SET
          team_id = EXCLUDED.team_id,
          shirt_number = EXCLUDED.shirt_number,
          left_date = NULL,
          updated_at = NOW()
      `;
      await sql`
        INSERT INTO team_seasons (team_id, competition_season_id, updated_at)
        SELECT DISTINCT t.team_id, t.competition_season_id, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.teamId))}::int[],
          ${sql.array(chunk.map((r) => r.competitionSeasonId))}::int[]
        ) AS t(team_id, competition_season_id)
        ON CONFLICT (team_id, competition_season_id)
        DO UPDATE SET updated_at = NOW()
      `;
    }
    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }

  await deleteMatchDetailsBatch(sql, matched);

  await sql`BEGIN`;
  try {
    for (let i = 0; i < uniqueLineupDrafts.length; i += BATCH_SIZE) {
      const chunk = uniqueLineupDrafts.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO match_lineups (
          match_id, match_date, team_id, player_id, shirt_number, position, is_starter,
          minutes_played, rating, source_details
        )
        SELECT
          t.match_id, t.match_date::date, t.team_id, p.id,
          t.shirt_number, t.position, t.is_starter,
          t.minutes_played, t.rating, t.source_details::jsonb
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.matchId))}::int[],
          ${sql.array(chunk.map((r) => r.matchDate))}::text[],
          ${sql.array(chunk.map((r) => r.teamId))}::int[],
          ${sql.array(chunk.map((r) => r.playerSlug))}::text[],
          ${sql.array(chunk.map((r) => r.shirtNumber))}::int[],
          ${sql.array(chunk.map((r) => r.position))}::text[],
          ${sql.array(chunk.map((r) => r.isStarter))}::bool[],
          ${sql.array(chunk.map((r) => r.minutesPlayed))}::int[],
          ${sql.array(chunk.map((r) => r.rating))}::numeric[],
          ${sql.array(chunk.map((r) => JSON.stringify(r.sourceDetails)))}::text[]
        ) AS t(match_id, match_date, team_id, player_slug, shirt_number, position, is_starter, minutes_played, rating, source_details)
        JOIN players p ON p.slug = t.player_slug
        ON CONFLICT (match_id, match_date, team_id, player_id)
        DO UPDATE SET
          shirt_number = EXCLUDED.shirt_number,
          position = EXCLUDED.position,
          is_starter = EXCLUDED.is_starter,
          minutes_played = EXCLUDED.minutes_played,
          rating = EXCLUDED.rating,
          source_details = EXCLUDED.source_details
      `;
    }
    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }

  const eventDraftsByMatch = groupEventDraftsByMatch(eventDrafts);

  for (const match of matched) {
    await persistSofascoreEventArtifacts(
      sql,
      match,
      eventDraftsByMatch.get(`${match.match_id}:${match.match_date}`) ?? [],
    );
  }

  await sql`BEGIN`;
  try {
    for (let i = 0; i < uniqueStatsDrafts.length; i += BATCH_SIZE) {
      const chunk = uniqueStatsDrafts.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO match_stats (
          match_id, match_date, team_id, is_home, possession, total_passes, accurate_passes,
          pass_accuracy, total_shots, shots_on_target, shots_off_target, blocked_shots,
          corner_kicks, free_kicks, throw_ins, fouls, offsides, gk_saves, expected_goals,
          big_chances, big_chances_missed
        )
        SELECT
          t.match_id, t.match_date::date, t.team_id, t.is_home, t.possession, t.total_passes, t.accurate_passes,
          t.pass_accuracy, t.total_shots, t.shots_on_target, t.shots_off_target, t.blocked_shots,
          t.corner_kicks, t.free_kicks, t.throw_ins, t.fouls, t.offsides, t.gk_saves, t.expected_goals,
          t.big_chances, t.big_chances_missed
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.matchId))}::int[],
          ${sql.array(chunk.map((r) => r.matchDate))}::text[],
          ${sql.array(chunk.map((r) => r.teamId))}::int[],
          ${sql.array(chunk.map((r) => r.isHome))}::bool[],
          ${sql.array(chunk.map((r) => r.possession))}::numeric[],
          ${sql.array(chunk.map((r) => r.totalPasses))}::int[],
          ${sql.array(chunk.map((r) => r.accuratePasses))}::int[],
          ${sql.array(chunk.map((r) => r.passAccuracy))}::int[],
          ${sql.array(chunk.map((r) => r.totalShots))}::int[],
          ${sql.array(chunk.map((r) => r.shotsOnTarget))}::int[],
          ${sql.array(chunk.map((r) => r.shotsOffTarget))}::int[],
          ${sql.array(chunk.map((r) => r.blockedShots))}::int[],
          ${sql.array(chunk.map((r) => r.cornerKicks))}::int[],
          ${sql.array(chunk.map((r) => r.freeKicks))}::int[],
          ${sql.array(chunk.map((r) => r.throwIns))}::int[],
          ${sql.array(chunk.map((r) => r.fouls))}::int[],
          ${sql.array(chunk.map((r) => r.offsides))}::int[],
          ${sql.array(chunk.map((r) => r.gkSaves))}::int[],
          ${sql.array(chunk.map((r) => r.expectedGoals))}::numeric[],
          ${sql.array(chunk.map((r) => r.bigChances))}::int[],
          ${sql.array(chunk.map((r) => r.bigChancesMissed))}::int[]
        ) AS t(match_id, match_date, team_id, is_home, possession, total_passes, accurate_passes,
               pass_accuracy, total_shots, shots_on_target, shots_off_target, blocked_shots,
               corner_kicks, free_kicks, throw_ins, fouls, offsides, gk_saves, expected_goals,
               big_chances, big_chances_missed)
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
          corner_kicks = EXCLUDED.corner_kicks,
          free_kicks = EXCLUDED.free_kicks,
          throw_ins = EXCLUDED.throw_ins,
          fouls = EXCLUDED.fouls,
          offsides = EXCLUDED.offsides,
          gk_saves = EXCLUDED.gk_saves,
          expected_goals = EXCLUDED.expected_goals,
          big_chances = EXCLUDED.big_chances,
          big_chances_missed = EXCLUDED.big_chances_missed
      `;
    }
    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }

  return summary;
}
