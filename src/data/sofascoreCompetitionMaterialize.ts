import postgres from 'postgres';

import { deriveCompetitionSeasonFormat, type CompetitionSeasonFormatType } from './competitionFormats.ts';
import { createTeamLookupKeys } from './teamLookupKeys.ts';

const BATCH_SIZE = 500;

type DbSql = ReturnType<typeof getMaterializeDb>;

interface SourceRow {
  id: number;
}

interface RawPayloadRow {
  endpoint: string;
  external_id: string | null;
  payload: unknown;
}

interface ExistingTeamLookupRow {
  id: number;
  slug: string;
  name: string | null;
  code_alpha3: string | null;
}

interface TeamLookupEntry {
  codeAlpha3: string | null;
  id: number;
  slug: string;
}

interface ExistingMatchLookupRow {
  id: number;
  match_date: string;
  stage: string | null;
  group_name: string | null;
  home_slug: string;
  away_slug: string;
}

interface ExistingEntityMappingRow {
  entity_id: number;
  external_id: string;
}

interface SofascoreCompetitionConfig {
  code: string;
  compType: 'international' | 'league';
  competitionSlug: string;
  countryCode: string | null;
  isInternational: boolean;
  league: string;
  name: string;
  shortName: string;
  seasonSlugFormat: 'single-year' | 'span';
}

interface LeagueInfoPayload {
  league: string;
  league_id: number;
  region?: string | null;
  slug?: string | null;
}

interface SeasonInfoPayload {
  league: string;
  league_id: number;
  season: string;
  season_id: number;
  year?: string | null;
}

interface LeagueTablePayload {
  GF: number | null;
  GA: number | null;
  GD: number | null;
  MP: number | null;
  Pts: number | null;
  D: number | null;
  L: number | null;
  W: number | null;
  groupName?: string | null;
  league: string;
  season: string;
  team: string;
}

interface SchedulePayload {
  away_score: number | null;
  away_team: string;
  date: string;
  game_id: number;
  home_score: number | null;
  home_team: string;
  league: string;
  round: number | null;
  roundName?: string | null;
  season: string;
  slug?: string | null;
  week?: number | null;
}

interface MatchOverviewPayload {
  event?: {
    awayScore?: { current?: number | null; display?: number | null; normaltime?: number | null };
    awayTeam?: { id?: number | string | null; name?: string | null };
    homeScore?: { current?: number | null; display?: number | null; normaltime?: number | null };
    homeTeam?: { id?: number | string | null; name?: string | null };
    startTimestamp?: number | null;
    status?: { code?: number | null; description?: string | null; type?: string | null };
    tournament?: { name?: string | null };
    venue?: { venueCoordinates?: unknown; city?: { name?: string | null }; country?: { alpha3?: string | null; name?: string | null }; id?: number | null; name?: string | null };
  };
  statistics?: unknown;
}

interface TeamDraft {
  countryCode: string;
  externalId?: string | null;
  name: string;
  shortName: string;
  slug: string;
}

interface MatchDraft {
  awayScore: number | null;
  awayTeamSlug: string;
  externalMatchId: number;
  groupName: string | null;
  homeScore: number | null;
  homeTeamSlug: string;
  kickoffAt: string | null;
  matchDate: string;
  matchId: number;
  matchWeek: number | null;
  seasonSlug: string;
  stage: string;
  status: string;
  sourceMetadata: Record<string, unknown>;
}

export interface MaterializeSofascoreCompetitionOptions {
  competitionCodes?: string[];
  dryRun?: boolean;
  seasonLabel: string;
  sourceSlug?: string;
}

export interface MaterializeSofascoreCompetitionSummary {
  competitionSeasons: number;
  competitions: number;
  dryRun: boolean;
  matches: number;
  rawRows?: number;
  scheduleRows?: number;
  seasonLabel: string;
  seasons: number;
  sourceSlug: string;
  teamSeasons: number;
  teams: number;
}

const COMPETITION_CONFIGS: Record<string, SofascoreCompetitionConfig> = {
  BL1: {
    code: 'bl1',
    compType: 'league',
    competitionSlug: '1-bundesliga',
    countryCode: 'DEU',
    isInternational: false,
    league: 'GER-Bundesliga',
    name: '1. Bundesliga',
    seasonSlugFormat: 'span',
    shortName: 'Bundesliga',
  },
  FL1: {
    code: 'fl1',
    compType: 'league',
    competitionSlug: 'ligue-1',
    countryCode: 'FRA',
    isInternational: false,
    league: 'FRA-Ligue 1',
    name: 'Ligue 1',
    seasonSlugFormat: 'span',
    shortName: 'Ligue 1',
  },
  PD: {
    code: 'pd',
    compType: 'league',
    competitionSlug: 'la-liga',
    countryCode: 'ESP',
    isInternational: false,
    league: 'ESP-La Liga',
    name: 'La Liga',
    seasonSlugFormat: 'span',
    shortName: 'La Liga',
  },
  PL: {
    code: 'pl',
    compType: 'league',
    competitionSlug: 'premier-league',
    countryCode: 'ENG',
    isInternational: false,
    league: 'ENG-Premier League',
    name: 'Premier League',
    seasonSlugFormat: 'span',
    shortName: 'Premier League',
  },
  SA: {
    code: 'sa',
    compType: 'league',
    competitionSlug: 'serie-a',
    countryCode: 'ITA',
    isInternational: false,
    league: 'ITA-Serie A',
    name: 'Serie A',
    seasonSlugFormat: 'span',
    shortName: 'Serie A',
  },
  UEL: {
    code: 'el',
    compType: 'international',
    competitionSlug: 'europa-league',
    countryCode: 'EUR',
    isInternational: true,
    league: 'INT-UEFA Europa League',
    name: 'Europa League',
    seasonSlugFormat: 'span',
    shortName: 'Europa League',
  },
  UCL: {
    code: 'cl',
    compType: 'international',
    competitionSlug: 'champions-league',
    countryCode: 'EUR',
    isInternational: true,
    league: 'INT-UEFA Champions League',
    name: 'Champions League',
    seasonSlugFormat: 'span',
    shortName: 'Champions League',
  },
};

function getMaterializeDb() {
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
    .replace(/&/g, ' and ')
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-')
    .toLowerCase();
}

function createShortName(name: string, maxLength: number = 24) {
  return name.length <= maxLength ? name : `${name.slice(0, maxLength - 1).trimEnd()}.`;
}

function normalizeTeamName(value: string | null | undefined) {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : null;
}

function createTeamSlug(name: string, countryCode?: string | null) {
  return slugify(countryCode ? `${name} ${countryCode}` : name);
}

function createSeasonBounds(seasonLabel: string, format: SofascoreCompetitionConfig['seasonSlugFormat']) {
  if (format === 'span') {
    const match = seasonLabel.match(/^(\d{4})[-/](\d{2,4})$/);
    if (!match) {
      throw new Error(`Invalid span season label: ${seasonLabel}`);
    }

    const startYear = Number.parseInt(match[1], 10);
    const rawEnd = match[2];
    const endYear = rawEnd.length === 2 ? Number.parseInt(`${String(startYear).slice(0, 2)}${rawEnd}`, 10) : Number.parseInt(rawEnd, 10);
    return {
      endDate: `${endYear}-06-30`,
      slug: `${startYear}/${String(endYear).slice(-2)}`,
      startDate: `${startYear}-07-01`,
    };
  }

  const year = Number.parseInt(seasonLabel, 10);
  return {
    endDate: `${year}-12-31`,
    slug: String(year),
    startDate: `${year}-01-01`,
  };
}

function toJsonValue(value: unknown) {
  return JSON.parse(JSON.stringify(value));
}

function normalizeMatchStatus(statusCode?: number | null) {
  switch (statusCode) {
    case 100:
      return 'finished';
    case 0:
    default:
      return 'scheduled';
  }
}

function resolveMatchStatus(
  overviewStatusCode: number | null | undefined,
  homeScore: number | null,
  awayScore: number | null,
) {
  if (homeScore !== null || awayScore !== null) {
    return 'finished';
  }

  return normalizeMatchStatus(overviewStatusCode);
}

function resolveEventScore(score?: { current?: number | null; display?: number | null; normaltime?: number | null } | null) {
  return score?.display ?? score?.normaltime ?? score?.current ?? null;
}

function getMatchDraftKey(draft: MatchDraft) {
  return `${draft.matchId}::${draft.matchDate}`;
}

function isBetterMatchDraft(next: MatchDraft, current: MatchDraft) {
  const nextHasScores = next.homeScore !== null || next.awayScore !== null;
  const currentHasScores = current.homeScore !== null || current.awayScore !== null;

  if (nextHasScores !== currentHasScores) {
    return nextHasScores;
  }

  if ((next.status === 'finished') !== (current.status === 'finished')) {
    return next.status === 'finished';
  }

  if ((next.kickoffAt !== null) !== (current.kickoffAt !== null)) {
    return next.kickoffAt !== null;
  }

  return next.externalMatchId > current.externalMatchId;
}

function isEuropeanLeaguePhaseSeason(config: SofascoreCompetitionConfig, seasonSlug: string) {
  if (config.compType !== 'international') {
    return false;
  }

  if (config.competitionSlug !== 'europa-league' && config.competitionSlug !== 'champions-league') {
    return false;
  }

  const match = seasonSlug.match(/^(\d{4})\//);
  const startYear = match ? Number.parseInt(match[1], 10) : Number.NaN;
  return Number.isFinite(startYear) && startYear >= 2024;
}

function normalizeStage(config: SofascoreCompetitionConfig, seasonSlug: string, roundName?: string | null, round?: number | null) {
  const normalized = (roundName ?? '').trim().toLowerCase();
  const isLeaguePhaseCompetition = isEuropeanLeaguePhaseSeason(config, seasonSlug);
  if (!normalized && round && round >= 1 && round <= 8) {
    if (isLeaguePhaseCompetition) {
      return { groupName: null, stage: 'LEAGUE_PHASE' };
    }

    if (config.compType === 'international' && round >= 1 && round <= 6) {
      return { groupName: null, stage: 'GROUP_STAGE' };
    }

    return { groupName: null, stage: 'REGULAR_SEASON' };
  }
  if (config.compType === 'league' && normalized === 'league phase') {
    return { groupName: null, stage: 'REGULAR_SEASON' };
  }
  if (normalized === 'league phase' || normalized === 'league stage') {
    if (!isLeaguePhaseCompetition && config.compType === 'international') {
      return { groupName: null, stage: 'GROUP_STAGE' };
    }

    return { groupName: null, stage: 'LEAGUE_PHASE' };
  }
  if (normalized.includes('round of 16')) {
    return { groupName: null, stage: 'ROUND_OF_16' };
  }
  if (normalized.includes('quarterfinal')) {
    return { groupName: null, stage: 'QUARTER_FINALS' };
  }
  if (normalized.includes('semifinal')) {
    return { groupName: null, stage: 'SEMI_FINALS' };
  }
  if (normalized === 'final') {
    return { groupName: null, stage: 'FINAL' };
  }
  if (normalized.includes('playoff') && normalized.includes('qualification')) {
    return { groupName: null, stage: 'QUALIFICATION' };
  }
  if (normalized.includes('qualification')) {
    return { groupName: null, stage: 'QUALIFICATION' };
  }
  if (normalized.includes('playoff')) {
    return { groupName: null, stage: 'PLAYOFFS' };
  }
  return {
    groupName: null,
    stage: normalized ? normalized.toUpperCase().replace(/[^A-Z0-9]+/g, '_') : (isLeaguePhaseCompetition ? 'LEAGUE_PHASE' : 'REGULAR_SEASON'),
  };
}

async function ensureSource(sql: DbSql, slug: string) {
  const rows = await sql<SourceRow[]>`
    SELECT id
    FROM data_sources
    WHERE slug = ${slug}
    LIMIT 1
  `;

  if (!rows[0]) {
    throw new Error(`${slug} source is not registered`);
  }

  return rows[0].id;
}

async function loadRawRows(sql: DbSql, sourceId: number, endpointPrefix: string, seasonLabel: string) {
  return sql<RawPayloadRow[]>`
    WITH target_run AS (
      SELECT MAX(sync_run_id) AS sync_run_id
      FROM raw_payloads
      WHERE source_id = ${sourceId}
        AND season_context = ${seasonLabel}
        AND endpoint LIKE ${`${endpointPrefix}%`}
    )
    SELECT endpoint, external_id, payload
    FROM raw_payloads
    WHERE source_id = ${sourceId}
      AND season_context = ${seasonLabel}
      AND endpoint LIKE ${`${endpointPrefix}%`}
      AND sync_run_id = (SELECT sync_run_id FROM target_run)
    ORDER BY id ASC
  `;
}

async function loadExistingTeamLookup(sql: DbSql) {
  const rows = await sql<ExistingTeamLookupRow[]>`
    SELECT DISTINCT slug, name, code_alpha3
    FROM (
      SELECT t.id, t.slug, tt.name, c.code_alpha3
      FROM teams t
      LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
      LEFT JOIN countries c ON c.id = t.country_id
      UNION ALL
      SELECT t.id, t.slug, ea.alias AS name, c.code_alpha3
      FROM teams t
      JOIN entity_aliases ea ON ea.entity_type = 'team' AND ea.entity_id = t.id
      LEFT JOIN countries c ON c.id = t.country_id
    ) lookup
  `;

  const lookup = new Map<string, TeamLookupEntry[]>();
  for (const row of rows) {
    for (const key of createTeamLookupKeys(row.name ?? row.slug)) {
      const entries = lookup.get(key) ?? [];
      if (!entries.some((entry) => entry.slug === row.slug)) {
        entries.push({ codeAlpha3: row.code_alpha3, id: row.id, slug: row.slug });
      }
      lookup.set(key, entries);
    }
  }
  return lookup;
}

async function loadExistingSourceEntityMapping(sql: DbSql, sourceId: number, entityType: 'team' | 'match') {
  const rows = await sql<ExistingEntityMappingRow[]>`
    SELECT entity_id, external_id
    FROM source_entity_mapping
    WHERE source_id = ${sourceId}
      AND entity_type = ${entityType}
  `;

  return new Map(rows.map((row) => [row.external_id, row.entity_id]));
}

function resolveCanonicalTeamSlug(lookup: Map<string, TeamLookupEntry[]>, teamName: string) {
  const candidates = createTeamLookupKeys(teamName).flatMap((key) => lookup.get(key) ?? []);
  const unique = [...new Map(candidates.map((entry) => [entry.slug, entry])).values()];
  return unique.length === 1 ? unique[0] : null;
}

function buildMatchLookupKey(
  matchDate: string,
  stage: string,
  groupName: string | null,
  homeTeamSlug: string,
  awayTeamSlug: string,
) {
  return `${matchDate}::${stage}::${groupName ?? ''}::${homeTeamSlug}::${awayTeamSlug}`;
}

async function loadExistingMatchLookup(sql: DbSql, competitionSlug: string, seasonSlug: string) {
  const rows = await sql<ExistingMatchLookupRow[]>`
    SELECT DISTINCT
      m.id,
      m.match_date::text AS match_date,
      m.stage,
      m.group_name,
      home.slug AS home_slug,
      away.slug AS away_slug
    FROM matches m
    JOIN competition_seasons cs ON cs.id = m.competition_season_id
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    JOIN teams home ON home.id = m.home_team_id
    JOIN teams away ON away.id = m.away_team_id
    WHERE c.slug = ${competitionSlug}
      AND s.slug = ${seasonSlug}
  `;

  return new Map(rows.map((row) => [buildMatchLookupKey(row.match_date, row.stage ?? 'REGULAR_SEASON', row.group_name, row.home_slug, row.away_slug), row.id]));
}

async function upsertCountry(sql: DbSql, codeAlpha3: string, name: string) {
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

async function upsertSeason(sql: DbSql, slug: string, startDate: string, endDate: string, isCurrent: boolean) {
  await sql`
    INSERT INTO seasons (slug, start_date, end_date, is_current)
    VALUES (${slug}, ${startDate}, ${endDate}, ${isCurrent})
    ON CONFLICT (slug)
    DO UPDATE SET
      start_date = EXCLUDED.start_date,
      end_date = EXCLUDED.end_date,
      is_current = EXCLUDED.is_current
  `;
}

async function upsertCompetition(sql: DbSql, config: SofascoreCompetitionConfig) {
  await sql`
    INSERT INTO competitions (slug, code, comp_type, gender, is_youth, is_international, country_id, is_active, updated_at)
    VALUES (${config.competitionSlug}, ${config.code}, ${config.compType}, 'male', FALSE, ${config.isInternational}, ${config.countryCode ? sql`(SELECT id FROM countries WHERE code_alpha3 = ${config.countryCode})` : sql`NULL`}, TRUE, NOW())
    ON CONFLICT (slug)
    DO UPDATE SET code = EXCLUDED.code, comp_type = EXCLUDED.comp_type, is_international = EXCLUDED.is_international, country_id = EXCLUDED.country_id, is_active = TRUE, updated_at = NOW()
  `;
  await sql`
    INSERT INTO competition_translations (competition_id, locale, name, short_name)
    VALUES ((SELECT id FROM competitions WHERE slug = ${config.competitionSlug}), 'en', ${config.name}, ${config.shortName})
    ON CONFLICT (competition_id, locale)
    DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
  `;
}

async function upsertTeam(sql: DbSql, draft: TeamDraft) {
  await sql`
    INSERT INTO teams (slug, country_id, gender, is_national, crest_url, is_active, updated_at)
    VALUES (${draft.slug}, (SELECT id FROM countries WHERE code_alpha3 = ${draft.countryCode}), 'male', FALSE, NULL, TRUE, NOW())
    ON CONFLICT (slug)
    DO UPDATE SET country_id = EXCLUDED.country_id, is_active = TRUE, updated_at = NOW()
  `;
  await sql`
    INSERT INTO team_translations (team_id, locale, name, short_name)
    VALUES ((SELECT id FROM teams WHERE slug = ${draft.slug}), 'en', ${draft.name}, ${draft.shortName})
    ON CONFLICT (team_id, locale)
    DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
  `;
}

async function upsertSourceEntityMapping(
  sql: DbSql,
  entityType: 'team' | 'match',
  entityId: number,
  sourceId: number,
  externalId: string,
  metadata: Record<string, unknown>,
) {
  await sql`
    INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata, updated_at)
    VALUES (${entityType}, ${entityId}, ${sourceId}, ${externalId}, ${sql.json(toJsonValue(metadata))}, NOW())
    ON CONFLICT (entity_type, source_id, external_id)
    DO UPDATE SET entity_id = EXCLUDED.entity_id, metadata = EXCLUDED.metadata, updated_at = NOW()
  `;
}

async function upsertCompetitionSeason(
  sql: DbSql,
  competitionSlug: string,
  seasonSlug: string,
  formatType: CompetitionSeasonFormatType,
  sourceMetadata: Record<string, unknown>,
) {
  await sql`
    INSERT INTO competition_seasons (competition_id, season_id, format_type, source_metadata, status, updated_at)
    VALUES (
      (SELECT id FROM competitions WHERE slug = ${competitionSlug}),
      (SELECT id FROM seasons WHERE slug = ${seasonSlug}),
      ${formatType},
      ${sql.json(toJsonValue(sourceMetadata))},
      'active',
      NOW()
    )
    ON CONFLICT (competition_id, season_id)
    DO UPDATE SET format_type = EXCLUDED.format_type, source_metadata = EXCLUDED.source_metadata, status = EXCLUDED.status, updated_at = NOW()
  `;
}

async function upsertTeamSeason(sql: DbSql, competitionSlug: string, seasonSlug: string, teamSlug: string) {
  await sql`
    INSERT INTO team_seasons (team_id, competition_season_id, updated_at)
    VALUES (
      (SELECT id FROM teams WHERE slug = ${teamSlug}),
      (
        SELECT cs.id
        FROM competition_seasons cs
        JOIN competitions c ON c.id = cs.competition_id
        JOIN seasons s ON s.id = cs.season_id
        WHERE c.slug = ${competitionSlug} AND s.slug = ${seasonSlug}
      ),
      NOW()
    )
    ON CONFLICT (team_id, competition_season_id)
    DO UPDATE SET updated_at = NOW()
  `;
}

async function upsertMatch(sql: DbSql, competitionSlug: string, draft: MatchDraft) {
  await sql`
    INSERT INTO matches (
      id,
      match_date,
      competition_season_id,
      matchday,
      stage,
      group_name,
      home_team_id,
      away_team_id,
      home_score,
      away_score,
      status,
      kickoff_at,
      source_metadata,
      updated_at
    )
    VALUES (
      ${draft.matchId},
      ${draft.matchDate},
      (
        SELECT cs.id
        FROM competition_seasons cs
        JOIN competitions c ON c.id = cs.competition_id
        JOIN seasons s ON s.id = cs.season_id
        WHERE c.slug = ${competitionSlug} AND s.slug = ${draft.seasonSlug}
      ),
      ${draft.matchWeek},
      ${draft.stage},
      ${draft.groupName},
      (SELECT id FROM teams WHERE slug = ${draft.homeTeamSlug}),
      (SELECT id FROM teams WHERE slug = ${draft.awayTeamSlug}),
      ${draft.homeScore},
      ${draft.awayScore},
      ${draft.status},
      ${draft.kickoffAt},
      ${sql.json(toJsonValue(draft.sourceMetadata))},
      NOW()
    )
    ON CONFLICT (id, match_date)
    DO UPDATE SET
      matchday = EXCLUDED.matchday,
      stage = EXCLUDED.stage,
      group_name = EXCLUDED.group_name,
      home_team_id = EXCLUDED.home_team_id,
      away_team_id = EXCLUDED.away_team_id,
      home_score = EXCLUDED.home_score,
      away_score = EXCLUDED.away_score,
      status = CASE
        WHEN EXCLUDED.home_score IS NOT NULL OR EXCLUDED.away_score IS NOT NULL THEN 'finished'
        WHEN matches.status IN ('finished', 'finished_aet', 'finished_pen') THEN matches.status
        ELSE EXCLUDED.status
      END,
      kickoff_at = EXCLUDED.kickoff_at,
      source_metadata = EXCLUDED.source_metadata,
      updated_at = NOW()
  `;
}

async function refreshDerivedViews(sql: DbSql) {
  await sql`REFRESH MATERIALIZED VIEW mv_team_form`;
  await sql`REFRESH MATERIALIZED VIEW mv_standings`;
  await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;
}

export async function materializeSofascoreCompetition(
  options: MaterializeSofascoreCompetitionOptions,
): Promise<MaterializeSofascoreCompetitionSummary> {
  const sourceSlug = options.sourceSlug?.trim() || 'soccerdata_sofascore';
  const competitionCode = options.competitionCodes?.[0]?.toUpperCase() || 'UEL';
  const config = COMPETITION_CONFIGS[competitionCode];
  if (!config) {
    throw new Error(`Unsupported Sofascore competition code: ${competitionCode}`);
  }

  const sql = getMaterializeDb();
  const sourceId = await ensureSource(sql, sourceSlug);
  const endpointPrefix = `sofascore://${config.league}/${options.seasonLabel}/`;
  const rawRows = await loadRawRows(sql, sourceId, endpointPrefix, options.seasonLabel);
  if (rawRows.length === 0) {
    throw new Error(`No Sofascore raw payloads found for ${competitionCode} ${options.seasonLabel}`);
  }

  const seasonBounds = createSeasonBounds(options.seasonLabel, config.seasonSlugFormat);
  const isCurrent = new Date().toISOString().slice(0, 10) <= seasonBounds.endDate;

  const leagueInfo = rawRows.find((row) => row.endpoint.endsWith('/league_info'))?.payload as LeagueInfoPayload | undefined;
  const seasonInfo = rawRows.find((row) => row.endpoint.endsWith('/season_info'))?.payload as SeasonInfoPayload | undefined;
  const scheduleRows = rawRows.filter((row) => row.endpoint.endsWith('/schedule'));
  const overviewRows = new Map(
    rawRows
      .filter((row) => row.endpoint.endsWith('/match_overview'))
      .map((row) => [row.external_id ?? '', row.payload as MatchOverviewPayload]),
  );

  const existingTeamLookup = await loadExistingTeamLookup(sql);
  const existingTeamMapping = await loadExistingSourceEntityMapping(sql, sourceId, 'team');
  const existingMatchLookup = await loadExistingMatchLookup(sql, config.competitionSlug, seasonBounds.slug);
  const existingMatchMapping = await loadExistingSourceEntityMapping(sql, sourceId, 'match');
  const teamNames = new Set<string>();
  const teamExternalIdByName = new Map<string, string>();
  for (const row of rawRows.filter((entry) => entry.endpoint.endsWith('/league_table'))) {
    const teamName = normalizeTeamName((row.payload as LeagueTablePayload).team);
    if (teamName) {
      teamNames.add(teamName);
    }
  }
  for (const row of scheduleRows) {
    const payload = row.payload as SchedulePayload;
    const homeTeam = normalizeTeamName(payload.home_team);
    const awayTeam = normalizeTeamName(payload.away_team);
    if (homeTeam) {
      teamNames.add(homeTeam);
    }
    if (awayTeam) {
      teamNames.add(awayTeam);
    }
  }
  for (const overview of overviewRows.values()) {
    const homeName = overview?.event?.homeTeam?.name?.trim();
    const awayName = overview?.event?.awayTeam?.name?.trim();
    const homeId = overview?.event?.homeTeam?.id;
    const awayId = overview?.event?.awayTeam?.id;
    if (homeName && homeId !== undefined && homeId !== null) teamExternalIdByName.set(homeName, String(homeId));
    if (awayName && awayId !== undefined && awayId !== null) teamExternalIdByName.set(awayName, String(awayId));
  }

  const teamDrafts = new Map<string, TeamDraft>();
  for (const teamName of teamNames) {
    const stableExternalId = teamExternalIdByName.get(teamName);
    const mappedTeamId = (stableExternalId ? existingTeamMapping.get(stableExternalId) : null) ?? existingTeamMapping.get(teamName);
    if (mappedTeamId) {
      const mapped = [...existingTeamLookup.values()].flat().find((entry) => entry.id === mappedTeamId);
      if (mapped) {
        teamDrafts.set(teamName, {
          countryCode: mapped.codeAlpha3 ?? 'ZZZ',
          externalId: stableExternalId ?? teamName,
          name: teamName,
          shortName: createShortName(teamName, 18),
          slug: mapped.slug,
        });
        continue;
      }
    }

    const existing = resolveCanonicalTeamSlug(existingTeamLookup, teamName);
    if (existing) {
      teamDrafts.set(teamName, {
        countryCode: existing.codeAlpha3 ?? 'ZZZ',
        externalId: stableExternalId ?? teamName,
        name: teamName,
        shortName: createShortName(teamName, 18),
        slug: existing.slug,
      });
      continue;
    }

    teamDrafts.set(teamName, {
      countryCode: 'ZZZ',
      externalId: stableExternalId ?? teamName,
      name: teamName,
      shortName: createShortName(teamName, 18),
      slug: createTeamSlug(teamName),
    });
  }

  const matchDraftByKey = new Map<string, MatchDraft>();

  for (const row of scheduleRows) {
    const payload = row.payload as SchedulePayload;
    const homeTeamName = normalizeTeamName(payload.home_team);
    const awayTeamName = normalizeTeamName(payload.away_team);
    if (!homeTeamName || !awayTeamName) {
      continue;
    }

    const overview = overviewRows.get(String(payload.game_id));
    const stageInfo = normalizeStage(config, seasonBounds.slug, payload.roundName, payload.round);
    const homeTeamSlug = teamDrafts.get(homeTeamName)?.slug ?? createTeamSlug(homeTeamName);
    const awayTeamSlug = teamDrafts.get(awayTeamName)?.slug ?? createTeamSlug(awayTeamName);
    const matchDate = payload.date.slice(0, 10);
    const existingMappedMatchId = existingMatchMapping.get(String(payload.game_id));
    const matchId = existingMappedMatchId
      ?? existingMatchLookup.get(buildMatchLookupKey(matchDate, stageInfo.stage, stageInfo.groupName, homeTeamSlug, awayTeamSlug))
      ?? payload.game_id;

    const draft: MatchDraft = {
      awayScore: resolveEventScore(overview?.event?.awayScore) ?? payload.away_score ?? null,
      awayTeamSlug,
      externalMatchId: payload.game_id,
      groupName: stageInfo.groupName,
      homeScore: resolveEventScore(overview?.event?.homeScore) ?? payload.home_score ?? null,
      homeTeamSlug,
      kickoffAt: overview?.event?.startTimestamp ? new Date(overview.event.startTimestamp * 1000).toISOString() : payload.date,
      matchDate,
      matchId,
      matchWeek: payload.week ?? payload.round ?? null,
      seasonSlug: seasonBounds.slug,
      sourceMetadata: {
        externalMatchId: payload.game_id,
        hasResult: resolveEventScore(overview?.event?.homeScore) !== null || resolveEventScore(overview?.event?.awayScore) !== null || payload.home_score !== null || payload.away_score !== null,
        overviewStatusCode: overview?.event?.status?.code ?? null,
        round: payload.round,
        roundName: payload.roundName ?? null,
        slug: payload.slug ?? null,
        source: 'sofascore',
      },
      stage: stageInfo.stage,
      status: resolveMatchStatus(
        overview?.event?.status?.code,
        resolveEventScore(overview?.event?.homeScore) ?? payload.home_score ?? null,
        resolveEventScore(overview?.event?.awayScore) ?? payload.away_score ?? null,
      ),
    };

    const draftKey = getMatchDraftKey(draft);
    const currentDraft = matchDraftByKey.get(draftKey);
    if (!currentDraft || isBetterMatchDraft(draft, currentDraft)) {
      matchDraftByKey.set(draftKey, draft);
    }
  }

  const matchDrafts = Array.from(matchDraftByKey.values());

  const summary: MaterializeSofascoreCompetitionSummary = {
    competitionSeasons: 1,
    competitions: 1,
    dryRun: options.dryRun ?? true,
    matches: matchDrafts.length,
    rawRows: rawRows.length,
    scheduleRows: scheduleRows.length,
    seasonLabel: options.seasonLabel,
    seasons: 1,
    sourceSlug,
    teamSeasons: teamDrafts.size,
    teams: teamDrafts.size,
  };

  if (summary.dryRun) {
    return summary;
  }

  await upsertCountry(sql, 'ZZZ', 'Unknown');
  await upsertCountry(sql, 'EUR', 'Europe');
  await upsertSeason(sql, seasonBounds.slug, seasonBounds.startDate, seasonBounds.endDate, isCurrent);
  await upsertCompetition(sql, config);
  await upsertCompetitionSeason(sql, config.competitionSlug, seasonBounds.slug, deriveCompetitionSeasonFormat({
    competitionSlug: config.competitionSlug,
    compType: config.compType,
    seasonStartDate: seasonBounds.startDate,
  }), {
    leagueInfo: leagueInfo ?? null,
    seasonId: seasonInfo?.season_id ?? null,
    source: 'sofascore',
  });

  const teamDraftList = Array.from(teamDrafts.values());

  await sql`BEGIN`;
  try {
    for (let i = 0; i < teamDraftList.length; i += BATCH_SIZE) {
      const chunk = teamDraftList.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO teams (slug, country_id, gender, is_national, crest_url, is_active, updated_at)
        SELECT t.slug, c.id, 'male', FALSE, NULL, TRUE, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.slug))}::text[],
          ${sql.array(chunk.map((r) => r.countryCode))}::text[]
        ) AS t(slug, country_code)
        LEFT JOIN countries c ON c.code_alpha3 = t.country_code
        ON CONFLICT (slug)
        DO UPDATE SET country_id = EXCLUDED.country_id, is_active = TRUE, updated_at = NOW()
      `;
      await sql`
        INSERT INTO team_translations (team_id, locale, name, short_name)
        SELECT tm.id, 'en', t.name, t.short_name
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.slug))}::text[],
          ${sql.array(chunk.map((r) => r.name))}::text[],
          ${sql.array(chunk.map((r) => r.shortName))}::text[]
        ) AS t(slug, name, short_name)
        JOIN teams tm ON tm.slug = t.slug
        ON CONFLICT (team_id, locale)
        DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
      `;
      await sql`
        INSERT INTO team_seasons (team_id, competition_season_id, updated_at)
        SELECT tm.id, cs.id, NOW()
        FROM UNNEST(${sql.array(chunk.map((r) => r.slug))}::text[]) AS t(slug)
        JOIN teams tm ON tm.slug = t.slug
        JOIN competition_seasons cs ON cs.competition_id = (SELECT id FROM competitions WHERE slug = ${config.competitionSlug})
          AND cs.season_id = (SELECT id FROM seasons WHERE slug = ${seasonBounds.slug})
        ON CONFLICT (team_id, competition_season_id)
        DO UPDATE SET updated_at = NOW()
      `;
    }
    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }

  const teamEntries = Array.from(teamDrafts.entries());

  await sql`BEGIN`;
  try {
    for (let i = 0; i < teamEntries.length; i += BATCH_SIZE) {
      const chunk = teamEntries.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata, updated_at)
        SELECT 'team', tm.id, ${sourceId}, t.external_id, t.metadata::jsonb, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map(([, d]) => d.slug))}::text[],
          ${sql.array(chunk.map(([name, d]) => d.externalId ?? name))}::text[],
          ${sql.array(chunk.map(([name, d]) => JSON.stringify({ source: 'sofascore', teamId: d.externalId ?? null, teamName: name })))}::text[]
        ) AS t(slug, external_id, metadata)
        JOIN teams tm ON tm.slug = t.slug
        ON CONFLICT (entity_type, source_id, external_id)
        DO UPDATE SET entity_id = EXCLUDED.entity_id, metadata = EXCLUDED.metadata, updated_at = NOW()
      `;
    }
    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }

  await sql`BEGIN`;
  try {
    for (let i = 0; i < matchDrafts.length; i += BATCH_SIZE) {
      const chunk = matchDrafts.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO matches (
          id, match_date, competition_season_id, matchday, stage, group_name,
          home_team_id, away_team_id, home_score, away_score, status, kickoff_at,
          source_metadata, updated_at
        )
        SELECT
          t.match_id, t.match_date::date,
          cs.id,
          t.matchday, t.stage, t.group_name,
          ht.id, at.id,
          t.home_score, t.away_score, t.status::match_status, t.kickoff_at::timestamptz,
          t.source_metadata::jsonb, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.matchId))}::int[],
          ${sql.array(chunk.map((r) => r.matchDate))}::text[],
          ${sql.array(chunk.map((r) => r.seasonSlug))}::text[],
          ${sql.array(chunk.map((r) => r.matchWeek))}::int[],
          ${sql.array(chunk.map((r) => r.stage))}::text[],
          ${sql.array(chunk.map((r) => r.groupName))}::text[],
          ${sql.array(chunk.map((r) => r.homeTeamSlug))}::text[],
          ${sql.array(chunk.map((r) => r.awayTeamSlug))}::text[],
          ${sql.array(chunk.map((r) => r.homeScore))}::int[],
          ${sql.array(chunk.map((r) => r.awayScore))}::int[],
          ${sql.array(chunk.map((r) => r.status))}::text[],
          ${sql.array(chunk.map((r) => r.kickoffAt))}::text[],
          ${sql.array(chunk.map((r) => JSON.stringify(r.sourceMetadata)))}::text[]
        ) AS t(match_id, match_date, season_slug, matchday, stage, group_name, home_slug, away_slug, home_score, away_score, status, kickoff_at, source_metadata)
        JOIN competition_seasons cs ON cs.competition_id = (SELECT id FROM competitions WHERE slug = ${config.competitionSlug})
          AND cs.season_id = (SELECT id FROM seasons WHERE slug = t.season_slug)
        JOIN teams ht ON ht.slug = t.home_slug
        JOIN teams at ON at.slug = t.away_slug
        ON CONFLICT (id, match_date)
        DO UPDATE SET
          matchday = EXCLUDED.matchday,
          stage = EXCLUDED.stage,
          group_name = EXCLUDED.group_name,
          home_team_id = EXCLUDED.home_team_id,
          away_team_id = EXCLUDED.away_team_id,
          home_score = EXCLUDED.home_score,
          away_score = EXCLUDED.away_score,
          status = CASE
            WHEN EXCLUDED.home_score IS NOT NULL OR EXCLUDED.away_score IS NOT NULL THEN 'finished'
            WHEN matches.status IN ('finished', 'finished_aet', 'finished_pen') THEN matches.status
            ELSE EXCLUDED.status
          END,
          kickoff_at = EXCLUDED.kickoff_at,
          source_metadata = EXCLUDED.source_metadata,
          updated_at = NOW()
      `;
      await sql`
        INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata, updated_at)
        SELECT 'match', t.match_id, ${sourceId}, t.external_id, t.metadata::jsonb, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.matchId))}::int[],
          ${sql.array(chunk.map((r) => String(r.externalMatchId)))}::text[],
          ${sql.array(chunk.map((r) => JSON.stringify({ source: 'sofascore', competition: competitionCode, seasonLabel: options.seasonLabel })))}::text[]
        ) AS t(match_id, external_id, metadata)
        ON CONFLICT (entity_type, source_id, external_id)
        DO UPDATE SET entity_id = EXCLUDED.entity_id, metadata = EXCLUDED.metadata, updated_at = NOW()
      `;
    }
    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }

  await refreshDerivedViews(sql);

  return summary;
}
