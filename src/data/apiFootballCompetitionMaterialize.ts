import postgres, { type Sql } from 'postgres';
import { deriveCompetitionSeasonFormat, type CompetitionSeasonFormatType } from './competitionFormats.ts';
import { loadCountryCodeResolver, type CountryCodeResolver } from './countryCodeResolver.ts';
import { createTeamLookupKeys } from './teamLookupKeys.ts';
import {
  buildApiFootballFixturesPath,
  buildApiFootballLeaguePath,
  buildApiFootballStandingsPath,
  parseApiFootballDataCompetitionTargets,
  type ApiFootballEnvelope,
  type ApiFootballFixtureResponseItem,
  type ApiFootballLeagueResponseItem,
} from './apiFootball.ts';

const BATCH_SIZE = 500;

interface CountryDraft {
  codeAlpha3: string;
  name: string;
}

interface SeasonDraft {
  slug: string;
  startDate: string;
  endDate: string;
}

interface CompetitionDraft {
  slug: string;
  code: string;
  name: string;
  shortName: string;
  countryCode: string | null;
  countryName: string | null;
  gender: 'male' | 'female' | 'mixed';
  isYouth: boolean;
  isInternational: boolean;
  compType: 'league' | 'international';
}

interface TeamDraft {
  slug: string;
  name: string;
  shortName: string;
  countryCode: string;
  crestUrl: string | null;
  gender: 'male' | 'female' | 'mixed';
  isNational: boolean;
}

interface CompetitionSeasonDraft {
  competitionSlug: string;
  seasonSlug: string;
  formatType: CompetitionSeasonFormatType;
  currentMatchday: number | null;
  totalMatchdays: number | null;
  status: string;
  sourceMetadata: Record<string, unknown>;
}

interface MatchDraft {
  matchId: number;
  externalMatchId: number;
  matchDate: string;
  competitionSlug: string;
  seasonSlug: string;
  homeTeamSlug: string;
  awayTeamSlug: string;
  homeScore: number | null;
  awayScore: number | null;
  matchWeek: number | null;
  stage: string;
  groupName: string | null;
  status: string;
  kickoffAt: string | null;
  sourceMetadata: Record<string, unknown>;
}

interface SourceRow {
  id: number;
}

interface RawPayloadRow {
  payload: unknown;
}

interface ExistingTeamCountryRow {
  code_alpha3: string | null;
}

interface ExistingTeamLookupRow {
  slug: string;
  name: string | null;
  code_alpha3: string | null;
}

interface ExistingMatchLookupRow {
  id: number;
  match_date: string;
  stage: string | null;
  group_name: string | null;
  home_slug: string;
  away_slug: string;
}

interface TeamLookupEntry {
  slug: string;
  codeAlpha3: string | null;
}

interface ApiFootballCompetitionConfig {
  slug: string;
  code: string;
  name: string;
  shortName: string;
  countryCode: string | null;
  isInternational: boolean;
  compType: 'league' | 'international';
}

export interface MaterializeApiFootballCompetitionOptions {
  dryRun?: boolean;
  competitionCodes?: string[];
  seasons?: number[];
}

export interface MaterializeApiFootballCompetitionSummary {
  dryRun: boolean;
  countries: number;
  competitions: number;
  seasons: number;
  teams: number;
  competitionSeasons: number;
  teamSeasons: number;
  matches: number;
}

const COMPETITION_CONFIGS: Record<string, ApiFootballCompetitionConfig> = {
  BL1: { slug: '1-bundesliga', code: 'bl1', name: '1. Bundesliga', shortName: 'Bundesliga', countryCode: 'DEU', isInternational: false, compType: 'league' },
  CDF: { slug: 'coupe-de-france', code: 'cdf', name: 'Coupe de France', shortName: 'Coupe de France', countryCode: 'FRA', isInternational: false, compType: 'international' },
  CDR: { slug: 'copa-del-rey', code: 'cdr', name: 'Copa del Rey', shortName: 'Copa del Rey', countryCode: 'ESP', isInternational: false, compType: 'international' },
  CI: { slug: 'coppa-italia', code: 'ci', name: 'Coppa Italia', shortName: 'Coppa Italia', countryCode: 'ITA', isInternational: false, compType: 'international' },
  CL: { slug: 'champions-league', code: 'cl', name: 'Champions League', shortName: 'Champions League', countryCode: null, isInternational: true, compType: 'international' },
  DFP: { slug: 'dfb-pokal', code: 'dfp', name: 'DFB Pokal', shortName: 'DFB Pokal', countryCode: 'DEU', isInternational: false, compType: 'international' },
  EL: { slug: 'europa-league', code: 'el', name: 'Europa League', shortName: 'Europa League', countryCode: null, isInternational: true, compType: 'international' },
  FAC: { slug: 'fa-cup', code: 'fac', name: 'FA Cup', shortName: 'FA Cup', countryCode: 'ENG', isInternational: false, compType: 'international' },
  FL1: { slug: 'ligue-1', code: 'fl1', name: 'Ligue 1', shortName: 'Ligue 1', countryCode: 'FRA', isInternational: false, compType: 'league' },
  PD: { slug: 'la-liga', code: 'pd', name: 'La Liga', shortName: 'La Liga', countryCode: 'ESP', isInternational: false, compType: 'league' },
  PL: { slug: 'premier-league', code: 'pl', name: 'Premier League', shortName: 'Premier League', countryCode: 'ENG', isInternational: false, compType: 'league' },
  SA: { slug: 'serie-a', code: 'sa', name: 'Serie A', shortName: 'Serie A', countryCode: 'ITA', isInternational: false, compType: 'league' },
  SPL: { slug: 'saudi-pro-league', code: 'spl', name: 'Saudi Pro League', shortName: 'Saudi Pro League', countryCode: 'SAU', isInternational: false, compType: 'league' },
};

function getMaterializeDb() {
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

function normalizeSeasons(input?: number[]) {
  if (input && input.length > 0) {
    return [...new Set(input)].sort((a, b) => a - b);
  }

  return [2023, 2024];
}

function slugify(value: string) {
  return value
    .normalize('NFKD')
    .replace(/['’.]/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-')
    .toLowerCase();
}

function createTeamSlug(name: string, countryName?: string | null) {
  return slugify(countryName ? `${name} ${countryName}` : name);
}

function createSeasonSlug(startDate: string, endDate: string) {
  const startYear = Number.parseInt(startDate.slice(0, 4), 10);
  const endYear = Number.parseInt(endDate.slice(0, 4), 10);
  if (Number.isFinite(startYear) && Number.isFinite(endYear) && startYear !== endYear) {
    return `${startYear}/${String(endYear).slice(-2)}`;
  }

  return String(startYear);
}

function createShortName(name: string, maxLength: number = 24) {
  return name.length <= maxLength ? name : `${name.slice(0, maxLength - 1).trimEnd()}.`;
}

function normalizeCountryCode(countryCodeResolver: CountryCodeResolver, code?: string | null) {
  if (!code) {
    return null;
  }

  const raw = code.toUpperCase();
  const aliases: Record<string, string> = {
    'GB-ENG': 'ENG',
    'GB-SCT': 'SCO',
    'GB-WLS': 'WAL',
    'GB-NIR': 'NIR',
    WORLD: 'WRL',
  };
  const aliased = aliases[raw] ?? raw;
  const normalized = countryCodeResolver.resolve(aliased);
  return countryCodeResolver.isSkipped(normalized) ? null : normalized;
}

function normalizeMatchStatus(status?: string | null): string {
  switch (status) {
    case 'FT':
      return 'finished';
    case 'AET':
      return 'finished_aet';
    case 'PEN':
      return 'finished_pen';
    case '1H':
    case 'LIVE':
      return 'live_1h';
    case 'HT':
      return 'live_ht';
    case '2H':
      return 'live_2h';
    case 'ET':
      return 'live_et';
    case 'BT':
    case 'P':
      return 'live_pen';
    case 'PST':
      return 'postponed';
    case 'SUSP':
    case 'INT':
      return 'suspended';
    case 'CANC':
      return 'cancelled';
    case 'ABD':
      return 'awarded';
    case 'TBD':
    case 'NS':
    default:
      return 'scheduled';
  }
}

function parseRound(round?: string | null) {
  if (!round) {
    return { stage: 'REGULAR_SEASON', groupName: null, matchWeek: null };
  }

  const [stagePart, trailingPart] = round.split(' - ', 2);
  const normalizedStage = stagePart.replace(/\w/g, (char) => char.toUpperCase()).replace(/of/g, 'of');
  const trailingNumber = trailingPart ? Number.parseInt(trailingPart, 10) : Number.NaN;
  const isRegularSeason = /regular season/i.test(stagePart);
  const isGroup = /group/i.test(stagePart);

  return {
    stage: (isRegularSeason ? 'REGULAR_SEASON' : normalizedStage.toUpperCase().replace(/[^A-Z0-9]+/g, '_')).slice(0, 30),
    groupName: isGroup ? stagePart.slice(0, 20) : null,
    matchWeek: Number.isFinite(trailingNumber) ? trailingNumber : null,
  };
}

function shouldIncludeLeagueFixture(fixture: ApiFootballFixtureResponseItem, competition: CompetitionDraft) {
  if (competition.compType !== 'league') {
    return true;
  }

  return parseRound(fixture.league?.round ?? null).stage === 'REGULAR_SEASON';
}

function buildCountryDraft(name: string, code: string): CountryDraft {
  return { codeAlpha3: code, name };
}

function registerTeamLookupEntry(lookup: Map<string, TeamLookupEntry[]>, name: string, entry: TeamLookupEntry) {
  for (const key of createTeamLookupKeys(name)) {
    const existing = lookup.get(key) ?? [];
    if (!existing.some((candidate) => candidate.slug === entry.slug)) {
      existing.push(entry);
      lookup.set(key, existing);
    }
  }
}

async function loadExistingTeamLookup(sql: Sql, countryCodeResolver: CountryCodeResolver) {
  const rows = await sql<ExistingTeamLookupRow[]>`
    SELECT DISTINCT slug, name, code_alpha3
    FROM (
      SELECT
        t.id AS team_id,
        t.slug,
        tt.name,
        c.code_alpha3
      FROM teams t
      LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
      LEFT JOIN countries c ON c.id = t.country_id
      UNION ALL
      SELECT
        t.id AS team_id,
        t.slug,
        ea.alias AS name,
        c.code_alpha3
      FROM teams t
      JOIN entity_aliases ea ON ea.entity_type = 'team' AND ea.entity_id = t.id
      LEFT JOIN countries c ON c.id = t.country_id
    ) lookup
  `;

  const lookup = new Map<string, TeamLookupEntry[]>();
  for (const row of rows) {
    registerTeamLookupEntry(lookup, row.name ?? row.slug, {
      slug: row.slug,
      codeAlpha3: normalizeCountryCode(countryCodeResolver, row.code_alpha3),
    });
  }

  return lookup;
}

function resolveCanonicalTeamSlug(
  lookup: Map<string, TeamLookupEntry[]>,
  teamName: string,
  preferredCountryCode: string | null,
) {
  const candidates = createTeamLookupKeys(teamName)
    .flatMap((key) => lookup.get(key) ?? []);
  const unique = [...new Map(candidates.map((entry) => [entry.slug, entry])).values()];

  if (preferredCountryCode) {
    const sameCountry = unique.filter((entry) => entry.codeAlpha3 === preferredCountryCode);
    if (sameCountry.length === 1) {
      return sameCountry[0].slug;
    }
  }

  return unique.length === 1 ? unique[0].slug : null;
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

async function loadExistingMatchLookup(sql: Sql, competitionSlug: string, seasonSlug: string) {
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

async function resolveExistingTeamCountryCode(sql: Sql, teamName: string) {
  const rows = await sql<ExistingTeamCountryRow[]>`
    SELECT c.code_alpha3
    FROM teams t
    LEFT JOIN countries c ON c.id = t.country_id
    LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
    WHERE LOWER(COALESCE(tt.name, t.slug)) = LOWER(${teamName})
    LIMIT 1
  `;

  return rows[0]?.code_alpha3 ?? null;
}

function buildCompetitionDraft(
  countryCodeResolver: CountryCodeResolver,
  code: string,
  payload: ApiFootballLeagueResponseItem | null,
): CompetitionDraft {
  const config = COMPETITION_CONFIGS[code];
  return {
    slug: config?.slug ?? slugify(payload?.league?.name ?? code),
    code: config?.code ?? code.toLowerCase(),
    name: payload?.league?.name ?? config?.name ?? code,
    shortName: config?.shortName ?? createShortName(payload?.league?.name ?? code, 20),
    countryCode: normalizeCountryCode(countryCodeResolver, payload?.country?.code) ?? config?.countryCode ?? null,
    countryName: payload?.country?.name ?? null,
    gender: 'male',
    isYouth: false,
    isInternational: config?.isInternational ?? ((payload?.country?.name ?? '').toLowerCase() === 'world'),
    compType: config?.compType ?? ((payload?.league?.type ?? 'League') === 'League' ? 'league' : 'international'),
  };
}

async function resolveCountryNameByCode(sql: Sql, countryCode: string) {
  const rows = await sql<{ name: string | null }[]>`
    SELECT COALESCE(ct.name, c.code_alpha3) AS name
    FROM countries c
    LEFT JOIN country_translations ct ON ct.country_id = c.id AND ct.locale = 'en'
    WHERE c.code_alpha3 = ${countryCode}
    LIMIT 1
  `;

  return rows[0]?.name ?? null;
}

function buildSeasonDraft(seasonInfo: NonNullable<ApiFootballLeagueResponseItem['seasons']>[number] | null, fallbackSeason: number): SeasonDraft {
  const startDate = seasonInfo?.start ?? `${fallbackSeason}-07-01`;
  const endDate = seasonInfo?.end ?? `${fallbackSeason + 1}-06-30`;
  return {
    slug: createSeasonSlug(startDate, endDate),
    startDate,
    endDate,
  };
}

async function buildTeamDraft(
  sql: Sql,
  team: NonNullable<ApiFootballFixtureResponseItem['teams']>['home'],
  competition: CompetitionDraft,
  countryName: string | null,
): Promise<TeamDraft> {
  const countryCode = competition.countryCode
    ?? await resolveExistingTeamCountryCode(sql, team?.name ?? '')
    ?? 'ZZZ';
  if (!countryCode) {
    throw new Error(`Unable to resolve country for API-Football team ${team?.name ?? team?.id ?? 'unknown'}`);
  }

  return {
    slug: createTeamSlug(team?.name ?? String(team?.id ?? 'team'), countryName),
    name: team?.name ?? String(team?.id ?? 'Team'),
    shortName: createShortName(team?.name ?? String(team?.id ?? 'Team'), 18),
    countryCode,
    crestUrl: team?.logo ?? null,
    gender: 'male',
    isNational: false,
  };
}

function buildCompetitionSeasonDraft(
  competition: CompetitionDraft,
  season: SeasonDraft,
  seasonInfo: NonNullable<ApiFootballLeagueResponseItem['seasons']>[number] | null,
  targetSeason: number,
  standingsPayload: ApiFootballEnvelope<unknown>,
  fixtureCount: number,
): CompetitionSeasonDraft {
  return {
    competitionSlug: competition.slug,
    seasonSlug: season.slug,
    formatType: deriveCompetitionSeasonFormat({
      competitionSlug: competition.slug,
      compType: competition.compType,
      seasonStartDate: season.startDate,
    }),
    currentMatchday: null,
    totalMatchdays: null,
    status: seasonInfo?.current ? 'active' : 'completed',
    sourceMetadata: {
      source: 'api_football',
      season: targetSeason,
      coverage: seasonInfo?.coverage ?? null,
      standingsCount: standingsPayload.results ?? 0,
      fixtureCount,
    },
  };
}

function buildMatchDraft(
  fixture: ApiFootballFixtureResponseItem,
  competition: CompetitionDraft,
  season: SeasonDraft,
  teamSlugByExternalId: Map<number, string>,
  existingMatchLookup: Map<string, number>,
): MatchDraft {
  const fixtureId = fixture.fixture?.id;
  const kickoffAt = fixture.fixture?.date;
  const homeId = fixture.teams?.home?.id;
  const awayId = fixture.teams?.away?.id;
  if (!fixtureId || !kickoffAt || !homeId || !awayId) {
    throw new Error('Incomplete API-Football fixture payload');
  }

  const homeTeamSlug = teamSlugByExternalId.get(homeId);
  const awayTeamSlug = teamSlugByExternalId.get(awayId);
  if (!homeTeamSlug || !awayTeamSlug) {
    throw new Error(`Unable to resolve canonical team slug for API-Football match ${fixtureId}`);
  }

  const roundInfo = parseRound(fixture.league?.round ?? null);
  const matchDate = kickoffAt.slice(0, 10);
  const existingMatchId = existingMatchLookup.get(buildMatchLookupKey(matchDate, roundInfo.stage, roundInfo.groupName, homeTeamSlug, awayTeamSlug));

  return {
    matchId: existingMatchId ?? fixtureId,
    externalMatchId: fixtureId,
    matchDate,
    competitionSlug: competition.slug,
    seasonSlug: season.slug,
    homeTeamSlug,
    awayTeamSlug,
    homeScore: fixture.goals?.home ?? null,
    awayScore: fixture.goals?.away ?? null,
    matchWeek: roundInfo.matchWeek,
    stage: roundInfo.stage,
    groupName: roundInfo.groupName,
    status: normalizeMatchStatus(fixture.fixture?.status?.short ?? null),
    kickoffAt,
    sourceMetadata: {
      source: 'api_football',
      externalLeagueId: fixture.league?.id ?? null,
      externalSeason: fixture.league?.season ?? null,
      externalFixtureId: fixtureId,
      referee: fixture.fixture?.referee ?? null,
    },
  };
}

async function ensureApiFootballSource(sql: Sql) {
  const rows = await sql<SourceRow[]>`
    SELECT id
    FROM data_sources
    WHERE slug = 'api_football'
    LIMIT 1
  `;

  if (!rows[0]) {
    throw new Error('api_football source is not registered; run competition-data:ingest-api-football --write first');
  }

  return rows[0].id;
}

async function loadLatestRawPayload<T>(sql: Sql, sourceId: number, endpoint: string): Promise<T | null> {
  const rows = await sql<RawPayloadRow[]>`
    SELECT payload
    FROM raw_payloads
    WHERE source_id = ${sourceId}
      AND endpoint = ${endpoint}
    ORDER BY fetched_at DESC, id DESC
    LIMIT 1
  `;

  return (rows[0]?.payload as T | undefined) ?? null;
}

async function refreshDerivedViews(sql: Sql) {
  await sql`REFRESH MATERIALIZED VIEW mv_team_form`;
  await sql`REFRESH MATERIALIZED VIEW mv_standings`;
  await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;
}

async function upsertEntityAlias(sql: Sql, entityType: 'competition' | 'team' | 'country', entityIdSql: ReturnType<Sql>, alias: string) {
  await sql`
    INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
    SELECT ${entityType}, entity.id, ${alias}, 'en', 'common', TRUE, 'pending', 'imported', 'api_football'
    FROM (${entityIdSql}) AS entity(id)
    WHERE entity.id IS NOT NULL
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

async function upsertTeamLookupAliases(sql: Sql, teamSlug: string, names: Array<string | null | undefined>) {
  const aliasValues = [...new Set(
    names
      .flatMap((name) => name ? createTeamLookupKeys(name) : [])
      .map((alias) => alias.trim())
      .filter((alias) => alias.length > 0)
  )];

  for (const alias of aliasValues) {
    await upsertEntityAlias(sql, 'team', sql`SELECT id FROM teams WHERE slug = ${teamSlug}`, alias);
  }
}

async function upsertCountry(sql: Sql, draft: CountryDraft) {
  await sql`
    INSERT INTO countries (code_alpha3, is_active, updated_at)
    VALUES (${draft.codeAlpha3}, TRUE, NOW())
    ON CONFLICT (code_alpha3)
    DO UPDATE SET is_active = TRUE, updated_at = NOW()
  `;

  await sql`
    INSERT INTO country_translations (country_id, locale, name)
    VALUES ((SELECT id FROM countries WHERE code_alpha3 = ${draft.codeAlpha3}), 'en', ${draft.name})
    ON CONFLICT (country_id, locale)
    DO UPDATE SET name = EXCLUDED.name
  `;

  await upsertEntityAlias(sql, 'country', sql`SELECT id FROM countries WHERE code_alpha3 = ${draft.codeAlpha3}`, draft.name);
}

async function upsertSeason(sql: Sql, draft: SeasonDraft) {
  await sql`
    INSERT INTO seasons (slug, start_date, end_date, is_current)
    VALUES (${draft.slug}, ${draft.startDate}, ${draft.endDate}, FALSE)
    ON CONFLICT (slug)
    DO UPDATE SET
      start_date = LEAST(seasons.start_date, EXCLUDED.start_date),
      end_date = GREATEST(seasons.end_date, EXCLUDED.end_date),
      is_current = EXCLUDED.is_current
  `;
}

async function upsertCompetition(sql: Sql, draft: CompetitionDraft) {
  await sql`
    INSERT INTO competitions (
      slug,
      code,
      comp_type,
      gender,
      is_youth,
      is_international,
      country_id,
      is_active,
      updated_at
    )
    VALUES (
      ${draft.slug},
      ${draft.code},
      ${draft.compType},
      ${draft.gender},
      ${draft.isYouth},
      ${draft.isInternational},
      ${draft.countryCode ? sql`(SELECT id FROM countries WHERE code_alpha3 = ${draft.countryCode})` : sql`NULL`},
      TRUE,
      NOW()
    )
    ON CONFLICT (slug)
    DO UPDATE SET
      code = EXCLUDED.code,
      comp_type = EXCLUDED.comp_type,
      gender = EXCLUDED.gender,
      is_youth = EXCLUDED.is_youth,
      is_international = EXCLUDED.is_international,
      country_id = EXCLUDED.country_id,
      is_active = TRUE,
      updated_at = NOW()
  `;

  await sql`
    INSERT INTO competition_translations (competition_id, locale, name, short_name)
    VALUES ((SELECT id FROM competitions WHERE slug = ${draft.slug}), 'en', ${draft.name}, ${draft.shortName})
    ON CONFLICT (competition_id, locale)
    DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
  `;

  await upsertEntityAlias(sql, 'competition', sql`SELECT id FROM competitions WHERE slug = ${draft.slug}`, draft.name);
}

async function upsertTeam(sql: Sql, draft: TeamDraft) {
  await sql`
    INSERT INTO teams (slug, country_id, gender, is_national, crest_url, is_active, updated_at)
    VALUES (
      ${draft.slug},
      (SELECT id FROM countries WHERE code_alpha3 = ${draft.countryCode}),
      ${draft.gender},
      ${draft.isNational},
      ${draft.crestUrl},
      TRUE,
      NOW()
    )
    ON CONFLICT (slug)
    DO UPDATE SET
      country_id = EXCLUDED.country_id,
      gender = EXCLUDED.gender,
      is_national = EXCLUDED.is_national,
      crest_url = COALESCE(EXCLUDED.crest_url, teams.crest_url),
      is_active = TRUE,
      updated_at = NOW()
  `;

  await sql`
    INSERT INTO team_translations (team_id, locale, name, short_name)
    VALUES ((SELECT id FROM teams WHERE slug = ${draft.slug}), 'en', ${draft.name}, ${draft.shortName})
    ON CONFLICT (team_id, locale)
    DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
  `;

  await upsertEntityAlias(sql, 'team', sql`SELECT id FROM teams WHERE slug = ${draft.slug}`, draft.name);
  await upsertTeamLookupAliases(sql, draft.slug, [draft.name]);
}

async function upsertCompetitionSeason(sql: Sql, draft: CompetitionSeasonDraft) {
  await sql`
    INSERT INTO competition_seasons (
      competition_id,
      season_id,
      format_type,
      current_matchday,
      total_matchdays,
      source_metadata,
      status,
      updated_at
    )
    VALUES (
      (SELECT id FROM competitions WHERE slug = ${draft.competitionSlug}),
      (SELECT id FROM seasons WHERE slug = ${draft.seasonSlug}),
      ${draft.formatType},
      ${draft.currentMatchday},
      ${draft.totalMatchdays},
      ${JSON.stringify(draft.sourceMetadata)}::jsonb,
      ${draft.status},
      NOW()
    )
    ON CONFLICT (competition_id, season_id)
    DO UPDATE SET
      format_type = EXCLUDED.format_type,
      current_matchday = EXCLUDED.current_matchday,
      total_matchdays = EXCLUDED.total_matchdays,
      source_metadata = EXCLUDED.source_metadata,
      status = EXCLUDED.status,
      updated_at = NOW()
  `;
}

async function upsertTeamSeason(sql: Sql, competitionSlug: string, seasonSlug: string, teamSlug: string) {
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

async function upsertMatch(sql: Sql, draft: MatchDraft) {
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
        WHERE c.slug = ${draft.competitionSlug} AND s.slug = ${draft.seasonSlug}
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
      ${JSON.stringify(draft.sourceMetadata)}::jsonb,
      NOW()
    )
    ON CONFLICT (id, match_date)
    DO UPDATE SET
      competition_season_id = EXCLUDED.competition_season_id,
      matchday = EXCLUDED.matchday,
      stage = EXCLUDED.stage,
      group_name = EXCLUDED.group_name,
      home_team_id = EXCLUDED.home_team_id,
      away_team_id = EXCLUDED.away_team_id,
      home_score = EXCLUDED.home_score,
      away_score = EXCLUDED.away_score,
      status = EXCLUDED.status,
      kickoff_at = EXCLUDED.kickoff_at,
      source_metadata = EXCLUDED.source_metadata,
      updated_at = NOW()
  `;
}

async function upsertSourceEntityMapping(
  sql: Sql,
  params: {
    entityType: 'competition' | 'team' | 'match';
    entityIdSql: ReturnType<Sql>;
    sourceId: number;
    externalId: string;
    externalCode?: string | null;
    seasonContext?: string | null;
    metadata: Record<string, unknown>;
  },
) {
  await sql`
    INSERT INTO source_entity_mapping (
      entity_type,
      entity_id,
      source_id,
      external_id,
      external_code,
      season_context,
      metadata,
      updated_at
    )
    VALUES (
      ${params.entityType},
      (${params.entityIdSql}),
      ${params.sourceId},
      ${params.externalId},
      ${params.externalCode ?? null},
      ${params.seasonContext ?? null},
      ${JSON.stringify(params.metadata)}::jsonb,
      NOW()
    )
    ON CONFLICT (entity_type, source_id, external_id)
    DO UPDATE SET
      entity_id = EXCLUDED.entity_id,
      external_code = EXCLUDED.external_code,
      season_context = EXCLUDED.season_context,
      metadata = EXCLUDED.metadata,
      updated_at = NOW()
  `;
}

async function cleanupCompetitionSeasonMatches(sql: Sql, competitionSlug: string, seasonSlug: string) {
  await sql`
    DELETE FROM match_stats ms
    USING matches m, competition_seasons cs, competitions c, seasons s
    WHERE ms.match_id = m.id
      AND ms.match_date = m.match_date
      AND m.competition_season_id = cs.id
      AND cs.competition_id = c.id
      AND cs.season_id = s.id
      AND c.slug = ${competitionSlug}
      AND s.slug = ${seasonSlug}
  `;

  await sql`
    DELETE FROM match_lineups ml
    USING matches m, competition_seasons cs, competitions c, seasons s
    WHERE ml.match_id = m.id
      AND ml.match_date = m.match_date
      AND m.competition_season_id = cs.id
      AND cs.competition_id = c.id
      AND cs.season_id = s.id
      AND c.slug = ${competitionSlug}
      AND s.slug = ${seasonSlug}
  `;

  await sql`
    DELETE FROM match_event_artifacts mea
    USING matches m, competition_seasons cs, competitions c, seasons s
    WHERE mea.match_id = m.id
      AND mea.match_date = m.match_date
      AND m.competition_season_id = cs.id
      AND cs.competition_id = c.id
      AND cs.season_id = s.id
      AND c.slug = ${competitionSlug}
      AND s.slug = ${seasonSlug}
  `;

  await sql`
    DELETE FROM matches m
    USING competition_seasons cs, competitions c, seasons s
    WHERE m.competition_season_id = cs.id
      AND cs.competition_id = c.id
      AND cs.season_id = s.id
      AND c.slug = ${competitionSlug}
      AND s.slug = ${seasonSlug}
  `;
}

async function cleanupCompetitionSeasonTeamSeasons(sql: Sql, competitionSlug: string, seasonSlug: string) {
  await sql`
    DELETE FROM team_seasons ts
    USING competition_seasons cs, competitions c, seasons s
    WHERE ts.competition_season_id = cs.id
      AND cs.competition_id = c.id
      AND cs.season_id = s.id
      AND c.slug = ${competitionSlug}
      AND s.slug = ${seasonSlug}
  `;
}

export async function materializeApiFootballCompetitions(
  options: MaterializeApiFootballCompetitionOptions = {},
): Promise<MaterializeApiFootballCompetitionSummary> {
  const targets = parseApiFootballDataCompetitionTargets(options.competitionCodes);
  const seasonsToProcess = normalizeSeasons(options.seasons);
  const sql = getMaterializeDb();
  const countryCodeResolver = await loadCountryCodeResolver(sql);
  const sourceId = await ensureApiFootballSource(sql);
  const existingTeamLookup = await loadExistingTeamLookup(sql, countryCodeResolver);

  const countries = new Map<string, CountryDraft>();
  const competitions = new Map<string, CompetitionDraft>();
  const seasons = new Map<string, SeasonDraft>();
  const teams = new Map<string, TeamDraft>();
  const competitionSeasons = new Map<string, CompetitionSeasonDraft>();
  const teamSeasonKeys = new Set<string>();
  const matches: MatchDraft[] = [];
  const competitionMappings: Array<{ slug: string; externalId: string; externalCode: string }> = [];
  const teamMappings: Array<{ slug: string; externalId: string; seasonContext: string }> = [];
  const matchMappings: Array<{ matchId: number; matchDate: string; externalId: string; seasonContext: string }> = [];

  try {
    for (const target of targets) {
      const leaguePayload = await loadLatestRawPayload<ApiFootballEnvelope<ApiFootballLeagueResponseItem>>(sql, sourceId, buildApiFootballLeaguePath(target.leagueId));
      const competitionPayload = leaguePayload?.response?.[0] ?? null;
      if (!competitionPayload) {
        continue;
      }

      const competitionDraft = buildCompetitionDraft(countryCodeResolver, target.code, competitionPayload);
      competitions.set(competitionDraft.slug, competitionDraft);
      competitionMappings.push({
        slug: competitionDraft.slug,
        externalId: String(target.leagueId),
        externalCode: target.code,
      });

      const competitionCountryCode = normalizeCountryCode(countryCodeResolver, competitionPayload.country?.code) ?? competitionDraft.countryCode;
      if (competitionCountryCode && competitionPayload.country?.name) {
        countries.set(competitionCountryCode, buildCountryDraft(competitionPayload.country.name, competitionCountryCode));
      }
      countries.set('ZZZ', buildCountryDraft('Unknown', 'ZZZ'));

      for (const seasonValue of seasonsToProcess) {
        const fixturesPayload = await loadLatestRawPayload<ApiFootballEnvelope<ApiFootballFixtureResponseItem>>(
          sql,
          sourceId,
          buildApiFootballFixturesPath(target.leagueId, seasonValue),
        );
        const standingsPayload = await loadLatestRawPayload<ApiFootballEnvelope<unknown>>(
          sql,
          sourceId,
          buildApiFootballStandingsPath(target.leagueId, seasonValue),
        );

        if (!fixturesPayload) {
          continue;
        }

        const seasonInfo = competitionPayload.seasons?.find((entry) => entry.year === seasonValue) ?? null;
        const seasonDraft = buildSeasonDraft(seasonInfo, seasonValue);
        seasons.set(seasonDraft.slug, seasonDraft);
        const existingMatchLookup = await loadExistingMatchLookup(sql, competitionDraft.slug, seasonDraft.slug);

        competitionSeasons.set(`${competitionDraft.slug}:${seasonDraft.slug}`, buildCompetitionSeasonDraft(
          competitionDraft,
          seasonDraft,
          seasonInfo,
          seasonValue,
          standingsPayload ?? { results: 0 },
          fixturesPayload.response?.length ?? fixturesPayload.results ?? 0,
        ));

        const teamSlugByExternalId = new Map<number, string>();

        for (const fixture of fixturesPayload.response ?? []) {
          if (!shouldIncludeLeagueFixture(fixture, competitionDraft)) {
            continue;
          }

          const participants = [fixture.teams?.home, fixture.teams?.away].filter((team): team is NonNullable<typeof team> => Boolean(team?.id && team?.name));
          for (const participant of participants) {
            if (teamSlugByExternalId.has(participant.id!)) {
              continue;
            }

            const resolvedCountryCode = competitionDraft.countryCode
              ?? await resolveExistingTeamCountryCode(sql, participant.name ?? '')
              ?? 'ZZZ';
            if (resolvedCountryCode === 'ZZZ') {
              countries.set('ZZZ', buildCountryDraft('Unknown', 'ZZZ'));
            }

            const resolvedCountryName = resolvedCountryCode === 'ZZZ'
              ? 'Unknown'
              : competitionDraft.countryName
                ?? await resolveCountryNameByCode(sql, resolvedCountryCode);

            const existingTeamSlug = resolveCanonicalTeamSlug(existingTeamLookup, participant.name ?? '', resolvedCountryCode);
            if (existingTeamSlug) {
              await upsertTeamLookupAliases(sql, existingTeamSlug, [participant.name]);
              teamSlugByExternalId.set(participant.id!, existingTeamSlug);
              teamSeasonKeys.add(`${competitionDraft.slug}:${seasonDraft.slug}:${existingTeamSlug}`);
              teamMappings.push({
                slug: existingTeamSlug,
                externalId: String(participant.id),
                seasonContext: String(seasonValue),
              });
              continue;
            }

            const teamDraft = await buildTeamDraft(sql, participant, {
              ...competitionDraft,
              countryCode: resolvedCountryCode,
            }, resolvedCountryName);
            teams.set(teamDraft.slug, teamDraft);
            registerTeamLookupEntry(existingTeamLookup, teamDraft.name, {
              slug: teamDraft.slug,
              codeAlpha3: teamDraft.countryCode,
            });
            teamSlugByExternalId.set(participant.id!, teamDraft.slug);
            teamSeasonKeys.add(`${competitionDraft.slug}:${seasonDraft.slug}:${teamDraft.slug}`);
            teamMappings.push({
              slug: teamDraft.slug,
              externalId: String(participant.id),
              seasonContext: String(seasonValue),
            });
          }
        }

        for (const fixture of fixturesPayload.response ?? []) {
          if (!shouldIncludeLeagueFixture(fixture, competitionDraft)) {
            continue;
          }

          const matchDraft = buildMatchDraft(fixture, competitionDraft, seasonDraft, teamSlugByExternalId, existingMatchLookup);
          matches.push(matchDraft);
  existingMatchLookup.set(
    buildMatchLookupKey(
      matchDraft.matchDate,
      matchDraft.stage,
      matchDraft.groupName,
      matchDraft.homeTeamSlug,
      matchDraft.awayTeamSlug,
    ),
    matchDraft.matchId,
  );
          matchMappings.push({
            matchId: matchDraft.matchId,
            matchDate: matchDraft.matchDate,
            externalId: String(matchDraft.externalMatchId),
            seasonContext: String(seasonValue),
          });
        }
      }
    }

    const summary = {
      dryRun: options.dryRun ?? false,
      countries: countries.size,
      competitions: competitions.size,
      seasons: seasons.size,
      teams: teams.size,
      competitionSeasons: competitionSeasons.size,
      teamSeasons: teamSeasonKeys.size,
      matches: matches.length,
    } satisfies MaterializeApiFootballCompetitionSummary;

    const uniqueTeamMappings = Array.from(
      new Map(teamMappings.map((entry) => [`${entry.externalId}`, entry])).values()
    );

    if (options.dryRun ?? false) {
      await sql.end({ timeout: 1 });
      return summary;
    }

    await sql`BEGIN`;
    try {
      const countryList = Array.from(countries.values());
      for (let i = 0; i < countryList.length; i += BATCH_SIZE) {
        const chunk = countryList.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO countries (code_alpha3, is_active, updated_at)
          SELECT * FROM UNNEST(
            ${sql.array(chunk.map((r) => r.codeAlpha3))}::text[],
            ${sql.array(chunk.map(() => true))}::bool[],
            ${sql.array(chunk.map(() => new Date().toISOString()))}::timestamptz[]
          ) AS t(code_alpha3, is_active, updated_at)
          ON CONFLICT (code_alpha3)
          DO UPDATE SET is_active = TRUE, updated_at = NOW()
        `;
        await sql`
          INSERT INTO country_translations (country_id, locale, name)
          SELECT c.id, 'en', t.name
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.codeAlpha3))}::text[],
            ${sql.array(chunk.map((r) => r.name))}::text[]
          ) AS t(code_alpha3, name)
          JOIN countries c ON c.code_alpha3 = t.code_alpha3
          ON CONFLICT (country_id, locale)
          DO UPDATE SET name = EXCLUDED.name
        `;
        await sql`
          INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
          SELECT 'country', c.id, t.alias, 'en', 'common', TRUE, 'pending', 'imported', 'api_football'
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.codeAlpha3))}::text[],
            ${sql.array(chunk.map((r) => r.name))}::text[]
          ) AS t(code_alpha3, alias)
          JOIN countries c ON c.code_alpha3 = t.code_alpha3
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

      const seasonList = Array.from(seasons.values());
      for (let i = 0; i < seasonList.length; i += BATCH_SIZE) {
        const chunk = seasonList.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO seasons (slug, start_date, end_date, is_current)
          SELECT * FROM UNNEST(
            ${sql.array(chunk.map((r) => r.slug))}::text[],
            ${sql.array(chunk.map((r) => r.startDate))}::date[],
            ${sql.array(chunk.map((r) => r.endDate))}::date[],
            ${sql.array(chunk.map(() => false))}::bool[]
          ) AS t(slug, start_date, end_date, is_current)
          ON CONFLICT (slug)
          DO UPDATE SET
            start_date = LEAST(seasons.start_date, EXCLUDED.start_date),
            end_date = GREATEST(seasons.end_date, EXCLUDED.end_date),
            is_current = EXCLUDED.is_current
        `;
      }

      const competitionList = Array.from(competitions.values());
      for (let i = 0; i < competitionList.length; i += BATCH_SIZE) {
        const chunk = competitionList.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO competitions (slug, code, comp_type, gender, is_youth, is_international, country_id, is_active, updated_at)
          SELECT t.slug, t.code, t.comp_type, t.gender, t.is_youth, t.is_international, c.id, TRUE, NOW()
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.slug))}::text[],
            ${sql.array(chunk.map((r) => r.code))}::text[],
            ${sql.array(chunk.map((r) => r.compType))}::competition_type[],
            ${sql.array(chunk.map((r) => r.gender))}::competition_gender[],
            ${sql.array(chunk.map((r) => r.isYouth))}::bool[],
            ${sql.array(chunk.map((r) => r.isInternational))}::bool[],
            ${sql.array(chunk.map((r) => r.countryCode))}::text[]
          ) AS t(slug, code, comp_type, gender, is_youth, is_international, country_code)
          LEFT JOIN countries c ON c.code_alpha3 = t.country_code
          ON CONFLICT (slug)
          DO UPDATE SET
            code = EXCLUDED.code,
            comp_type = EXCLUDED.comp_type,
            gender = EXCLUDED.gender,
            is_youth = EXCLUDED.is_youth,
            is_international = EXCLUDED.is_international,
            country_id = EXCLUDED.country_id,
            is_active = TRUE,
            updated_at = NOW()
        `;
        await sql`
          INSERT INTO competition_translations (competition_id, locale, name, short_name)
          SELECT c.id, 'en', t.name, t.short_name
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.slug))}::text[],
            ${sql.array(chunk.map((r) => r.name))}::text[],
            ${sql.array(chunk.map((r) => r.shortName))}::text[]
          ) AS t(slug, name, short_name)
          JOIN competitions c ON c.slug = t.slug
          ON CONFLICT (competition_id, locale)
          DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
        `;
        await sql`
          INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
          SELECT 'competition', c.id, t.alias, 'en', 'common', TRUE, 'pending', 'imported', 'api_football'
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.slug))}::text[],
            ${sql.array(chunk.map((r) => r.name))}::text[]
          ) AS t(slug, alias)
          JOIN competitions c ON c.slug = t.slug
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

      const teamList = Array.from(teams.values());
      for (let i = 0; i < teamList.length; i += BATCH_SIZE) {
        const chunk = teamList.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO teams (slug, country_id, gender, is_national, crest_url, is_active, updated_at)
          SELECT t.slug, c.id, t.gender, t.is_national, t.crest_url, TRUE, NOW()
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.slug))}::text[],
            ${sql.array(chunk.map((r) => r.countryCode))}::text[],
            ${sql.array(chunk.map((r) => r.gender))}::competition_gender[],
            ${sql.array(chunk.map((r) => r.isNational))}::bool[],
            ${sql.array(chunk.map((r) => r.crestUrl))}::text[]
          ) AS t(slug, country_code, gender, is_national, crest_url)
          LEFT JOIN countries c ON c.code_alpha3 = t.country_code
          ON CONFLICT (slug)
          DO UPDATE SET
            country_id = EXCLUDED.country_id,
            gender = EXCLUDED.gender,
            is_national = EXCLUDED.is_national,
            crest_url = COALESCE(EXCLUDED.crest_url, teams.crest_url),
            is_active = TRUE,
            updated_at = NOW()
        `;
        await sql`
          INSERT INTO team_translations (team_id, locale, name, short_name)
          SELECT t2.id, 'en', t.name, t.short_name
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.slug))}::text[],
            ${sql.array(chunk.map((r) => r.name))}::text[],
            ${sql.array(chunk.map((r) => r.shortName))}::text[]
          ) AS t(slug, name, short_name)
          JOIN teams t2 ON t2.slug = t.slug
          ON CONFLICT (team_id, locale)
          DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
        `;
        await sql`
          INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
          SELECT 'team', t2.id, t.alias, 'en', 'common', TRUE, 'pending', 'imported', 'api_football'
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.slug))}::text[],
            ${sql.array(chunk.map((r) => r.name))}::text[]
          ) AS t(slug, alias)
          JOIN teams t2 ON t2.slug = t.slug
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
        const lookupAliasRows = chunk.flatMap((r) =>
          [...new Set(
            createTeamLookupKeys(r.name)
              .map((alias) => alias.trim())
              .filter((alias) => alias.length > 0)
          )].map((alias) => ({ slug: r.slug, alias }))
        );
        if (lookupAliasRows.length > 0) {
          await sql`
            INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
          SELECT 'team', t2.id, t.alias, 'en', 'common', FALSE, 'pending', 'imported', 'api_football'
            FROM UNNEST(
              ${sql.array(lookupAliasRows.map((r) => r.slug))}::text[],
              ${sql.array(lookupAliasRows.map((r) => r.alias))}::text[]
            ) AS t(slug, alias)
            JOIN teams t2 ON t2.slug = t.slug
            ON CONFLICT (entity_type, entity_id, alias_normalized)
            DO UPDATE SET
              locale = EXCLUDED.locale,
              alias_kind = EXCLUDED.alias_kind,
              source_type = EXCLUDED.source_type,
              source_ref = EXCLUDED.source_ref
            WHERE entity_aliases.status <> 'approved'
          `;
        }
      }

      const competitionSeasonList = Array.from(competitionSeasons.values());
      for (let i = 0; i < competitionSeasonList.length; i += BATCH_SIZE) {
        const chunk = competitionSeasonList.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO competition_seasons (
            competition_id, season_id, format_type, current_matchday, total_matchdays,
            source_metadata, status, updated_at
          )
          SELECT comp.id, s.id, t.format_type, t.current_matchday, t.total_matchdays,
            t.source_metadata::jsonb, t.status, NOW()
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.competitionSlug))}::text[],
            ${sql.array(chunk.map((r) => r.seasonSlug))}::text[],
            ${sql.array(chunk.map((r) => r.formatType))}::competition_format_type[],
            ${sql.array(chunk.map((r) => r.currentMatchday))}::int[],
            ${sql.array(chunk.map((r) => r.totalMatchdays))}::int[],
            ${sql.array(chunk.map((r) => JSON.stringify(r.sourceMetadata)))}::text[],
            ${sql.array(chunk.map((r) => r.status))}::text[]
          ) AS t(competition_slug, season_slug, format_type, current_matchday, total_matchdays, source_metadata, status)
          JOIN competitions comp ON comp.slug = t.competition_slug
          JOIN seasons s ON s.slug = t.season_slug
          ON CONFLICT (competition_id, season_id)
          DO UPDATE SET
            format_type = EXCLUDED.format_type,
            current_matchday = EXCLUDED.current_matchday,
            total_matchdays = EXCLUDED.total_matchdays,
            source_metadata = EXCLUDED.source_metadata,
            status = EXCLUDED.status,
            updated_at = NOW()
        `;
      }

      for (const competitionSeason of competitionSeasons.values()) {
        await cleanupCompetitionSeasonMatches(sql, competitionSeason.competitionSlug, competitionSeason.seasonSlug);
        await cleanupCompetitionSeasonTeamSeasons(sql, competitionSeason.competitionSlug, competitionSeason.seasonSlug);
      }

      const teamSeasonParsed = Array.from(teamSeasonKeys).map((key) => {
        const [competitionSlug, seasonSlug, teamSlug] = key.split(':');
        return { competitionSlug, seasonSlug, teamSlug };
      });
      for (let i = 0; i < teamSeasonParsed.length; i += BATCH_SIZE) {
        const chunk = teamSeasonParsed.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO team_seasons (team_id, competition_season_id, updated_at)
          SELECT tm.id, cs.id, NOW()
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.competitionSlug))}::text[],
            ${sql.array(chunk.map((r) => r.seasonSlug))}::text[],
            ${sql.array(chunk.map((r) => r.teamSlug))}::text[]
          ) AS t(competition_slug, season_slug, team_slug)
          JOIN teams tm ON tm.slug = t.team_slug
          JOIN competitions comp ON comp.slug = t.competition_slug
          JOIN seasons s ON s.slug = t.season_slug
          JOIN competition_seasons cs ON cs.competition_id = comp.id AND cs.season_id = s.id
          ON CONFLICT (team_id, competition_season_id)
          DO UPDATE SET updated_at = NOW()
        `;
      }

      for (let i = 0; i < matches.length; i += BATCH_SIZE) {
        const chunk = matches.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO matches (
            id, match_date, competition_season_id, matchday, stage, group_name,
            home_team_id, away_team_id, home_score, away_score, status, kickoff_at,
            source_metadata, updated_at
          )
          SELECT
            t.match_id, t.match_date, cs.id, t.matchday, t.stage, t.group_name,
            home_tm.id, away_tm.id, t.home_score, t.away_score, t.status, t.kickoff_at,
            t.source_metadata::jsonb, NOW()
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.matchId))}::int[],
            ${sql.array(chunk.map((r) => r.matchDate))}::date[],
            ${sql.array(chunk.map((r) => r.competitionSlug))}::text[],
            ${sql.array(chunk.map((r) => r.seasonSlug))}::text[],
            ${sql.array(chunk.map((r) => r.homeTeamSlug))}::text[],
            ${sql.array(chunk.map((r) => r.awayTeamSlug))}::text[],
            ${sql.array(chunk.map((r) => r.homeScore))}::int[],
            ${sql.array(chunk.map((r) => r.awayScore))}::int[],
            ${sql.array(chunk.map((r) => r.matchWeek))}::int[],
            ${sql.array(chunk.map((r) => r.stage))}::text[],
            ${sql.array(chunk.map((r) => r.groupName))}::text[],
            ${sql.array(chunk.map((r) => r.status))}::match_status[],
            ${sql.array(chunk.map((r) => r.kickoffAt))}::timestamptz[],
            ${sql.array(chunk.map((r) => JSON.stringify(r.sourceMetadata)))}::text[]
          ) AS t(match_id, match_date, competition_slug, season_slug,
                 home_team_slug, away_team_slug, home_score, away_score,
                 matchday, stage, group_name, status, kickoff_at, source_metadata)
          JOIN competitions comp ON comp.slug = t.competition_slug
          JOIN seasons s ON s.slug = t.season_slug
          JOIN competition_seasons cs ON cs.competition_id = comp.id AND cs.season_id = s.id
          JOIN teams home_tm ON home_tm.slug = t.home_team_slug
          JOIN teams away_tm ON away_tm.slug = t.away_team_slug
          ON CONFLICT (id, match_date)
          DO UPDATE SET
            competition_season_id = EXCLUDED.competition_season_id,
            matchday = EXCLUDED.matchday,
            stage = EXCLUDED.stage,
            group_name = EXCLUDED.group_name,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            status = EXCLUDED.status,
            kickoff_at = EXCLUDED.kickoff_at,
            source_metadata = EXCLUDED.source_metadata,
            updated_at = NOW()
        `;
      }

      for (let i = 0; i < competitionMappings.length; i += BATCH_SIZE) {
        const chunk = competitionMappings.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata, updated_at)
          SELECT 'competition', c.id, ${sourceId}, t.external_id, t.external_code, NULL, '{"source":"api_football"}'::jsonb, NOW()
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.slug))}::text[],
            ${sql.array(chunk.map((r) => r.externalId))}::text[],
            ${sql.array(chunk.map((r) => r.externalCode))}::text[]
          ) AS t(slug, external_id, external_code)
          JOIN competitions c ON c.slug = t.slug
          ON CONFLICT (entity_type, source_id, external_id)
          DO UPDATE SET
            entity_id = EXCLUDED.entity_id,
            external_code = EXCLUDED.external_code,
            season_context = EXCLUDED.season_context,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        `;
      }

      for (let i = 0; i < uniqueTeamMappings.length; i += BATCH_SIZE) {
        const chunk = uniqueTeamMappings.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata, updated_at)
          SELECT 'team', tm.id, ${sourceId}, t.external_id, NULL, t.season_context, '{"source":"api_football"}'::jsonb, NOW()
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.slug))}::text[],
            ${sql.array(chunk.map((r) => r.externalId))}::text[],
            ${sql.array(chunk.map((r) => r.seasonContext))}::text[]
          ) AS t(slug, external_id, season_context)
          JOIN teams tm ON tm.slug = t.slug
          ON CONFLICT (entity_type, source_id, external_id)
          DO UPDATE SET
            entity_id = EXCLUDED.entity_id,
            external_code = EXCLUDED.external_code,
            season_context = EXCLUDED.season_context,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        `;
      }

      for (let i = 0; i < matchMappings.length; i += BATCH_SIZE) {
        const chunk = matchMappings.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata, updated_at)
          SELECT 'match', m.id, ${sourceId}, t.external_id, NULL, t.season_context,
            jsonb_build_object('source', 'api_football', 'matchDate', t.match_date), NOW()
          FROM UNNEST(
            ${sql.array(chunk.map((r) => r.matchId))}::int[],
            ${sql.array(chunk.map((r) => r.matchDate))}::date[],
            ${sql.array(chunk.map((r) => r.externalId))}::text[],
            ${sql.array(chunk.map((r) => r.seasonContext))}::text[]
          ) AS t(match_id, match_date, external_id, season_context)
          JOIN matches m ON m.id = t.match_id AND m.match_date = t.match_date
          ON CONFLICT (entity_type, source_id, external_id)
          DO UPDATE SET
            entity_id = EXCLUDED.entity_id,
            external_code = EXCLUDED.external_code,
            season_context = EXCLUDED.season_context,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        `;
      }

      await refreshDerivedViews(sql);
      await sql`COMMIT`;
    } catch (error) {
      await sql`ROLLBACK`;
      throw error;
    }

    await sql.end({ timeout: 1 });
    return summary;
  } catch (error) {
    await sql.end({ timeout: 1 }).catch(() => undefined);
    throw error;
  }
}
