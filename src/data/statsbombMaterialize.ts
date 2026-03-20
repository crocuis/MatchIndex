import postgres, { type Sql } from 'postgres';

const BATCH_SIZE = 500;
import { deriveCompetitionSeasonFormat, type CompetitionSeasonFormatType } from './competitionFormats.ts';
import { loadCountryCodeResolver, type CountryCodeResolver } from './countryCodeResolver.ts';
import { createTeamLookupKeys } from './teamLookupKeys.ts';
import type { StatsBombCompetitionEntry, StatsBombMatchEntry } from './statsbomb.ts';

interface CompetitionDraft {
  slug: string;
  code: string;
  name: string;
  shortName: string;
  countryCode: string;
  gender: 'male' | 'female' | 'mixed';
  isYouth: boolean;
  isInternational: boolean;
  compType: 'league' | 'international';
}

interface SeasonDraft {
  slug: string;
  startDate: string;
  endDate: string;
}

interface CountryDraft {
  codeAlpha3: string;
  name: string;
}

interface VenueDraft {
  slug: string;
  name: string;
  countryCode: string;
}

interface TeamDraft {
  slug: string;
  name: string;
  shortName: string;
  countryCode: string;
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
  sourceMatchUpdatedAt: string | null;
  sourceMatchAvailableAt: string | null;
  sourceMatchUpdated360At: string | null;
  sourceMatchAvailable360At: string | null;
}

interface MatchDraft {
  matchId: number;
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
  venueSlug: string | null;
  refereeName: string | null;
  sourceLastUpdatedAt: string | null;
  sourceLastUpdated360At: string | null;
  sourceDataVersion: string | null;
  sourceShotFidelityVersion: string | null;
  sourceXyFidelityVersion: string | null;
}

interface ExistingMatchLookupRow {
  id: number;
  match_date: string;
  stage: string | null;
  group_name: string | null;
  home_slug: string;
  away_slug: string;
}

interface ExistingTeamLookupRow {
  slug: string;
  name: string | null;
  code_alpha3: string | null;
}

interface TeamLookupEntry {
  slug: string;
  codeAlpha3: string | null;
}

type AliasEntityType = 'competition' | 'team' | 'country' | 'venue';

export interface MaterializeStatsBombCoreOptions {
  dryRun?: boolean;
  competitionLimit?: number;
  matchesPerSeasonLimit?: number;
}

export interface MaterializeStatsBombCoreSummary {
  dryRun: boolean;
  countries: number;
  competitions: number;
  seasons: number;
  venues: number;
  teams: number;
  competitionSeasons: number;
  teamSeasons: number;
  matches: number;
}

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

function createShortName(name: string, maxLength: number = 24) {
  return name.length <= maxLength ? name : `${name.slice(0, maxLength - 1).trimEnd()}.`;
}

function createCompetitionCode(sourceCompetitionId: number) {
  return `sb${String(sourceCompetitionId)}`;
}

function addDays(date: string, days: number) {
  const value = new Date(`${date}T00:00:00Z`);
  value.setUTCDate(value.getUTCDate() + days);
  return value.toISOString().slice(0, 10);
}

function inferSeasonBounds(seasonName: string) {
  const yearRange = seasonName.match(/(\d{4})\D+(\d{4})/);
  if (yearRange) {
    const startYear = yearRange[1];
    const endYear = yearRange[2];
    return {
      startDate: `${startYear}-07-01`,
      endDate: `${endYear}-06-30`,
    };
  }

  const singleYear = seasonName.match(/\d{4}/)?.[0];
  if (singleYear) {
    return {
      startDate: `${singleYear}-01-01`,
      endDate: `${singleYear}-12-31`,
    };
  }

  return {
    startDate: '2000-01-01',
    endDate: '2000-12-31',
  };
}

function normalizeCompetitionType(entry: StatsBombCompetitionEntry): 'league' | 'international' {
  return entry.competition_international ? 'international' : 'league';
}

function normalizeMatchStatus(status?: string | null) {
  if (status === 'available') {
    return 'finished';
  }

  return 'scheduled';
}

function buildKickoffAt(matchDate: string, kickOff: string) {
  if (!kickOff) {
    return null;
  }

  return `${matchDate}T${kickOff}Z`;
}

function buildCompetitionDraft(
  countryCodeResolver: CountryCodeResolver,
  entry: StatsBombCompetitionEntry,
  helpers: typeof import('./statsbomb'),
): CompetitionDraft {
  return {
    slug: helpers.createCompetitionSlug(entry),
    code: createCompetitionCode(entry.competition_id),
    name: entry.competition_name,
    shortName: createShortName(entry.competition_name, 20),
    countryCode: countryCodeResolver.resolve(helpers.createCountryCode(entry.country_name)) ?? 'ZZZ',
    gender: entry.competition_gender,
    isYouth: entry.competition_youth,
    isInternational: entry.competition_international,
    compType: normalizeCompetitionType(entry),
  };
}

function buildSeasonDraft(entry: StatsBombCompetitionEntry, matches: StatsBombMatchEntry[], helpers: typeof import('./statsbomb')): SeasonDraft {
  const inferredBounds = inferSeasonBounds(entry.season_name);

  return {
    slug: helpers.createSeasonSlug(entry.season_name, entry.season_id),
    startDate: inferredBounds.startDate,
    endDate: inferredBounds.endDate,
  };
}

function buildCountryDraft(
  countryCodeResolver: CountryCodeResolver,
  name: string,
  helpers: typeof import('./statsbomb'),
): CountryDraft {
  return {
    codeAlpha3: countryCodeResolver.resolve(helpers.createCountryCode(name)) ?? 'ZZZ',
    name,
  };
}

function buildVenueDraft(
  countryCodeResolver: CountryCodeResolver,
  match: StatsBombMatchEntry,
  helpers: typeof import('./statsbomb'),
): VenueDraft | null {
  if (!match.stadium?.name) {
    return null;
  }

  return {
    slug: helpers.createStatsBombSlug(match.stadium.name),
    name: match.stadium.name,
    countryCode: countryCodeResolver.resolve(helpers.createCountryCode(match.stadium.country?.name ?? match.competition.country_name)) ?? 'ZZZ',
  };
}

function buildTeamDraft(
  countryCodeResolver: CountryCodeResolver,
  params: {
    name: string;
    countryName?: string;
    gender?: 'male' | 'female';
    isNational: boolean;
  },
  helpers: typeof import('./statsbomb')
): TeamDraft {
  return {
    slug: helpers.createTeamSlug(params.name, params.isNational ? undefined : params.countryName),
    name: params.name,
    shortName: createShortName(params.name, 18),
    countryCode: countryCodeResolver.resolve(helpers.createCountryCode(params.countryName ?? params.name)) ?? 'ZZZ',
    gender: params.gender ?? 'mixed',
    isNational: params.isNational,
  };
}

function buildCompetitionSeasonDraft(entry: StatsBombCompetitionEntry, matches: StatsBombMatchEntry[], helpers: typeof import('./statsbomb')): CompetitionSeasonDraft {
  const matchWeeks = matches.map((match) => match.match_week).filter((value): value is number => value !== null);

  return {
    competitionSlug: helpers.createCompetitionSlug(entry),
    seasonSlug: helpers.createSeasonSlug(entry.season_name, entry.season_id),
    formatType: deriveCompetitionSeasonFormat({
      competitionSlug: helpers.createCompetitionSlug(entry),
      compType: entry.competition_international ? 'international' : 'league',
      seasonStartDate: entry.season_name,
    }),
    currentMatchday: matchWeeks.length > 0 ? Math.max(...matchWeeks) : null,
    totalMatchdays: matchWeeks.length > 0 ? Math.max(...matchWeeks) : null,
    status: 'active',
    sourceMatchUpdatedAt: entry.match_updated,
    sourceMatchAvailableAt: entry.match_available,
    sourceMatchUpdated360At: entry.match_updated_360,
    sourceMatchAvailable360At: entry.match_available_360,
  };
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
        t.slug,
        tt.name,
        c.code_alpha3
      FROM teams t
      LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
      LEFT JOIN countries c ON c.id = t.country_id
      UNION ALL
      SELECT
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
      codeAlpha3: countryCodeResolver.resolve(row.code_alpha3 ?? '') ?? row.code_alpha3,
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

function buildMatchDraft(
  match: StatsBombMatchEntry,
  competitionEntry: StatsBombCompetitionEntry,
  helpers: typeof import('./statsbomb'),
  existingMatchLookup: Map<string, number>,
): MatchDraft {
  const homeCountryName = match.home_team.country?.name ?? competitionEntry.country_name;
  const awayCountryName = match.away_team.country?.name ?? competitionEntry.country_name;
  const competitionSlug = helpers.createCompetitionSlug(competitionEntry);
  const seasonSlug = helpers.createSeasonSlug(match.season.season_name, match.season.season_id);
  const stage = match.competition_stage?.name ?? 'REGULAR_SEASON';
  const groupName = match.home_team.home_team_group ?? match.away_team.away_team_group ?? null;
  const homeTeamSlug = helpers.createTeamSlug(
    match.home_team.home_team_name,
    competitionEntry.competition_international ? undefined : homeCountryName,
  );
  const awayTeamSlug = helpers.createTeamSlug(
    match.away_team.away_team_name,
    competitionEntry.competition_international ? undefined : awayCountryName,
  );
  const matchId = existingMatchLookup.get(
    buildMatchLookupKey(match.match_date, stage, groupName, homeTeamSlug, awayTeamSlug),
  ) ?? match.match_id;

  return {
    matchId,
    matchDate: match.match_date,
    competitionSlug,
    seasonSlug,
    homeTeamSlug,
    awayTeamSlug,
    homeScore: match.home_score,
    awayScore: match.away_score,
    matchWeek: match.match_week,
    stage,
    groupName,
    status: normalizeMatchStatus(match.match_status),
    kickoffAt: buildKickoffAt(match.match_date, match.kick_off),
    venueSlug: match.stadium?.name ? helpers.createStatsBombSlug(match.stadium.name) : null,
    refereeName: match.referee?.name ?? null,
    sourceLastUpdatedAt: match.last_updated,
    sourceLastUpdated360At: match.last_updated_360,
    sourceDataVersion: match.metadata?.data_version ?? null,
    sourceShotFidelityVersion: match.metadata?.shot_fidelity_version ?? null,
    sourceXyFidelityVersion: match.metadata?.xy_fidelity_version ?? null,
  };
}

function canonicalizeTeamDraft(
  teamDraft: TeamDraft,
  lookup: Map<string, TeamLookupEntry[]>,
) {
  const canonicalSlug = resolveCanonicalTeamSlug(lookup, teamDraft.name, teamDraft.countryCode);
  return canonicalSlug ? { ...teamDraft, slug: canonicalSlug } : teamDraft;
}

async function upsertCountry(sql: Sql, draft: CountryDraft) {
  await sql`
    INSERT INTO countries (code_alpha3, is_active, updated_at)
    VALUES (${draft.codeAlpha3}, TRUE, NOW())
    ON CONFLICT (code_alpha3)
    DO UPDATE SET updated_at = NOW(), is_active = TRUE
  `;

  await sql`
    INSERT INTO country_translations (country_id, locale, name)
    VALUES ((SELECT id FROM countries WHERE code_alpha3 = ${draft.codeAlpha3}), 'en', ${draft.name})
    ON CONFLICT (country_id, locale)
    DO UPDATE SET name = EXCLUDED.name
  `;

  await upsertEntityAlias(sql, 'country', sql`SELECT id FROM countries WHERE code_alpha3 = ${draft.codeAlpha3}`, draft.name);
}

async function upsertSeason(sql: Sql, draft: SeasonDraft, isCurrent: boolean) {
  await sql`
    INSERT INTO seasons (slug, start_date, end_date, is_current)
    VALUES (${draft.slug}, ${draft.startDate}, ${draft.endDate}, ${isCurrent})
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
      (SELECT id FROM countries WHERE code_alpha3 = ${draft.countryCode}),
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

async function upsertVenue(sql: Sql, draft: VenueDraft) {
  await sql`
    INSERT INTO venues (slug, country_id, updated_at)
    VALUES (${draft.slug}, (SELECT id FROM countries WHERE code_alpha3 = ${draft.countryCode}), NOW())
    ON CONFLICT (slug)
    DO UPDATE SET country_id = EXCLUDED.country_id, updated_at = NOW()
  `;

  await sql`
    INSERT INTO venue_translations (venue_id, locale, name)
    VALUES ((SELECT id FROM venues WHERE slug = ${draft.slug}), 'en', ${draft.name})
    ON CONFLICT (venue_id, locale)
    DO UPDATE SET name = EXCLUDED.name
  `;

  await upsertEntityAlias(sql, 'venue', sql`SELECT id FROM venues WHERE slug = ${draft.slug}`, draft.name);
}

async function upsertTeam(sql: Sql, draft: TeamDraft) {
  await sql`
    INSERT INTO teams (slug, country_id, gender, is_national, is_active, updated_at)
    VALUES (
      ${draft.slug},
      (SELECT id FROM countries WHERE code_alpha3 = ${draft.countryCode}),
      ${draft.gender},
      ${draft.isNational},
      TRUE,
      NOW()
    )
    ON CONFLICT (slug)
    DO UPDATE SET
      country_id = EXCLUDED.country_id,
      gender = EXCLUDED.gender,
      is_national = EXCLUDED.is_national,
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
}

async function upsertCompetitionSeason(sql: Sql, draft: CompetitionSeasonDraft) {
  await sql`
    INSERT INTO competition_seasons (
      competition_id,
      season_id,
      format_type,
      current_matchday,
      total_matchdays,
      source_match_updated_at,
      source_match_available_at,
      source_match_updated_360_at,
      source_match_available_360_at,
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
      ${draft.sourceMatchUpdatedAt},
      ${draft.sourceMatchAvailableAt},
      ${draft.sourceMatchUpdated360At},
      ${draft.sourceMatchAvailable360At},
      ${JSON.stringify({ source: 'statsbomb_open_data' })}::jsonb,
      ${draft.status},
      NOW()
    )
    ON CONFLICT (competition_id, season_id)
    DO UPDATE SET
      format_type = EXCLUDED.format_type,
      current_matchday = EXCLUDED.current_matchday,
      total_matchdays = EXCLUDED.total_matchdays,
      source_match_updated_at = EXCLUDED.source_match_updated_at,
      source_match_available_at = EXCLUDED.source_match_available_at,
      source_match_updated_360_at = EXCLUDED.source_match_updated_360_at,
      source_match_available_360_at = EXCLUDED.source_match_available_360_at,
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
      venue_id,
      referee,
      source_last_updated_at,
      source_last_updated_360_at,
      source_data_version,
      source_shot_fidelity_version,
      source_xy_fidelity_version,
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
      ${draft.venueSlug ? sql`(SELECT id FROM venues WHERE slug = ${draft.venueSlug})` : sql`NULL`},
      ${draft.refereeName},
      ${draft.sourceLastUpdatedAt},
      ${draft.sourceLastUpdated360At},
      ${draft.sourceDataVersion},
      ${draft.sourceShotFidelityVersion},
      ${draft.sourceXyFidelityVersion},
      ${JSON.stringify({ source: 'statsbomb_open_data' })}::jsonb,
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
      venue_id = EXCLUDED.venue_id,
      referee = EXCLUDED.referee,
      source_last_updated_at = EXCLUDED.source_last_updated_at,
      source_last_updated_360_at = EXCLUDED.source_last_updated_360_at,
      source_data_version = EXCLUDED.source_data_version,
      source_shot_fidelity_version = EXCLUDED.source_shot_fidelity_version,
      source_xy_fidelity_version = EXCLUDED.source_xy_fidelity_version,
      source_metadata = EXCLUDED.source_metadata,
      updated_at = NOW()
  `;
}

export async function materializeStatsBombCore(
  options: MaterializeStatsBombCoreOptions = {}
): Promise<MaterializeStatsBombCoreSummary> {
  const sql = getMaterializeDb();
  const countryCodeResolver = await loadCountryCodeResolver(sql);
  const existingTeamLookup = await loadExistingTeamLookup(sql, countryCodeResolver);
  const helpers = await loadStatsBombModule();
  const competitionEntries = await helpers.fetchStatsBombJson<StatsBombCompetitionEntry[]>('data/competitions.json');
  const limitedCompetitionEntries = competitionEntries.slice(0, options.competitionLimit ?? competitionEntries.length);

  const countries = new Map<string, CountryDraft>();
  const competitions = new Map<string, CompetitionDraft>();
  const seasons = new Map<string, SeasonDraft>();
  const venues = new Map<string, VenueDraft>();
  const teams = new Map<string, TeamDraft>();
  const competitionSeasons = new Map<string, CompetitionSeasonDraft>();
  const teamSeasonKeys = new Set<string>();
  const matches: MatchDraft[] = [];

  for (const competitionEntry of limitedCompetitionEntries) {
    const matchEntries = await helpers.fetchStatsBombJson<StatsBombMatchEntry[]>(
      `data/matches/${competitionEntry.competition_id}/${competitionEntry.season_id}.json`
    );
    const limitedMatchEntries = matchEntries.slice(0, options.matchesPerSeasonLimit ?? matchEntries.length);

    const competitionDraft = buildCompetitionDraft(countryCodeResolver, competitionEntry, helpers);
    const seasonDraft = buildSeasonDraft(competitionEntry, limitedMatchEntries, helpers);
    const existingMatchLookup = await loadExistingMatchLookup(sql, competitionDraft.slug, seasonDraft.slug);

    competitions.set(competitionDraft.slug, competitionDraft);
    seasons.set(seasonDraft.slug, seasonDraft);
    countries.set(competitionDraft.countryCode, buildCountryDraft(countryCodeResolver, competitionEntry.country_name, helpers));
    competitionSeasons.set(
      `${competitionDraft.slug}:${seasonDraft.slug}`,
      buildCompetitionSeasonDraft(competitionEntry, limitedMatchEntries, helpers)
    );

    for (const matchEntry of limitedMatchEntries) {
      const homeCountryName = matchEntry.home_team.country?.name ?? competitionEntry.country_name;
      const awayCountryName = matchEntry.away_team.country?.name ?? competitionEntry.country_name;
      const homeTeam = canonicalizeTeamDraft(buildTeamDraft(countryCodeResolver, {
        name: matchEntry.home_team.home_team_name,
        countryName: homeCountryName,
        gender: matchEntry.home_team.home_team_gender,
        isNational: competitionEntry.competition_international,
      }, helpers), existingTeamLookup);
      const awayTeam = canonicalizeTeamDraft(buildTeamDraft(countryCodeResolver, {
        name: matchEntry.away_team.away_team_name,
        countryName: awayCountryName,
        gender: matchEntry.away_team.away_team_gender,
        isNational: competitionEntry.competition_international,
      }, helpers), existingTeamLookup);

      const homeCountryCode = countryCodeResolver.resolve(helpers.createCountryCode(homeCountryName)) ?? 'ZZZ';
      const awayCountryCode = countryCodeResolver.resolve(helpers.createCountryCode(awayCountryName)) ?? 'ZZZ';
      countries.set(homeCountryCode, buildCountryDraft(countryCodeResolver, homeCountryName, helpers));
      countries.set(awayCountryCode, buildCountryDraft(countryCodeResolver, awayCountryName, helpers));
      registerTeamLookupEntry(existingTeamLookup, homeTeam.name, { slug: homeTeam.slug, codeAlpha3: homeTeam.countryCode });
      registerTeamLookupEntry(existingTeamLookup, awayTeam.name, { slug: awayTeam.slug, codeAlpha3: awayTeam.countryCode });
      teams.set(homeTeam.slug, homeTeam);
      teams.set(awayTeam.slug, awayTeam);
      teamSeasonKeys.add(`${competitionDraft.slug}:${seasonDraft.slug}:${homeTeam.slug}`);
      teamSeasonKeys.add(`${competitionDraft.slug}:${seasonDraft.slug}:${awayTeam.slug}`);

      const venueDraft = buildVenueDraft(countryCodeResolver, matchEntry, helpers);
      if (venueDraft) {
        venues.set(venueDraft.slug, venueDraft);
        countries.set(
          venueDraft.countryCode,
          buildCountryDraft(countryCodeResolver, matchEntry.stadium?.country?.name ?? competitionEntry.country_name, helpers),
        );
      }

      matches.push(buildMatchDraft(matchEntry, competitionEntry, helpers, existingMatchLookup));
    }
  }

  const seasonDrafts = Array.from(seasons.values()).sort((left, right) => left.endDate.localeCompare(right.endDate));
  const currentSeasonSlug = seasonDrafts.at(-1)?.slug ?? null;
  const summary = {
    dryRun: options.dryRun ?? false,
    countries: countries.size,
    competitions: competitions.size,
    seasons: seasons.size,
    venues: venues.size,
    teams: teams.size,
    competitionSeasons: competitionSeasons.size,
    teamSeasons: teamSeasonKeys.size,
    matches: matches.length,
  } satisfies MaterializeStatsBombCoreSummary;

  if (options.dryRun ?? false) {
    return summary;
  }

  await sql`BEGIN`;

  try {
    await sql`UPDATE seasons SET is_current = FALSE WHERE is_current = TRUE`;

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
        DO UPDATE SET updated_at = NOW(), is_active = TRUE
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
        SELECT 'country', c.id, t.alias, 'en', 'common', TRUE, 'pending', 'imported', 'statsbomb_open_data'
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
          ${sql.array(chunk.map((r) => r.slug === currentSeasonSlug))}::bool[]
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
        JOIN countries c ON c.code_alpha3 = t.country_code
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
        SELECT 'competition', c.id, t.alias, 'en', 'common', TRUE, 'pending', 'imported', 'statsbomb_open_data'
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

    const venueList = Array.from(venues.values());
    for (let i = 0; i < venueList.length; i += BATCH_SIZE) {
      const chunk = venueList.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO venues (slug, country_id, updated_at)
        SELECT t.slug, c.id, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.slug))}::text[],
          ${sql.array(chunk.map((r) => r.countryCode))}::text[]
        ) AS t(slug, country_code)
        JOIN countries c ON c.code_alpha3 = t.country_code
        ON CONFLICT (slug)
        DO UPDATE SET country_id = EXCLUDED.country_id, updated_at = NOW()
      `;
      await sql`
        INSERT INTO venue_translations (venue_id, locale, name)
        SELECT v.id, 'en', t.name
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.slug))}::text[],
          ${sql.array(chunk.map((r) => r.name))}::text[]
        ) AS t(slug, name)
        JOIN venues v ON v.slug = t.slug
        ON CONFLICT (venue_id, locale)
        DO UPDATE SET name = EXCLUDED.name
      `;
      await sql`
        INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
        SELECT 'venue', v.id, t.alias, 'en', 'common', TRUE, 'pending', 'imported', 'statsbomb_open_data'
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.slug))}::text[],
          ${sql.array(chunk.map((r) => r.name))}::text[]
        ) AS t(slug, alias)
        JOIN venues v ON v.slug = t.slug
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
        INSERT INTO teams (slug, country_id, gender, is_national, is_active, updated_at)
        SELECT t.slug, c.id, t.gender, t.is_national, TRUE, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.slug))}::text[],
          ${sql.array(chunk.map((r) => r.countryCode))}::text[],
          ${sql.array(chunk.map((r) => r.gender))}::competition_gender[],
          ${sql.array(chunk.map((r) => r.isNational))}::bool[]
        ) AS t(slug, country_code, gender, is_national)
        JOIN countries c ON c.code_alpha3 = t.country_code
        ON CONFLICT (slug)
        DO UPDATE SET
          country_id = EXCLUDED.country_id,
          gender = EXCLUDED.gender,
          is_national = EXCLUDED.is_national,
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
        SELECT 'team', t2.id, t.alias, 'en', 'common', TRUE, 'pending', 'imported', 'statsbomb_open_data'
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
    }

    const competitionSeasonList = Array.from(competitionSeasons.values());
    for (let i = 0; i < competitionSeasonList.length; i += BATCH_SIZE) {
      const chunk = competitionSeasonList.slice(i, i + BATCH_SIZE);
      await sql`
        INSERT INTO competition_seasons (
          competition_id, season_id, format_type, current_matchday, total_matchdays,
          source_match_updated_at, source_match_available_at,
          source_match_updated_360_at, source_match_available_360_at,
          source_metadata, status, updated_at
        )
        SELECT comp.id, s.id, t.format_type, t.current_matchday, t.total_matchdays,
          t.source_match_updated_at, t.source_match_available_at,
          t.source_match_updated_360_at, t.source_match_available_360_at,
          ${JSON.stringify({ source: 'statsbomb_open_data' })}::jsonb, t.status, NOW()
        FROM UNNEST(
          ${sql.array(chunk.map((r) => r.competitionSlug))}::text[],
          ${sql.array(chunk.map((r) => r.seasonSlug))}::text[],
          ${sql.array(chunk.map((r) => r.formatType))}::competition_format_type[],
          ${sql.array(chunk.map((r) => r.currentMatchday))}::int[],
          ${sql.array(chunk.map((r) => r.totalMatchdays))}::int[],
          ${sql.array(chunk.map((r) => r.sourceMatchUpdatedAt))}::timestamptz[],
          ${sql.array(chunk.map((r) => r.sourceMatchAvailableAt))}::timestamptz[],
          ${sql.array(chunk.map((r) => r.sourceMatchUpdated360At))}::timestamptz[],
          ${sql.array(chunk.map((r) => r.sourceMatchAvailable360At))}::timestamptz[],
          ${sql.array(chunk.map((r) => r.status))}::text[]
        ) AS t(competition_slug, season_slug, format_type, current_matchday, total_matchdays,
               source_match_updated_at, source_match_available_at,
               source_match_updated_360_at, source_match_available_360_at, status)
        JOIN competitions comp ON comp.slug = t.competition_slug
        JOIN seasons s ON s.slug = t.season_slug
        ON CONFLICT (competition_id, season_id)
        DO UPDATE SET
          format_type = EXCLUDED.format_type,
          current_matchday = EXCLUDED.current_matchday,
          total_matchdays = EXCLUDED.total_matchdays,
          source_match_updated_at = EXCLUDED.source_match_updated_at,
          source_match_available_at = EXCLUDED.source_match_available_at,
          source_match_updated_360_at = EXCLUDED.source_match_updated_360_at,
          source_match_available_360_at = EXCLUDED.source_match_available_360_at,
          source_metadata = EXCLUDED.source_metadata,
          status = EXCLUDED.status,
          updated_at = NOW()
      `;
    }

    const teamSeasonKeyList = Array.from(teamSeasonKeys);
    const teamSeasonParsed = teamSeasonKeyList.map((key) => {
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
          venue_id, referee,
          source_last_updated_at, source_last_updated_360_at,
          source_data_version, source_shot_fidelity_version, source_xy_fidelity_version,
          source_metadata, updated_at
        )
        SELECT
          t.match_id, t.match_date, cs.id, t.matchday, t.stage, t.group_name,
          home_tm.id, away_tm.id, t.home_score, t.away_score, t.status, t.kickoff_at,
          v.id, t.referee,
          t.source_last_updated_at, t.source_last_updated_360_at,
          t.source_data_version, t.source_shot_fidelity_version, t.source_xy_fidelity_version,
          ${JSON.stringify({ source: 'statsbomb_open_data' })}::jsonb, NOW()
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
          ${sql.array(chunk.map((r) => r.venueSlug))}::text[],
          ${sql.array(chunk.map((r) => r.refereeName))}::text[],
          ${sql.array(chunk.map((r) => r.sourceLastUpdatedAt))}::timestamptz[],
          ${sql.array(chunk.map((r) => r.sourceLastUpdated360At))}::timestamptz[],
          ${sql.array(chunk.map((r) => r.sourceDataVersion))}::text[],
          ${sql.array(chunk.map((r) => r.sourceShotFidelityVersion))}::text[],
          ${sql.array(chunk.map((r) => r.sourceXyFidelityVersion))}::text[]
        ) AS t(match_id, match_date, competition_slug, season_slug,
               home_team_slug, away_team_slug, home_score, away_score,
               matchday, stage, group_name, status, kickoff_at, venue_slug, referee,
               source_last_updated_at, source_last_updated_360_at,
               source_data_version, source_shot_fidelity_version, source_xy_fidelity_version)
        JOIN competitions comp ON comp.slug = t.competition_slug
        JOIN seasons s ON s.slug = t.season_slug
        JOIN competition_seasons cs ON cs.competition_id = comp.id AND cs.season_id = s.id
        JOIN teams home_tm ON home_tm.slug = t.home_team_slug
        JOIN teams away_tm ON away_tm.slug = t.away_team_slug
        LEFT JOIN venues v ON v.slug = t.venue_slug
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
          venue_id = EXCLUDED.venue_id,
          referee = EXCLUDED.referee,
          source_last_updated_at = EXCLUDED.source_last_updated_at,
          source_last_updated_360_at = EXCLUDED.source_last_updated_360_at,
          source_data_version = EXCLUDED.source_data_version,
          source_shot_fidelity_version = EXCLUDED.source_shot_fidelity_version,
          source_xy_fidelity_version = EXCLUDED.source_xy_fidelity_version,
          source_metadata = EXCLUDED.source_metadata,
          updated_at = NOW()
      `;
    }

    await refreshDerivedViews(sql);
    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }

  return summary;
}
