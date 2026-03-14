import type { Sql } from 'postgres';
import { getSingleConnectionDb } from '@/lib/db';
import { loadCountryCodeResolver, type CountryCodeResolver } from './countryCodeResolver';
import type { StatsBombCompetitionEntry, StatsBombMatchEntry } from './statsbomb';

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
  return getSingleConnectionDb('statsbomb-materialize');
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
    currentMatchday: matchWeeks.length > 0 ? Math.max(...matchWeeks) : null,
    totalMatchdays: matchWeeks.length > 0 ? Math.max(...matchWeeks) : null,
    status: 'active',
    sourceMatchUpdatedAt: entry.match_updated,
    sourceMatchAvailableAt: entry.match_available,
    sourceMatchUpdated360At: entry.match_updated_360,
    sourceMatchAvailable360At: entry.match_available_360,
  };
}

function buildMatchDraft(match: StatsBombMatchEntry, competitionEntry: StatsBombCompetitionEntry, helpers: typeof import('./statsbomb')): MatchDraft {
  const homeCountryName = match.home_team.country?.name ?? competitionEntry.country_name;
  const awayCountryName = match.away_team.country?.name ?? competitionEntry.country_name;

  return {
    matchId: match.match_id,
    matchDate: match.match_date,
    competitionSlug: helpers.createCompetitionSlug(competitionEntry),
    seasonSlug: helpers.createSeasonSlug(match.season.season_name, match.season.season_id),
    homeTeamSlug: helpers.createTeamSlug(
      match.home_team.home_team_name,
      competitionEntry.competition_international ? undefined : homeCountryName
    ),
    awayTeamSlug: helpers.createTeamSlug(
      match.away_team.away_team_name,
      competitionEntry.competition_international ? undefined : awayCountryName
    ),
    homeScore: match.home_score,
    awayScore: match.away_score,
    matchWeek: match.match_week,
    stage: match.competition_stage?.name ?? 'REGULAR_SEASON',
    groupName: match.home_team.home_team_group ?? match.away_team.away_team_group ?? null,
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
      const homeTeam = buildTeamDraft(countryCodeResolver, {
        name: matchEntry.home_team.home_team_name,
        countryName: homeCountryName,
        gender: matchEntry.home_team.home_team_gender,
        isNational: competitionEntry.competition_international,
      }, helpers);
      const awayTeam = buildTeamDraft(countryCodeResolver, {
        name: matchEntry.away_team.away_team_name,
        countryName: awayCountryName,
        gender: matchEntry.away_team.away_team_gender,
        isNational: competitionEntry.competition_international,
      }, helpers);

      const homeCountryCode = countryCodeResolver.resolve(helpers.createCountryCode(homeCountryName)) ?? 'ZZZ';
      const awayCountryCode = countryCodeResolver.resolve(helpers.createCountryCode(awayCountryName)) ?? 'ZZZ';
      countries.set(homeCountryCode, buildCountryDraft(countryCodeResolver, homeCountryName, helpers));
      countries.set(awayCountryCode, buildCountryDraft(countryCodeResolver, awayCountryName, helpers));
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

      matches.push(buildMatchDraft(matchEntry, competitionEntry, helpers));
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

    for (const country of countries.values()) {
      await upsertCountry(sql, country);
    }

    for (const season of seasons.values()) {
      await upsertSeason(sql, season, season.slug === currentSeasonSlug);
    }

    for (const competition of competitions.values()) {
      await upsertCompetition(sql, competition);
    }

    for (const venue of venues.values()) {
      await upsertVenue(sql, venue);
    }

    for (const team of teams.values()) {
      await upsertTeam(sql, team);
    }

    for (const competitionSeason of competitionSeasons.values()) {
      await upsertCompetitionSeason(sql, competitionSeason);
    }

    for (const teamSeasonKey of teamSeasonKeys) {
      const [competitionSlug, seasonSlug, teamSlug] = teamSeasonKey.split(':');
      await upsertTeamSeason(sql, competitionSlug, seasonSlug, teamSlug);
    }

    for (const match of matches) {
      await upsertMatch(sql, match);
    }

    await refreshDerivedViews(sql);
    await sql`COMMIT`;
  } catch (error) {
    await sql`ROLLBACK`;
    throw error;
  }

  return summary;
}
