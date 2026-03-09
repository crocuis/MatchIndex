import postgres, { type Sql } from 'postgres';
import { NATION_CODE_SKIP, resolveNationCodeAlias } from './nationCodeAliases.ts';
import {
  buildFootballDataCompetitionMatchesPath,
  buildFootballDataCompetitionTeamsPath,
  parseFootballDataCompetitionTargets,
  type FootballDataOrgCompetitionResponse,
  type FootballDataOrgMatchSummary,
  type FootballDataOrgMatchesResponse,
  type FootballDataOrgSeasonSummary,
  type FootballDataOrgTeamSummary,
  type FootballDataOrgTeamsResponse,
} from './footballDataOrg.ts';

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
  gender: 'male' | 'female' | 'mixed';
  isNational: boolean;
}

interface CompetitionSeasonDraft {
  competitionSlug: string;
  seasonSlug: string;
  currentMatchday: number | null;
  totalMatchdays: number | null;
  status: string;
  sourceMetadata: Record<string, unknown>;
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
  venueName: string | null;
  sourceMetadata: Record<string, unknown>;
}

interface SourceRow {
  id: number;
}

interface RawPayloadRow {
  payload: unknown;
}

interface FootballDataOrgCompetitionConfig {
  slug: string;
  code: string;
  name: string;
  shortName: string;
  countryCode: string | null;
  compType: 'league' | 'international';
  isInternational: boolean;
}

export interface MaterializeFootballDataOrgCoreOptions {
  dryRun?: boolean;
  competitionCodes?: string[];
  seasons?: number[];
}

export interface MaterializeFootballDataOrgCoreSummary {
  dryRun: boolean;
  countries: number;
  competitions: number;
  seasons: number;
  teams: number;
  competitionSeasons: number;
  teamSeasons: number;
  matches: number;
}

const COMPETITION_CONFIGS: Record<string, FootballDataOrgCompetitionConfig> = {
  BL1: {
    slug: '1-bundesliga',
    code: 'bl1',
    name: '1. Bundesliga',
    shortName: 'Bundesliga',
    countryCode: 'DEU',
    compType: 'league',
    isInternational: false,
  },
  CL: {
    slug: 'champions-league',
    code: 'cl',
    name: 'Champions League',
    shortName: 'Champions League',
    countryCode: null,
    compType: 'international',
    isInternational: true,
  },
  FL1: {
    slug: 'ligue-1',
    code: 'fl1',
    name: 'Ligue 1',
    shortName: 'Ligue 1',
    countryCode: 'FRA',
    compType: 'league',
    isInternational: false,
  },
  PD: {
    slug: 'la-liga',
    code: 'pd',
    name: 'La Liga',
    shortName: 'La Liga',
    countryCode: 'ESP',
    compType: 'league',
    isInternational: false,
  },
  PL: {
    slug: 'premier-league',
    code: 'pl',
    name: 'Premier League',
    shortName: 'Premier League',
    countryCode: 'ENG',
    compType: 'league',
    isInternational: false,
  },
  SA: {
    slug: 'serie-a',
    code: 'sa',
    name: 'Serie A',
    shortName: 'Serie A',
    countryCode: 'ITA',
    compType: 'league',
    isInternational: false,
  },
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

  return [new Date().getUTCFullYear()];
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

function normalizeCountryCode(code?: string | null) {
  if (!code) {
    return null;
  }

  const normalized = resolveNationCodeAlias(code);
  return NATION_CODE_SKIP.has(normalized) ? null : normalized;
}

function normalizeMatchStatus(status?: string | null) {
  switch (status) {
    case 'FINISHED':
      return 'finished';
    case 'TIMED':
      return 'timed';
    case 'IN_PLAY':
      return 'live_1h';
    case 'PAUSED':
      return 'live_ht';
    case 'EXTRA_TIME':
      return 'live_et';
    case 'PENALTY_SHOOTOUT':
      return 'live_pen';
    case 'POSTPONED':
      return 'postponed';
    case 'SUSPENDED':
      return 'suspended';
    case 'CANCELLED':
      return 'cancelled';
    case 'AWARDED':
      return 'awarded';
    case 'SCHEDULED':
    default:
      return 'scheduled';
  }
}

function buildCountryDraft(name: string, code: string): CountryDraft {
  return {
    codeAlpha3: code,
    name,
  };
}

function buildCompetitionDraft(code: string, payload: FootballDataOrgCompetitionResponse): CompetitionDraft {
  const config = COMPETITION_CONFIGS[code] ?? {
    slug: slugify(payload.name ?? code),
    code: code.toLowerCase(),
    name: payload.name ?? code,
    shortName: createShortName(payload.name ?? code, 20),
    countryCode: normalizeCountryCode(payload.area?.code),
    compType: payload.type === 'CUP' ? 'international' : 'league',
    isInternational: payload.type === 'CUP',
  };

  return {
    slug: config.slug,
    code: config.code,
    name: payload.name ?? config.name,
    shortName: config.shortName,
    countryCode: config.countryCode,
    gender: 'male',
    isYouth: false,
    isInternational: config.isInternational,
    compType: config.compType,
  };
}

function resolveSeasonSummary(
  competitionPayload: FootballDataOrgCompetitionResponse,
  teamsPayload: FootballDataOrgTeamsResponse,
  season: number,
) {
  const exact = competitionPayload.seasons?.find((entry) => entry.startDate?.startsWith(String(season)));
  return exact ?? teamsPayload.season ?? competitionPayload.currentSeason ?? null;
}

function buildSeasonDraft(season: FootballDataOrgSeasonSummary | null, fallbackSeason: number): SeasonDraft {
  const startDate = season?.startDate ?? `${fallbackSeason}-07-01`;
  const endYear = startDate.slice(0, 4) === (season?.endDate ?? '').slice(0, 4) ? fallbackSeason : fallbackSeason + 1;
  const endDate = season?.endDate ?? `${endYear}-06-30`;

  return {
    slug: createSeasonSlug(startDate, endDate),
    startDate,
    endDate,
  };
}

function buildTeamDraft(team: FootballDataOrgTeamSummary, competition: CompetitionDraft): TeamDraft {
  const countryCode = normalizeCountryCode(team.area?.code) ?? competition.countryCode;

  if (!countryCode) {
    throw new Error(`Unable to resolve country for team ${team.name ?? 'unknown'}`);
  }

  return {
    slug: createTeamSlug(team.name ?? String(team.id ?? 'team'), team.area?.name ?? null),
    name: team.name ?? String(team.id ?? 'Team'),
    shortName: team.shortName?.trim() || createShortName(team.name ?? String(team.id ?? 'Team'), 18),
    countryCode,
    gender: 'male',
    isNational: false,
  };
}

function buildCompetitionSeasonDraft(
  competition: CompetitionDraft,
  season: SeasonDraft,
  seasonSummary: FootballDataOrgSeasonSummary | null,
  targetSeason: number,
  matchCount: number,
): CompetitionSeasonDraft {
  return {
    competitionSlug: competition.slug,
    seasonSlug: season.slug,
    currentMatchday: seasonSummary?.currentMatchday ?? null,
    totalMatchdays: seasonSummary?.currentMatchday ?? null,
    status: 'active',
    sourceMetadata: {
      source: 'football_data_org',
      season: targetSeason,
      coverageLevel: 'metadata_only',
      matchCount,
    },
  };
}

function buildMatchDraft(
  match: FootballDataOrgMatchSummary,
  competition: CompetitionDraft,
  season: SeasonDraft,
  teamSlugByExternalId: Map<number, string>,
): MatchDraft {
  if (!match.id || !match.utcDate || !match.homeTeam?.id || !match.awayTeam?.id) {
    throw new Error('Incomplete football-data.org match payload');
  }

  const homeTeamSlug = teamSlugByExternalId.get(match.homeTeam.id);
  const awayTeamSlug = teamSlugByExternalId.get(match.awayTeam.id);

  if (!homeTeamSlug || !awayTeamSlug) {
    throw new Error(`Unable to resolve canonical team slug for match ${match.id}`);
  }

  return {
    matchId: match.id,
    matchDate: match.utcDate.slice(0, 10),
    competitionSlug: competition.slug,
    seasonSlug: season.slug,
    homeTeamSlug,
    awayTeamSlug,
    homeScore: match.score?.fullTime?.home ?? null,
    awayScore: match.score?.fullTime?.away ?? null,
    matchWeek: match.matchday ?? null,
    stage: match.stage ?? 'REGULAR_SEASON',
    groupName: match.group ?? null,
    status: normalizeMatchStatus(match.status),
    kickoffAt: match.utcDate,
    venueName: match.venue ?? null,
    sourceMetadata: {
      source: 'football_data_org',
      coverageLevel: 'metadata_only',
      externalCompetitionCode: competition.code.toUpperCase(),
      externalMatchId: match.id,
    },
  };
}

async function ensureFootballDataSource(sql: Sql) {
  const rows = await sql<SourceRow[]>`
    SELECT id
    FROM data_sources
    WHERE slug = 'football_data_org'
    LIMIT 1
  `;

  if (!rows[0]) {
    throw new Error('football_data_org source is not registered');
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
    INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary)
    VALUES (${entityType}, (${entityIdSql}), ${alias}, 'en', 'common', TRUE)
    ON CONFLICT (entity_type, entity_id, alias_normalized)
    DO UPDATE SET locale = EXCLUDED.locale, alias_kind = EXCLUDED.alias_kind, is_primary = EXCLUDED.is_primary
  `;
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
      source_metadata,
      status,
      updated_at
    )
    VALUES (
      (SELECT id FROM competitions WHERE slug = ${draft.competitionSlug}),
      (SELECT id FROM seasons WHERE slug = ${draft.seasonSlug}),
      ${draft.currentMatchday},
      ${draft.totalMatchdays},
      ${JSON.stringify(draft.sourceMetadata)}::jsonb,
      ${draft.status},
      NOW()
    )
    ON CONFLICT (competition_id, season_id)
    DO UPDATE SET
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

export async function materializeFootballDataOrgCore(
  options: MaterializeFootballDataOrgCoreOptions = {},
): Promise<MaterializeFootballDataOrgCoreSummary> {
  const targets = parseFootballDataCompetitionTargets(options.competitionCodes);
  const seasonsToProcess = normalizeSeasons(options.seasons);
  const sql = getMaterializeDb();
  const sourceId = await ensureFootballDataSource(sql);

  const countries = new Map<string, CountryDraft>();
  const competitions = new Map<string, CompetitionDraft>();
  const seasons = new Map<string, SeasonDraft>();
  const teams = new Map<string, TeamDraft>();
  const competitionSeasons = new Map<string, CompetitionSeasonDraft>();
  const teamSeasonKeys = new Set<string>();
  const matches: MatchDraft[] = [];
  const teamMappings: Array<{ slug: string; externalId: string; seasonContext: string }> = [];
  const matchMappings: Array<{ matchId: number; matchDate: string; externalId: string; seasonContext: string }> = [];
  const competitionMappings: Array<{ slug: string; externalId: string; externalCode: string }> = [];

  try {
    for (const target of targets) {
      const competitionPayload = await loadLatestRawPayload<FootballDataOrgCompetitionResponse>(sql, sourceId, `/competitions/${target.code}`);
      if (!competitionPayload) {
        continue;
      }

      const competitionDraft = buildCompetitionDraft(target.code, competitionPayload);
      competitions.set(competitionDraft.slug, competitionDraft);
      competitionMappings.push({
        slug: competitionDraft.slug,
        externalId: String(competitionPayload.id ?? target.code),
        externalCode: target.code,
      });

      const competitionCountryCode = normalizeCountryCode(competitionPayload.area?.code) ?? competitionDraft.countryCode;
      if (competitionCountryCode && competitionPayload.area?.name) {
        countries.set(competitionCountryCode, buildCountryDraft(competitionPayload.area.name, competitionCountryCode));
      }

      for (const seasonValue of seasonsToProcess) {
        const teamsPayload = await loadLatestRawPayload<FootballDataOrgTeamsResponse>(sql, sourceId, buildFootballDataCompetitionTeamsPath(target.code, seasonValue));
        const matchesPayload = await loadLatestRawPayload<FootballDataOrgMatchesResponse>(
          sql,
          sourceId,
          `${buildFootballDataCompetitionMatchesPath(target.code, seasonValue)}&status=FINISHED`,
        );

        if (!teamsPayload || !matchesPayload) {
          continue;
        }

        const seasonSummary = resolveSeasonSummary(competitionPayload, teamsPayload, seasonValue);
        const seasonDraft = buildSeasonDraft(seasonSummary, seasonValue);
        seasons.set(seasonDraft.slug, seasonDraft);

        const competitionSeasonDraft = buildCompetitionSeasonDraft(
          competitionDraft,
          seasonDraft,
          seasonSummary,
          seasonValue,
          matchesPayload.matches?.length ?? 0,
        );
        competitionSeasons.set(`${competitionDraft.slug}:${seasonDraft.slug}`, competitionSeasonDraft);

        const teamSlugByExternalId = new Map<number, string>();

        for (const team of teamsPayload.teams ?? []) {
          if (!team.id || !team.name) {
            continue;
          }

          const teamDraft = buildTeamDraft(team, competitionDraft);
          teams.set(teamDraft.slug, teamDraft);
          teamSlugByExternalId.set(team.id, teamDraft.slug);
          teamSeasonKeys.add(`${competitionDraft.slug}:${seasonDraft.slug}:${teamDraft.slug}`);
          teamMappings.push({
            slug: teamDraft.slug,
            externalId: String(team.id),
            seasonContext: String(seasonValue),
          });

          const teamCountryCode = normalizeCountryCode(team.area?.code) ?? competitionDraft.countryCode;
          if (teamCountryCode && team.area?.name) {
            countries.set(teamCountryCode, buildCountryDraft(team.area.name, teamCountryCode));
          }
        }

        for (const match of matchesPayload.matches ?? []) {
          const matchDraft = buildMatchDraft(match, competitionDraft, seasonDraft, teamSlugByExternalId);
          matches.push(matchDraft);
          matchMappings.push({
            matchId: matchDraft.matchId,
            matchDate: matchDraft.matchDate,
            externalId: String(matchDraft.matchId),
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
    } satisfies MaterializeFootballDataOrgCoreSummary;

    if (options.dryRun ?? false) {
      await sql.end({ timeout: 1 });
      return summary;
    }

    await sql`BEGIN`;
    try {
      for (const country of countries.values()) {
        await upsertCountry(sql, country);
      }

      for (const season of seasons.values()) {
        await upsertSeason(sql, season);
      }

      for (const competition of competitions.values()) {
        await upsertCompetition(sql, competition);
      }

      for (const team of teams.values()) {
        await upsertTeam(sql, team);
      }

      for (const competitionSeason of competitionSeasons.values()) {
        await upsertCompetitionSeason(sql, competitionSeason);
      }

      for (const key of teamSeasonKeys) {
        const [competitionSlug, seasonSlug, teamSlug] = key.split(':');
        await upsertTeamSeason(sql, competitionSlug, seasonSlug, teamSlug);
      }

      for (const match of matches) {
        await upsertMatch(sql, match);
      }

      for (const mapping of competitionMappings) {
        await upsertSourceEntityMapping(sql, {
          entityType: 'competition',
          entityIdSql: sql`SELECT id FROM competitions WHERE slug = ${mapping.slug}`,
          sourceId,
          externalId: mapping.externalId,
          externalCode: mapping.externalCode,
          metadata: { source: 'football_data_org' },
        });
      }

      for (const mapping of teamMappings) {
        await upsertSourceEntityMapping(sql, {
          entityType: 'team',
          entityIdSql: sql`SELECT id FROM teams WHERE slug = ${mapping.slug}`,
          sourceId,
          externalId: mapping.externalId,
          seasonContext: mapping.seasonContext,
          metadata: { source: 'football_data_org' },
        });
      }

      for (const mapping of matchMappings) {
        await upsertSourceEntityMapping(sql, {
          entityType: 'match',
          entityIdSql: sql`SELECT id FROM matches WHERE id = ${mapping.matchId} AND match_date = ${mapping.matchDate}`,
          sourceId,
          externalId: mapping.externalId,
          seasonContext: mapping.seasonContext,
          metadata: { source: 'football_data_org', matchDate: mapping.matchDate },
        });
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
