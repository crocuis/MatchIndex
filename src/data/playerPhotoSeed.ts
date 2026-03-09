import { readFile } from 'node:fs/promises';
import path from 'node:path';
import type { Sql } from 'postgres';
import { getSingleConnectionDb } from '@/lib/db';
import { getNationBadgeUrl, getNationFlagUrl } from './nationVisuals';
import { resolveNationCodeAlias } from './nationCodeAliases';

interface ApiFootballPlayerMapping {
  playerId: string;
  externalId: string;
}

interface SourceRow {
  id: number;
}

interface MockClubRecord {
  id: string;
  name: string;
  shortName: string;
  country: string;
  founded: number;
  stadium: string;
  stadiumCapacity: number;
  leagueId: string;
  logo?: string;
}

interface MockLeagueRecord {
  id: string;
  name: string;
  country: string;
  season: string;
  logo?: string;
}

export interface SeedPlayerPhotoFixturesOptions {
  dryRun?: boolean;
  limit?: number;
  playerId?: string;
}

export interface SeedPlayerPhotoFixturesSummary {
  dryRun: boolean;
  countriesPlanned: number;
  leaguesPlanned: number;
  clubsPlanned: number;
  playersPlanned: number;
  translationsPlanned: number;
  mappingsPlanned: number;
  rowsWritten: number;
}

function getSeedDb() {
  return getSingleConnectionDb('photo-seed');
}

function getSelectedPlayers(playerId?: string, limit?: number) {
  return loadPlayers().then((allPlayers) => {
    const filtered = playerId ? allPlayers.filter((player) => player.id === playerId) : allPlayers;
    return filtered.slice(0, limit ?? filtered.length);
  });
}

async function loadPlayers() {
  const playersModule = await import(new URL('./players.ts', import.meta.url).href);
  return playersModule.players as typeof import('./players').players;
}

async function loadClubs() {
  const clubsModule = await import(new URL('./clubs.ts', import.meta.url).href);
  return clubsModule.clubs as MockClubRecord[];
}

async function loadLeagues() {
  const leaguesModule = await import(new URL('./leagues.ts', import.meta.url).href);
  return leaguesModule.leagues as MockLeagueRecord[];
}

async function loadNations() {
  const nationsModule = await import(new URL('./nations.ts', import.meta.url).href);
  return nationsModule.nations as typeof import('./nations').nations;
}

async function getSelectedNations(selectedPlayers: Awaited<ReturnType<typeof getSelectedPlayers>>) {
  const allNations = await loadNations();
  const usedNationIds = new Set(selectedPlayers.map((player) => player.nationId));
  return allNations.filter((nation) => usedNationIds.has(nation.id));
}

async function getSelectedClubs(selectedPlayers: Awaited<ReturnType<typeof getSelectedPlayers>>) {
  const allClubs = await loadClubs();
  const usedClubIds = new Set(selectedPlayers.map((player) => player.clubId));
  return allClubs.filter((club) => usedClubIds.has(club.id));
}

async function getSelectedLeagues(selectedPlayers: Awaited<ReturnType<typeof getSelectedPlayers>>) {
  const [allLeagues, selectedClubs] = await Promise.all([loadLeagues(), getSelectedClubs(selectedPlayers)]);
  const usedLeagueIds = new Set(selectedClubs.map((club) => club.leagueId));
  return allLeagues.filter((league) => usedLeagueIds.has(league.id));
}

async function getCurrentSeasonSlug(selectedPlayers: Awaited<ReturnType<typeof getSelectedPlayers>>) {
  const firstLeague = (await getSelectedLeagues(selectedPlayers))[0];
  return firstLeague?.season ?? '2025/26';
}

async function loadApiFootballMappings(): Promise<Map<string, string>> {
  const customPath = process.env.API_FOOTBALL_PLAYER_MAPPINGS_FILE?.trim();
  const filePath = customPath
    ? (path.isAbsolute(customPath) ? customPath : path.join(process.cwd(), customPath))
    : path.join(process.cwd(), 'data', 'api-football-player-mappings.json');

  try {
    const payload = JSON.parse(await readFile(filePath, 'utf8')) as ApiFootballPlayerMapping[];
    return new Map(payload.map((entry) => [entry.playerId, entry.externalId]));
  } catch {
    return new Map();
  }
}

async function ensureApiFootballSource(sql: Sql) {
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, priority)
    VALUES ('api_football', 'API-Football v3', 'https://v3.football.api-sports.io', 'api', 2)
    ON CONFLICT (slug)
    DO UPDATE SET
      name = EXCLUDED.name,
      base_url = EXCLUDED.base_url,
      source_kind = EXCLUDED.source_kind,
      priority = EXCLUDED.priority
    RETURNING id
  `;

  return rows[0].id;
}

async function seedCountries(sql: Sql, selectedPlayers: Awaited<ReturnType<typeof getSelectedPlayers>>) {
  const [selectedNations, selectedClubs, selectedLeagues] = await Promise.all([
    getSelectedNations(selectedPlayers),
    getSelectedClubs(selectedPlayers),
    getSelectedLeagues(selectedPlayers),
  ]);
  const neededCountryNames = new Set([
    ...selectedNations.map((nation) => nation.name),
    ...selectedClubs.map((club) => club.country),
    ...selectedLeagues.map((league) => league.country),
  ]);
  const allNations = await loadNations();
  const selectedNationsByName = new Map(allNations.map((nation) => [nation.name, nation]));
  const selectedNationRecords = Array.from(neededCountryNames)
    .map((name) => selectedNationsByName.get(name))
    .filter((nation): nation is (typeof allNations)[number] => Boolean(nation));

  for (const nation of selectedNationRecords) {
    const canonicalCode = resolveNationCodeAlias(nation.code);

    await sql`
      INSERT INTO countries (
        code_alpha3,
        confederation,
        flag_url,
        crest_url,
        is_active,
        updated_at
      )
      VALUES (
        ${canonicalCode},
        ${nation.confederation},
        ${getNationFlagUrl(canonicalCode, nation.flag) ?? null},
        ${getNationBadgeUrl(canonicalCode, nation.crest) ?? null},
        TRUE,
        NOW()
      )
      ON CONFLICT (code_alpha3)
      DO UPDATE SET
        confederation = EXCLUDED.confederation,
        flag_url = EXCLUDED.flag_url,
        crest_url = EXCLUDED.crest_url,
        is_active = TRUE,
        updated_at = NOW()
    `;

    await sql`
      INSERT INTO country_translations (country_id, locale, name)
      VALUES (
        (SELECT id FROM countries WHERE code_alpha3 = ${canonicalCode}),
        'en',
        ${nation.name}
      )
      ON CONFLICT (country_id, locale)
      DO UPDATE SET name = EXCLUDED.name
    `;
  }

  return selectedNationRecords.length;
}

async function seedSeasonGraph(sql: Sql, selectedPlayers: Awaited<ReturnType<typeof getSelectedPlayers>>) {
  const [selectedLeagues, selectedClubs] = await Promise.all([
    getSelectedLeagues(selectedPlayers),
    getSelectedClubs(selectedPlayers),
  ]);
  const allNations = await loadNations();
  const nationByName = new Map(allNations.map((nation) => [nation.name, nation]));
  const currentSeasonSlug = await getCurrentSeasonSlug(selectedPlayers);
  const [startYear, endYearSuffix] = currentSeasonSlug.split('/');
  const endYear = Number.parseInt(startYear.slice(0, 2) + endYearSuffix, 10);

  await sql`
    INSERT INTO seasons (slug, start_date, end_date, is_current)
    VALUES (${currentSeasonSlug}, ${`${startYear}-08-01`}, ${`${endYear}-05-31`}, FALSE)
    ON CONFLICT (slug)
    DO UPDATE SET
      start_date = EXCLUDED.start_date,
      end_date = EXCLUDED.end_date,
      is_current = FALSE
  `;

  await sql`
    UPDATE seasons
    SET is_current = FALSE
    WHERE is_current = TRUE
  `;

  await sql`
    UPDATE seasons
    SET is_current = TRUE
    WHERE slug = ${currentSeasonSlug}
  `;

  for (const league of selectedLeagues) {
    const leagueCountry = league.country === 'Europe' ? null : league.country;
    const leagueNationCode = leagueCountry ? nationByName.get(leagueCountry)?.code ?? null : null;

    await sql`
      INSERT INTO competitions (
        slug,
        code,
        comp_type,
        gender,
        is_international,
        country_id,
        emblem_url,
        is_active,
        updated_at
      )
      VALUES (
        ${league.id},
        ${league.id.toUpperCase().slice(0, 10)},
        'league',
        'male',
        ${leagueCountry ? false : true},
        ${leagueNationCode ? sql`(SELECT id FROM countries WHERE code_alpha3 = ${leagueNationCode})` : null},
        ${league.logo ?? null},
        TRUE,
        NOW()
      )
      ON CONFLICT (slug)
      DO UPDATE SET
        emblem_url = EXCLUDED.emblem_url,
        country_id = EXCLUDED.country_id,
        is_active = TRUE,
        updated_at = NOW()
    `;

    await sql`
      INSERT INTO competition_translations (competition_id, locale, name, short_name)
      VALUES (
        (SELECT id FROM competitions WHERE slug = ${league.id}),
        'en',
        ${league.name},
        ${league.name}
      )
      ON CONFLICT (competition_id, locale)
      DO UPDATE SET
        name = EXCLUDED.name,
        short_name = EXCLUDED.short_name
    `;

    await sql`
      INSERT INTO competition_seasons (competition_id, season_id, status, updated_at)
      VALUES (
        (SELECT id FROM competitions WHERE slug = ${league.id}),
        (SELECT id FROM seasons WHERE slug = ${currentSeasonSlug}),
        'active',
        NOW()
      )
      ON CONFLICT (competition_id, season_id)
      DO UPDATE SET
        status = EXCLUDED.status,
        updated_at = NOW()
    `;
  }

  for (const club of selectedClubs) {
    const nation = nationByName.get(club.country);

    await sql`
      INSERT INTO venues (slug, country_id, capacity, updated_at)
      VALUES (
        ${club.id},
        (SELECT id FROM countries WHERE code_alpha3 = ${nation?.code ?? null}),
        ${club.stadiumCapacity || null},
        NOW()
      )
      ON CONFLICT (slug)
      DO UPDATE SET
        capacity = EXCLUDED.capacity,
        updated_at = NOW()
    `;

    await sql`
      INSERT INTO venue_translations (venue_id, locale, name)
      VALUES (
        (SELECT id FROM venues WHERE slug = ${club.id}),
        'en',
        ${club.stadium}
      )
      ON CONFLICT (venue_id, locale)
      DO UPDATE SET name = EXCLUDED.name
    `;

    await sql`
      INSERT INTO teams (
        slug,
        country_id,
        venue_id,
        founded_year,
        gender,
        is_national,
        crest_url,
        is_active,
        updated_at
      )
      VALUES (
        ${club.id},
        (SELECT id FROM countries WHERE code_alpha3 = ${nation?.code ?? null}),
        (SELECT id FROM venues WHERE slug = ${club.id}),
        ${club.founded || null},
        'male',
        FALSE,
        ${club.logo ?? null},
        TRUE,
        NOW()
      )
      ON CONFLICT (slug)
      DO UPDATE SET
        country_id = EXCLUDED.country_id,
        venue_id = EXCLUDED.venue_id,
        founded_year = EXCLUDED.founded_year,
        crest_url = EXCLUDED.crest_url,
        is_active = TRUE,
        updated_at = NOW()
    `;

    await sql`
      INSERT INTO team_translations (team_id, locale, name, short_name)
      VALUES (
        (SELECT id FROM teams WHERE slug = ${club.id}),
        'en',
        ${club.name},
        ${club.shortName}
      )
      ON CONFLICT (team_id, locale)
      DO UPDATE SET
        name = EXCLUDED.name,
        short_name = EXCLUDED.short_name
    `;

    await sql`
      INSERT INTO team_seasons (team_id, competition_season_id, updated_at)
      VALUES (
        (SELECT id FROM teams WHERE slug = ${club.id}),
        (
          SELECT cs.id
          FROM competition_seasons cs
          JOIN competitions c ON c.id = cs.competition_id
          JOIN seasons s ON s.id = cs.season_id
          WHERE c.slug = ${club.leagueId} AND s.slug = ${currentSeasonSlug}
        ),
        NOW()
      )
      ON CONFLICT (team_id, competition_season_id)
      DO UPDATE SET updated_at = NOW()
    `;
  }

  return {
    leaguesCount: selectedLeagues.length,
    clubsCount: selectedClubs.length,
    seasonSlug: currentSeasonSlug,
  };
}

async function seedPlayers(sql: Sql, selectedPlayers: Awaited<ReturnType<typeof getSelectedPlayers>>) {
  const seasonSlug = await getCurrentSeasonSlug(selectedPlayers);
  const selectedClubs = await getSelectedClubs(selectedPlayers);
  const clubById = new Map(selectedClubs.map((club) => [club.id, club]));

  for (const player of selectedPlayers) {
    const club = clubById.get(player.clubId);

    await sql`
      INSERT INTO players (
        slug,
        date_of_birth,
        country_id,
        position,
        height_cm,
        preferred_foot,
        photo_url,
        is_active,
        updated_at
      )
      VALUES (
        ${player.id},
        ${player.dateOfBirth || null},
        (SELECT id FROM countries WHERE code_alpha3 = ${player.nationId.toUpperCase()}),
        ${player.position},
        ${player.height || null},
        ${player.preferredFoot},
        ${player.photoUrl ?? null},
        TRUE,
        NOW()
      )
      ON CONFLICT (slug)
      DO UPDATE SET
        date_of_birth = EXCLUDED.date_of_birth,
        country_id = EXCLUDED.country_id,
        position = EXCLUDED.position,
        height_cm = EXCLUDED.height_cm,
        preferred_foot = EXCLUDED.preferred_foot,
        photo_url = CASE
          WHEN players.photo_url IS NULL OR players.photo_url LIKE '/player-faces/%' THEN EXCLUDED.photo_url
          ELSE players.photo_url
        END,
        is_active = TRUE,
        updated_at = NOW()
    `;

    await sql`
      INSERT INTO player_translations (player_id, locale, first_name, last_name, known_as)
      VALUES (
        (SELECT id FROM players WHERE slug = ${player.id}),
        'en',
        ${player.firstName || null},
        ${player.lastName || null},
        ${player.name}
      )
      ON CONFLICT (player_id, locale)
      DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        known_as = EXCLUDED.known_as
    `;

    await sql`
      INSERT INTO player_contracts (
        player_id,
        team_id,
        competition_season_id,
        shirt_number,
        left_date,
        updated_at
      )
      VALUES (
        (SELECT id FROM players WHERE slug = ${player.id}),
        (SELECT id FROM teams WHERE slug = ${player.clubId}),
        (
          SELECT cs.id
          FROM competition_seasons cs
          JOIN competitions c ON c.id = cs.competition_id
          JOIN seasons s ON s.id = cs.season_id
          WHERE c.slug = ${club?.leagueId ?? null}
            AND s.slug = ${seasonSlug}
        ),
        ${player.shirtNumber || null},
        NULL,
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
      INSERT INTO player_season_stats (
        player_id,
        competition_season_id,
        appearances,
        starts,
        minutes_played,
        goals,
        assists,
        yellow_cards,
        red_cards,
        clean_sheets,
        updated_at
      )
      VALUES (
        (SELECT id FROM players WHERE slug = ${player.id}),
        (
          SELECT cs.id
          FROM competition_seasons cs
          JOIN competitions c ON c.id = cs.competition_id
          JOIN teams t ON t.slug = ${player.clubId}
          JOIN team_seasons ts ON ts.team_id = t.id AND ts.competition_season_id = cs.id
          JOIN seasons s ON s.id = cs.season_id
          WHERE s.slug = ${seasonSlug}
          LIMIT 1
        ),
        ${player.seasonStats.appearances},
        ${player.seasonStats.appearances},
        ${player.seasonStats.minutesPlayed},
        ${player.seasonStats.goals},
        ${player.seasonStats.assists},
        ${player.seasonStats.yellowCards},
        ${player.seasonStats.redCards},
        ${player.seasonStats.cleanSheets ?? 0},
        NOW()
      )
      ON CONFLICT (player_id, competition_season_id)
      DO UPDATE SET
        appearances = EXCLUDED.appearances,
        starts = EXCLUDED.starts,
        minutes_played = EXCLUDED.minutes_played,
        goals = EXCLUDED.goals,
        assists = EXCLUDED.assists,
        yellow_cards = EXCLUDED.yellow_cards,
        red_cards = EXCLUDED.red_cards,
        clean_sheets = EXCLUDED.clean_sheets,
        updated_at = NOW()
    `;
  }

  return selectedPlayers.length;
}

async function seedMappings(
  sql: Sql,
  sourceId: number,
  selectedPlayers: Awaited<ReturnType<typeof getSelectedPlayers>>,
  mappings: Map<string, string>
) {
  let seededCount = 0;

  for (const player of selectedPlayers) {
    const externalId = mappings.get(player.id);
    if (!externalId) {
      continue;
    }

    await sql`
      INSERT INTO source_entity_mapping (
        entity_type,
        entity_id,
        source_id,
        external_id,
        metadata,
        updated_at
      )
      VALUES (
        'player',
        (SELECT id FROM players WHERE slug = ${player.id}),
        ${sourceId},
        ${externalId},
        ${JSON.stringify({ seededFrom: 'api-football-player-mappings.json' })}::jsonb,
        NOW()
      )
      ON CONFLICT (entity_type, source_id, external_id)
      DO UPDATE SET
        entity_id = EXCLUDED.entity_id,
        metadata = EXCLUDED.metadata,
        updated_at = NOW()
    `;

    seededCount += 1;
  }

  return seededCount;
}

export async function seedPlayerPhotoFixtures(
  options: SeedPlayerPhotoFixturesOptions = {}
): Promise<SeedPlayerPhotoFixturesSummary> {
  const selectedPlayers = await getSelectedPlayers(options.playerId, options.limit);
  const mappings = await loadApiFootballMappings();
  const [selectedNations, selectedLeagues, selectedClubs] = await Promise.all([
    getSelectedNations(selectedPlayers),
    getSelectedLeagues(selectedPlayers),
    getSelectedClubs(selectedPlayers),
  ]);

  if (options.dryRun ?? true) {
    const countriesPlanned = new Set([
      ...selectedNations.map((nation) => nation.name),
      ...selectedClubs.map((club) => club.country),
      ...selectedLeagues.map((league) => league.country),
    ]).size;
    const mappingsPlanned = selectedPlayers.filter((player) => mappings.has(player.id)).length;

    return {
      dryRun: true,
      countriesPlanned,
      leaguesPlanned: selectedLeagues.length,
      clubsPlanned: selectedClubs.length,
      playersPlanned: selectedPlayers.length,
      translationsPlanned: selectedPlayers.length,
      mappingsPlanned,
      rowsWritten: 0,
    };
  }

  const sql = getSeedDb();
  const sourceId = await ensureApiFootballSource(sql);
  const countriesPlanned = await seedCountries(sql, selectedPlayers);
  const { leaguesCount, clubsCount } = await seedSeasonGraph(sql, selectedPlayers);
  const playersPlanned = await seedPlayers(sql, selectedPlayers);
  const mappingsPlanned = await seedMappings(sql, sourceId, selectedPlayers, mappings);

  return {
    dryRun: false,
    countriesPlanned,
    leaguesPlanned: leaguesCount,
    clubsPlanned: clubsCount,
    playersPlanned,
    translationsPlanned: playersPlanned,
    mappingsPlanned,
    rowsWritten: countriesPlanned + leaguesCount + clubsCount + (playersPlanned * 4) + mappingsPlanned,
  };
}
