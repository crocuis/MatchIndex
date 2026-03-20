import postgres from 'postgres';

const BATCH_SIZE = 500;

interface BaseClubRecord {
  id: string;
  name: string;
  country: string;
  founded: number;
  stadium: string;
  stadiumCapacity: number;
}

interface TeamRow {
  id: number;
  slug: string;
  name: string;
  country_id: number | null;
  founded_year: number | null;
  venue_id: number | null;
  venue_capacity: number | null;
}

interface DonorRow extends TeamRow {
  venue_capacity: number | null;
}

interface CountryRow {
  id: number;
  name: string;
}

interface VenueRow {
  id: number;
  name: string;
  country_id: number | null;
  capacity: number | null;
  slug: string;
}

interface TeamUpdateDraft {
  countryId: number | null;
  foundedYear: number | null;
  teamId: number;
  venueId: number | null;
}

interface VenueSeedDraft {
  capacity: number;
  countryId: number | null;
  name: string;
  slug: string;
}

interface InsertedVenueRow {
  inserted: boolean;
}

function normalizeName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\b(fc|cf|afc|sc|club|de|del|futbol|football|balompie)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function slugify(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-')
    .toLowerCase();
}

function getBaseClubMetadataKey(club: BaseClubRecord) {
  return [club.country, club.founded, club.stadium, club.stadiumCapacity].join('::');
}

function lowerKey(value: string) {
  return value.trim().toLowerCase();
}

function buildCountryIdByName(countries: CountryRow[]) {
  return new Map(countries.map((country) => [lowerKey(country.name), country.id]));
}

function buildVenueByName(venues: VenueRow[]) {
  return new Map(venues.map((venue) => [lowerKey(venue.name), venue]));
}

async function loadCountries(sql: ReturnType<typeof postgres>) {
  return sql<CountryRow[]>`
    SELECT c.id, ct.name
    FROM countries c
    JOIN country_translations ct ON ct.country_id = c.id
    WHERE ct.locale = 'en'
  `;
}

async function loadVenues(sql: ReturnType<typeof postgres>) {
  return sql<VenueRow[]>`
    SELECT v.id, v.slug, v.country_id, v.capacity, vt.name
    FROM venues v
    JOIN venue_translations vt ON vt.venue_id = v.id
    WHERE vt.locale = 'en'
  `;
}

async function insertVenueSeeds(sql: ReturnType<typeof postgres>, venueSeeds: VenueSeedDraft[]) {
  let insertedCount = 0;

  for (let i = 0; i < venueSeeds.length; i += BATCH_SIZE) {
    const chunk = venueSeeds.slice(i, i + BATCH_SIZE);
    const rows = await sql<InsertedVenueRow[]>`
      INSERT INTO venues (slug, country_id, capacity, updated_at)
      SELECT t.slug, t.country_id, t.capacity, NOW()
      FROM UNNEST(
        ${sql.array(chunk.map((venue) => venue.slug))}::text[],
        ${sql.array(chunk.map((venue) => venue.countryId))}::int[],
        ${sql.array(chunk.map((venue) => venue.capacity))}::int[]
      ) AS t(slug, country_id, capacity)
      ON CONFLICT (slug)
      DO UPDATE SET
        country_id = COALESCE(venues.country_id, EXCLUDED.country_id),
        capacity = CASE
          WHEN venues.capacity IS NULL OR venues.capacity = 0 THEN EXCLUDED.capacity
          ELSE venues.capacity
        END,
        updated_at = NOW()
      RETURNING (xmax = 0) AS inserted
    `;
    insertedCount += rows.filter((row) => row.inserted).length;
  }

  return insertedCount;
}

async function upsertVenueTranslations(sql: ReturnType<typeof postgres>, venues: VenueRow[]) {
  for (let i = 0; i < venues.length; i += BATCH_SIZE) {
    const chunk = venues.slice(i, i + BATCH_SIZE);
    await sql`
      INSERT INTO venue_translations (venue_id, locale, name)
      SELECT t.venue_id, 'en', t.name
      FROM UNNEST(
        ${sql.array(chunk.map((venue) => venue.id))}::int[],
        ${sql.array(chunk.map((venue) => venue.name))}::text[]
      ) AS t(venue_id, name)
      ON CONFLICT (venue_id, locale)
      DO UPDATE SET name = EXCLUDED.name
    `;
  }
}

async function updateVenuesBatch(sql: ReturnType<typeof postgres>, venues: VenueRow[]) {
  for (let i = 0; i < venues.length; i += BATCH_SIZE) {
    const chunk = venues.slice(i, i + BATCH_SIZE);
    await sql`
      UPDATE venues v
      SET
        country_id = COALESCE(v.country_id, t.country_id),
        capacity = CASE
          WHEN v.capacity IS NULL OR v.capacity = 0 THEN t.capacity
          ELSE v.capacity
        END,
        updated_at = NOW()
      FROM UNNEST(
        ${sql.array(chunk.map((venue) => venue.id))}::int[],
        ${sql.array(chunk.map((venue) => venue.country_id))}::int[],
        ${sql.array(chunk.map((venue) => venue.capacity))}::int[]
      ) AS t(id, country_id, capacity)
      WHERE v.id = t.id
    `;
  }
}

async function updateTeamsBatch(sql: ReturnType<typeof postgres>, drafts: TeamUpdateDraft[]) {
  for (let i = 0; i < drafts.length; i += BATCH_SIZE) {
    const chunk = drafts.slice(i, i + BATCH_SIZE);
    await sql`
      UPDATE teams t
      SET
        country_id = COALESCE(t.country_id, d.country_id),
        founded_year = COALESCE(t.founded_year, d.founded_year),
        venue_id = COALESCE(t.venue_id, d.venue_id),
        updated_at = NOW()
      FROM UNNEST(
        ${sql.array(chunk.map((draft) => draft.teamId))}::int[],
        ${sql.array(chunk.map((draft) => draft.countryId))}::int[],
        ${sql.array(chunk.map((draft) => draft.foundedYear))}::int[],
        ${sql.array(chunk.map((draft) => draft.venueId))}::int[]
      ) AS d(team_id, country_id, founded_year, venue_id)
      WHERE t.id = d.team_id
    `;
  }
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const { baseClubs } = await import(new URL('../src/data/clubs.ts', import.meta.url).href) as { baseClubs: BaseClubRecord[] };
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(connectionString, { max: 1, idle_timeout: 20, prepare: false });

  try {
    const teams = await sql<TeamRow[]>`
      SELECT
        t.id,
        t.slug,
        COALESCE(tt.name, t.slug) AS name,
        t.country_id,
        t.founded_year,
        t.venue_id,
        v.capacity AS venue_capacity
      FROM teams t
      LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
      LEFT JOIN venues v ON v.id = t.venue_id
      WHERE t.is_national = FALSE
    `;

    const donors = await sql<DonorRow[]>`
      SELECT
        t.id,
        t.slug,
        COALESCE(tt.name, t.slug) AS name,
        t.country_id,
        t.founded_year,
        t.venue_id,
        v.capacity AS venue_capacity
      FROM teams t
      LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
      LEFT JOIN venues v ON v.id = t.venue_id
      WHERE t.is_national = FALSE
        AND t.founded_year IS NOT NULL
        AND t.venue_id IS NOT NULL
        AND v.capacity IS NOT NULL
    `;
    const countries = await loadCountries(sql);
    let venueByName = buildVenueByName(await loadVenues(sql));
    const countryIdByName = buildCountryIdByName(countries);

    const donorMap = new Map<string, DonorRow[]>();
    for (const donor of donors) {
      const key = normalizeName(donor.name);
      const existing = donorMap.get(key) ?? [];
      existing.push(donor);
      donorMap.set(key, existing);
    }

    const baseClubMap = new Map<string, BaseClubRecord[]>();
    for (const club of baseClubs) {
      const key = normalizeName(club.name);
      const existing = baseClubMap.get(key) ?? [];
      existing.push(club);
      baseClubMap.set(key, existing);
    }

    let updatedFromDonor = 0;
    let updatedFromBaseClub = 0;
    let venuesCreated = 0;

    const donorTeamUpdates: TeamUpdateDraft[] = [];
    const baseClubAssignments = new Map<number, BaseClubRecord>();

    for (const team of teams) {
      if (
        team.country_id !== null
        && team.founded_year !== null
        && team.venue_id !== null
        && team.venue_capacity !== null
        && team.venue_capacity !== 0
      ) {
        continue;
      }

      const key = normalizeName(team.name);
      const donorCandidates = (donorMap.get(key) ?? []).filter((donor) => donor.id !== team.id);
      if (donorCandidates.length === 1) {
        const donor = donorCandidates[0]!;
        donorTeamUpdates.push({
          countryId: donor.country_id,
          foundedYear: donor.founded_year,
          teamId: team.id,
          venueId: donor.venue_id,
        });
        continue;
      }

      const baseClubCandidates = baseClubMap.get(key) ?? [];
      const uniqueBaseClubCandidates = [...new Map(baseClubCandidates.map((club) => [getBaseClubMetadataKey(club), club])).values()];
      if (uniqueBaseClubCandidates.length !== 1) {
        continue;
      }

      baseClubAssignments.set(team.id, uniqueBaseClubCandidates[0]!);
    }

    const venueSeedMap = new Map<string, VenueSeedDraft>();
    for (const baseClub of baseClubAssignments.values()) {
      const venueNameKey = lowerKey(baseClub.stadium);
      if (venueByName.has(venueNameKey)) {
        continue;
      }

      if (!venueSeedMap.has(venueNameKey)) {
        venueSeedMap.set(venueNameKey, {
          capacity: baseClub.stadiumCapacity,
          countryId: countryIdByName.get(lowerKey(baseClub.country)) ?? null,
          name: baseClub.stadium,
          slug: slugify(baseClub.stadium),
        });
      }
    }

    await sql`BEGIN`;
    try {
      const venueSeeds = Array.from(venueSeedMap.values());
      venuesCreated = await insertVenueSeeds(sql, venueSeeds);

      if (venueSeeds.length > 0) {
        venueByName = buildVenueByName(await loadVenues(sql));
      }

      const venueMetadataRows = Array.from(
        new Map(
          Array.from(baseClubAssignments.values()).flatMap((baseClub) => {
            const venue = venueByName.get(lowerKey(baseClub.stadium));
            if (!venue) {
              return [] as VenueRow[];
            }

            return [{
              id: venue.id,
              slug: venue.slug,
              name: baseClub.stadium,
              country_id: countryIdByName.get(lowerKey(baseClub.country)) ?? null,
              capacity: baseClub.stadiumCapacity,
            }];
          }).map((venue) => [venue.id, venue]),
        ).values(),
      );

      await updateVenuesBatch(sql, venueMetadataRows);
      await upsertVenueTranslations(sql, venueMetadataRows);

      const baseClubTeamUpdates: TeamUpdateDraft[] = [];
      for (const [teamId, baseClub] of baseClubAssignments.entries()) {
        baseClubTeamUpdates.push({
          countryId: countryIdByName.get(lowerKey(baseClub.country)) ?? null,
          foundedYear: baseClub.founded,
          teamId,
          venueId: venueByName.get(lowerKey(baseClub.stadium))?.id ?? null,
        });
      }

      await updateTeamsBatch(sql, donorTeamUpdates);
      await updateTeamsBatch(sql, baseClubTeamUpdates);
      updatedFromDonor = donorTeamUpdates.length;
      updatedFromBaseClub = baseClubTeamUpdates.length;

      await sql`COMMIT`;
    } catch (error) {
      await sql`ROLLBACK`;
      throw error;
    }

    const remaining = await sql`
      SELECT count(*)::int AS count
      FROM teams t
      LEFT JOIN venues v ON v.id = t.venue_id
      WHERE t.is_national = FALSE
        AND (t.founded_year IS NULL OR t.country_id IS NULL OR t.venue_id IS NULL OR v.capacity IS NULL)
    `;

    console.log(JSON.stringify({
      updatedFromDonor,
      updatedFromBaseClub,
      venuesCreated,
      remainingMissing: remaining[0]?.count ?? 0,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}

await main();
