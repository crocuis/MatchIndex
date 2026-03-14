import postgres from 'postgres';

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
}

interface VenueRow {
  id: number;
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

    await sql`BEGIN`;
    try {
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
          const donor = donorCandidates[0];
          await sql`
            UPDATE teams
            SET
              country_id = COALESCE(country_id, ${donor.country_id}),
              founded_year = COALESCE(founded_year, ${donor.founded_year}),
              venue_id = COALESCE(venue_id, ${donor.venue_id}),
              updated_at = NOW()
            WHERE id = ${team.id}
          `;
          updatedFromDonor += 1;
          continue;
        }

        const baseClubCandidates = baseClubMap.get(key) ?? [];
        const uniqueBaseClubCandidates = [...new Map(baseClubCandidates.map((club) => [getBaseClubMetadataKey(club), club])).values()];
        if (uniqueBaseClubCandidates.length !== 1) {
          continue;
        }

        const baseClub = uniqueBaseClubCandidates[0];
        const countryRows = await sql<CountryRow[]>`
          SELECT c.id
          FROM countries c
          JOIN country_translations ct ON ct.country_id = c.id
          WHERE ct.locale = 'en'
            AND LOWER(ct.name) = LOWER(${baseClub.country})
          LIMIT 1
        `;
        const countryId = countryRows[0]?.id ?? null;

        const venueRows = await sql<VenueRow[]>`
          SELECT v.id
          FROM venues v
          JOIN venue_translations vt ON vt.venue_id = v.id
          WHERE vt.locale = 'en'
            AND LOWER(vt.name) = LOWER(${baseClub.stadium})
          LIMIT 1
        `;

        let venueId = venueRows[0]?.id ?? null;
        if (!venueId) {
          const insertedVenueRows = await sql<VenueRow[]>`
            INSERT INTO venues (slug, country_id, capacity, updated_at)
            VALUES (${slugify(baseClub.stadium)}, ${countryId}, ${baseClub.stadiumCapacity}, NOW())
            ON CONFLICT (slug)
            DO UPDATE SET
              country_id = COALESCE(venues.country_id, EXCLUDED.country_id),
              capacity = COALESCE(venues.capacity, EXCLUDED.capacity),
              updated_at = NOW()
            RETURNING id
          `;
          venueId = insertedVenueRows[0]?.id ?? null;
          if (venueId) {
            venuesCreated += 1;
          }
        }

        if (venueId) {
          await sql`
            UPDATE venues
            SET
              country_id = COALESCE(country_id, ${countryId}),
              capacity = CASE
                WHEN capacity IS NULL OR capacity = 0 THEN ${baseClub.stadiumCapacity}
                ELSE capacity
              END,
              updated_at = NOW()
            WHERE id = ${venueId}
          `;
        }

        if (venueId) {
          await sql`
            INSERT INTO venue_translations (venue_id, locale, name)
            VALUES (${venueId}, 'en', ${baseClub.stadium})
            ON CONFLICT (venue_id, locale)
            DO UPDATE SET name = EXCLUDED.name
          `;
        }

        await sql`
          UPDATE teams
          SET
            country_id = COALESCE(country_id, ${countryId}),
            founded_year = COALESCE(founded_year, ${baseClub.founded}),
            venue_id = COALESCE(venue_id, ${venueId}),
            updated_at = NOW()
          WHERE id = ${team.id}
        `;
        updatedFromBaseClub += 1;
      }

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
