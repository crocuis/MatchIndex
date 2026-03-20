import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { TEAM_LOOKUP_ALIAS_SEEDS } from '../src/data/teamLookupAliases.ts';
import { createTeamLookupKeys } from '../src/data/teamLookupKeys.ts';

const BATCH_SIZE = 500;

interface TeamLookupRow {
  id: number;
  slug: string;
  name: string | null;
  code_alpha3: string | null;
}

interface TeamLookupEntry {
  slug: string;
  codeAlpha3: string | null;
  name: string;
}

interface TeamLookupState {
  lookup: Map<string, TeamLookupEntry[]>;
  entries: TeamLookupEntry[];
  teamIdBySlug: Map<string, number>;
}

interface TeamAliasDraft {
  alias: string;
  entityId: number;
}

interface CliOptions {
  dryRun: boolean;
}

function parseArgs(argv: string[]): CliOptions {
  return {
    dryRun: argv.includes('--dry-run'),
  };
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    max: 1,
    idle_timeout: 5,
    prepare: false,
  });
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

function resolveCanonicalTeamSlug(
  lookup: Map<string, TeamLookupEntry[]>,
  teamName: string,
  preferredCountryCode: string | null = null,
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

function resolveLongestCanonicalTeamSlug(
  lookup: Map<string, TeamLookupEntry[]>,
  preferredNames: string[],
  preferredCountryCode: string | null = null,
) {
  const candidates = preferredNames
    .flatMap((name) => createTeamLookupKeys(name))
    .flatMap((key) => lookup.get(key) ?? []);
  const unique = [...candidates.reduce((map, entry) => {
    const existing = map.get(entry.slug);
    if (!existing || entry.name.length > existing.name.length) {
      map.set(entry.slug, entry);
    }

    return map;
  }, new Map<string, TeamLookupEntry>()).values()];

  const filtered = preferredCountryCode
    ? unique.filter((entry) => entry.codeAlpha3 === preferredCountryCode)
    : unique;
  const pool = filtered.length > 0 ? filtered : unique;

  if (pool.length === 0) {
    return null;
  }

  const sorted = [...pool].sort((left, right) => {
    const lengthDiff = right.name.length - left.name.length;
    if (lengthDiff !== 0) {
      return lengthDiff;
    }

    return left.name.localeCompare(right.name, 'en');
  });

  const top = sorted[0];
  const sameLength = sorted.filter((entry) => entry.name.length === top.name.length);
  return sameLength.length === 1 ? top.slug : null;
}

function buildLookupEntryKey(name: string) {
  return createTeamLookupKeys(name).at(-1) ?? name.toLowerCase().trim();
}

function resolveLongestCanonicalFromEntries(
  entries: TeamLookupEntry[],
  preferredNames: string[],
  preferredCountryCode: string | null = null,
) {
  const preferredKeys = new Set(preferredNames.map((name) => buildLookupEntryKey(name)));
  const matched = entries.filter((entry) => preferredKeys.has(buildLookupEntryKey(entry.name)));
  const filtered = preferredCountryCode
    ? matched.filter((entry) => entry.codeAlpha3 === preferredCountryCode)
    : matched;
  const pool = filtered.length > 0 ? filtered : matched;

  if (pool.length === 0) {
    return null;
  }

  const unique = [...pool.reduce((map, entry) => {
    const existing = map.get(entry.slug);
    if (!existing || entry.name.length > existing.name.length) {
      map.set(entry.slug, entry);
    }

    return map;
  }, new Map<string, TeamLookupEntry>()).values()];

  const sorted = [...unique].sort((left, right) => {
    const lengthDiff = right.name.length - left.name.length;
    if (lengthDiff !== 0) {
      return lengthDiff;
    }

    return left.name.localeCompare(right.name, 'en');
  });

  return sorted[0]?.slug ?? null;
}

async function loadExistingTeamLookup(sql: ReturnType<typeof getSql>): Promise<TeamLookupState> {
  const rows = await sql<TeamLookupRow[]>`
    SELECT DISTINCT id, slug, name, code_alpha3
    FROM (
      SELECT
        t.id,
        t.slug,
        tt.name,
        c.code_alpha3
      FROM teams t
      LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
      LEFT JOIN countries c ON c.id = t.country_id
      UNION ALL
      SELECT
        t.id,
        t.slug,
        ea.alias AS name,
        c.code_alpha3
      FROM teams t
      JOIN entity_aliases ea ON ea.entity_type = 'team' AND ea.entity_id = t.id
      LEFT JOIN countries c ON c.id = t.country_id
    ) lookup
  `;

  const lookup = new Map<string, TeamLookupEntry[]>();
  const entries: TeamLookupEntry[] = [];
  const teamIdBySlug = new Map<string, number>();
  for (const row of rows) {
    teamIdBySlug.set(row.slug, row.id);
    const entry = {
      slug: row.slug,
      codeAlpha3: row.code_alpha3,
      name: row.name ?? row.slug,
    };
    registerTeamLookupEntry(lookup, row.name ?? row.slug, entry);
    entries.push(entry);
  }

  return { lookup, entries, teamIdBySlug };
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  const sql = getSql();

  try {
    const { lookup, entries, teamIdBySlug } = await loadExistingTeamLookup(sql);
    const unresolved: string[] = [];
    let inserted = 0;
    const drafts: TeamAliasDraft[] = [];

    for (const seed of TEAM_LOOKUP_ALIAS_SEEDS) {
      const teamSlug = resolveCanonicalTeamSlug(lookup, seed.canonicalName)
        ?? resolveCanonicalTeamSlug(lookup, seed.alias)
        ?? resolveLongestCanonicalTeamSlug(lookup, [seed.alias, seed.canonicalName])
        ?? resolveLongestCanonicalFromEntries(entries, [seed.alias, seed.canonicalName]);
      if (!teamSlug) {
        unresolved.push(`${seed.alias} -> ${seed.canonicalName}`);
        continue;
      }

      const teamId = teamIdBySlug.get(teamSlug);
      if (!teamId) {
        unresolved.push(`${seed.alias} -> ${seed.canonicalName}`);
        continue;
      }

      drafts.push({ alias: seed.alias, entityId: teamId });

      const seededEntry = { slug: teamSlug, codeAlpha3: null, name: seed.alias };
      registerTeamLookupEntry(lookup, seed.alias, seededEntry);
      entries.push(seededEntry);
      inserted += 1;
    }

    if (!options.dryRun) {
      for (let i = 0; i < drafts.length; i += BATCH_SIZE) {
        const chunk = drafts.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
          SELECT 'team', t.entity_id, t.alias, 'en', 'common', FALSE, 'approved', 'legacy', 'team_lookup_alias_seed'
          FROM UNNEST(
            ${sql.array(chunk.map((draft) => draft.entityId))}::int[],
            ${sql.array(chunk.map((draft) => draft.alias))}::text[]
          ) AS t(entity_id, alias)
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
    }

    console.log(JSON.stringify({
      dryRun: options.dryRun,
      inserted,
      unresolved,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

void main();
