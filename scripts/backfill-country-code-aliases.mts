import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { COUNTRY_CODE_ALIASES, COUNTRY_CODE_SKIP } from '../src/data/countryCodeAliasSeeds.ts';

const BATCH_SIZE = 500;

interface CountryRow {
  code_alpha3: string;
  id: number;
}

interface AliasDraft {
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

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  const sql = getSql();

  try {
    const countries = await sql<CountryRow[]>`
      SELECT id, code_alpha3
      FROM countries
    `;
    const countryIdByCode = new Map(countries.map((country) => [country.code_alpha3, country.id]));
    const unresolved: string[] = [];
    let inserted = 0;
    const drafts: AliasDraft[] = [];

    for (const [aliasCode, canonicalCode] of Object.entries(COUNTRY_CODE_ALIASES)) {
      if (COUNTRY_CODE_SKIP.has(canonicalCode)) {
        continue;
      }

      const countryId = countryIdByCode.get(canonicalCode);

      if (!countryId) {
        unresolved.push(`${aliasCode} -> ${canonicalCode}`);
        continue;
      }

      drafts.push({ alias: aliasCode, entityId: countryId });
      inserted += 1;
    }

    if (!options.dryRun) {
      for (let i = 0; i < drafts.length; i += BATCH_SIZE) {
        const chunk = drafts.slice(i, i + BATCH_SIZE);
        await sql`
          INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
          SELECT 'country', t.entity_id, t.alias, NULL, 'common', FALSE, 'approved', 'legacy', 'country_code_alias_seed'
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
