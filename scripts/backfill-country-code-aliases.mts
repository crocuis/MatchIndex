import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { COUNTRY_CODE_ALIASES, COUNTRY_CODE_SKIP } from '../src/data/countryCodeAliasSeeds.ts';

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
    const unresolved: string[] = [];
    let inserted = 0;

    for (const [aliasCode, canonicalCode] of Object.entries(COUNTRY_CODE_ALIASES)) {
      if (COUNTRY_CODE_SKIP.has(canonicalCode)) {
        continue;
      }

      const rows = await sql<Array<{ id: number }>>`
        SELECT id
        FROM countries
        WHERE code_alpha3 = ${canonicalCode}
        LIMIT 1
      `;
      const countryId = rows[0]?.id;

      if (!countryId) {
        unresolved.push(`${aliasCode} -> ${canonicalCode}`);
        continue;
      }

      if (!options.dryRun) {
        await sql`
          INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
          VALUES ('country', ${countryId}, ${aliasCode}, NULL, 'common', FALSE, 'approved', 'legacy', 'country_code_alias_seed')
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

      inserted += 1;
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
