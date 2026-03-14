import type { Sql } from 'postgres';
import { COUNTRY_CODE_ALIASES, COUNTRY_CODE_SKIP } from './countryCodeAliasSeeds';

interface CountryAliasRow {
  canonical_code: string;
  alias: string | null;
}

export interface CountryCodeResolver {
  isSkipped(code?: string | null): boolean;
  resolve(code?: string | null): string | null;
}

export async function loadCountryCodeResolver(sql: Sql): Promise<CountryCodeResolver> {
  const rows = await sql<CountryAliasRow[]>`
    SELECT
      c.code_alpha3 AS canonical_code,
      ea.alias
    FROM countries c
    LEFT JOIN entity_aliases ea
      ON ea.entity_type = 'country'
      AND ea.entity_id = c.id
      AND ea.status = 'approved'
      AND ea.source_ref = 'country_code_alias_seed'
  `;

  const aliasMap = new Map<string, string>();

  for (const [alias, canonical] of Object.entries(COUNTRY_CODE_ALIASES)) {
    aliasMap.set(alias, canonical);
  }

  for (const row of rows) {
    aliasMap.set(row.canonical_code, row.canonical_code);
    if (row.alias) {
      aliasMap.set(row.alias.trim().toUpperCase(), row.canonical_code);
    }
  }

  function resolve(code?: string | null) {
    if (!code) {
      return null;
    }

    const normalized = code.trim().toUpperCase();
    return aliasMap.get(normalized) ?? normalized;
  }

  return {
    isSkipped(code?: string | null) {
      const resolved = resolve(code);
      return resolved !== null && COUNTRY_CODE_SKIP.has(resolved);
    },
    resolve,
  };
}
