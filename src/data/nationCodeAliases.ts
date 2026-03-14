import { COUNTRY_CODE_ALIASES, COUNTRY_CODE_SKIP } from './countryCodeAliasSeeds';

export const NATION_CODE_ALIASES = COUNTRY_CODE_ALIASES;
export const NATION_CODE_SKIP = COUNTRY_CODE_SKIP;

export function resolveNationCodeAlias(code: string) {
  const normalized = code.trim().toUpperCase();
  return NATION_CODE_ALIASES[normalized] ?? normalized;
}
