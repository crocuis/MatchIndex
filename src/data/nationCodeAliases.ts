export const NATION_CODE_ALIASES: Record<string, string> = {
  BUR: 'BFA',
  CAP: 'CPV',
  CON: 'COD',
  COS: 'CRC',
  COT: 'CIV',
  EQU: 'EQG',
  ICE: 'ISL',
  IRA: 'IRN',
  IRE: 'IRL',
  JAP: 'JPN',
  MAL: 'MLI',
  MAU: 'MTN',
  MOR: 'MAR',
  NET: 'NED',
  NEW: 'NZL',
  ROM: 'ROU',
  SAU: 'KSA',
  SER: 'SRB',
  SLO: 'SVK',
  SOU: 'RSA',
  SPA: 'ESP',
  SWI: 'SUI',
  UNI: 'USA',
};

export const NATION_CODE_SKIP = new Set(['AFR', 'EUR', 'INT', 'MON']);

export function resolveNationCodeAlias(code: string) {
  const normalized = code.trim().toUpperCase();
  return NATION_CODE_ALIASES[normalized] ?? normalized;
}
