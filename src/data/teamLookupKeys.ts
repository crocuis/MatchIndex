export function normalizeTeamLookupName(value: string | null | undefined) {
  return (value ?? '')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\b(fc|cf|afc|ac|as|fk|sk|ssc|club|de|del|futbol|football|balompie|town|eindhoven)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function createTeamLookupKeys(name: string | null | undefined) {
  const rawName = (name ?? '').trim();
  if (rawName.length === 0) {
    return [];
  }

  const normalized = normalizeTeamLookupName(rawName);
  const withoutNumericTokens = normalized
    .replace(/\b\d+\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const expandedAbbreviations = withoutNumericTokens
    .replace(/\brc\b/g, 'racing')
    .replace(/\s+/g, ' ')
    .trim();

  return [...new Set([
    rawName.toLowerCase(),
    normalized,
    withoutNumericTokens,
    expandedAbbreviations,
  ].filter((value) => value.length > 0))];
}
