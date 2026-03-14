export function normalizeTeamLookupName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\b(fc|cf|afc|sc|club|de|del|futbol|football|balompie|town|eindhoven)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function createTeamLookupKeys(name: string) {
  const normalized = normalizeTeamLookupName(name);
  return [...new Set([name.toLowerCase().trim(), normalized].filter((value) => value.length > 0))];
}
