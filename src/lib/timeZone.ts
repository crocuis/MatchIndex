export const TIME_ZONE_COOKIE = 'MATCHINDEX_TIME_ZONE';
export const DEFAULT_TIME_ZONE = 'UTC';

export function normalizeTimeZone(value: string | undefined | null) {
  if (!value) {
    return DEFAULT_TIME_ZONE;
  }

  try {
    new Intl.DateTimeFormat('en-US', { timeZone: value }).format(new Date());
    return value;
  } catch {
    return DEFAULT_TIME_ZONE;
  }
}
