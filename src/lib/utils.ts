import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';
import type { Club, Match } from '@/data/types';

function hasConcreteMatchTime(timeStr?: string | null): timeStr is string {
  return Boolean(timeStr && timeStr.includes(':'));
}

function getFixedDate(dateStr: string) {
  const [year, month, day] = dateStr.split('-').map(Number);
  return new Date(Date.UTC(year, month - 1, day, 0, 0));
}

/** Merge Tailwind classes with conflict resolution */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/** Format a date string to a compact display format */
export function formatDate(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-GB', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  });
}

/** Format a date string to a short display format (e.g., "7 Mar") */
export function formatDateShort(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-GB', {
    day: 'numeric',
    month: 'short',
  });
}

function getMatchDate(dateStr: string, timeStr?: string | null, sourceOffsetMinutes: number = 0): Date {
  const [year, month, day] = dateStr.split('-').map(Number);
  const normalizedTime = hasConcreteMatchTime(timeStr) ? timeStr : '00:00';
  const [hour, minute] = normalizedTime.split(':').map(Number);

  return new Date(Date.UTC(year, month - 1, day, hour, minute) - (sourceOffsetMinutes * 60 * 1000));
}

export function getMatchSourceOffsetMinutes(match: Pick<Match, 'id' | 'venue'>): number {
  if (!match.id.startsWith('m-wc26-')) {
    return 0;
  }

  if (
    match.venue.includes('Mexico City')
    || match.venue.includes('Guadalajara')
    || match.venue.includes('Monterrey')
  ) {
    return -360;
  }

  if (match.venue.includes('Toronto') || match.venue.includes('New York') || match.venue.includes('Boston') || match.venue.includes('Philadelphia') || match.venue.includes('Miami') || match.venue.includes('Atlanta')) {
    return -240;
  }

  if (match.venue.includes('Houston') || match.venue.includes('Dallas') || match.venue.includes('Kansas City')) {
    return -300;
  }

  if (
    match.venue.includes('Vancouver')
    || match.venue.includes('Seattle')
    || match.venue.includes('Los Angeles')
    || match.venue.includes('San Francisco Bay Area')
  ) {
    return -420;
  }

  return 0;
}

export function getMatchDateKeyForTimeZone(dateStr: string, timeStr: string | null | undefined, timeZone: string, sourceOffsetMinutes: number = 0): string {
  if (!hasConcreteMatchTime(timeStr)) {
    return dateStr;
  }

  return new Intl.DateTimeFormat('sv-SE', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    timeZone,
  }).format(getMatchDate(dateStr, timeStr, sourceOffsetMinutes));
}

export function formatMatchDateForTimeZone(dateStr: string, timeStr: string | null | undefined, locale: string, timeZone: string, sourceOffsetMinutes: number = 0): string {
  if (!hasConcreteMatchTime(timeStr)) {
    return new Intl.DateTimeFormat(locale, {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      timeZone: 'UTC',
    }).format(getFixedDate(dateStr));
  }

  return new Intl.DateTimeFormat(locale, {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    timeZone,
  }).format(getMatchDate(dateStr, timeStr, sourceOffsetMinutes));
}

export function formatMatchDateShortForTimeZone(dateStr: string, timeStr: string | null | undefined, locale: string, timeZone: string, sourceOffsetMinutes: number = 0): string {
  if (!hasConcreteMatchTime(timeStr)) {
    return new Intl.DateTimeFormat(locale, {
      day: 'numeric',
      month: 'short',
      timeZone: 'UTC',
    }).format(getFixedDate(dateStr));
  }

  return new Intl.DateTimeFormat(locale, {
    day: 'numeric',
    month: 'short',
    timeZone,
  }).format(getMatchDate(dateStr, timeStr, sourceOffsetMinutes));
}

export function formatMatchTimeForTimeZone(dateStr: string, timeStr: string | null | undefined, locale: string, timeZone: string, sourceOffsetMinutes: number = 0): string {
  if (!hasConcreteMatchTime(timeStr)) {
    return '--:--';
  }

  return new Intl.DateTimeFormat(locale, {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone,
  }).format(getMatchDate(dateStr, timeStr, sourceOffsetMinutes));
}

export function formatMatchDateLabelForTimeZone(dateStr: string, timeStr: string | null | undefined, locale: string, timeZone: string, sourceOffsetMinutes: number = 0): string {
  if (!hasConcreteMatchTime(timeStr)) {
    return new Intl.DateTimeFormat(locale, {
      month: 'short',
      day: 'numeric',
      weekday: 'short',
      timeZone: 'UTC',
    }).format(getFixedDate(dateStr));
  }

  return new Intl.DateTimeFormat(locale, {
    month: 'short',
    day: 'numeric',
    weekday: 'short',
    timeZone,
  }).format(getMatchDate(dateStr, timeStr, sourceOffsetMinutes));
}

export function formatMatchDateTimeForTimeZone(dateStr: string, timeStr: string | null | undefined, locale: string, timeZone: string, sourceOffsetMinutes: number = 0): string {
  if (!hasConcreteMatchTime(timeStr)) {
    return formatMatchDateForTimeZone(dateStr, timeStr, locale, timeZone, sourceOffsetMinutes);
  }

  return new Intl.DateTimeFormat(locale, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    timeZone,
  }).format(getMatchDate(dateStr, timeStr, sourceOffsetMinutes));
}

/** Format large numbers with commas (e.g., 60000 → "60,000") */
export function formatNumber(num: number): string {
  return num.toLocaleString('en-US');
}

export function getClubDisplayName(club: Pick<Club, 'name' | 'koreanName'>, locale: string): string {
  return locale === 'ko' ? club.koreanName : club.name;
}

export function getCanonicalSeasonSlug(value: string): string {
  const trimmed = value.trim();
  const singleYearMatch = trimmed.match(/^(\d{4})$/);

  if (singleYearMatch) {
    return singleYearMatch[1];
  }

  const seasonRangeMatch = trimmed.match(/^(\d{4})[-/](\d{2}|\d{4})$/);
  if (!seasonRangeMatch) {
    return trimmed;
  }

  const startYear = seasonRangeMatch[1];
  const endYear = seasonRangeMatch[2].slice(-2);
  return `${startYear}/${endYear}`;
}

export function seasonSlugMatches(left: string, right: string): boolean {
  return getCanonicalSeasonSlug(left) === getCanonicalSeasonSlug(right);
}

/** Calculate age from date of birth string */
export function calculateAge(dateOfBirth: string): number {
  const dob = new Date(dateOfBirth);
  const today = new Date();
  let age = today.getFullYear() - dob.getFullYear();
  const monthDiff = today.getMonth() - dob.getMonth();
  if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < dob.getDate())) {
    age--;
  }
  return age;
}

/** Get position color class for player positions */
export function getPositionColor(position: string): string {
  switch (position) {
    case 'GK':
      return 'text-amber-400 bg-amber-400/10';
    case 'DEF':
      return 'text-blue-400 bg-blue-400/10';
    case 'MID':
      return 'text-emerald-400 bg-emerald-400/10';
    case 'FWD':
      return 'text-red-400 bg-red-400/10';
    default:
      return 'text-zinc-400 bg-zinc-400/10';
  }
}

/** Get form indicator color class */
export function getFormColor(result: 'W' | 'D' | 'L'): string {
  switch (result) {
    case 'W':
      return 'bg-emerald-500';
    case 'D':
      return 'bg-zinc-500';
    case 'L':
      return 'bg-red-500';
  }
}

/** Get match status display text and color */
export function getMatchStatusDisplay(status: string): { text: string; className: string } {
  switch (status) {
    case 'finished':
      return { text: 'FT', className: 'text-zinc-400' };
    case 'live':
      return { text: 'LIVE', className: 'text-red-400 animate-pulse' };
    case 'timed':
    case 'scheduled':
      return { text: 'SCH', className: 'text-zinc-500' };
    case 'postponed':
      return { text: 'PPD', className: 'text-amber-400' };
    case 'suspended':
      return { text: 'SUSP', className: 'text-amber-400' };
    case 'cancelled':
      return { text: 'CANC', className: 'text-zinc-300' };
    case 'awarded':
      return { text: 'AWD', className: 'text-zinc-300' };
    default:
      return { text: status, className: 'text-zinc-500' };
  }
}
