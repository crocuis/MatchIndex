import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

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

/** Format large numbers with commas (e.g., 60000 → "60,000") */
export function formatNumber(num: number): string {
  return num.toLocaleString('en-US');
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
    case 'scheduled':
      return { text: 'SCH', className: 'text-zinc-500' };
    default:
      return { text: status, className: 'text-zinc-500' };
  }
}
