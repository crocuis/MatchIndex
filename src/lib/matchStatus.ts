import type { MatchStatus } from '@/data/types';

const UPCOMING_MATCH_STATUSES: MatchStatus[] = ['scheduled', 'timed', 'postponed'];

export function isUpcomingMatchStatus(status: MatchStatus) {
  return UPCOMING_MATCH_STATUSES.includes(status);
}

export function isFinishedMatchStatus(status: MatchStatus) {
  return status === 'finished';
}

export function getMatchStatusClassName(status: MatchStatus) {
  switch (status) {
    case 'finished':
      return 'text-zinc-400';
    case 'live':
      return 'text-red-400 animate-pulse';
    case 'postponed':
    case 'suspended':
      return 'text-amber-400';
    case 'cancelled':
    case 'awarded':
      return 'text-zinc-300';
    case 'scheduled':
    case 'timed':
    default:
      return 'text-zinc-500';
  }
}

export function getMatchStatusBadgeVariant(status: MatchStatus): 'default' | 'danger' | 'warning' | 'info' {
  switch (status) {
    case 'finished':
      return 'default';
    case 'live':
      return 'danger';
    case 'postponed':
    case 'suspended':
    case 'cancelled':
    case 'awarded':
      return 'warning';
    case 'scheduled':
    case 'timed':
    default:
      return 'info';
  }
}
