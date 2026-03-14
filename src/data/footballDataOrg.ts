const FOOTBALL_DATA_SOURCE_SLUG = 'football_data_org';
const FOOTBALL_DATA_SOURCE_NAME = 'football-data.org v4';
const FOOTBALL_DATA_BASE_URL = 'https://api.football-data.org/v4';
const DEFAULT_FOOTBALL_DATA_REQUEST_DELAY_MS = 6500;
const DEFAULT_FOOTBALL_DATA_RATE_LIMIT_RETRY_MS = 30000;

const DEFAULT_COMPETITION_CODES = ['CL', 'EL', 'PD', 'PL', 'BL1', 'FL1', 'SA'] as const;

export interface FootballDataOrgCompetitionTarget {
  code: string;
  name: string;
}

export interface FootballDataCompetitionMatchesFilterOptions {
  dateFrom?: string;
  dateTo?: string;
  localDate?: string;
  status?: string;
  timeZone?: string;
}

export interface FootballDataOrgMatchSummary {
  id?: number;
  utcDate?: string;
  status?: string;
  matchday?: number;
  stage?: string;
  group?: string | null;
  venue?: string | null;
  homeTeam?: { id?: number; name?: string };
  awayTeam?: { id?: number; name?: string };
  score?: {
    fullTime?: {
      home?: number | null;
      away?: number | null;
    };
  };
}

export interface FootballDataOrgAreaSummary {
  id?: number;
  name?: string;
  code?: string | null;
}

export interface FootballDataOrgSeasonSummary {
  id?: number;
  startDate?: string;
  endDate?: string;
  currentMatchday?: number | null;
}

export interface FootballDataOrgCompetitionResponse {
  id?: number;
  code?: string;
  name?: string;
  type?: string;
  emblem?: string | null;
  area?: FootballDataOrgAreaSummary;
  currentSeason?: FootballDataOrgSeasonSummary;
  seasons?: FootballDataOrgSeasonSummary[];
}

export interface FootballDataOrgTeamSummary {
  id?: number;
  name?: string;
  shortName?: string;
  tla?: string;
  crest?: string | null;
  area?: FootballDataOrgAreaSummary;
}

export interface FootballDataOrgTeamsResponse {
  competition?: FootballDataOrgCompetitionResponse;
  season?: FootballDataOrgSeasonSummary;
  teams?: FootballDataOrgTeamSummary[];
}

export interface FootballDataOrgMatchesResponse {
  competition?: FootballDataOrgCompetitionResponse;
  filters?: {
    season?: string;
  };
  resultSet?: {
    count?: number;
  };
  matches?: FootballDataOrgMatchSummary[];
}

const COMPETITION_LABELS: Record<string, string> = {
  BL1: '1. Bundesliga',
  CL: 'Champions League',
  EL: 'Europa League',
  FL1: 'Ligue 1',
  PD: 'La Liga',
  PL: 'Premier League',
  SA: 'Serie A',
};

let lastFootballDataRequestStartedAt = 0;

function getFootballDataRequestDelayMs() {
  const raw = process.env.FOOTBALL_DATA_REQUEST_DELAY_MS?.trim();
  if (!raw) {
    return DEFAULT_FOOTBALL_DATA_REQUEST_DELAY_MS;
  }

  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : DEFAULT_FOOTBALL_DATA_REQUEST_DELAY_MS;
}

function getFootballDataRateLimitRetryMs() {
  const raw = process.env.FOOTBALL_DATA_RATE_LIMIT_RETRY_MS?.trim();
  if (!raw) {
    return DEFAULT_FOOTBALL_DATA_RATE_LIMIT_RETRY_MS;
  }

  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_FOOTBALL_DATA_RATE_LIMIT_RETRY_MS;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseFootballDataRetryDelayMs(response: Response, bodyText: string) {
  const retryAfterHeader = response.headers.get('retry-after');
  if (retryAfterHeader) {
    const retryAfterSeconds = Number.parseInt(retryAfterHeader, 10);
    if (Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0) {
      return retryAfterSeconds * 1000;
    }
  }

  const retryMatch = bodyText.match(/wait\s+(\d+)\s+seconds/i);
  if (retryMatch) {
    const retrySeconds = Number.parseInt(retryMatch[1], 10);
    if (Number.isFinite(retrySeconds) && retrySeconds > 0) {
      return retrySeconds * 1000;
    }
  }

  return getFootballDataRateLimitRetryMs();
}

export function getFootballDataSourceConfig() {
  return {
    slug: FOOTBALL_DATA_SOURCE_SLUG,
    name: FOOTBALL_DATA_SOURCE_NAME,
    baseUrl: process.env.FOOTBALL_DATA_BASE_URL?.trim() || FOOTBALL_DATA_BASE_URL,
    apiKey: process.env.FOOTBALL_DATA_API_KEY?.trim(),
  };
}

export function getDefaultFootballDataCompetitionTargets(): FootballDataOrgCompetitionTarget[] {
  return DEFAULT_COMPETITION_CODES.map((code) => ({
    code,
    name: COMPETITION_LABELS[code] ?? code,
  }));
}

export function parseFootballDataCompetitionTargets(rawCodes?: string[]) {
  const normalizedCodes = (rawCodes ?? []).map((code) => code.trim().toUpperCase()).filter(Boolean);
  const codes = normalizedCodes.length > 0 ? normalizedCodes : [...DEFAULT_COMPETITION_CODES];

  return codes.map((code) => ({
    code,
    name: COMPETITION_LABELS[code] ?? code,
  }));
}

function normalizeFootballDataDateFilter(value: string | undefined) {
  const normalized = value?.trim();
  if (!normalized) {
    return undefined;
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    throw new Error(`Invalid football-data date filter: ${normalized}`);
  }

  return normalized;
}

function normalizeFootballDataTimeZone(value: string | undefined) {
  const normalized = value?.trim();
  if (!normalized) {
    return undefined;
  }

  try {
    new Intl.DateTimeFormat('en-CA', { timeZone: normalized }).format(new Date());
  } catch {
    throw new Error(`Invalid football-data timezone: ${normalized}`);
  }

  return normalized;
}

export function formatFootballDataDateInTimeZone(date: Date, timeZone: string) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  const parts = formatter.formatToParts(date);
  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;

  if (!year || !month || !day) {
    throw new Error(`Failed to format football-data date in timezone: ${timeZone}`);
  }

  return `${year}-${month}-${day}`;
}

function shiftFootballDataIsoDate(isoDate: string, days: number) {
  const shifted = new Date(`${isoDate}T00:00:00Z`);
  shifted.setUTCDate(shifted.getUTCDate() + days);
  return shifted.toISOString().slice(0, 10);
}

function getFootballDataOverlappingUtcDates(localDate: string, timeZone: string) {
  const candidates = [
    shiftFootballDataIsoDate(localDate, -1),
    localDate,
    shiftFootballDataIsoDate(localDate, 1),
  ];

  return candidates.filter((candidate) => {
    const startMatches = formatFootballDataDateInTimeZone(new Date(`${candidate}T00:00:00Z`), timeZone) === localDate;
    const endMatches = formatFootballDataDateInTimeZone(new Date(`${candidate}T23:59:59Z`), timeZone) === localDate;
    return startMatches || endMatches;
  });
}

export function resolveFootballDataCompetitionMatchesFilters(
  filters: FootballDataCompetitionMatchesFilterOptions = {},
) {
  const normalizedStatus = filters.status?.trim().toUpperCase();
  const normalizedDateFrom = normalizeFootballDataDateFilter(filters.dateFrom);
  const normalizedDateTo = normalizeFootballDataDateFilter(filters.dateTo);
  const normalizedLocalDate = normalizeFootballDataDateFilter(filters.localDate);
  const normalizedTimeZone = normalizeFootballDataTimeZone(filters.timeZone);

  if (normalizedLocalDate && !normalizedTimeZone) {
    throw new Error('football-data localDate filter requires timeZone');
  }

  if (normalizedLocalDate && (normalizedDateFrom || normalizedDateTo)) {
    throw new Error('football-data localDate filter cannot be combined with dateFrom/dateTo');
  }

  let dateFrom = normalizedDateFrom;
  let dateTo = normalizedDateTo;

  if (normalizedLocalDate && normalizedTimeZone) {
    const overlappingDates = getFootballDataOverlappingUtcDates(normalizedLocalDate, normalizedTimeZone);
    dateFrom = overlappingDates[0];
    dateTo = overlappingDates.at(-1);
  }

  if (dateFrom && dateTo && dateFrom > dateTo) {
    throw new Error('football-data dateFrom must be earlier than or equal to dateTo');
  }

  return {
    dateFrom,
    dateTo,
    localDate: normalizedLocalDate,
    status: normalizedStatus,
    timeZone: normalizedTimeZone,
  };
}

export function filterFootballDataMatchesResponse(
  payload: FootballDataOrgMatchesResponse,
  filters: FootballDataCompetitionMatchesFilterOptions = {},
): FootballDataOrgMatchesResponse {
  const resolvedFilters = resolveFootballDataCompetitionMatchesFilters(filters);
  if (!resolvedFilters.localDate || !resolvedFilters.timeZone) {
    return payload;
  }

  const { localDate, timeZone } = resolvedFilters;

  const matches = (payload.matches ?? []).filter((match) => {
    if (!match.utcDate) {
      return false;
    }

    return formatFootballDataDateInTimeZone(new Date(match.utcDate), timeZone) === localDate;
  });

  return {
    ...payload,
    matches,
    resultSet: {
      ...(payload.resultSet ?? {}),
      count: matches.length,
    },
  };
}

export function buildFootballDataCompetitionMatchesPath(
  code: string,
  season: number,
  filters: FootballDataCompetitionMatchesFilterOptions = {},
) {
  const resolvedFilters = resolveFootballDataCompetitionMatchesFilters(filters);
  const params = new URLSearchParams({
    season: String(season),
  });

  if (resolvedFilters.status) {
    params.set('status', resolvedFilters.status);
  }

  if (resolvedFilters.dateFrom) {
    params.set('dateFrom', resolvedFilters.dateFrom);
  }

  if (resolvedFilters.dateTo) {
    params.set('dateTo', resolvedFilters.dateTo);
  }

  return `/competitions/${code}/matches?${params.toString()}`;
}

export function buildFootballDataCompetitionTeamsPath(code: string, season: number) {
  return `/competitions/${code}/teams?season=${season}`;
}

export async function fetchFootballDataJson<T>(path: string): Promise<T> {
  return fetchFootballDataJsonWithRetry<T>(path);
}

async function fetchFootballDataJsonWithRetry<T>(path: string, attempt: number = 1): Promise<T> {
  const { apiKey, baseUrl } = getFootballDataSourceConfig();

  if (!apiKey) {
    throw new Error('FOOTBALL_DATA_API_KEY is not set');
  }

  const delayMs = getFootballDataRequestDelayMs();
  const elapsed = Date.now() - lastFootballDataRequestStartedAt;
  if (lastFootballDataRequestStartedAt > 0 && elapsed < delayMs) {
    await sleep(delayMs - elapsed);
  }

  lastFootballDataRequestStartedAt = Date.now();

  const response = await fetch(`${baseUrl}${path}`, {
    headers: {
      'X-Auth-Token': apiKey,
    },
  });

  const bodyText = await response.text();

  if (response.status === 429 && attempt < 3) {
    await sleep(parseFootballDataRetryDelayMs(response, bodyText));
    return fetchFootballDataJsonWithRetry<T>(path, attempt + 1);
  }

  if (!response.ok) {
    throw new Error(`football-data.org request failed: ${response.status} ${response.statusText}${bodyText ? ` - ${bodyText}` : ''}`);
  }

  return JSON.parse(bodyText) as T;
}
