import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

interface LocalPlayerRecord {
  id: string;
  name: string;
  firstName: string;
  lastName: string;
  dateOfBirth: string;
  nationality: string;
}

interface ApiFootballPlayerMapping {
  playerId: string;
  externalId: string;
}

interface ApiFootballSearchResponse {
  errors?: Record<string, string>;
  results?: number;
  response?: unknown[];
}

interface ApiFootballCandidate {
  id: string;
  name: string;
  firstName?: string;
  lastName?: string;
  birthDate?: string;
}

export interface GenerateApiFootballPlayerMappingsOptions {
  dryRun?: boolean;
  limit?: number;
  playerId?: string;
}

export interface GenerateApiFootballPlayerMappingsSummary {
  dryRun: boolean;
  searchedPlayers: number;
  matchedPlayers: number;
  writtenMappings: number;
}

class ApiFootballRateLimitError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ApiFootballRateLimitError';
  }
}

function normalizeName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .toLowerCase();
}

function getMappingsFilePath() {
  const customPath = process.env.API_FOOTBALL_PLAYER_MAPPINGS_FILE?.trim();
  if (customPath) {
    return path.isAbsolute(customPath) ? customPath : path.join(process.cwd(), customPath);
  }

  return path.join(process.cwd(), 'data', 'api-football-player-mappings.json');
}

function getApiBaseUrl() {
  return process.env.API_FOOTBALL_BASE_URL?.trim() || 'https://v3.football.api-sports.io';
}

function getSearchPathTemplate() {
  return process.env.API_FOOTBALL_PLAYER_SEARCH_PATH?.trim() || '/players/profiles?search={search}';
}

function getRequestDelayMs() {
  const raw = process.env.API_FOOTBALL_REQUEST_DELAY_MS?.trim();
  if (!raw) {
    return 7000;
  }

  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 7000;
}

function getRateLimitRetryMs() {
  const raw = process.env.API_FOOTBALL_RATE_LIMIT_RETRY_MS?.trim();
  if (!raw) {
    return 65000;
  }

  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 65000;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function loadPlayers() {
  const playersModule = await import(new URL('./players.ts', import.meta.url).href);
  return playersModule.players as Array<LocalPlayerRecord>;
}

async function readExistingMappings() {
  const filePath = getMappingsFilePath();

  try {
    const payload = JSON.parse(await readFile(filePath, 'utf8')) as ApiFootballPlayerMapping[];
    return payload;
  } catch {
    return [] as ApiFootballPlayerMapping[];
  }
}

function selectPlayers(players: LocalPlayerRecord[], options: GenerateApiFootballPlayerMappingsOptions) {
  const filtered = options.playerId ? players.filter((player) => player.id === options.playerId) : players;
  return filtered.slice(0, options.limit ?? filtered.length);
}

function getSearchNames(player: LocalPlayerRecord) {
  const searchNames = [player.lastName, player.name, `${player.firstName} ${player.lastName}`.trim()];
  return Array.from(new Set(searchNames.map((value) => value.trim()).filter(Boolean)));
}

function extractCandidate(item: unknown): ApiFootballCandidate | null {
  if (!item || typeof item !== 'object') {
    return null;
  }

  const record = item as {
    player?: {
      id?: number | string;
      name?: string;
      firstname?: string;
      lastname?: string;
      birth?: { date?: string | null };
    };
    id?: number | string;
    name?: string;
    firstname?: string;
    lastname?: string;
    birth?: { date?: string | null };
  };

  const source = record.player ?? record;
  if (!source.id || !source.name) {
    return null;
  }

  return {
    id: String(source.id),
    name: source.name,
    firstName: source.firstname,
    lastName: source.lastname,
    birthDate: source.birth?.date ?? undefined,
  };
}

function scoreCandidate(player: LocalPlayerRecord, candidate: ApiFootballCandidate) {
  let score = 0;

  if (normalizeName(candidate.name) === normalizeName(player.name)) {
    score += 3;
  }

  if (candidate.firstName && normalizeName(candidate.firstName) === normalizeName(player.firstName)) {
    score += 2;
  }

  if (candidate.lastName && normalizeName(candidate.lastName) === normalizeName(player.lastName)) {
    score += 2;
  }

  if (candidate.birthDate && player.dateOfBirth && candidate.birthDate === player.dateOfBirth) {
    score += 4;
  }

  return score;
}

function pickBestCandidate(player: LocalPlayerRecord, candidates: ApiFootballCandidate[]) {
  const ranked = candidates
    .map((candidate) => ({ candidate, score: scoreCandidate(player, candidate) }))
    .filter((entry) => entry.score >= 5)
    .sort((left, right) => right.score - left.score);

  return ranked[0]?.candidate ?? null;
}

async function searchApiFootball(search: string, apiKey: string) {
  const template = getSearchPathTemplate();
  const apiBaseUrl = getApiBaseUrl();
  const targetUrl = new URL(template.replace('{search}', encodeURIComponent(search)), apiBaseUrl);
  const response = await fetch(targetUrl, {
    headers: {
      'x-apisports-key': apiKey,
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`API-Football search failed: HTTP ${response.status}`);
  }

  const payload = await response.json() as ApiFootballSearchResponse;
  if (payload.errors?.rateLimit) {
    throw new ApiFootballRateLimitError(payload.errors.rateLimit);
  }

  return (payload.response ?? []).map(extractCandidate).filter((value): value is ApiFootballCandidate => value !== null);
}

async function searchApiFootballWithRetry(search: string, apiKey: string) {
  try {
    return await searchApiFootball(search, apiKey);
  } catch (error) {
    if (!(error instanceof ApiFootballRateLimitError)) {
      throw error;
    }

    await sleep(getRateLimitRetryMs());
    return searchApiFootball(search, apiKey);
  }
}

export async function generateApiFootballPlayerMappings(
  options: GenerateApiFootballPlayerMappingsOptions = {}
): Promise<GenerateApiFootballPlayerMappingsSummary> {
  const apiKey = process.env.API_FOOTBALL_KEY?.trim();
  if (!apiKey) {
    throw new Error('API_FOOTBALL_KEY is not set');
  }

  const players = selectPlayers(await loadPlayers(), options);
  const existingMappings = await readExistingMappings();
  const mappingByPlayerId = new Map(existingMappings.map((entry) => [entry.playerId, entry.externalId]));
  let matchedPlayers = 0;

  for (const player of players) {
    if (mappingByPlayerId.has(player.id)) {
      matchedPlayers += 1;
      continue;
    }

    let resolvedCandidate: ApiFootballCandidate | null = null;
    for (const searchName of getSearchNames(player)) {
      const candidates = await searchApiFootballWithRetry(searchName, apiKey);
      resolvedCandidate = pickBestCandidate(player, candidates);
      if (resolvedCandidate) {
        await sleep(getRequestDelayMs());
        break;
      }

      await sleep(getRequestDelayMs());
    }

    if (!resolvedCandidate) {
      continue;
    }

    mappingByPlayerId.set(player.id, resolvedCandidate.id);
    matchedPlayers += 1;
  }

  const sortedMappings = Array.from(mappingByPlayerId.entries())
    .map(([playerId, externalId]) => ({ playerId, externalId }))
    .sort((left, right) => left.playerId.localeCompare(right.playerId));

  if (!(options.dryRun ?? true)) {
    await writeFile(getMappingsFilePath(), `${JSON.stringify(sortedMappings, null, 2)}\n`, 'utf8');
  }

  return {
    dryRun: options.dryRun ?? true,
    searchedPlayers: players.length,
    matchedPlayers,
    writtenMappings: options.dryRun ?? true ? 0 : sortedMappings.length,
  };
}
