import postgres, { type Sql } from 'postgres';
import { getApiFootballSourceConfig } from './apiFootball.ts';
import { normalizePlayerSeasonYears } from './playerSeasonWindow.ts';

interface SourceRow {
  id: number;
}

interface PlayerRow {
  id: number;
  slug: string;
  known_as: string | null;
  first_name: string | null;
  last_name: string | null;
}

interface CandidateMatch {
  player: PlayerRow;
  matchedBy: 'exact_normalized_name' | 'exact_slug_full_name';
}

interface ExistingMappingRow {
  external_id: string;
}

interface RawPayloadRow {
  payload: unknown;
}

interface ContractTeamRow {
  player_id: number;
  team_slug: string;
}

interface ApiFootballPlayerPayload {
  id?: number | string;
  name?: string;
  firstname?: string;
  lastname?: string;
}

interface ApiFootballPlayerEntry {
  player?: ApiFootballPlayerPayload;
  statistics?: Array<{
    team?: { name?: string };
  }>;
}

interface ApiFootballPlayerCandidate {
  player: ApiFootballPlayerPayload;
  teamName: string | null;
}

export interface BackfillApiFootballPlayerMappingsOptions {
  dryRun?: boolean;
  seasons?: number[];
}

export interface BackfillApiFootballPlayerMappingsSummary {
  dryRun: boolean;
  sourcePlayersSeen: number;
  existingMappings: number;
  candidateMatches: number;
  mappingsWritten: number;
  skippedAmbiguous: number;
}

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

function getDb() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    max: 1,
    idle_timeout: 20,
    prepare: false,
  });
}

function normalizeName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .toLowerCase();
}

function resolveApiTeamSlug(teamName: string) {
  const normalized = normalizeName(teamName);
  const aliases = new Map<string, string>([
    ['wolves', 'wolverhampton-wanderers-fc-england'],
    ['tottenham', 'tottenham-hotspur-fc-england'],
    ['west ham', 'west-ham-united-fc-england'],
    ['sheffield utd', 'sheffield-united-england'],
    ['alaves', 'deportivo-alaves-spain'],
    ['sevilla', 'sevilla'],
    ['granada cf', 'granada-spain'],
  ]);

  return aliases.get(normalized) ?? null;
}

function parseInitialLastName(value: string) {
  const match = normalizeName(value).match(/^([a-z])\s+(.+)$/);
  if (!match) {
    return null;
  }

  return {
    initial: match[1],
    lastName: match[2],
  };
}

function toJsonValue(value: unknown): JsonValue {
  return JSON.parse(JSON.stringify(value)) as JsonValue;
}

function getPayloadPlayers(payload: unknown) {
  const parsed = typeof payload === 'string' ? JSON.parse(payload) : payload;
  if (!parsed || typeof parsed !== 'object') {
    return [] as ApiFootballPlayerCandidate[];
  }

  const response = (parsed as { response?: ApiFootballPlayerEntry[] }).response;
  if (!Array.isArray(response)) {
    return [] as ApiFootballPlayerCandidate[];
  }

  return response
    .map((item) => ({
      player: item.player,
      teamName: item.statistics?.[0]?.team?.name?.trim() ?? null,
    }))
    .filter((entry): entry is ApiFootballPlayerCandidate => Boolean(entry.player?.id));
}

async function ensureApiFootballSource(sql: Sql) {
  const config = getApiFootballSourceConfig();
  const rows = await sql<SourceRow[]>`
    SELECT id
    FROM data_sources
    WHERE slug = ${config.slug}
    LIMIT 1
  `;

  if (!rows[0]) {
    throw new Error('API-Football data source is not initialized');
  }

  return rows[0].id;
}

async function loadPlayers(sql: Sql) {
  return sql<PlayerRow[]>`
    SELECT p.id, p.slug, pt.known_as, pt.first_name, pt.last_name
    FROM players p
    LEFT JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
  `;
}

async function loadExistingMappings(sql: Sql, sourceId: number) {
  const rows = await sql<ExistingMappingRow[]>`
    SELECT external_id
    FROM source_entity_mapping
    WHERE entity_type = 'player'
      AND source_id = ${sourceId}
  `;

  return new Set(rows.map((row) => row.external_id));
}

async function loadRawPayloads(sql: Sql, sourceId: number, seasons: number[]) {
  return sql<RawPayloadRow[]>`
    SELECT DISTINCT ON (endpoint) payload
    FROM raw_payloads
    WHERE source_id = ${sourceId}
      AND entity_type = 'player'
      AND season_context = ANY(${seasons.map(String)})
    ORDER BY endpoint, fetched_at DESC
  `;
}

async function loadSeasonContracts(sql: Sql, seasons: number[]) {
  return sql<ContractTeamRow[]>`
    SELECT pc.player_id, t.slug AS team_slug
    FROM player_contracts pc
    JOIN teams t ON t.id = pc.team_id
    JOIN competition_seasons cs ON cs.id = pc.competition_season_id
    JOIN seasons s ON s.id = cs.season_id
    WHERE EXTRACT(YEAR FROM s.start_date)::INT = ANY(${seasons})
  `;
}

function buildPlayerIndex(players: PlayerRow[]) {
  const nameIndex = new Map<string, PlayerRow[]>();
  const slugIndex = new Map<string, PlayerRow[]>();

  for (const player of players) {
    const variants = [
      player.known_as,
      [player.first_name, player.last_name].filter(Boolean).join(' '),
    ]
      .filter((value): value is string => Boolean(value && value.trim()))
      .map(normalizeName);

    for (const variant of new Set(variants)) {
      const current = nameIndex.get(variant) ?? [];
      current.push(player);
      nameIndex.set(variant, current);
    }

    const slugVariant = normalizeName(player.slug.replace(/-/g, ' '));
    const currentSlugMatches = slugIndex.get(slugVariant) ?? [];
    currentSlugMatches.push(player);
    slugIndex.set(slugVariant, currentSlugMatches);
  }

  return { nameIndex, slugIndex };
}

export async function backfillApiFootballPlayerMappings(
  options: BackfillApiFootballPlayerMappingsOptions = {},
): Promise<BackfillApiFootballPlayerMappingsSummary> {
  const seasons = normalizePlayerSeasonYears(options.seasons);
  const sql = getDb();

  try {
    const sourceId = await ensureApiFootballSource(sql);
    const existingMappings = await loadExistingMappings(sql, sourceId);
    const players = await loadPlayers(sql);
    const { nameIndex, slugIndex } = buildPlayerIndex(players);
    const rawPayloads = await loadRawPayloads(sql, sourceId, seasons);
    const seasonContracts = await loadSeasonContracts(sql, seasons);
    const teamSlugsByPlayerId = new Map<string, Set<string>>();
    for (const contract of seasonContracts) {
      const key = String(contract.player_id);
      const current = teamSlugsByPlayerId.get(key) ?? new Set<string>();
      current.add(contract.team_slug);
      teamSlugsByPlayerId.set(key, current);
    }

    const seenSourcePlayers = new Set<string>();
    const candidateMappings = new Map<string, CandidateMatch>();
    let skippedAmbiguous = 0;

    for (const rawPayload of rawPayloads) {
      for (const entry of getPayloadPlayers(rawPayload.payload)) {
        const player = entry.player;
        const externalId = String(player.id);
        if (existingMappings.has(externalId) || seenSourcePlayers.has(externalId)) {
          seenSourcePlayers.add(externalId);
          continue;
        }

        seenSourcePlayers.add(externalId);
        const fullDisplayName = normalizeName(player.name ?? [player.firstname, player.lastname].filter(Boolean).join(' '));
        const fullSourceName = normalizeName([player.firstname, player.lastname].filter(Boolean).join(' '));
        if (!fullDisplayName && !fullSourceName) {
          continue;
        }

        const directCandidates = fullDisplayName ? nameIndex.get(fullDisplayName) ?? [] : [];
        if (directCandidates.length === 1) {
          candidateMappings.set(externalId, {
            player: directCandidates[0],
            matchedBy: 'exact_normalized_name',
          });
          continue;
        }

        if (directCandidates.length > 1) {
          skippedAmbiguous += 1;
          continue;
        }

        const slugCandidates = fullSourceName ? slugIndex.get(fullSourceName) ?? [] : [];
        if (slugCandidates.length === 1) {
          candidateMappings.set(externalId, {
            player: slugCandidates[0],
            matchedBy: 'exact_slug_full_name',
          });
          continue;
        }

        if (slugCandidates.length > 1) {
          skippedAmbiguous += 1;
          continue;
        }

        if (directCandidates.length > 1 || slugCandidates.length > 1) {
          skippedAmbiguous += 1;
          continue;
        }

        const abbreviatedName = parseInitialLastName(player.name ?? '');
        const teamSlug = entry.teamName ? resolveApiTeamSlug(entry.teamName) : null;
        if (abbreviatedName && teamSlug) {
          const abbreviatedCandidates = players.filter((candidate) => {
            const firstName = normalizeName(candidate.first_name ?? '').split(' ')[0];
            const lastName = normalizeName(candidate.last_name ?? '');
            const slugName = normalizeName(candidate.slug.replace(/-/g, ' '));
            const hasTeam = teamSlugsByPlayerId.get(String(candidate.id))?.has(teamSlug) ?? false;
            return hasTeam
              && firstName.startsWith(abbreviatedName.initial)
              && (lastName.includes(abbreviatedName.lastName) || slugName.includes(abbreviatedName.lastName));
          });

          if (abbreviatedCandidates.length === 1) {
            candidateMappings.set(externalId, {
              player: abbreviatedCandidates[0],
              matchedBy: 'exact_slug_full_name',
            });
            continue;
          }

          if (abbreviatedCandidates.length > 1) {
            skippedAmbiguous += 1;
          }
        }
      }
    }

    if (options.dryRun ?? true) {
      return {
        dryRun: true,
        sourcePlayersSeen: seenSourcePlayers.size,
        existingMappings: existingMappings.size,
        candidateMatches: candidateMappings.size,
        mappingsWritten: 0,
        skippedAmbiguous,
      };
    }

    for (const [externalId, match] of candidateMappings.entries()) {
      await sql`
        INSERT INTO source_entity_mapping (
          entity_type,
          entity_id,
          source_id,
          external_id,
          metadata,
          updated_at
        )
        VALUES (
          'player',
          ${match.player.id},
          ${sourceId},
          ${externalId},
          ${sql.json(toJsonValue({
            matchedBy: match.matchedBy,
            playerSlug: match.player.slug,
          }))},
          NOW()
        )
        ON CONFLICT (entity_type, source_id, external_id)
        DO UPDATE SET
          entity_id = EXCLUDED.entity_id,
          metadata = EXCLUDED.metadata,
          updated_at = NOW()
      `;
    }

    return {
      dryRun: false,
      sourcePlayersSeen: seenSourcePlayers.size,
      existingMappings: existingMappings.size,
      candidateMatches: candidateMappings.size,
      mappingsWritten: candidateMappings.size,
      skippedAmbiguous,
    };
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
