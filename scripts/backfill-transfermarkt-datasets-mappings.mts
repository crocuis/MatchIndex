import { createReadStream } from 'node:fs';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { createInterface } from 'node:readline';
import postgres, { type Sql } from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

const SOURCE_SLUG = 'transfermarkt-datasets';
const TARGET_COMPETITION_IDS = new Set(['GB1', 'ES1', 'IT1', 'L1', 'FR1']);
const BATCH_SIZE = 500;

interface CliOptions {
  dir?: string;
  dryRun: boolean;
  help: boolean;
  mappingsPath?: string;
}

interface SourceRow { id: number; }
interface PlayerSlugRow { id: number; slug: string; }
interface TeamFallbackRow {
  id: number;
  slug: string;
  name: string | null;
  short_name: string | null;
  country_code: string | null;
  competition_slug: string | null;
}

interface PlayerMappingFileRow {
  playerSlug: string;
  sourceUrl: string;
}

interface SourceMappingDraft {
  entityId: number;
  entityType: 'player' | 'team';
  externalId: string;
  metadata: string;
}

interface MappingSummary {
  dryRun: boolean;
  sourceId: number | null;
  playerMappingsRead: number;
  playerMappingsMatched: number;
  playerMappingsWritten: number;
  playerMappingsUnmatched: number;
  teamRowsRead: number;
  teamMappingsMatched: number;
  teamMappingsWritten: number;
  teamMappingsUnmatched: number;
  unmatchedPlayerSlugsSample: string[];
  unmatchedTeamsSample: Array<{ clubId: string; clubCode: string; name: string }>;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: true,
    help: false,
  };

  for (const arg of argv) {
    if (arg === '--write') {
      options.dryRun = false;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--dir=')) {
      options.dir = arg.slice('--dir='.length).trim();
      continue;
    }
    if (arg.startsWith('--mappings=')) {
      options.mappingsPath = arg.slice('--mappings='.length).trim();
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/backfill-transfermarkt-datasets-mappings.mts --dir=<path> [options]

Options:
  --dir=<path>         transfermarkt-datasets CSV directory
  --mappings=<path>    transfermarkt player mapping JSON (default: data/transfermarkt-player-mappings.json)
  --write              Persist mappings into source_entity_mapping
  --help, -h           Show this help message
`);
}

function resolvePathLike(value: string) {
  return path.isAbsolute(value) ? value : path.join(process.cwd(), value);
}

function getSql(): Sql {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(databaseUrl, {
    max: 1,
    idle_timeout: 5,
    prepare: false,
  });
}

function normalizeText(value: string | null | undefined) {
  return value
    ?.normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .toLowerCase() ?? '';
}

function buildTeamKeys(value: string | null | undefined) {
  const normalized = normalizeText(value);
  if (!normalized) {
    return [] as string[];
  }

  const compact = normalized
    .replace(/\b(fc|cf|afc|cfc|sc|ac|club|football|futbol|clube|sporting|sociedad|societa|associazione|de|futbol|calcio|srl|spa|s p a|s a d|sad|a d|1909|1913|1899|1898|1897)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  return [...new Set([normalized, compact].filter(Boolean))];
}

function buildAcronym(value: string | null | undefined) {
  const normalized = normalizeText(value);
  if (!normalized) {
    return '';
  }

  return normalized
    .split(' ')
    .filter(Boolean)
    .map((part) => part[0])
    .join('');
}

function buildPlayerNameKeys(values: Array<string | null | undefined>) {
  const keys = new Set<string>();
  for (const value of values) {
    const normalized = normalizeText(value);
    if (normalized) {
      keys.add(normalized);
    }
  }
  return [...keys];
}

function extractTransfermarktPlayerId(sourceUrl: string) {
  return sourceUrl.match(/\/spieler\/(\d+)/)?.[1] ?? null;
}

function splitCsvLine(line: string) {
  const fields: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];

    if (char === '"') {
      if (inQuotes && line[index + 1] === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === ',' && !inQuotes) {
      fields.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  fields.push(current);
  return fields;
}

async function* parseCsvFile(filePath: string): AsyncGenerator<Record<string, string>> {
  const readline = createInterface({
    input: createReadStream(filePath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });

  let headers: string[] | null = null;

  for await (const line of readline) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }

    const fields = splitCsvLine(trimmed);
    if (!headers) {
      headers = fields;
      continue;
    }

    const row: Record<string, string> = {};
    for (let index = 0; index < headers.length; index += 1) {
      row[headers[index]] = fields[index] ?? '';
    }

    yield row;
  }
}

async function findSourceId(sql: Sql) {
  const rows = await sql<SourceRow[]>`
    SELECT id FROM data_sources WHERE slug = ${SOURCE_SLUG} LIMIT 1
  `;
  return rows[0]?.id ?? null;
}

async function ensureSource(sql: Sql) {
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
    VALUES (
      ${SOURCE_SLUG},
      'Transfermarkt Datasets',
      'https://github.com/dcaribou/transfermarkt-datasets',
      'dataset',
      'dcaribou/transfermarkt-datasets',
      3
    )
    ON CONFLICT (slug)
    DO UPDATE SET
      name = EXCLUDED.name,
      base_url = EXCLUDED.base_url,
      source_kind = EXCLUDED.source_kind,
      upstream_ref = EXCLUDED.upstream_ref,
      priority = EXCLUDED.priority
    RETURNING id
  `;

  const sourceId = rows[0]?.id;
  if (!sourceId) {
    throw new Error('Failed to ensure transfermarkt-datasets source');
  }

  return sourceId;
}

async function loadPlayersBySlug(sql: Sql) {
  const rows = await sql<PlayerSlugRow[]>`SELECT id, slug FROM players`;
  return new Map(rows.map((row) => [row.slug, row.id]));
}

async function loadPlayerFallbacks(sql: Sql) {
  const rows = await sql<Array<{ id: number; known_as: string | null; first_name: string | null; last_name: string | null }>>`
    SELECT p.id, pt.known_as, pt.first_name, pt.last_name
    FROM players p
    LEFT JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
  `;

  const counts = new Map<string, number>();
  const result = new Map<string, number>();

  for (const row of rows) {
    const keys = buildPlayerNameKeys([
      row.known_as,
      row.first_name && row.last_name ? `${row.first_name} ${row.last_name}` : null,
    ]);

    for (const key of keys) {
      counts.set(key, (counts.get(key) ?? 0) + 1);
      result.set(key, row.id);
    }
  }

  for (const [key, count] of counts) {
    if (count > 1) {
      result.delete(key);
    }
  }

  return result;
}

async function loadTeamFallbacks(sql: Sql) {
  const rows = await sql<TeamFallbackRow[]>`
    WITH latest_team_season AS (
      SELECT DISTINCT ON (ts.team_id)
        ts.team_id,
        c.slug AS competition_slug
      FROM team_seasons ts
      JOIN competition_seasons cs ON cs.id = ts.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      JOIN seasons s ON s.id = cs.season_id
      ORDER BY ts.team_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, cs.id DESC
    )
    SELECT t.id, t.slug, tt.name, tt.short_name, c.code_alpha3 AS country_code, lts.competition_slug
    FROM teams t
    LEFT JOIN countries c ON c.id = t.country_id
    LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
    LEFT JOIN latest_team_season lts ON lts.team_id = t.id
  `;

  return rows;
}

async function readPlayerMappingFile(filePath: string) {
  return JSON.parse(await readFile(filePath, 'utf8')) as PlayerMappingFileRow[];
}

async function writeSourceMappingsBatch(sql: Sql, sourceId: number, drafts: SourceMappingDraft[]) {
  for (let i = 0; i < drafts.length; i += BATCH_SIZE) {
    const chunk = drafts.slice(i, i + BATCH_SIZE);
    await sql`
      INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata, updated_at)
      SELECT
        t.entity_type,
        t.entity_id,
        ${sourceId},
        t.external_id,
        t.metadata::jsonb,
        NOW()
      FROM UNNEST(
        ${sql.array(chunk.map((draft) => draft.entityType))}::text[],
        ${sql.array(chunk.map((draft) => draft.entityId))}::int[],
        ${sql.array(chunk.map((draft) => draft.externalId))}::text[],
        ${sql.array(chunk.map((draft) => draft.metadata))}::text[]
      ) AS t(entity_type, entity_id, external_id, metadata)
      ON CONFLICT (entity_type, source_id, external_id)
      DO UPDATE SET
        entity_id = EXCLUDED.entity_id,
        metadata = COALESCE(source_entity_mapping.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        updated_at = NOW()
    `;
  }
}

async function backfillPlayerMappings(sql: Sql, sourceId: number | null, filePath: string, playersDir: string, dryRun: boolean) {
  const playersBySlug = await loadPlayersBySlug(sql);
  const playerFallbacks = await loadPlayerFallbacks(sql);
  const mappings = await readPlayerMappingFile(filePath);
  const unmatchedSlugs: string[] = [];
  const seenExternalIds = new Set<string>();
  const drafts: SourceMappingDraft[] = [];
  let matched = 0;

  for (const mapping of mappings) {
    const playerId = playersBySlug.get(mapping.playerSlug);
    const externalId = extractTransfermarktPlayerId(mapping.sourceUrl);
    if (!playerId || !externalId) {
      unmatchedSlugs.push(mapping.playerSlug);
      continue;
    }

    matched += 1;
    seenExternalIds.add(externalId);
    if (dryRun || sourceId === null) {
      continue;
    }

    drafts.push({
      entityId: playerId,
      entityType: 'player',
      externalId,
      metadata: JSON.stringify({ playerSlug: mapping.playerSlug, sourceUrl: mapping.sourceUrl, provider: SOURCE_SLUG }),
    });
  }

  const playersPath = path.join(playersDir, 'players.csv');
  try {
    for await (const row of parseCsvFile(playersPath)) {
      const externalId = row.player_id?.trim();
      if (!externalId || seenExternalIds.has(externalId)) {
        continue;
      }

      const playerId = buildPlayerNameKeys([
        row.name,
        row.first_name && row.last_name ? `${row.first_name} ${row.last_name}` : null,
      ]).map((key) => playerFallbacks.get(key)).find((value) => value !== undefined);

      if (!playerId) {
        continue;
      }

      matched += 1;
      seenExternalIds.add(externalId);
      if (dryRun || sourceId === null) {
        continue;
      }

      drafts.push({
        entityId: playerId,
        entityType: 'player',
        externalId,
        metadata: JSON.stringify({
          playerName: row.name ?? null,
          sourceUrl: row.url ?? null,
          provider: SOURCE_SLUG,
          strategy: 'players-csv-name-fallback',
        }),
      });
    }
  } catch {
    // players.csv가 없으면 JSON 매핑만 사용한다.
  }

  if (!dryRun && sourceId !== null) {
    await writeSourceMappingsBatch(sql, sourceId, drafts);
  }

  return {
    read: mappings.length,
    matched,
    written: drafts.length,
    unmatched: unmatchedSlugs.length,
    unmatchedSample: unmatchedSlugs.slice(0, 20),
  };
}

async function backfillTeamMappings(sql: Sql, sourceId: number | null, dir: string, dryRun: boolean) {
  const teamFallbacks = await loadTeamFallbacks(sql);
  const unmatched: Array<{ clubId: string; clubCode: string; name: string }> = [];
  const clubsPath = path.join(dir, 'clubs.csv');
  const drafts: SourceMappingDraft[] = [];
  let rowsRead = 0;
  let matched = 0;

  for await (const row of parseCsvFile(clubsPath)) {
    if (!TARGET_COMPETITION_IDS.has(row.domestic_competition_id)) {
      continue;
    }

    rowsRead += 1;
    const clubId = row.club_id?.trim();
    if (!clubId) {
      continue;
    }

    const keys = [
      ...buildTeamKeys(row.club_code),
      ...buildTeamKeys(row.name),
      buildAcronym(row.name || row.club_code),
    ];
    const competitionSlug = row.domestic_competition_id === 'GB1'
      ? 'premier-league'
      : row.domestic_competition_id === 'ES1'
        ? 'la-liga'
        : row.domestic_competition_id === 'IT1'
          ? 'serie-a'
          : row.domestic_competition_id === 'L1'
            ? 'bundesliga'
            : row.domestic_competition_id === 'FR1'
              ? 'ligue-1'
              : null;
    const competitionMatches = competitionSlug
      ? teamFallbacks.filter((candidate) => candidate.competition_slug === competitionSlug)
      : teamFallbacks;
    const narrowedByCompetition = competitionMatches.length > 0 ? competitionMatches : teamFallbacks;
    const countryMatches = narrowedByCompetition.filter((candidate) => candidate.country_code === 'ENG' && row.domestic_competition_id === 'GB1'
      || candidate.country_code === 'ESP' && row.domestic_competition_id === 'ES1'
      || candidate.country_code === 'ITA' && row.domestic_competition_id === 'IT1'
      || candidate.country_code === 'DEU' && row.domestic_competition_id === 'L1'
      || candidate.country_code === 'FRA' && row.domestic_competition_id === 'FR1');
    const finalCandidates = countryMatches.length > 0 ? countryMatches : narrowedByCompetition;
    const targetKeys = new Set(keys.filter(Boolean));
    const targetAcronym = buildAcronym(row.name || row.club_code);
    const scored = finalCandidates
      .map((candidate) => {
        const candidateKeys = new Set([
          ...buildTeamKeys(candidate.slug),
          ...buildTeamKeys(candidate.name),
          ...buildTeamKeys(candidate.short_name),
          buildAcronym(candidate.name || candidate.short_name || candidate.slug),
        ]);
        const candidateAcronym = buildAcronym(candidate.name || candidate.short_name || candidate.slug);
        let score = 0;
        for (const key of targetKeys) {
          if (candidateKeys.has(key)) {
            score += 10;
          }
        }
        for (const key of targetKeys) {
          for (const candidateKey of candidateKeys) {
            if (!key || !candidateKey) {
              continue;
            }
            if (candidateKey.includes(key) || key.includes(candidateKey)) {
              score += 3;
            }
          }
        }
        if (targetAcronym && candidateAcronym === targetAcronym) {
          score += 8;
        }
        if (normalizeText(row.club_code) === normalizeText(candidate.slug)) {
          score += 6;
        }
        if (normalizeText(row.name) === normalizeText(candidate.name) || normalizeText(row.name) === normalizeText(candidate.short_name)) {
          score += 6;
        }
        return { candidate, score };
      })
      .filter((entry) => entry.score > 0)
      .sort((a, b) => b.score - a.score);
    const bestScore = scored[0]?.score ?? 0;
    const bestCandidates = scored.filter((entry) => entry.score === bestScore);
    const teamId = bestScore > 0 && bestCandidates.length === 1 ? bestCandidates[0].candidate.id : undefined;

    if (!teamId) {
      unmatched.push({
        clubId,
        clubCode: row.club_code ?? '',
        name: row.name ?? '',
      });
      continue;
    }

    matched += 1;
    if (dryRun || sourceId === null) {
      continue;
    }

    drafts.push({
      entityId: teamId,
      entityType: 'team',
      externalId: clubId,
      metadata: JSON.stringify({ clubCode: row.club_code ?? null, clubName: row.name ?? null, provider: SOURCE_SLUG }),
    });
  }

  if (!dryRun && sourceId !== null) {
    await writeSourceMappingsBatch(sql, sourceId, drafts);
  }

  return {
    rowsRead,
    matched,
    written: drafts.length,
    unmatched: unmatched.length,
    unmatchedSample: unmatched.slice(0, 20),
  };
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  if (!options.dir) {
    throw new Error('--dir=<path> is required');
  }

  const dir = resolvePathLike(options.dir);
  const mappingsPath = resolvePathLike(options.mappingsPath ?? 'data/transfermarkt-player-mappings.json');
  const sql = getSql();

  try {
    const sourceId = options.dryRun ? await findSourceId(sql) : await ensureSource(sql);
    if (!options.dryRun) {
      await sql`BEGIN`;
    }

    try {
      const playerSummary = await backfillPlayerMappings(sql, sourceId, mappingsPath, dir, options.dryRun);
      const teamSummary = await backfillTeamMappings(sql, sourceId, dir, options.dryRun);

      if (!options.dryRun) {
        await sql`COMMIT`;
      }

      const summary: MappingSummary = {
        dryRun: options.dryRun,
        sourceId,
        playerMappingsRead: playerSummary.read,
        playerMappingsMatched: playerSummary.matched,
        playerMappingsWritten: playerSummary.written,
        playerMappingsUnmatched: playerSummary.unmatched,
        teamRowsRead: teamSummary.rowsRead,
        teamMappingsMatched: teamSummary.matched,
        teamMappingsWritten: teamSummary.written,
        teamMappingsUnmatched: teamSummary.unmatched,
        unmatchedPlayerSlugsSample: playerSummary.unmatchedSample,
        unmatchedTeamsSample: teamSummary.unmatchedSample,
      };

      console.log(JSON.stringify(summary, null, 2));
    } catch (error) {
      if (!options.dryRun) {
        await sql`ROLLBACK`;
      }
      throw error;
    }
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
