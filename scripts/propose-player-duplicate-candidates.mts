import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  help: boolean;
  limit?: number;
  output?: string;
  write: boolean;
}

interface PlayerDuplicateRow {
  id: number;
  slug: string;
  known_as: string | null;
  full_name: string | null;
  date_of_birth: string | null;
  position: string | null;
  country_id: number | null;
  active_team_slug: string | null;
  contract_count: number;
  transfer_count: number;
  source_slugs: string[] | null;
}

interface DuplicateCandidate {
  canonicalSlug: string;
  canonicalPlayerId: number;
  aliasSlug: string;
  aliasPlayerId: number;
  confidence: 'high' | 'medium';
  normalizedName: string;
  reasons: string[];
  canonical: CandidatePlayer;
  alias: CandidatePlayer;
}

interface CandidatePlayer {
  slug: string;
  playerId: number;
  knownAs: string | null;
  fullName: string | null;
  dateOfBirth: string | null;
  position: string | null;
  countryId: number | null;
  activeTeamSlug: string | null;
  contractCount: number;
  transferCount: number;
  sourceSlugs: string[];
}

function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    help: false,
    write: false,
  };

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg === '--write') {
      options.write = true;
      continue;
    }
    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
      continue;
    }
    if (arg.startsWith('--output=')) {
      options.output = arg.slice('--output='.length).trim();
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/propose-player-duplicate-candidates.mts [options]

Options:
  --limit=<n>           Limit returned duplicate candidates
  --output=<path>       Output JSON path (default: logs/player-duplicate-candidates.json)
  --write               Write JSON file instead of stdout
  --help, -h            Show this help message
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    max: 1,
    idle_timeout: 5,
    prepare: false,
  });
}

function normalizePlayerName(value: string | null | undefined) {
  if (!value) {
    return '';
  }

  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[''.-]/g, ' ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function chooseCanonical(left: CandidatePlayer, right: CandidatePlayer) {
  const score = (player: CandidatePlayer) => {
    const sourceCount = player.sourceSlugs.length;
    const canonicalBonus = player.slug.startsWith('sofascore-') ? 0 : 2;
    return player.contractCount * 4 + player.transferCount * 3 + sourceCount * 2 + canonicalBonus;
  };

  return score(left) >= score(right) ? [left, right] as const : [right, left] as const;
}

function buildCandidate(left: PlayerDuplicateRow, right: PlayerDuplicateRow): DuplicateCandidate | null {
  const leftPlayer: CandidatePlayer = {
    slug: left.slug,
    playerId: left.id,
    knownAs: left.known_as,
    fullName: left.full_name,
    dateOfBirth: left.date_of_birth,
    position: left.position,
    countryId: left.country_id,
    activeTeamSlug: left.active_team_slug,
    contractCount: left.contract_count,
    transferCount: left.transfer_count,
    sourceSlugs: left.source_slugs ?? [],
  };
  const rightPlayer: CandidatePlayer = {
    slug: right.slug,
    playerId: right.id,
    knownAs: right.known_as,
    fullName: right.full_name,
    dateOfBirth: right.date_of_birth,
    position: right.position,
    countryId: right.country_id,
    activeTeamSlug: right.active_team_slug,
    contractCount: right.contract_count,
    transferCount: right.transfer_count,
    sourceSlugs: right.source_slugs ?? [],
  };

  const normalizedName = normalizePlayerName(left.known_as ?? left.full_name);
  const reasons: string[] = [];

  if (left.date_of_birth && right.date_of_birth && left.date_of_birth === right.date_of_birth) {
    reasons.push(`same date of birth (${left.date_of_birth})`);
  }
  if (left.position && right.position && left.position === right.position) {
    reasons.push(`same position (${left.position})`);
  }
  if (left.country_id && right.country_id && left.country_id === right.country_id) {
    reasons.push(`same country_id (${left.country_id})`);
  }
  if (left.active_team_slug && right.active_team_slug && left.active_team_slug === right.active_team_slug) {
    reasons.push(`same active team (${left.active_team_slug})`);
  }
  if (left.slug.startsWith('sofascore-') !== right.slug.startsWith('sofascore-')) {
    reasons.push('mixed canonical/source-style slug pair');
  }

  const sharedSources = (left.source_slugs ?? []).filter((source) => (right.source_slugs ?? []).includes(source));
  if (sharedSources.length > 0) {
    reasons.push(`shared source mappings (${sharedSources.join(', ')})`);
  }

  if (reasons.length === 0) {
    return null;
  }

  const confidence: 'high' | 'medium' = reasons.some((reason) => reason.startsWith('same date of birth'))
    ? 'high'
    : reasons.length >= 3
      ? 'high'
      : 'medium';

  const [canonical, alias] = chooseCanonical(leftPlayer, rightPlayer);

  return {
    canonicalSlug: canonical.slug,
    canonicalPlayerId: canonical.playerId,
    aliasSlug: alias.slug,
    aliasPlayerId: alias.playerId,
    confidence,
    normalizedName,
    reasons,
    canonical,
    alias,
  };
}

function resolveOutputPath(output?: string) {
  const fallback = path.join('logs', 'player-duplicate-candidates.json');
  const target = output?.trim() || fallback;
  return path.isAbsolute(target) ? target : path.join(process.cwd(), target);
}

async function loadPlayerRows(sql: ReturnType<typeof postgres>) {
  return sql<PlayerDuplicateRow[]>`
    WITH latest_contracts AS (
      SELECT DISTINCT ON (pc.player_id)
        pc.player_id,
        t.slug AS team_slug,
        pc.joined_date,
        pc.left_date
      FROM player_contracts pc
      JOIN teams t ON t.id = pc.team_id
      JOIN competition_seasons cs ON cs.id = pc.competition_season_id
      JOIN seasons s ON s.id = cs.season_id
      ORDER BY pc.player_id,
        COALESCE(pc.left_date, s.end_date, s.start_date) DESC NULLS LAST,
        pc.joined_date DESC NULLS LAST,
        pc.id DESC
    ), source_map AS (
      SELECT
        sem.entity_id AS player_id,
        ARRAY_AGG(DISTINCT ds.slug ORDER BY ds.slug) AS source_slugs
      FROM source_entity_mapping sem
      JOIN data_sources ds ON ds.id = sem.source_id
      WHERE sem.entity_type = 'player'
      GROUP BY sem.entity_id
    ), transfer_counts AS (
      SELECT pt.player_id, COUNT(*)::INT AS transfer_count
      FROM player_transfers pt
      GROUP BY pt.player_id
    ), contract_counts AS (
      SELECT pc.player_id, COUNT(*)::INT AS contract_count
      FROM player_contracts pc
      GROUP BY pc.player_id
    )
    SELECT
      p.id,
      p.slug,
      pt.known_as,
      CONCAT_WS(' ', pt.first_name, pt.last_name) AS full_name,
      p.date_of_birth::TEXT AS date_of_birth,
      p.position,
      p.country_id,
      lc.team_slug AS active_team_slug,
      COALESCE(cc.contract_count, 0) AS contract_count,
      COALESCE(tc.transfer_count, 0) AS transfer_count,
      sm.source_slugs
    FROM players p
    LEFT JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
    LEFT JOIN latest_contracts lc ON lc.player_id = p.id
    LEFT JOIN contract_counts cc ON cc.player_id = p.id
    LEFT JOIN transfer_counts tc ON tc.player_id = p.id
    LEFT JOIN source_map sm ON sm.player_id = p.id
    WHERE COALESCE(pt.known_as, CONCAT_WS(' ', pt.first_name, pt.last_name), '') <> ''
  `;
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const sql = getSql();
  try {
    const rows = await loadPlayerRows(sql);
    const rowsByName = new Map<string, PlayerDuplicateRow[]>();
    for (const row of rows) {
      const key = normalizePlayerName(row.known_as ?? row.full_name);
      if (!key) {
        continue;
      }
      const bucket = rowsByName.get(key);
      if (bucket) {
        bucket.push(row);
      } else {
        rowsByName.set(key, [row]);
      }
    }

    const candidates: DuplicateCandidate[] = [];
    for (const [normalizedName, bucket] of rowsByName) {
      if (bucket.length < 2) {
        continue;
      }
      for (let index = 0; index < bucket.length; index += 1) {
        for (let innerIndex = index + 1; innerIndex < bucket.length; innerIndex += 1) {
          const candidate = buildCandidate(bucket[index], bucket[innerIndex]);
          if (!candidate) {
            continue;
          }
          candidates.push({ ...candidate, normalizedName });
        }
      }
    }

    const sorted = candidates
      .sort((left, right) => {
        if (left.confidence !== right.confidence) {
          return left.confidence === 'high' ? -1 : 1;
        }
        return left.normalizedName.localeCompare(right.normalizedName);
      })
      .slice(0, options.limit ?? candidates.length);

    const payload = {
      generatedAt: new Date().toISOString(),
      candidateCount: sorted.length,
      candidates: sorted,
    };

    if (!options.write) {
      console.log(JSON.stringify(payload, null, 2));
      return;
    }

    const outputPath = resolveOutputPath(options.output);
    await mkdir(path.dirname(outputPath), { recursive: true });
    await writeFile(outputPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
    console.log(JSON.stringify({ candidateCount: sorted.length, outputPath }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
