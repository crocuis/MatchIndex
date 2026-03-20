import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  autoBatchOutput?: string;
  help: boolean;
  kickoffWindowMinutes: number;
  limit?: number;
  output?: string;
  recentTeamHours: number;
  write: boolean;
}

interface TeamRow {
  id: number;
  slug: string;
  name: string | null;
  short_name: string | null;
  country_code: string;
  gender: 'male' | 'female' | 'mixed';
  crest_url: string | null;
  has_mapping: boolean;
  mapping_count: number;
  match_count: number;
  team_season_count: number;
  created_at: string;
  aliases: string[];
  league_slug: string | null;
}

interface TeamMatchSignatureRow {
  team_slug: string;
  opponent_slug: string;
  competition_slug: string;
  kickoff_bucket: string;
  side: 'home' | 'away';
}

interface CandidateMatch {
  aliasSlug: string;
  aliasName: string;
  canonicalSlug: string;
  canonicalName: string;
  countryCode: string;
  gender: 'male' | 'female' | 'mixed';
  leagueSlug: string | null;
  reason: string;
  confidence: 'high' | 'very_high';
  strategy: 'exact_name' | 'match_signature';
  autoMerge: boolean;
  sharedMatchCount: number;
}

interface MergeBatchEntry {
  aliasSlug: string;
  canonicalSlug: string;
  aliasName: string;
  canonicalName: string;
  countryCode: string;
  leagueSlug: string | null;
  reason: string;
}

const DISTINCT_TEAM_PAIRS = new Set([
  'afc-liverpool::liverpool-fc-england',
  'bury::bury-afc',
]);

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    help: false,
    kickoffWindowMinutes: 5,
    recentTeamHours: 72,
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
      const parsed = Number.parseInt(arg.slice('--limit='.length), 10);
      if (Number.isFinite(parsed) && parsed > 0) {
        options.limit = parsed;
      }
      continue;
    }

    if (arg.startsWith('--output=')) {
      options.output = arg.slice('--output='.length).trim() || undefined;
      continue;
    }

    if (arg.startsWith('--auto-batch-output=')) {
      options.autoBatchOutput = arg.slice('--auto-batch-output='.length).trim() || undefined;
      continue;
    }

    if (arg.startsWith('--kickoff-window-minutes=')) {
      const parsed = Number.parseInt(arg.slice('--kickoff-window-minutes='.length), 10);
      if (Number.isFinite(parsed) && parsed >= 0) {
        options.kickoffWindowMinutes = parsed;
      }
      continue;
    }

    if (arg.startsWith('--recent-team-hours=')) {
      const parsed = Number.parseInt(arg.slice('--recent-team-hours='.length), 10);
      if (Number.isFinite(parsed) && parsed > 0) {
        options.recentTeamHours = parsed;
      }
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/propose-club-canonical-matches.mts [options]

Options:
  --limit=<n>                   Limit candidate rows in output (default: 100)
  --output=<path>               Output JSON path (default: logs/club-canonical-match-candidates.json)
  --auto-batch-output=<path>    Auto-merge batch JSON path (default: logs/club-auto-merge-batch.json)
  --kickoff-window-minutes=<n>  Match kickoff bucketing window in minutes (default: 5)
  --recent-team-hours=<n>       Newly-created team recency window in hours (default: 72)
  --write                       Write report to disk in addition to stdout
  --help, -h                    Show this help message
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

function normalizeStrictClubName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/["'’.]/g, '')
    .replace(/&/g, ' and ')
    .replace(/\b(football club|futbol club|club de futbol)\b/gi, ' ')
    .replace(/\b(fc|cf|ac|sc|afc|cfc|fk|sk)\b/gi, ' ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function normalizeLooseClubName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/["'’.]/g, '')
    .replace(/&/g, ' and ')
    .replace(/\b(football club|futbol club|club de futbol)\b/gi, ' ')
    .replace(/\b(fc|cf|ac|sc|afc|cfc|fk|sk|rc|rcd|sco|de|del|de la|de las|de los)\b/gi, ' ')
    .replace(/\b\d+\b/g, ' ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function shouldPrioritize(left: TeamRow, right: TeamRow) {
  const leftScore = Number(left.has_mapping) * 4 + Number(Boolean(left.crest_url)) * 2 + left.mapping_count;
  const rightScore = Number(right.has_mapping) * 4 + Number(Boolean(right.crest_url)) * 2 + right.mapping_count;
  if (leftScore !== rightScore) {
    return leftScore > rightScore;
  }

  if (left.match_count !== right.match_count) {
    return left.match_count > right.match_count;
  }

  if (left.team_season_count !== right.team_season_count) {
    return left.team_season_count > right.team_season_count;
  }

  const leftCreatedAt = new Date(left.created_at).getTime();
  const rightCreatedAt = new Date(right.created_at).getTime();
  if (Number.isFinite(leftCreatedAt) && Number.isFinite(rightCreatedAt) && leftCreatedAt !== rightCreatedAt) {
    return leftCreatedAt < rightCreatedAt;
  }

  if (left.slug.length !== right.slug.length) {
    return left.slug.length < right.slug.length;
  }

  return left.slug.localeCompare(right.slug) < 0;
}

function hasAfcToken(value: string) {
  return /\bafc\b/i.test(value);
}

function shouldSkipOrphanAfcVariant(target: TeamRow, canonical: TeamRow) {
  if (target.has_mapping || target.match_count > 0 || target.team_season_count > 0) {
    return false;
  }

  const targetNames = getTeamNames(target);
  const canonicalNames = getTeamNames(canonical);

  return targetNames.some(hasAfcToken) && canonicalNames.every((value) => !hasAfcToken(value));
}

function buildDistinctPairKey(leftSlug: string, rightSlug: string) {
  return [leftSlug, rightSlug].sort((left, right) => left.localeCompare(right, 'en')).join('::');
}

function isKnownDistinctTeamPair(target: TeamRow, canonical: TeamRow) {
  return DISTINCT_TEAM_PAIRS.has(buildDistinctPairKey(target.slug, canonical.slug));
}

function getTeamNames(team: TeamRow) {
  return [team.name, team.short_name, ...team.aliases].filter((value): value is string => Boolean(value));
}

function buildNameKeys(team: TeamRow, normalizer: (value: string) => string) {
  return [...new Set(getTeamNames(team)
    .map((value) => normalizer(value))
    .filter(Boolean))];
}

function buildCanonicalByKey(rows: TeamRow[], normalizer: (value: string) => string) {
  const canonicalByKey = new Map<string, TeamRow>();

  for (const row of rows) {
    if (!row.has_mapping && !row.crest_url) {
      continue;
    }

    for (const normalized of buildNameKeys(row, normalizer)) {
      const key = `${row.country_code}:${row.gender}:${normalized}`;
      const current = canonicalByKey.get(key);
      if (!current || shouldPrioritize(row, current)) {
        canonicalByKey.set(key, row);
      }
    }
  }

  return canonicalByKey;
}

function findCanonicalByKey(team: TeamRow, canonicalByKey: Map<string, TeamRow>, normalizer: (value: string) => string) {
  for (const normalized of buildNameKeys(team, normalizer)) {
    const canonical = canonicalByKey.get(`${team.country_code}:${team.gender}:${normalized}`);
    if (canonical) {
      return canonical;
    }
  }

  return null;
}

function sharesLooseIdentity(left: TeamRow, right: TeamRow) {
  const leftKeys = new Set(buildNameKeys(left, normalizeLooseClubName));
  const rightKeys = new Set(buildNameKeys(right, normalizeLooseClubName));
  return [...leftKeys].some((key) => rightKeys.has(key));
}

function toKickoffBucket(value: string, windowMinutes: number) {
  const timestamp = new Date(value).getTime();
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  const windowMs = Math.max(1, windowMinutes || 1) * 60 * 1000;
  const bucket = Math.floor(timestamp / windowMs) * windowMs;
  return new Date(bucket).toISOString();
}

function isRecentlyCreatedTeam(target: TeamRow, canonical: TeamRow, recentTeamHours: number) {
  const now = Date.now();
  const targetCreatedAt = new Date(target.created_at).getTime();
  const canonicalCreatedAt = new Date(canonical.created_at).getTime();
  if (!Number.isFinite(targetCreatedAt)) {
    return false;
  }

  const recentWindowMs = recentTeamHours * 60 * 60 * 1000;
  const recentlyCreated = now - targetCreatedAt <= recentWindowMs;
  const smallFootprint = target.team_season_count <= 1 && target.match_count <= 4;
  const canonicalIsOlder = Number.isFinite(canonicalCreatedAt) ? canonicalCreatedAt < targetCreatedAt : true;
  const canonicalIsBetterEstablished = canonical.match_count > target.match_count
    || canonical.team_season_count > target.team_season_count
    || canonical.mapping_count > target.mapping_count
    || canonicalIsOlder;

  return recentlyCreated && smallFootprint && canonicalIsBetterEstablished;
}

function toMergeBatchEntry(candidate: CandidateMatch): MergeBatchEntry {
  return {
    aliasSlug: candidate.aliasSlug,
    canonicalSlug: candidate.canonicalSlug,
    aliasName: candidate.aliasName,
    canonicalName: candidate.canonicalName,
    countryCode: candidate.countryCode,
    leagueSlug: candidate.leagueSlug,
    reason: candidate.reason,
  };
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const sql = getSql();
  const limit = options.limit ?? 100;
  const outputPath = options.output ?? path.join('logs', 'club-canonical-match-candidates.json');
  const autoBatchOutputPath = options.autoBatchOutput ?? path.join('logs', 'club-auto-merge-batch.json');

  try {
    const rows = await sql<TeamRow[]>`
      WITH latest_team_season AS (
        SELECT DISTINCT ON (ts.team_id)
          ts.team_id,
          c.slug AS league_slug
        FROM team_seasons ts
        JOIN competition_seasons cs ON cs.id = ts.competition_season_id
        JOIN competitions c ON c.id = cs.competition_id
        JOIN seasons s ON s.id = cs.season_id
        ORDER BY ts.team_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, cs.id DESC
      )
      SELECT
        t.id,
        t.slug,
        tt.name,
        tt.short_name,
        c.code_alpha3 AS country_code,
        t.gender,
        t.crest_url,
        EXISTS(
          SELECT 1
          FROM source_entity_mapping sem
          WHERE sem.entity_type = 'team'
            AND sem.entity_id = t.id
        ) AS has_mapping,
        (
          SELECT COUNT(*)::INT
          FROM source_entity_mapping sem
          WHERE sem.entity_type = 'team'
            AND sem.entity_id = t.id
        ) AS mapping_count,
        (
          SELECT COUNT(*)::INT
          FROM matches m
          WHERE m.home_team_id = t.id OR m.away_team_id = t.id
        ) AS match_count,
        (
          SELECT COUNT(*)::INT
          FROM team_seasons ts
          WHERE ts.team_id = t.id
        ) AS team_season_count,
        t.created_at::TEXT AS created_at,
        COALESCE(
          ARRAY_AGG(DISTINCT ea.alias) FILTER (WHERE ea.alias IS NOT NULL),
          ARRAY[]::TEXT[]
        ) AS aliases,
        lts.league_slug
      FROM teams t
      JOIN countries c ON c.id = t.country_id
      LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
      LEFT JOIN entity_aliases ea ON ea.entity_type = 'team' AND ea.entity_id = t.id
      LEFT JOIN latest_team_season lts ON lts.team_id = t.id
      WHERE t.is_national = FALSE
        AND t.is_active = TRUE
        AND t.slug NOT LIKE 'archived-team-%'
      GROUP BY t.id, t.slug, tt.name, tt.short_name, c.code_alpha3, t.gender, t.crest_url, t.created_at, lts.league_slug
    `;

    const matchRows = await sql<TeamMatchSignatureRow[]>`
      SELECT
        team.slug AS team_slug,
        opponent.slug AS opponent_slug,
        c.slug AS competition_slug,
        date_trunc('minute', m.kickoff_at)::TEXT AS kickoff_bucket,
        side.side
      FROM matches m
      JOIN competition_seasons cs ON cs.id = m.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      JOIN LATERAL (
        VALUES
          (m.home_team_id, m.away_team_id, 'home'::TEXT),
          (m.away_team_id, m.home_team_id, 'away'::TEXT)
      ) AS side(team_id, opponent_team_id, side) ON TRUE
      JOIN teams team ON team.id = side.team_id
      JOIN teams opponent ON opponent.id = side.opponent_team_id
      WHERE m.kickoff_at IS NOT NULL
        AND team.is_national = FALSE
        AND opponent.is_national = FALSE
        AND team.slug NOT LIKE 'archived-team-%'
        AND opponent.slug NOT LIKE 'archived-team-%'
    `;

    const targets = rows.filter((row) => {
      const reviewableCountry = !['USA', 'IND', 'ARG', 'BRA'].includes(row.country_code);
      const supportedGenderSlug = !row.slug.includes('-wfc-')
        && !row.slug.includes('-fcw-')
        && !row.slug.includes('-women-');
      return reviewableCountry && supportedGenderSlug;
    });

    const teamBySlug = new Map(rows.map((row) => [row.slug, row]));
    const canonicalByStrictKey = buildCanonicalByKey(rows, normalizeStrictClubName);
    const canonicalByLooseKey = buildCanonicalByKey(rows, normalizeLooseClubName);
    const directCandidates = new Map<string, CandidateMatch>();

    for (const target of targets) {
      const canonical = findCanonicalByKey(target, canonicalByStrictKey, normalizeStrictClubName);
      if (!canonical || canonical.slug === target.slug) {
        continue;
      }

      if (shouldSkipOrphanAfcVariant(target, canonical)) {
        continue;
      }

      if (isKnownDistinctTeamPair(target, canonical)) {
        continue;
      }

      directCandidates.set(target.slug, {
        aliasSlug: target.slug,
        aliasName: target.name ?? target.slug,
        canonicalSlug: canonical.slug,
        canonicalName: canonical.name ?? canonical.slug,
        countryCode: target.country_code,
        gender: target.gender,
        leagueSlug: target.league_slug,
        reason: `exact normalized name match on ${buildNameKeys(target, normalizeStrictClubName)[0] ?? target.slug}`,
        confidence: 'high',
        strategy: 'exact_name',
        autoMerge: isRecentlyCreatedTeam(target, canonical, options.recentTeamHours),
        sharedMatchCount: 0,
      });
    }

    const canonicalSlugByTeamSlug = new Map<string, string>();
    for (const row of rows) {
      const strictCanonical = findCanonicalByKey(row, canonicalByStrictKey, normalizeStrictClubName);
      const looseCanonical = strictCanonical ?? findCanonicalByKey(row, canonicalByLooseKey, normalizeLooseClubName);
      canonicalSlugByTeamSlug.set(row.slug, looseCanonical?.slug ?? row.slug);
    }

    const signatureToTeamSlugs = new Map<string, Set<string>>();
    const signaturesByTeamSlug = new Map<string, Set<string>>();

    for (const row of matchRows) {
      const team = teamBySlug.get(row.team_slug);
      const opponent = teamBySlug.get(row.opponent_slug);
      if (!team || !opponent) {
        continue;
      }

      const kickoffBucket = toKickoffBucket(row.kickoff_bucket, options.kickoffWindowMinutes);
      if (!kickoffBucket) {
        continue;
      }

      const opponentCanonicalSlug = canonicalSlugByTeamSlug.get(opponent.slug) ?? opponent.slug;
      const signature = [row.competition_slug, kickoffBucket, row.side, opponentCanonicalSlug].join('::');
      const teamSet = signatureToTeamSlugs.get(signature) ?? new Set<string>();
      teamSet.add(team.slug);
      signatureToTeamSlugs.set(signature, teamSet);

      const signatures = signaturesByTeamSlug.get(team.slug) ?? new Set<string>();
      signatures.add(signature);
      signaturesByTeamSlug.set(team.slug, signatures);
    }

    const matchCandidates = new Map<string, CandidateMatch>();

    for (const target of targets) {
      if (directCandidates.has(target.slug)) {
        continue;
      }

      const signatures = signaturesByTeamSlug.get(target.slug);
      if (!signatures || signatures.size === 0) {
        continue;
      }

      const candidateCounts = new Map<string, number>();
      for (const signature of signatures) {
        const teamSlugs = [...(signatureToTeamSlugs.get(signature) ?? [])].filter((slug) => slug !== target.slug);
        if (teamSlugs.length !== 1) {
          continue;
        }

        const candidateSlug = teamSlugs[0];
        if (!candidateSlug) {
          continue;
        }

        candidateCounts.set(candidateSlug, (candidateCounts.get(candidateSlug) ?? 0) + 1);
      }

      const bestCandidateEntry = [...candidateCounts.entries()]
        .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))[0];
      if (!bestCandidateEntry) {
        continue;
      }

      const [candidateSlug, sharedMatchCount] = bestCandidateEntry;
      const canonical = teamBySlug.get(candidateSlug);
      if (!canonical || canonical.slug === target.slug) {
        continue;
      }

      if (canonical.country_code !== target.country_code || canonical.gender !== target.gender) {
        continue;
      }

      if (!sharesLooseIdentity(target, canonical)) {
        continue;
      }

      if (shouldSkipOrphanAfcVariant(target, canonical)) {
        continue;
      }

      if (isKnownDistinctTeamPair(target, canonical)) {
        continue;
      }

      matchCandidates.set(target.slug, {
        aliasSlug: target.slug,
        aliasName: target.name ?? target.slug,
        canonicalSlug: canonical.slug,
        canonicalName: canonical.name ?? canonical.slug,
        countryCode: target.country_code,
        gender: target.gender,
        leagueSlug: target.league_slug,
        reason: `match signature overlap on ${sharedMatchCount} fixture(s): same competition, kickoff bucket, side, and opponent canonical slug`,
        confidence: isRecentlyCreatedTeam(target, canonical, options.recentTeamHours) ? 'very_high' : 'high',
        strategy: 'match_signature',
        autoMerge: isRecentlyCreatedTeam(target, canonical, options.recentTeamHours),
        sharedMatchCount,
      });
    }

    const candidates = [...directCandidates.values(), ...matchCandidates.values()]
      .sort((left, right) => {
        if (left.autoMerge !== right.autoMerge) {
          return left.autoMerge ? -1 : 1;
        }

        if (left.sharedMatchCount !== right.sharedMatchCount) {
          return right.sharedMatchCount - left.sharedMatchCount;
        }

        if (left.confidence !== right.confidence) {
          return left.confidence === 'very_high' ? -1 : 1;
        }

        return left.aliasSlug.localeCompare(right.aliasSlug);
      });

    const autoMergeEntries = candidates
      .filter((candidate) => candidate.autoMerge)
      .map(toMergeBatchEntry);

    const report = {
      generatedAt: new Date().toISOString(),
      totalTargets: targets.length,
      candidateCount: candidates.length,
      autoMergeCount: autoMergeEntries.length,
      kickoffWindowMinutes: options.kickoffWindowMinutes,
      recentTeamHours: options.recentTeamHours,
      candidates: candidates.slice(0, limit),
    };

    if (options.write) {
      await mkdir(path.dirname(outputPath), { recursive: true });
      await writeFile(outputPath, JSON.stringify(report, null, 2), 'utf8');

      await mkdir(path.dirname(autoBatchOutputPath), { recursive: true });
      await writeFile(autoBatchOutputPath, JSON.stringify({
        generatedAt: new Date().toISOString(),
        sourceCandidatePath: outputPath,
        selectedCount: autoMergeEntries.length,
        mergeEntries: autoMergeEntries,
        mergeCommand: `node --experimental-strip-types scripts/apply-team-merge-batch-sequential.mts --batch-file=${autoBatchOutputPath}`,
      }, null, 2), 'utf8');
    }

    console.log(JSON.stringify({
      ...report,
      outputPath: options.write ? outputPath : undefined,
      autoBatchOutputPath: options.write ? autoBatchOutputPath : undefined,
      autoMergeCommand: options.write
        ? `node --experimental-strip-types scripts/apply-team-merge-batch-sequential.mts --batch-file=${autoBatchOutputPath}`
        : undefined,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
