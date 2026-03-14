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

interface TeamRow {
  id: number;
  slug: string;
  name: string | null;
  short_name: string | null;
  country_code: string;
  gender: 'male' | 'female' | 'mixed';
  crest_url: string | null;
  has_mapping: boolean;
  match_count: number;
  team_season_count: number;
  aliases: string[];
  league_slug: string | null;
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
  confidence: 'high';
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { help: false, write: false };

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
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/propose-club-canonical-matches.mts [options]

Options:
  --limit=<n>        Limit candidate rows in output (default: 100)
  --output=<path>    Output JSON path (default: logs/club-canonical-match-candidates.json)
  --write            Write report to disk in addition to stdout
  --help, -h         Show this help message
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

function normalizeClubName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/['’.]/g, '')
    .replace(/&/g, ' and ')
    .replace(/\b(football club|futbol club|club de futbol)\b/gi, ' ')
    .replace(/\b(fc|cf|ac|sc|afc|cfc|fk|sk)\b/gi, ' ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function shouldPrioritize(left: TeamRow, right: TeamRow) {
  const leftScore = Number(left.has_mapping) * 2 + Number(Boolean(left.crest_url));
  const rightScore = Number(right.has_mapping) * 2 + Number(Boolean(right.crest_url));
  if (leftScore !== rightScore) {
    return leftScore > rightScore;
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

  const targetNames = [target.name, target.short_name, ...target.aliases].filter((value): value is string => Boolean(value));
  const canonicalNames = [canonical.name, canonical.short_name, ...canonical.aliases].filter((value): value is string => Boolean(value));

  return targetNames.some(hasAfcToken) && canonicalNames.every((value) => !hasAfcToken(value));
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
          FROM matches m
          WHERE m.home_team_id = t.id OR m.away_team_id = t.id
        ) AS match_count,
        (
          SELECT COUNT(*)::INT
          FROM team_seasons ts
          WHERE ts.team_id = t.id
        ) AS team_season_count,
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
      GROUP BY t.id, t.slug, tt.name, tt.short_name, c.code_alpha3, t.gender, t.crest_url, lts.league_slug
    `;

    const targets = rows.filter((row) => {
      const reviewableCountry = !['USA', 'IND', 'ARG', 'BRA'].includes(row.country_code);
      const supportedGenderSlug = !row.slug.includes('-wfc-')
        && !row.slug.includes('-fcw-')
        && !row.slug.includes('-women-');
      return reviewableCountry && supportedGenderSlug;
    });

    const canonicalByKey = new Map<string, TeamRow>();

    for (const row of rows) {
      if (!row.has_mapping && !row.crest_url) {
        continue;
      }

      const rawNames = [row.name, row.short_name, ...row.aliases].filter((value): value is string => Boolean(value));
      for (const rawName of rawNames) {
        const normalized = normalizeClubName(rawName);
        if (!normalized) {
          continue;
        }

        const key = `${row.country_code}:${row.gender}:${normalized}`;
        const current = canonicalByKey.get(key);
        if (!current || shouldPrioritize(row, current)) {
          canonicalByKey.set(key, row);
        }
      }
    }

    const candidates: CandidateMatch[] = [];

    for (const target of targets) {
      const targetNames = [target.name, target.short_name, ...target.aliases].filter((value): value is string => Boolean(value));
      const seenKeys = new Set<string>();

      for (const rawName of targetNames) {
        const normalized = normalizeClubName(rawName);
        if (!normalized) {
          continue;
        }

        const key = `${target.country_code}:${target.gender}:${normalized}`;
        if (seenKeys.has(key)) {
          continue;
        }
        seenKeys.add(key);

        const canonical = canonicalByKey.get(key);
        if (!canonical || canonical.slug === target.slug) {
          continue;
        }

        if (shouldSkipOrphanAfcVariant(target, canonical)) {
          continue;
        }

        candidates.push({
          aliasSlug: target.slug,
          aliasName: target.name ?? target.slug,
          canonicalSlug: canonical.slug,
          canonicalName: canonical.name ?? canonical.slug,
          countryCode: target.country_code,
          gender: target.gender,
          leagueSlug: target.league_slug,
          reason: `exact normalized name match on ${normalized}`,
          confidence: 'high',
        });
        break;
      }
    }

    const report = {
      generatedAt: new Date().toISOString(),
      totalTargets: targets.length,
      candidateCount: candidates.length,
      candidates: candidates.slice(0, limit),
    };

    if (options.write) {
      await mkdir(path.dirname(outputPath), { recursive: true });
      await writeFile(outputPath, JSON.stringify(report, null, 2), 'utf8');
    }

    console.log(JSON.stringify({ ...report, outputPath: options.write ? outputPath : undefined }, null, 2));
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
