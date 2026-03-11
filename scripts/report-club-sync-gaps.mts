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

interface SummaryRow {
  total: number;
  missing_crest: number;
  missing_mapping: number;
  missing_both: number;
  crest_but_no_mapping: number;
  mapping_but_no_crest: number;
}

interface SourceCoverageRow {
  source_count: number;
  clubs: number;
  missing_crest: number;
}

interface BucketRow {
  bucket: string;
  clubs: number;
  missing_mapping: number;
  missing_crest: number;
}

interface PriorityClubRow {
  slug: string;
  name: string;
  gender: 'male' | 'female' | 'mixed';
  country_code: string;
  country_name: string;
  league_slug: string | null;
  league_name: string | null;
  season_label: string | null;
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
  console.log(`Usage: node --experimental-strip-types scripts/report-club-sync-gaps.mts [options]

Options:
  --limit=<n>        Limit priority candidate rows (default: 100)
  --output=<path>    Output JSON path (default: logs/club-sync-gaps.json)
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

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const sql = getSql();
  const limit = options.limit ?? 100;
  const outputPath = options.output ?? path.join('logs', 'club-sync-gaps.json');

  try {
    const [summaryRows, sourceCoverageRows, bucketRows, priorityRows] = await Promise.all([
      sql<SummaryRow[]>`
        WITH base AS (
          SELECT
            t.id,
            t.crest_url,
            EXISTS(
              SELECT 1
              FROM source_entity_mapping sem
              WHERE sem.entity_type = 'team'
                AND sem.entity_id = t.id
            ) AS has_mapping
          FROM teams t
          WHERE t.is_national = FALSE
            AND t.is_active = TRUE
            AND t.slug NOT LIKE 'archived-team-%'
        )
        SELECT
          COUNT(*)::INT AS total,
          COUNT(*) FILTER (WHERE crest_url IS NULL OR crest_url = '')::INT AS missing_crest,
          COUNT(*) FILTER (WHERE NOT has_mapping)::INT AS missing_mapping,
          COUNT(*) FILTER (WHERE (crest_url IS NULL OR crest_url = '') AND NOT has_mapping)::INT AS missing_both,
          COUNT(*) FILTER (WHERE (crest_url IS NOT NULL AND crest_url <> '') AND NOT has_mapping)::INT AS crest_but_no_mapping,
          COUNT(*) FILTER (WHERE (crest_url IS NULL OR crest_url = '') AND has_mapping)::INT AS mapping_but_no_crest
        FROM base
      `,
      sql<SourceCoverageRow[]>`
        WITH club_sources AS (
          SELECT
            t.id,
            t.crest_url,
            COUNT(DISTINCT sem.source_id)::INT AS source_count
          FROM teams t
          LEFT JOIN source_entity_mapping sem
            ON sem.entity_type = 'team'
           AND sem.entity_id = t.id
          WHERE t.is_national = FALSE
            AND t.is_active = TRUE
            AND t.slug NOT LIKE 'archived-team-%'
          GROUP BY t.id, t.crest_url
        )
        SELECT
          source_count,
          COUNT(*)::INT AS clubs,
          COUNT(*) FILTER (WHERE crest_url IS NULL OR crest_url = '')::INT AS missing_crest
        FROM club_sources
        GROUP BY source_count
        ORDER BY source_count ASC
      `,
      sql<BucketRow[]>`
        WITH club_state AS (
          SELECT
            t.slug,
            t.gender,
            c.code_alpha3,
            t.crest_url,
            EXISTS(
              SELECT 1
              FROM source_entity_mapping sem
              WHERE sem.entity_type = 'team'
                AND sem.entity_id = t.id
            ) AS has_mapping
          FROM teams t
          JOIN countries c ON c.id = t.country_id
          WHERE t.is_national = FALSE
            AND t.is_active = TRUE
            AND t.slug NOT LIKE 'archived-team-%'
        )
        SELECT
          CASE
            WHEN slug LIKE '%-wfc-%' OR slug LIKE '%-fcw-%' OR slug LIKE '%-women-%' THEN 'women-named'
            WHEN code_alpha3 IN ('USA', 'IND', 'ARG', 'BRA', 'NED', 'POR') THEN 'non-top5-country'
            ELSE 'europe-core'
          END AS bucket,
          COUNT(*)::INT AS clubs,
          COUNT(*) FILTER (WHERE NOT has_mapping)::INT AS missing_mapping,
          COUNT(*) FILTER (WHERE crest_url IS NULL OR crest_url = '')::INT AS missing_crest
        FROM club_state
        GROUP BY bucket
        ORDER BY clubs DESC
      `,
      sql<PriorityClubRow[]>`
        WITH latest_team_season AS (
          SELECT DISTINCT ON (ts.team_id)
            ts.team_id,
            c.slug AS league_slug,
            COALESCE(ct.name, c.slug) AS league_name,
            CASE
              WHEN s.start_date IS NOT NULL AND s.end_date IS NOT NULL THEN
                CASE
                  WHEN EXTRACT(YEAR FROM s.start_date) = EXTRACT(YEAR FROM s.end_date)
                    THEN EXTRACT(YEAR FROM s.start_date)::INT::TEXT
                  ELSE CONCAT(
                    EXTRACT(YEAR FROM s.start_date)::INT::TEXT,
                    '/',
                    LPAD((EXTRACT(YEAR FROM s.end_date)::INT % 100)::TEXT, 2, '0')
                  )
                END
              ELSE s.slug
            END AS season_label
          FROM team_seasons ts
          JOIN competition_seasons cs ON cs.id = ts.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          LEFT JOIN competition_translations ct ON ct.competition_id = c.id AND ct.locale = 'en'
          JOIN seasons s ON s.id = cs.season_id
          ORDER BY ts.team_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, cs.id DESC
        )
        SELECT
          t.slug,
          COALESCE(tt.name, t.slug) AS name,
          t.gender,
          c.code_alpha3 AS country_code,
          COALESCE(ctr.name, c.code_alpha3) AS country_name,
          lts.league_slug,
          lts.league_name,
          lts.season_label
        FROM teams t
        JOIN countries c ON c.id = t.country_id
        LEFT JOIN country_translations ctr ON ctr.country_id = c.id AND ctr.locale = 'en'
        LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
        LEFT JOIN latest_team_season lts ON lts.team_id = t.id
        WHERE t.is_national = FALSE
          AND t.is_active = TRUE
          AND t.slug NOT LIKE 'archived-team-%'
          AND (t.crest_url IS NULL OR t.crest_url = '')
          AND NOT EXISTS (
            SELECT 1
            FROM source_entity_mapping sem
            WHERE sem.entity_type = 'team'
              AND sem.entity_id = t.id
          )
        ORDER BY
          CASE
            WHEN lts.league_slug IN ('premier-league', 'la-liga', '1-bundesliga', 'serie-a', 'ligue-1', 'champions-league', 'uefa-europa-league') THEN 0
            WHEN t.gender = 'female' THEN 2
            ELSE 1
          END ASC,
          COALESCE(lts.league_name, '~') ASC,
          name ASC
        LIMIT ${limit}
      `,
    ]);

    const summary = summaryRows[0];
    const report = {
      generatedAt: new Date().toISOString(),
      summary,
      ratios: summary ? {
        missingCrestRate: Number((summary.missing_crest / summary.total).toFixed(4)),
        missingMappingRate: Number((summary.missing_mapping / summary.total).toFixed(4)),
        missingBothRate: Number((summary.missing_both / summary.total).toFixed(4)),
      } : null,
      sourceCoverage: sourceCoverageRows,
      buckets: bucketRows,
      priorityCandidates: priorityRows,
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
