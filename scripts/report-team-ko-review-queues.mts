import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  limit: number;
  outputPath: string | null;
  scope: 'all' | 'latest';
}

interface ReviewRow {
  slug: string;
  en_name: string;
  ko_name: string | null;
  ko_short_name: string | null;
  proposed_name?: string | null;
  proposed_short_name?: string | null;
  source_type?: string | null;
  status?: string | null;
}

const DEEPL_SOURCE_REF = 'scripts/import-deepl-team-ko-candidates.mts';

function getArgValue(argv: string[], key: string) {
  return argv.find((arg) => arg.startsWith(`${key}=`))?.slice(key.length + 1) ?? null;
}

function parseArgs(argv: string[]): CliOptions {
  const limitRaw = getArgValue(argv, '--limit');
  const parsedLimit = limitRaw ? Number.parseInt(limitRaw, 10) : 20;
  const scope = getArgValue(argv, '--scope');
  const outputPath = getArgValue(argv, '--output');

  return {
    limit: Number.isFinite(parsedLimit) && parsedLimit > 0 ? parsedLimit : 20,
    outputPath: outputPath?.trim() || null,
    scope: scope === 'all' ? 'all' : 'latest',
  };
}

function resolveOutputPath(outputPath: string | null, scope: 'all' | 'latest') {
  if (outputPath) {
    return path.isAbsolute(outputPath) ? outputPath : path.join(process.cwd(), outputPath);
  }

  return path.join(process.cwd(), '.sisyphus', 'team-ko-review', `deepl-pending-${scope}.json`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, prepare: false, idle_timeout: 5 });
}

function latestTeamsCte(sql: postgres.Sql, scope: 'all' | 'latest') {
  return scope === 'latest'
    ? sql`
        WITH review_teams AS (
          WITH latest_team_seasons AS (
            SELECT DISTINCT ON (ts.team_id)
              ts.team_id
            FROM team_seasons ts
            JOIN competition_seasons cs ON cs.id = ts.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            ORDER BY ts.team_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, cs.id DESC
          )
          SELECT t.id, t.slug
          FROM teams t
          JOIN latest_team_seasons lts ON lts.team_id = t.id
          WHERE t.is_national = FALSE
        )
      `
    : sql`
        WITH review_teams AS (
          SELECT t.id, t.slug
          FROM teams t
          WHERE t.is_national = FALSE
            AND t.is_active = TRUE
        )
      `;
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  const sql = getSql();

  try {
    const deeplPending = await sql<ReviewRow[]>`
      ${latestTeamsCte(sql, options.scope)}
      SELECT
        rt.slug,
        COALESCE(en.name, rt.slug) AS en_name,
        ko.name AS ko_name,
        ko.short_name AS ko_short_name,
        pending.proposed_name,
        pending.proposed_short_name,
        pending.source_type,
        pending.status
      FROM review_teams rt
      LEFT JOIN team_translations en ON en.team_id = rt.id AND en.locale = 'en'
      LEFT JOIN team_translations ko ON ko.team_id = rt.id AND ko.locale = 'ko'
      JOIN LATERAL (
        SELECT ttc.proposed_name, ttc.proposed_short_name, ttc.source_type, ttc.status
        FROM team_translation_candidates ttc
        WHERE ttc.team_id = rt.id
          AND ttc.locale = 'ko'
          AND ttc.status = 'pending'
          AND ttc.source_ref = ${DEEPL_SOURCE_REF}
        ORDER BY ttc.created_at DESC, ttc.id DESC
        LIMIT 1
      ) pending ON TRUE
      ORDER BY rt.slug ASC
      LIMIT ${options.limit}
    `;

    const manualReviewNeeded = await sql<ReviewRow[]>`
      ${latestTeamsCte(sql, options.scope)}
      SELECT
        rt.slug,
        COALESCE(en.name, rt.slug) AS en_name,
        ko.name AS ko_name,
        ko.short_name AS ko_short_name
      FROM review_teams rt
      LEFT JOIN team_translations en ON en.team_id = rt.id AND en.locale = 'en'
      LEFT JOIN team_translations ko ON ko.team_id = rt.id AND ko.locale = 'ko'
      LEFT JOIN team_translation_candidates pending
        ON pending.team_id = rt.id
        AND pending.locale = 'ko'
        AND pending.status IN ('pending', 'approved')
      WHERE pending.id IS NULL
        AND (
          ko.name IS NULL
          OR (ko.name ~ '[A-Za-z]' AND lower(ko.name) = lower(COALESCE(en.name, rt.slug)))
        )
      ORDER BY rt.slug ASC
      LIMIT ${options.limit}
    `;

    const [pendingCountRow] = await sql<{ count: number }[]>`
      SELECT COUNT(*)::INT AS count
      FROM team_translation_candidates
      WHERE locale = 'ko'
        AND status = 'pending'
        AND source_ref = ${DEEPL_SOURCE_REF}
    `;

    const report = {
      scope: options.scope,
      limit: options.limit,
      summary: {
        deeplPendingCount: pendingCountRow?.count ?? 0,
        deeplPendingPreviewCount: deeplPending.length,
        manualReviewNeededPreviewCount: manualReviewNeeded.length,
      },
      deeplPending,
      manualReviewNeeded,
    };

    const outputPath = resolveOutputPath(options.outputPath, options.scope);
    mkdirSync(path.dirname(outputPath), { recursive: true });
    writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);

    console.log(JSON.stringify({
      ...report,
      outputPath,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
