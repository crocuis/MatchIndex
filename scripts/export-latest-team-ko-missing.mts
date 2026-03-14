import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface MissingTeamRow {
  slug: string;
  enName: string;
  countryCode: string;
  gender: 'male' | 'female' | 'mixed';
  isNational: boolean;
}

async function main() {
  loadProjectEnv();

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(connectionString, { max: 1, prepare: false, idle_timeout: 5 });

  try {
    const rows = await sql<MissingTeamRow[]>`
      WITH latest_team_seasons AS (
        SELECT DISTINCT ON (ts.team_id) ts.team_id
        FROM team_seasons ts
        JOIN competition_seasons cs ON cs.id = ts.competition_season_id
        JOIN seasons s ON s.id = cs.season_id
        ORDER BY ts.team_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, cs.id DESC
      )
      SELECT
        t.slug,
        COALESCE(en.name, t.slug) AS "enName",
        TRIM(c.code_alpha3) AS "countryCode",
        t.gender,
        t.is_national AS "isNational"
      FROM latest_team_seasons lts
      JOIN teams t ON t.id = lts.team_id
      JOIN countries c ON c.id = t.country_id
      LEFT JOIN team_translations en ON en.team_id = t.id AND en.locale = 'en'
      LEFT JOIN team_translations ko ON ko.team_id = t.id AND ko.locale = 'ko'
      LEFT JOIN team_translation_candidates ttc
        ON ttc.team_id = t.id
        AND ttc.locale = 'ko'
        AND ttc.status IN ('pending', 'approved')
      WHERE ko.name IS NULL
        AND ttc.id IS NULL
      ORDER BY t.slug ASC
    `;

    const outputDir = path.join(process.cwd(), '.sisyphus', 'team-ko-review');
    mkdirSync(outputDir, { recursive: true });

    const outputPath = path.join(outputDir, 'latest-team-ko-missing.full.json');
    writeFileSync(outputPath, `${JSON.stringify(rows, null, 2)}\n`);

    console.log(JSON.stringify({ count: rows.length, outputPath }, null, 2));
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
