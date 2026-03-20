import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import postgres from 'postgres';

interface DuplicatePlayerCandidateRow {
  alias_id: number;
  alias_name: string;
  alias_slug: string;
  canonical_name: string;
  canonical_slug: string;
}

interface DuplicateMatchGroupRow {
  competition_slug: string;
  season_slug: string;
  match_count: number;
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const argv = process.argv.slice(2);
  const getOption = (name: string) => argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  const outputPath = getOption('output') || 'data/sofascore-duplicate-cleanup-plan.json';
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const db = postgres(connectionString, { max: 1, idle_timeout: 20, prepare: false });

  try {
    const playerRows = await db.unsafe<DuplicatePlayerCandidateRow[]>(`
      WITH alias_players AS (
        SELECT p.id, p.slug, COALESCE(pt.known_as, p.slug) AS player_name
        FROM players p
        LEFT JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
        WHERE p.slug LIKE 'sofascore-%'
           OR p.slug LIKE '%-fbref-events'
      ), canonical_players AS (
        SELECT p.id, p.slug, COALESCE(pt.known_as, p.slug) AS player_name
        FROM players p
        LEFT JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
        WHERE p.slug NOT LIKE 'sofascore-%'
          AND p.slug NOT LIKE '%-fbref-events'
      )
      SELECT DISTINCT ON (a.id)
        a.id AS alias_id,
        a.player_name AS alias_name,
        a.slug AS alias_slug,
        c.player_name AS canonical_name,
        c.slug AS canonical_slug
      FROM alias_players a
      JOIN canonical_players c
        ON lower(a.player_name) = lower(c.player_name)
      ORDER BY a.id, c.slug
      LIMIT 500
    `);

    const matchRows = await db.unsafe<DuplicateMatchGroupRow[]>(`
      SELECT
        c.slug AS competition_slug,
        s.slug AS season_slug,
        COUNT(*)::int AS match_count
      FROM (
        SELECT competition_season_id, match_date, home_team_id, away_team_id, COUNT(*) AS cnt
        FROM matches
        GROUP BY 1, 2, 3, 4
        HAVING COUNT(*) > 1
      ) dup
      JOIN competition_seasons cs ON cs.id = dup.competition_season_id
      JOIN competitions c ON c.id = cs.competition_id
      JOIN seasons s ON s.id = cs.season_id
      GROUP BY c.slug, s.slug
      ORDER BY match_count DESC, competition_slug, season_slug
    `);

    const payload = {
      generatedAt: new Date().toISOString(),
      playerMergeEntries: playerRows.map((row) => ({
        aliasSlug: row.alias_slug,
        canonicalSlug: row.canonical_slug,
        note: `${row.alias_name} -> ${row.canonical_name}`,
      })),
      duplicateMatchGroups: matchRows,
    };

    const resolvedOutputPath = path.isAbsolute(outputPath) ? outputPath : path.join(process.cwd(), outputPath);
    await writeFile(resolvedOutputPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
    console.log(JSON.stringify({
      outputPath: resolvedOutputPath,
      playerMergeEntries: payload.playerMergeEntries.length,
      duplicateMatchGroups: payload.duplicateMatchGroups.length,
    }, null, 2));
  } finally {
    await db.end({ timeout: 1 }).catch(() => undefined);
  }
}

await main();
