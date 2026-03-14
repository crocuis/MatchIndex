import postgres from 'postgres';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  help: boolean;
  outputPath?: string;
  playerSlug?: string;
  limit?: number;
}

interface TargetRow {
  external_id: string;
  player_slug: string;
  player_name: string;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { help: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--output=')) {
      options.outputPath = arg.slice('--output='.length).trim();
      continue;
    }
    if (arg.startsWith('--player=')) {
      options.playerSlug = arg.slice('--player='.length).trim();
      continue;
    }
    if (arg.startsWith('--limit=')) {
      const parsed = Number.parseInt(arg.slice('--limit='.length), 10);
      if (Number.isFinite(parsed) && parsed > 0) options.limit = parsed;
    }
  }
  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/export-transfermarkt-profile-targets.mts --output=<path> [options]

Options:
  --output=<path>      Write targets JSON to this path
  --player=<slug>      Restrict export to one internal player slug
  --limit=<n>          Limit exported players
  --help, -h           Show this help message
`);
}

function resolvePath(filePath: string) {
  return path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }
  if (!options.outputPath) {
    throw new Error('--output is required');
  }
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(process.env.DATABASE_URL, { max: 1, idle_timeout: 5, prepare: false });
  try {
    const rows = await sql<TargetRow[]>`
      SELECT DISTINCT ON (p.slug)
        sem.external_id,
        p.slug AS player_slug,
        COALESCE(pt.known_as, p.slug) AS player_name
      FROM source_entity_mapping sem
      JOIN data_sources ds ON ds.id = sem.source_id
      JOIN players p ON p.id = sem.entity_id
      LEFT JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
      WHERE sem.entity_type = 'player'
        AND ds.slug = 'transfermarkt'
        AND (${options.playerSlug ?? null}::text IS NULL OR p.slug = ${options.playerSlug ?? null})
      ORDER BY p.slug, sem.updated_at DESC NULLS LAST, sem.id DESC
      LIMIT ${options.limit ?? 500}
    `;

    const payload = {
      provider: 'transfermarkt',
      exportedAt: new Date().toISOString(),
      targets: rows.map((row) => ({
        playerSlug: row.player_slug,
        playerName: row.player_name,
        sourceUrl: `https://www.transfermarkt.com/-/profil/spieler/${row.external_id}`,
      })),
    };

    await writeFile(resolvePath(options.outputPath), `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
    console.log(JSON.stringify({ outputPath: resolvePath(options.outputPath), targets: payload.targets.length }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
