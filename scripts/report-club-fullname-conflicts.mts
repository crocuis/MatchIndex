import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  help: boolean;
  output: string;
  write: boolean;
}

interface TeamRow {
  slug: string;
  name: string;
  country_code: string;
  gender: 'male' | 'female' | 'mixed';
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    help: false,
    output: path.join('logs', 'club-fullname-conflicts.json'),
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

    if (arg.startsWith('--output=')) {
      options.output = arg.slice('--output='.length).trim() || options.output;
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/report-club-fullname-conflicts.mts [options]

Options:
  --output=<path>  Output JSON path (default: logs/club-fullname-conflicts.json)
  --write          Write report to disk in addition to stdout
  --help, -h       Show this help message
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });
}

function normalizeClubName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/["'’.]/g, '')
    .replace(/&/g, ' and ')
    .replace(/\b(football club|futbol club|club de futbol)\b/gi, ' ')
    .replace(/\b(fc|cf|ac|sc|afc|cfc|fk|sk|wfc|fcw|lfc|rc|rcd|ca|cd|ud|club)\b/gi, ' ')
    .replace(/\b(de|del|de la|de las|de los)\b/gi, ' ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function hasClubToken(value: string) {
  return /\b(fc|cf|ac|afc|cfc|rc|rcd|ca|cd|ud|club)\b/i.test(value);
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
    const rows = await sql<TeamRow[]>`
      SELECT
        t.slug,
        COALESCE(tt.name, t.slug) AS name,
        c.code_alpha3 AS country_code,
        t.gender
      FROM teams t
      JOIN countries c ON c.id = t.country_id
      LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
      WHERE t.is_national = FALSE
        AND t.is_active = TRUE
        AND t.slug NOT LIKE 'archived-team-%'
        AND t.gender = 'male'
      ORDER BY name ASC
    `;

    const groups = new Map<string, TeamRow[]>();
    for (const row of rows) {
      const key = `${row.country_code}:${normalizeClubName(row.name)}`;
      const list = groups.get(key) ?? [];
      list.push(row);
      groups.set(key, list);
    }

    const conflicts = [...groups.entries()]
      .filter(([, list]) => list.length > 1)
      .map(([key, list]) => ({
        key,
        names: [...new Set(list.map((row) => row.name))],
        slugs: list.map((row) => row.slug),
        rows: list,
      }))
      .filter((group) => {
        const tokenCount = group.rows.filter((row) => hasClubToken(row.name)).length;
        return tokenCount > 0 && tokenCount < group.rows.length;
      })
      .sort((left, right) => left.key.localeCompare(right.key));

    const report = {
      generatedAt: new Date().toISOString(),
      conflictCount: conflicts.length,
      conflicts: conflicts.map(({ rows, ...rest }) => rest),
    };

    if (options.write) {
      await writeFile(options.output, JSON.stringify(report, null, 2), 'utf8');
    }

    console.log(JSON.stringify({ ...report, outputPath: options.write ? options.output : undefined }, null, 2));
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
