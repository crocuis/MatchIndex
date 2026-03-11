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

interface ConflictEntry {
  key: string;
  aliasSlug: string;
  aliasName: string;
  canonicalSlug: string;
  canonicalName: string;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    help: false,
    output: path.join('logs', 'club-substring-conflicts.json'),
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
  console.log(`Usage: node --experimental-strip-types scripts/report-club-substring-conflicts.mts [options]

Options:
  --output=<path>  Output JSON path (default: logs/club-substring-conflicts.json)
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

function transliterationNormalize(value: string) {
  return normalizeClubName(value)
    .replace(/munchen/g, 'munich')
    .replace(/koeln/g, 'koln')
    .replace(/futbol/g, 'football')
    .replace(/balompie/g, 'balompie');
}

function hasFullnameSignal(name: string) {
  const normalized = transliterationNormalize(name);
  return /\b(fc|cf|ac|afc|cfc|rc|rcd|ca|cd|ud|club)\b/i.test(normalized)
    || /\bde\b/i.test(normalized)
    || /\bdel\b/i.test(normalized)
    || /\bfootball\b/i.test(normalized)
    || /\bfutbol\b/i.test(normalized)
    || /\bbalompie\b/i.test(normalized)
    || /\bmilan\b/i.test(normalized)
    || /\bmunich\b/i.test(normalized)
    || /\bmunchen\b/i.test(normalized);
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
      ORDER BY c.code_alpha3, name ASC
    `;

    const conflicts = new Map<string, ConflictEntry>();

    for (let i = 0; i < rows.length; i += 1) {
      for (let j = 0; j < rows.length; j += 1) {
        if (i === j) continue;

        const alias = rows[i];
        const canonical = rows[j];

        if (alias.country_code !== canonical.country_code || alias.gender !== canonical.gender) {
          continue;
        }

        const aliasNormalized = transliterationNormalize(alias.name);
        const canonicalNormalized = transliterationNormalize(canonical.name);
        if (!aliasNormalized || !canonicalNormalized) {
          continue;
        }

        const canonicalStartsWithAlias = canonicalNormalized.startsWith(aliasNormalized);
        const aliasTokens = aliasNormalized.split(' ').filter(Boolean);
        const canonicalTokens = canonicalNormalized.split(' ').filter(Boolean);
        const sameNormalizedCore = canonicalNormalized === aliasNormalized;

        if (!canonicalStartsWithAlias && !sameNormalizedCore) {
          continue;
        }

        if (canonicalNormalized.length <= aliasNormalized.length + 3 && canonicalTokens.length <= aliasTokens.length) {
          continue;
        }

        if (!hasFullnameSignal(canonical.name)) {
          continue;
        }

        if (sameNormalizedCore) {
          if (canonical.name.length <= alias.name.length + 2) {
            continue;
          }
        } else if (canonicalTokens.length <= aliasTokens.length) {
          continue;
        }

        const key = `${alias.slug}->${canonical.slug}`;
        if (!conflicts.has(key)) {
          conflicts.set(key, {
            key: `${alias.country_code}:${aliasNormalized}`,
            aliasSlug: alias.slug,
            aliasName: alias.name,
            canonicalSlug: canonical.slug,
            canonicalName: canonical.name,
          });
        }
      }
    }

    const report = {
      generatedAt: new Date().toISOString(),
      conflictCount: conflicts.size,
      conflicts: [...conflicts.values()].sort((left, right) => left.key.localeCompare(right.key) || left.aliasSlug.localeCompare(right.aliasSlug)),
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
