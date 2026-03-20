import { mkdir, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';
import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  help: boolean;
  limit?: number;
  write: boolean;
}

interface TeamRow {
  id: number;
  slug: string;
  name: string | null;
  short_name: string | null;
  aliases: string[];
  country_name: string | null;
  country_code: string;
}

interface MergeEntry {
  aliasSlug: string;
  canonicalSlug: string;
  aliasName: string;
  canonicalName: string;
  countryCode: string;
  leagueSlug: null;
  reason: string;
}

interface RenameEntry {
  fromSlug: string;
  toSlug: string;
  name: string;
  countryCode: string;
}

const DISTINCT_TEAM_PAIRS = new Set([
  buildDistinctPairKey('afc-liverpool', 'liverpool-fc-england'),
  buildDistinctPairKey('bury', 'bury-afc'),
]);

function normalizeClubName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/['’.]/g, '')
    .replace(/&/g, ' and ')
    .replace(/\b(football club|futbol club|club de futbol)\b/gi, ' ')
    .replace(/\b(fc|cf|ac|sc|afc|cfc|fk|sk)\b/gi, ' ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function buildDistinctPairKey(leftSlug: string, rightSlug: string) {
  return [leftSlug, rightSlug].sort((left, right) => left.localeCompare(right, 'en')).join('::');
}

function isKnownDistinctTeamPair(leftSlug: string, rightSlug: string) {
  return DISTINCT_TEAM_PAIRS.has(buildDistinctPairKey(leftSlug, rightSlug));
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
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/migrate-team-slugs-to-fullname.mts [options]

Options:
  --limit=<n>  Limit processed teams for testing
  --write      Execute merge + rename changes
  --help, -h   Show this help message
`);
}

function slugify(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/['’.]/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-')
    .toLowerCase();
}

function createTeamSlug(name: string, countryName?: string | null) {
  return slugify(countryName ? `${name} ${countryName}` : name);
}

function choosePreferredFullName(row: TeamRow) {
  const values = [row.name, row.short_name, ...row.aliases.filter((alias) => /[A-Z\s&./'’]/.test(alias))]
    .filter((value): value is string => Boolean(value))
    .map((value) => value.trim())
    .filter(Boolean);

  if (values.length === 0) {
    return row.slug;
  }

  return values.sort((left, right) => {
    const leftHasClubToken = /\b(fc|cf|ac|afc|cfc|wfc|fcw)\b/i.test(left);
    const rightHasClubToken = /\b(fc|cf|ac|afc|cfc|wfc|fcw)\b/i.test(right);
    if (leftHasClubToken !== rightHasClubToken) {
      return leftHasClubToken ? -1 : 1;
    }

    if (left.length !== right.length) {
      return right.length - left.length;
    }

    return left.localeCompare(right);
  })[0];
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

async function runMergeBatch(batchFile: string) {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(process.execPath, [
      '--experimental-strip-types',
      'scripts/merge-duplicate-entities.mts',
      '--teams-only',
      `--batch-file=${batchFile}`,
    ], {
      stdio: 'inherit',
      cwd: process.cwd(),
      env: process.env,
    });

    child.on('error', reject);
    child.on('exit', (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(`merge batch failed with exit code ${code ?? 'unknown'}`));
    });
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
  const batchPath = path.join('logs', 'team-fullname-canonical-merge-batch.json');

  try {
    const rows = await sql<TeamRow[]>`
      SELECT
        t.id,
        t.slug,
        COALESCE(tt.name, t.slug) AS name,
        tt.short_name,
        COALESCE(ARRAY_AGG(DISTINCT ea.alias) FILTER (WHERE ea.alias IS NOT NULL), ARRAY[]::TEXT[]) AS aliases,
        COALESCE(ctr.name, c.code_alpha3) AS country_name,
        c.code_alpha3 AS country_code
      FROM teams t
      JOIN countries c ON c.id = t.country_id
      LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
      LEFT JOIN country_translations ctr ON ctr.country_id = c.id AND ctr.locale = 'en'
      LEFT JOIN entity_aliases ea ON ea.entity_type = 'team' AND ea.entity_id = t.id
      WHERE t.is_national = FALSE
        AND t.is_active = TRUE
        AND t.slug NOT LIKE 'archived-team-%'
      GROUP BY t.id, t.slug, tt.name, tt.short_name, ctr.name, c.code_alpha3
      ORDER BY t.slug ASC
    `;

    const limitedRows = options.limit ? rows.slice(0, options.limit) : rows;
    const bySlug = new Map(limitedRows.map((row) => [row.slug, row]));
    const preferredExistingTargetByKey = new Map<string, TeamRow>();
    const mergeEntries: MergeEntry[] = [];
    const renameEntries: RenameEntry[] = [];
    const seenMerge = new Set<string>();
    const seenRename = new Set<string>();

    for (const row of limitedRows) {
      const key = `${row.country_code}:${normalizeClubName(row.name ?? row.slug)}`;
      const current = preferredExistingTargetByKey.get(key);
      if (!current) {
        preferredExistingTargetByKey.set(key, row);
        continue;
      }

      const preferRow = row.slug.length > current.slug.length
        || (row.slug.length === current.slug.length && row.slug.localeCompare(current.slug) < 0);

      if (preferRow) {
        preferredExistingTargetByKey.set(key, row);
      }
    }

    for (const row of limitedRows) {
      const name = row.name ?? row.slug;
      const preferredFullName = choosePreferredFullName(row);
      const targetSlug = createTeamSlug(preferredFullName, row.country_name);
      const countrySuffix = slugify(row.country_name ?? row.country_code);
      const normalizedKey = `${row.country_code}:${normalizeClubName(name)}`;
      const preferredExistingTarget = preferredExistingTargetByKey.get(normalizedKey);
      if (!targetSlug || targetSlug === row.slug) {
        continue;
      }

      if (preferredExistingTarget && preferredExistingTarget.id !== row.id) {
        if (isKnownDistinctTeamPair(row.slug, preferredExistingTarget.slug)) {
          continue;
        }

        const preferredKey = `${row.slug}->${preferredExistingTarget.slug}`;
        if (!seenMerge.has(preferredKey)) {
          seenMerge.add(preferredKey);
          mergeEntries.push({
            aliasSlug: row.slug,
            canonicalSlug: preferredExistingTarget.slug,
            aliasName: name,
            canonicalName: preferredExistingTarget.name ?? preferredExistingTarget.slug,
            countryCode: row.country_code,
            leagueSlug: null,
            reason: 'promote preferred existing full-name slug to canonical',
          });
        }
        continue;
      }

      const existingTarget = bySlug.get(targetSlug);
      if (existingTarget && existingTarget.id !== row.id) {
        if (isKnownDistinctTeamPair(row.slug, targetSlug)) {
          continue;
        }

        const key = `${row.slug}->${targetSlug}`;
        if (!seenMerge.has(key)) {
          seenMerge.add(key);
          mergeEntries.push({
            aliasSlug: row.slug,
            canonicalSlug: targetSlug,
            aliasName: name,
            canonicalName: existingTarget.name ?? existingTarget.slug,
            countryCode: row.country_code,
            leagueSlug: null,
            reason: 'promote existing full-name slug to canonical',
          });
        }
        continue;
      }

      if (countrySuffix && row.slug.includes(countrySuffix)) {
        continue;
      }

      const key = `${row.slug}->${targetSlug}`;
      if (!seenRename.has(key)) {
        seenRename.add(key);
        renameEntries.push({
          fromSlug: row.slug,
          toSlug: targetSlug,
          name: preferredFullName,
          countryCode: row.country_code,
        });
      }
    }

    const report = {
      generatedAt: new Date().toISOString(),
      write: options.write,
      totalTeamsScanned: limitedRows.length,
      mergeCount: mergeEntries.length,
      renameCount: renameEntries.length,
      mergeEntries,
      renameEntries,
      batchPath,
    };

    await mkdir(path.dirname(batchPath), { recursive: true });
    await writeFile(batchPath, JSON.stringify({ mergeEntries }, null, 2), 'utf8');

    if (!options.write) {
      console.log(JSON.stringify(report, null, 2));
      return;
    }

    if (mergeEntries.length > 0) {
      await runMergeBatch(batchPath);
    }

    if (renameEntries.length > 0) {
      await sql`BEGIN`;
      try {
        for (const entry of renameEntries) {
          await sql`
            INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary, status, source_type, source_ref)
            VALUES (
              'team',
              (SELECT id FROM teams WHERE slug = ${entry.fromSlug}),
              ${entry.fromSlug},
              NULL,
              'historical',
              FALSE,
              'approved',
              'historical_rule',
              'migrate-team-slugs-to-fullname'
            )
            ON CONFLICT (entity_type, entity_id, alias_normalized)
            DO NOTHING
          `;

          await sql`
            UPDATE teams
            SET slug = ${entry.toSlug}, updated_at = NOW()
            WHERE slug = ${entry.fromSlug}
          `;
        }

        await sql`COMMIT`;
      } catch (error) {
        await sql`ROLLBACK`;
        throw error;
      }
    }

    console.log(JSON.stringify(report, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
    if (!options.write) {
      await rm(batchPath, { force: true }).catch(() => undefined);
    }
  }
}

await main();
