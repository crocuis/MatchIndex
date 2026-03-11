import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface TeamRow {
  slug: string;
  name: string;
  country_code: string;
  gender: 'male' | 'female' | 'mixed';
}

function normalize(value: string) {
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

function signalNormalize(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/["'’.]/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function translit(value: string) {
  return normalize(value)
    .replace(/munchen/g, 'munich')
    .replace(/koln/g, 'cologne')
    .replace(/koeln/g, 'cologne')
    .replace(/internazionale/g, 'inter')
    .trim();
}

function hasCanonicalSignal(value: string) {
  const normalized = signalNormalize(value)
    .replace(/munchen/g, 'munich')
    .replace(/koeln/g, 'koln');

  return /\b(fc|cf|afc|rc|rcd|club|milan|munich|koln|cologne|vfl|vfb|fsv|tsg|sv)\b/i.test(normalized)
    || /^1 fc\b/i.test(normalized)
    || /\b1899\b/.test(normalized)
    || /\b04\b/.test(normalized)
    || /\b1848\b/.test(normalized);
}

async function main() {
  loadProjectEnv();
  const sql = postgres(process.env.DATABASE_URL!, { max: 1, idle_timeout: 5, prepare: false });
  try {
    const rows = await sql<TeamRow[]>`
      SELECT t.slug, COALESCE(tt.name, t.slug) AS name, c.code_alpha3 AS country_code, t.gender
      FROM teams t
      JOIN countries c ON c.id = t.country_id
      LEFT JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
      WHERE t.is_national = FALSE
        AND t.is_active = TRUE
        AND t.slug NOT LIKE 'archived-team-%'
        AND t.gender = 'male'
      ORDER BY c.code_alpha3, name ASC
    `;

    const conflicts = [] as Array<{ aliasSlug: string; aliasName: string; canonicalSlug: string; canonicalName: string; key: string }>;
    for (let i = 0; i < rows.length; i += 1) {
      for (let j = 0; j < rows.length; j += 1) {
        if (i === j) continue;
        const alias = rows[i];
        const canonical = rows[j];
        if (alias.country_code !== canonical.country_code || alias.gender !== canonical.gender) continue;
        const a = translit(alias.name);
        const c = translit(canonical.name);
        if (!a || !c) continue;
        const aliasTokens = a.split(' ').filter(Boolean);
        const canonicalTokens = c.split(' ').filter(Boolean);
        const sameNormalizedCore = a === c;
        if (!sameNormalizedCore) {
          if (canonicalTokens.length <= aliasTokens.length) continue;
          if (!canonicalTokens.slice(0, aliasTokens.length).every((token, index) => token === aliasTokens[index])) continue;
        }
        if (!hasCanonicalSignal(canonical.name)) continue;
        if (sameNormalizedCore && canonical.name.length <= alias.name.length + 2) continue;
        conflicts.push({
          key: `${alias.country_code}:${a}`,
          aliasSlug: alias.slug,
          aliasName: alias.name,
          canonicalSlug: canonical.slug,
          canonicalName: canonical.name,
        });
      }
    }

    const dedup = new Map(conflicts.map((entry) => [`${entry.aliasSlug}->${entry.canonicalSlug}`, entry]));
    const report = {
      generatedAt: new Date().toISOString(),
      conflictCount: dedup.size,
      conflicts: [...dedup.values()].sort((l, r) => l.key.localeCompare(r.key) || l.aliasSlug.localeCompare(r.aliasSlug)),
    };

    const outputPath = path.join('logs', 'club-transliteration-conflicts.json');
    await writeFile(outputPath, JSON.stringify(report, null, 2), 'utf8');
    console.log(JSON.stringify({ ...report, outputPath }, null, 2));
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
