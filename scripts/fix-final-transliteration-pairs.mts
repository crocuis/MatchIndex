import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

interface Pair {
  aliasSlug: string;
  canonicalSlug: string;
}

const PAIRS: Pair[] = [
  { aliasSlug: 'strasbourg-france', canonicalSlug: 'rc-strasbourg-alsace-france' },
  { aliasSlug: 'bologna-italy', canonicalSlug: 'bologna-fc-1909-italy' },
  { aliasSlug: 'borussia-monchengladbach-germany', canonicalSlug: 'borussia-mo-nchengladbach-germany' },
  { aliasSlug: 'werder-bremen-germany', canonicalSlug: 'sv-werder-bremen-germany' },
  { aliasSlug: 'wolfsburg-germany', canonicalSlug: 'vfl-wolfsburg-germany' },
  { aliasSlug: 'augsburg-germany', canonicalSlug: 'fc-augsburg-germany' },
  { aliasSlug: 'bochum-germany', canonicalSlug: 'vfl-bochum-1848-germany' },
];

function getArchivedTeamSlug(aliasId: number) {
  return `archived-team-${aliasId}`;
}

async function main() {
  loadProjectEnv();

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });

  try {
    const completed: string[] = [];

    for (const pair of PAIRS) {
      await sql`BEGIN`;

      try {
        const rows = await sql<{ id: number; slug: string }[]>`
          SELECT id, slug
          FROM teams
          WHERE slug IN (${pair.aliasSlug}, ${pair.canonicalSlug})
          ORDER BY slug
        `;

        const alias = rows.find((row) => row.slug === pair.aliasSlug);
        const canonical = rows.find((row) => row.slug === pair.canonicalSlug);

        if (!alias || !canonical) {
          await sql`ROLLBACK`;
          continue;
        }

        await sql`
          INSERT INTO entity_aliases (entity_type, entity_id, alias, locale, alias_kind, is_primary)
          VALUES ('team', ${canonical.id}, ${pair.aliasSlug}, NULL, 'historical', FALSE)
          ON CONFLICT (entity_type, entity_id, alias_normalized)
          DO NOTHING
        `;

        await sql`
          INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, external_code, season_context, metadata)
          SELECT entity_type, ${canonical.id}, source_id, external_id, external_code, season_context, metadata
          FROM source_entity_mapping
          WHERE entity_type = 'team'
            AND entity_id = ${alias.id}
          ON CONFLICT (entity_type, source_id, external_id)
          DO UPDATE SET
            entity_id = EXCLUDED.entity_id,
            external_code = COALESCE(source_entity_mapping.external_code, EXCLUDED.external_code),
            season_context = COALESCE(source_entity_mapping.season_context, EXCLUDED.season_context),
            metadata = COALESCE(source_entity_mapping.metadata, EXCLUDED.metadata),
            updated_at = NOW()
        `;

        await sql`DELETE FROM source_entity_mapping WHERE entity_type = 'team' AND entity_id = ${alias.id}`;
        await sql`DELETE FROM entity_aliases WHERE entity_type = 'team' AND entity_id = ${alias.id}`;
        await sql`DELETE FROM team_translations WHERE team_id = ${alias.id}`;
        await sql`DELETE FROM team_seasons WHERE team_id = ${alias.id}`;
        await sql`DELETE FROM player_contracts WHERE team_id = ${alias.id}`;
        await sql`
          UPDATE teams
          SET slug = ${getArchivedTeamSlug(alias.id)}, is_active = FALSE, updated_at = NOW()
          WHERE id = ${alias.id}
        `;

        await sql`COMMIT`;
        completed.push(`${pair.aliasSlug}->${pair.canonicalSlug}`);
      } catch (error) {
        await sql`ROLLBACK`;
        throw error;
      }
    }

    console.log(JSON.stringify({ ok: true, completed }, null, 2));
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
