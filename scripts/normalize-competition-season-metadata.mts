import postgres from 'postgres';

interface TargetRow {
  id: number;
  source_metadata: string;
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const argv = process.argv.slice(2);
  const write = argv.includes('--write');

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const db = postgres(connectionString, { max: 1, idle_timeout: 20, prepare: false });

  try {
    const rows = await db<TargetRow[]>`
      SELECT id, source_metadata::text AS source_metadata
      FROM competition_seasons
      WHERE jsonb_typeof(source_metadata) = 'string'
      ORDER BY id
    `;

    const normalized = rows.map((row) => {
      const parsed = JSON.parse(JSON.parse(row.source_metadata)) as Record<string, unknown>;
      return {
        id: row.id,
        metadata: parsed,
      };
    });

    if (write) {
      for (const row of normalized) {
        await db`
          UPDATE competition_seasons
          SET source_metadata = ${db.json(JSON.parse(JSON.stringify(row.metadata)))},
              updated_at = NOW()
          WHERE id = ${row.id}
        `;
      }
    }

    console.log(JSON.stringify({
      dryRun: !write,
      normalizedRows: normalized.length,
      sample: normalized.slice(0, 5),
    }, null, 2));
  } finally {
    await db.end({ timeout: 1 }).catch(() => undefined);
  }
}

await main();
