import postgres from 'postgres';

import { loadProjectEnv } from './load-project-env.mts';

async function main() {
  loadProjectEnv();

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  const sql = postgres(connectionString, { max: 1, idle_timeout: 5, prepare: false });

  try {
    const sessions = await sql<{ pid: number }[]>`
      SELECT pid
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND pid <> pg_backend_pid()
        AND state <> 'idle'
        AND (
          query ILIKE '%merge-duplicate-entities%'
          OR query ILIKE '%migrate-team-slugs-to-fullname%'
          OR query ILIKE '%fix-atletico-canonical%'
          OR query ILIKE '%UPDATE matches SET home_team_id%'
          OR query ILIKE '%UPDATE teams%' 
        )
      ORDER BY pid
    `;

    const terminated: number[] = [];
    for (const session of sessions) {
      await sql.unsafe(`SELECT pg_terminate_backend(${session.pid})`);
      terminated.push(session.pid);
    }

    console.log(JSON.stringify({ terminated }, null, 2));
  } finally {
    await sql.end({ timeout: 1 });
  }
}

await main();
