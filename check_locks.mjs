import postgres from 'postgres';
const sql = postgres(process.env.DATABASE_URL, { max: 1 });
async function test() {
  const locks = await sql`SELECT relation::regclass, mode, granted, pid FROM pg_locks WHERE NOT granted OR relation::regclass::text = 'countries'`;
  console.log("Locks:", locks);
  
  const queries = await sql`SELECT pid, state, query, wait_event_type, wait_event FROM pg_stat_activity WHERE state != 'idle'`;
  console.log("Activity:", queries);
  await sql.end();
}
test();
