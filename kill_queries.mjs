import postgres from 'postgres';
const sql = postgres(process.env.DATABASE_URL, { max: 1 });
async function test() {
  const killed1 = await sql`SELECT pg_terminate_backend(12556)`;
  const killed2 = await sql`SELECT pg_terminate_backend(10773)`;
  const killed3 = await sql`SELECT pg_terminate_backend(12100)`;
  console.log("Killed?", killed1, killed2, killed3);
  await sql.end();
}
test();
