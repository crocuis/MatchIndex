import postgres from 'postgres';

const sql = postgres(process.env.DATABASE_URL, { max: 1, idle_timeout: 5 });

async function test() {
  console.log("Connecting to DB...");
  try {
    const result = await sql`SELECT 1 as x`;
    console.log("Result:", result);
  } catch(e) {
    console.error("DB error:", e);
  } finally {
    console.log("Closing...");
    await sql.end({timeout: 5});
    console.log("Closed.");
  }
}
test();
