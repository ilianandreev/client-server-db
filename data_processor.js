import fs from "fs";
import pkg from "pg";
const { Pool } = pkg;
import readline from "readline";

const pool = new Pool({
  host: "localhost",
  database: "client_server_db",
  password: "postgres",
  user: "postgres",
  port: 5432,
});

const processEvents = async (filePath) => {
  console.log("Processing events...");

  const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  const batchSize = 1000;
  let userRevenue = {};
  let processedEvents = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;

    try {
      const { userId, name, value } = JSON.parse(line);
      const revenueChange = name === "add_revenue" ? value : -value;
      userRevenue[userId] = (userRevenue[userId] || 0) + revenueChange;
      processedEvents++;

      if (processedEvents % batchSize === 0) {
        await updateRevenuesBatch(userRevenue);
        userRevenue = {};
        console.log(`Processed ${processedEvents} events`);
      }
    } catch (err) {
      console.error("Error processing event:", err.message);
    }
  }

  if (Object.keys(userRevenue).length > 0) {
    await updateRevenuesBatch(userRevenue);
    console.log(`Finished processing ${processedEvents} events`);
  }
};

const updateRevenuesBatch = async (userRevenue) => {
  const client = await pool.connect();
  try {
    for (const [userId, revenue] of Object.entries(userRevenue)) {
      let retries = 10;
      while (retries > 0) {
        try {
          await client.query("BEGIN");
          await client.query(
            `INSERT INTO users_revenue (user_id, revenue) 
             VALUES ($1, $2) 
             ON CONFLICT (user_id) DO UPDATE 
             SET revenue = users_revenue.revenue + $2
             WHERE users_revenue.user_id = $1`,
            [userId, revenue]
          );
          await client.query("COMMIT");
          console.log(`Updated revenue for user ${userId}: ${revenue}`);
          break;
        } catch (err) {
          await client.query("ROLLBACK");
          retries--;
          if (retries === 0) {
            console.error(
              `Failed to update revenue for user ${userId} after all retries:`,
              err.message
            );
          } else {
            console.log(
              `Retrying update for user ${userId}. Attempts remaining: ${retries}`
            );
            await new Promise((resolve) => setTimeout(resolve, 1000));
          }
        }
      }
    }
  } finally {
    client.release();
  }
};

const main = async () => {
  const filePath = process.argv[2];

  try {
    await processEvents(filePath);
  } catch (err) {
    console.error("Fatal error:", err);
    process.exit(1);
  } finally {
    await pool.end();
  }
};

main();
