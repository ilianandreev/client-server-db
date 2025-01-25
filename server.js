import express from "express";
import bodyParser from "body-parser";
import pkg from "pg";
const { Pool } = pkg;
import { createWriteStream } from "fs";

const app = express();
app.use(bodyParser.json());

const PORT = 5000;
const AUTH_SECRET = "secret";

const pool = new Pool({
  host: "localhost",
  database: "client_server_db",
  password: "postgres",
  user: "postgres",
  port: 5432,
});

const getEventFileName = () => {
  const now = new Date();
  return `server_events_${now.getFullYear()}${String(
    now.getMonth() + 1
  ).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}${String(
    now.getHours()
  ).padStart(2, "0")}${String(now.getMinutes()).padStart(2, "0")}.jsonl`;
};

let eventStream;

const createNewEventStream = () => {
  if (eventStream) {
    eventStream.end();
  }
  const fileName = getEventFileName();
  console.log(`Creating new event stream: ${fileName}`);
  return createWriteStream(fileName, { flags: "a" });
};

eventStream = createNewEventStream();

setInterval(() => {
  eventStream = createNewEventStream();
}, 60 * 1000); // Every minute

app.post("/liveEvent", (req, res) => {
  const authHeader = req.headers["authorization"];
  if (authHeader !== AUTH_SECRET) {
    return res.status(403).send("Forbidden");
  }

  const event = JSON.stringify(req.body) + "\n";

  eventStream.write(event, (err) => {
    if (err) {
      console.error("Error writing to event stream: ", err);
      return res.status(500).send("Error processing event");
    }
    console.log("Event successfully written to stream:", req.body);
    res.status(200).send("Event received");
  });
});

app.get("/userEvents/:userid", async (req, res) => {
  const userId = req.params.userid;
  try {
    // For userEvents endpoint data consistency we have two options:
    // 1. FOR SHARE blockss the row from being updated by other transactions while being read
    // 2. Another option is await pool.query('BEGIN ISOLATION LEVEL SERIALIZABLE');
    // but this might lead to conflicts that need to be retried
    const result = await pool.query(
      "SELECT user_id, revenue FROM users_revenue WHERE user_id = $1 FOR SHARE",
      [userId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "User not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Database error occurred" });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});

process.on("SIGINT", () => {
  if (eventStream) {
    eventStream.end(() => {
      console.log("Event stream closed");
      process.exit(0);
    });
  } else {
    process.exit(0);
  }
  pool.end();
});
