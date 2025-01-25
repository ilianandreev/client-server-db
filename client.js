import fs from "fs";
import axios from "axios";
import readline from "readline";

const EVENTS_FILE = "events.jsonl";
const SERVER_ENDPOINT = "http://localhost:5000/liveEvent";
const AUTH_SECRET = "secret";
const CONCURRENCY_LIMIT = 25;

const sendEvent = async (event) => {
  try {
    await axios.post(SERVER_ENDPOINT, event, {
      headers: {
        Authorization: AUTH_SECRET,
        "Content-Type": "application/json",
      },
    });

    console.log(`Event sent successfully: `, event);
  } catch (err) {
    console.error("Error sending event, server may be down.", err.code);
    throw err;
  }
};

const sendEvents = async () => {
  console.log("Sending events...");

  const fileStream = fs.createReadStream(EVENTS_FILE, { encoding: "utf-8" });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let processingQueue = [];
  let activeRequests = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;

    try {
      const parsedEvent = JSON.parse(line);

      // if we've hit the concurrency limit, wait for one request to complete
      if (activeRequests >= CONCURRENCY_LIMIT) {
        await Promise.race(processingQueue);
      }

      activeRequests++;
      const requestPromise = sendEvent(parsedEvent).finally(() => {
        activeRequests--;

        const index = processingQueue.indexOf(requestPromise);
        if (index > -1) {
          processingQueue.splice(index, 1);
        }
      });

      processingQueue.push(requestPromise);
    } catch (err) {
      console.error("Error processing event:", err.code);
    }
  }

  // wait for all remaining promises to complete
  if (processingQueue.length > 0) {
    await Promise.all(processingQueue);
  }
};

sendEvents().catch((err) => {
  console.error("Fatal error:", err.code);
  process.exit(1);
});
