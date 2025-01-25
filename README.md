# Node.js ETL Project

A Node.js-based ETL (Extract, Transform, Load) system that processes user revenue events through a client-server architecture and updates a PostgreSQL database.

## System Architecture

The project consists of three main components:

1. **Client**: Reads events from a JSONL file and sends them to the server
2. **Server**: Receives events via HTTP, writes them to server events file
3. **Data Processor**: Processes server events files and updates the PostgreSQL database

## Prerequisites

- Node.js
- PostgreSQL database
- npm package manager

## Installation

1. Clone the repository
2. Install dependencies:

```bash
npm install
```

3. Run the `db.sql` file in your PostgreSQL database to create the `users_revenue` table.

## Running the Application

### 1. Start the Server

```bash
npm run server
```

The server will:

- Listen on port 5000
- Accept POST requests at `/liveEvent`
- Write events to `server_events_YYYYMMDDHHMM.jsonl` files
- Append to a new file every minute (this is done so data processor can potentially run on different server files instead of only one)
- Provide user revenue data via GET `/userEvents/:userid`

### 2. Run the Client

```bash
npm run client
```

The client will:

- Read events from `events.jsonl`
- Send events to server's `/liveEvent` endpoint
- Handle concurrent requests (limit: 25)
- Authenticate using the secret key

### 3. Process Data

```bash
npm run processor -- <filename>
```

Where `<filename>` is a server event file (e.g., `server_events_202501251430.jsonl`)

The processor will:

- Read events from the specified file
- Process events in batches (1000 events per batch)
- Update user revenue in the database using transactions
- Handle concurrent updates with retry mechanism

## Performance Notes

The data processor has been tested with:

- 3 simultaneous data processor instances running on different server files
- ~200 unique users
- ~120MB server files
- Local PostgreSQL database