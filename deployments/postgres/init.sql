-- Required PostgreSQL setup for Wavicle:
-- 1. Logical replication settings (handled in docker-compose.yml command flags)

-- 2. Create the publication for all tables
-- This allows Wavicle to listen to all changes in the database.
CREATE PUBLICATION wavicle_proofs FOR ALL TABLES;

-- 3. Create the logical replication slot
-- This ensures the WAL is not cleaned up before Wavicle consumes it.
SELECT pg_create_logical_replication_slot('wavicle_slot', 'pgoutput');

-- 4. Create example tables for testing Phase 1
CREATE TABLE users (
    id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id TEXT PRIMARY KEY,
    name TEXT,
    price_cents INTEGER,
    stock_count INTEGER
);

-- 5. Seed some data
INSERT INTO users (id, name, email) VALUES ('123', 'Alice', 'alice@example.com');
INSERT INTO products (id, name, price_cents, stock_count) VALUES ('p1', 'Wavicle Token', 100, 1000);
