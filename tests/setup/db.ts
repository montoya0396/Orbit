import { setTimeout as sleep } from "node:timers/promises";

import { Client } from "@neondatabase/serverless";
import { sql } from "drizzle-orm";
import { migrate } from "drizzle-orm/neon-serverless/migrator";

import { createDbClient } from "@/lib/db/client";

import { TEST_DB_LOCK_KEY } from "./db-lock-key";
import { resolveTestDatabaseUrl } from "./env";

export const MIGRATIONS_FOLDER = "drizzle";

const client = createDbClient(resolveTestDatabaseUrl());

export const testDb = client.db;

let closed = false;

// A full run takes about 20 minutes, so a queued run must wait longer than that.
const DEFAULT_LOCK_TIMEOUT_MS = 30 * 60_000;
const DEFAULT_LOCK_RETRY_MS = 1_000;

export interface AcquireTestDbLockOptions {
  key?: number;
  timeoutMs?: number;
  retryMs?: number;
}

/**
 * Neon's `-pooler` endpoint is PgBouncer in transaction mode: session-level
 * advisory locks do not pin to a connection there, so two runners could both
 * "hold" the same key. The lock must use the direct endpoint of the same branch.
 */
export function resolveTestDbLockUrl(): string {
  const url = new URL(resolveTestDatabaseUrl());
  url.hostname = url.hostname.replace(/-pooler(?=\.)/, "");
  return url.toString();
}

/**
 * Advisory locks belong to one Postgres session, so the lock lives on a single
 * dedicated connection (never a Pool) from acquire until release.
 */
let lockClient: Client | undefined;
let lockKey: number | undefined;

/**
 * Takes the lock that serializes runners on the Vitest Neon branch, polling
 * until `timeoutMs` elapses. Closing the connection frees the lock, so a
 * crashed run cannot block the next one.
 */
export async function acquireTestDbLock({
  key = TEST_DB_LOCK_KEY,
  timeoutMs = DEFAULT_LOCK_TIMEOUT_MS,
  retryMs = DEFAULT_LOCK_RETRY_MS,
}: AcquireTestDbLockOptions = {}): Promise<void> {
  if (lockClient) {
    throw new Error("The Vitest database lock is already held by this process.");
  }

  const pending = new Client({ connectionString: resolveTestDbLockUrl() });
  try {
    await pending.connect();
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const result = await pending.query<{ locked: boolean }>(
        "select pg_try_advisory_lock($1) as locked",
        [key],
      );
      if (result.rows[0]?.locked) break;
      if (Date.now() + retryMs > deadline) {
        throw new Error(
          `Timed out after ${timeoutMs}ms waiting for advisory lock ${key}: ` +
            "another test run holds the Vitest database branch " +
            "(DATABASE_URL_TEST). Wait for it to finish or stop it, then retry.",
        );
      }
      await sleep(retryMs);
    }
  } catch (error) {
    await pending.end().catch(() => {});
    throw error;
  }
  lockClient = pending;
  lockKey = key;
}

/** Unlocks on the connection that took the lock, then closes it. */
export async function releaseTestDbLock(): Promise<void> {
  const held = lockClient;
  const key = lockKey;
  if (!held) return;
  lockClient = undefined;
  lockKey = undefined;
  try {
    const result = await held.query<{ unlocked: boolean }>(
      "select pg_advisory_unlock($1) as unlocked",
      [key],
    );
    if (!result.rows[0]?.unlocked) {
      throw new Error(
        `pg_advisory_unlock(${key}) returned false: the lock was not held on ` +
          "the lock connection.",
      );
    }
  } finally {
    await held.end();
  }
}

export async function migrateTestDb(): Promise<void> {
  await migrate(testDb, { migrationsFolder: MIGRATIONS_FOLDER });
}

/** Returns the branch to a pre-migration state so a clean apply can be proven. */
export async function resetTestSchema(): Promise<void> {
  await testDb.execute(sql`drop schema if exists public cascade`);
  await testDb.execute(sql`drop schema if exists drizzle cascade`);
  await testDb.execute(sql`create schema public`);
}

export async function truncateAll(): Promise<void> {
  const result = await testDb.execute<{ tablename: string }>(
    sql`select tablename from pg_tables where schemaname = 'public'`,
  );
  const tables = result.rows.map((r) => `"public"."${r.tablename}"`);
  if (tables.length === 0) return;
  await testDb.execute(
    sql.raw(`truncate table ${tables.join(", ")} restart identity cascade`),
  );
}

export async function listPublicTables(): Promise<string[]> {
  const result = await testDb.execute<{ tablename: string }>(
    sql`select tablename from pg_tables where schemaname = 'public' order by tablename`,
  );
  return result.rows.map((r) => r.tablename);
}

export async function countAppliedMigrations(): Promise<number> {
  const result = await testDb.execute<{ count: number }>(
    sql`select count(*)::int as count from drizzle."__drizzle_migrations"`,
  );
  return result.rows[0]?.count ?? 0;
}

export async function closeTestDb(): Promise<void> {
  if (closed) return;
  closed = true;
  await client.close();
}
