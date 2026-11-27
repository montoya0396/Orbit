import { Client } from "@neondatabase/serverless";
import { afterEach, describe, expect, it } from "vitest";

import {
  acquireTestDbLock,
  releaseTestDbLock,
  resolveTestDbLockUrl,
} from "../setup/db";

// Distinct from TEST_DB_LOCK_KEY, which global setup holds for the whole run.
const KEY = 0x4c4f434b;
const OPTIONS = { key: KEY, timeoutMs: 1_500, retryMs: 200 };

const opened: Client[] = [];

async function openClient(): Promise<Client> {
  const client = new Client({ connectionString: resolveTestDbLockUrl() });
  opened.push(client);
  await client.connect();
  return client;
}

async function closeClient(client: Client): Promise<void> {
  opened.splice(opened.indexOf(client), 1);
  await client.end();
}

async function tryLock(client: Client): Promise<boolean> {
  const result = await client.query<{ locked: boolean }>(
    "select pg_try_advisory_lock($1) as locked",
    [KEY],
  );
  return result.rows[0]?.locked ?? false;
}

async function unlock(client: Client): Promise<boolean> {
  const result = await client.query<{ unlocked: boolean }>(
    "select pg_advisory_unlock($1) as unlocked",
    [KEY],
  );
  return result.rows[0]?.unlocked ?? false;
}

describe("test database lock", () => {
  afterEach(async () => {
    try {
      await releaseTestDbLock();
    } finally {
      // Ending a connection frees any advisory lock it still holds.
      await Promise.all(opened.slice().map(closeClient));
    }
  });

  it("gives up with an actionable error while another connection holds the key", async () => {
    const holder = await openClient();
    expect(await tryLock(holder)).toBe(true);

    const startedAt = Date.now();
    await expect(acquireTestDbLock(OPTIONS)).rejects.toThrow(
      /another test run holds the Vitest database branch/,
    );
    expect(Date.now() - startedAt).toBeLessThan(10_000);

    expect(await unlock(holder)).toBe(true);
  });

  it("acquires after the holder lets go and frees the lock on release", async () => {
    const holder = await openClient();
    expect(await tryLock(holder)).toBe(true);
    await expect(acquireTestDbLock(OPTIONS)).rejects.toThrow(
      /another test run holds/,
    );
    expect(await unlock(holder)).toBe(true);
    await closeClient(holder);

    await acquireTestDbLock(OPTIONS);

    const contender = await openClient();
    expect(await tryLock(contender)).toBe(false);

    await releaseTestDbLock();

    expect(await tryLock(contender)).toBe(true);
    expect(await unlock(contender)).toBe(true);
  });

  it("unlocks on the lock connection itself and is safe to release twice", async () => {
    await acquireTestDbLock(OPTIONS);
    // Rejects with "unlock returned false" if the lock were not held on the
    // connection that release unlocks on.
    await releaseTestDbLock();
    await releaseTestDbLock();

    const other = await openClient();
    expect(await tryLock(other)).toBe(true);
    expect(await unlock(other)).toBe(true);
  });

  it("refuses a second acquire while this process already holds the lock", async () => {
    await acquireTestDbLock(OPTIONS);
    await expect(acquireTestDbLock(OPTIONS)).rejects.toThrow(/already held/);
  });
});
