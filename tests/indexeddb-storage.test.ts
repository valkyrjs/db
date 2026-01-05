import { afterAll, afterEach, beforeAll, describe, it } from "@std/testing/bdd";
import { expect } from "expect";

import "fake-indexeddb/auto";

import z from "zod";

import { IndexedDB } from "../src/databases/indexeddb/database.ts";
import type { DBLogger } from "../src/logger.ts";

const log: DBLogger = () => {};

describe("IndexedDB Storage Integration", { sanitizeOps: false, sanitizeResources: false }, () => {
  let db: IndexedDB<{ name: string; registrars: any[]; log?: DBLogger }>;

  let collection: any;

  beforeAll(async () => {
    db = new IndexedDB({
      name: "test-db",
      registrars: [
        {
          name: "users",
          schema: {
            id: z.string(),
            name: z.string().optional(),
            age: z.number().optional(),
          },
          indexes: [
            { field: "id", kind: "primary" },
            { field: "name", kind: "unique" },
          ],
        },
      ],
      log,
    });

    collection = db.collection("users");

    await collection.storage.resolve();
    await collection.flush();
  });

  afterEach(async () => {
    await db.flush();
  });

  afterAll(async () => {
    await db.close();
  });

  it("should insert and find documents", async () => {
    await collection.storage.insert([
      { id: "1", name: "Alice", age: 30 },
      { id: "2", name: "Bob", age: 25 },
    ]);

    const all = await collection.storage.find({});
    expect(all).toHaveLength(2);

    const alice = await collection.storage.find({ name: "Alice" });
    expect(alice).toHaveLength(1);
    expect(alice[0].age).toBe(30);
  });

  it("should get documents by index", async () => {
    await collection.storage.insert([{ id: "1", name: "Alice" }]);
    const byIndex = await collection.storage.getByIndex("id", "1");
    expect(byIndex).toHaveLength(1);
    expect(byIndex[0].name).toBe("Alice");
  });

  it("should update documents", async () => {
    await collection.storage.insert([{ id: "1", name: "Alice", age: 30 }]);

    const result = await collection.storage.update({ id: "1" }, { $set: { age: 31 } });
    expect(result.matchedCount).toBe(1);
    expect(result.modifiedCount).toBe(1);

    const updated = await collection.storage.find({ id: "1" });
    expect(updated[0].age).toBe(31);
  });

  it("should remove documents", async () => {
    await collection.storage.insert([
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ]);

    const removedCount = await collection.storage.remove({ name: "Bob" });
    expect(removedCount).toBe(1);

    const remaining = await collection.storage.find({});
    expect(remaining).toHaveLength(1);
    expect(remaining[0].name).toBe("Alice");
  });

  it("should count documents", async () => {
    await collection.storage.insert([
      { id: "1", age: 30 },
      { id: "2", age: 25 },
      { id: "3", age: 30 },
    ]);

    const count = await collection.storage.count({ age: 30 });
    expect(count).toBe(2);
  });

  it("should flush the collection", async () => {
    await collection.storage.insert([{ id: "1", name: "Alice" }]);
    await collection.flush();

    const all = await collection.storage.find({});
    expect(all).toHaveLength(0);
  });
});
