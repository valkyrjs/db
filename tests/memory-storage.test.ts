import { describe, it } from "@std/testing/bdd";
import { expect } from "expect";

import { MemoryStorage } from "../src/databases/memory/storage.ts";

interface TestDoc {
  id: string;
  name?: string;
  age?: number;
  tags?: string[];
}

describe("MemoryStorage", () => {
  it("should insert new records", async () => {
    const storage = new MemoryStorage<TestDoc>("test", [{ field: "id", kind: "primary" }]);
    const documents: TestDoc[] = [{ id: "abc", name: "Alice", age: 30 }];

    await storage.insert(documents);

    expect(storage.documents).toHaveLength(1);
    expect(storage.documents[0]).toEqual(documents[0]);
  });

  it("should retrieve records by index", async () => {
    const storage = new MemoryStorage<TestDoc>("test", [{ field: "id", kind: "primary" }]);
    await storage.insert([
      { id: "abc", name: "Alice" },
      { id: "def", name: "Bob" },
    ]);

    const result = await storage.getByIndex("id", "abc");
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe("Alice");
  });

  it("should find documents by criteria", async () => {
    const storage = new MemoryStorage<TestDoc>("test", [{ field: "id", kind: "primary" }]);
    await storage.insert([
      { id: "1", name: "Alice", age: 30 },
      { id: "2", name: "Bob", age: 25 },
      { id: "3", name: "Charlie", age: 30 },
    ]);

    const results = await storage.find({ age: 30 });
    expect(results).toHaveLength(2);
    expect(results.map((r) => r.name).sort()).toEqual(["Alice", "Charlie"]);
  });

  it("should update documents matching a condition", async () => {
    const storage = new MemoryStorage<TestDoc>("test", [{ field: "id", kind: "primary" }]);
    await storage.insert([{ id: "1", name: "Alice", age: 30 }]);

    const updateResult = await storage.update({ id: "1" }, { $set: { age: 31 } });
    expect(updateResult.matchedCount).toBe(1);
    expect(updateResult.modifiedCount).toBe(1);

    const updated = await storage.find({ id: "1" });
    expect(updated[0].age).toBe(31);
  });

  it("should remove documents by condition", async () => {
    const storage = new MemoryStorage<TestDoc>("test", [{ field: "id", kind: "primary" }]);
    await storage.insert([
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ]);

    const removedCount = await storage.remove({ name: "Bob" });
    expect(removedCount).toBe(1);

    const remaining = await storage.find({});
    expect(remaining).toHaveLength(1);
    expect(remaining[0].name).toBe("Alice");
  });

  it("should count documents matching a condition", async () => {
    const storage = new MemoryStorage<TestDoc>("test", [{ field: "id", kind: "primary" }]);
    await storage.insert([
      { id: "1", name: "Alice", age: 30 },
      { id: "2", name: "Bob", age: 25 },
      { id: "3", name: "Charlie", age: 30 },
    ]);

    const count = await storage.count({ age: 30 });
    expect(count).toBe(2);
  });

  it("should return itself from resolve", async () => {
    const storage = new MemoryStorage<TestDoc>("test", [{ field: "id", kind: "primary" }]);
    const resolved = await storage.resolve();
    expect(resolved).toBe(storage);
  });
});
