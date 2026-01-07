import { describe, it } from "@std/testing/bdd";
import { expect } from "expect";

import { IndexManager, type IndexSpec } from "../src/index/manager.ts";

describe("IndexManager", { sanitizeOps: false, sanitizeResources: false }, () => {
  type User = {
    id: string;
    email: string;
    group: string;
    name: string;
  };

  const specs: IndexSpec<User>[] = [
    { field: "id", kind: "primary" },
    { field: "email", kind: "unique" },
    { field: "group", kind: "shared" },
  ];

  it("should insert and retrieve documents by primary, unique, and shared indexes", () => {
    const manager = new IndexManager<User>(specs);

    const user1: User = { id: "u1", email: "a@example.com", group: "staff", name: "Alice" };
    const user2: User = { id: "u2", email: "b@example.com", group: "staff", name: "Bob" };
    const user3: User = { id: "u3", email: "c@example.com", group: "admin", name: "Carol" };

    // insert
    manager.insert(user1);
    manager.insert(user2);
    manager.insert(user3);

    // primary lookup
    expect(manager.getByPrimary("u1")).toEqual(user1);
    expect(manager.getByPrimary("u2")).toEqual(user2);
    expect(manager.getByPrimary("u3")).toEqual(user3);

    // unique lookup
    expect(manager.getByUnique("email", "a@example.com")).toEqual(user1);
    expect(manager.getByUnique("email", "b@example.com")).toEqual(user2);

    // shared lookup
    const staff = manager.getByIndex("group", "staff");
    expect(staff).toHaveLength(2);
    expect(staff).toContainEqual(user1);
    expect(staff).toContainEqual(user2);

    const admin = manager.getByIndex("group", "admin");
    expect(admin).toHaveLength(1);
    expect(admin[0]).toEqual(user3);

    // unknown lookup
    expect(manager.getByPrimary("unknown")).toBeUndefined();
    expect(manager.getByUnique("email", "notfound@example.com")).toBeUndefined();
    expect(manager.getByIndex("group", "none")).toEqual([]);
  });

  it("should enforce unique constraints", () => {
    const manager = new IndexManager<User>(specs);

    const user: User = { id: "u1", email: "a@example.com", group: "staff", name: "Alice" };
    manager.insert(user);

    const dupEmail: User = { id: "u2", email: "a@example.com", group: "admin", name: "Bob" };
    expect(() => manager.insert(dupEmail)).toThrow(/Unique constraint violation/);
  });

  it("should remove documents and clean up indexes", () => {
    const manager = new IndexManager<User>(specs);

    const user: User = { id: "u1", email: "a@example.com", group: "staff", name: "Alice" };
    manager.insert(user);

    // sanity
    expect(manager.getByPrimary("u1")).toEqual(user);
    expect(manager.getByUnique("email", "a@example.com")).toEqual(user);
    expect(manager.getByIndex("group", "staff")).toContainEqual(user);

    // remove
    manager.remove(user);

    expect(manager.getByPrimary("u1")).toBeUndefined();
    expect(manager.getByUnique("email", "a@example.com")).toBeUndefined();
    expect(manager.getByIndex("group", "staff")).toEqual([]);
  });

  it("should update existing documents", () => {
    const manager = new IndexManager<User>(specs);

    const user: User = { id: "u1", email: "a@example.com", group: "staff", name: "Alice" };
    manager.insert(user);

    // update email and group
    const updated: User = { ...user, email: "a_new@example.com", group: "admin" };
    manager.update(updated);

    // old unique index cleared
    expect(manager.getByUnique("email", "a@example.com")).toBeUndefined();

    // new unique index works
    expect(manager.getByUnique("email", "a_new@example.com")).toEqual(updated);

    // old shared index cleared
    expect(manager.getByIndex("group", "staff")).toEqual([]);

    // new shared index works
    expect(manager.getByIndex("group", "admin")).toContainEqual(updated);

    // primary still points to updated document
    expect(manager.getByPrimary("u1")).toEqual(updated);
  });

  it("should perform upsert if primary key does not exist", () => {
    const manager = new IndexManager<User>(specs);

    const user: User = { id: "u1", email: "a@example.com", group: "staff", name: "Alice" };

    // update on non-existent PK acts as insert
    manager.update(user);

    expect(manager.getByPrimary("u1")).toEqual(user);
    expect(manager.getByUnique("email", "a@example.com")).toEqual(user);
    expect(manager.getByIndex("group", "staff")).toContainEqual(user);
  });

  it("should lazily clean up stale shared index references", () => {
    const manager = new IndexManager<User>(specs);

    const user: User = { id: "u1", email: "a@example.com", group: "staff", name: "Alice" };
    manager.insert(user);

    // manually delete primary without cleaning shared
    manager.primary.delete("u1");

    // getByIndex should remove stale reference
    const result = manager.getByIndex("group", "staff");
    expect(result).toEqual([]);
    // after lazy cleanup, lookup should also be empty
    expect(manager.getPrimaryKeysByIndex("group", "staff")).toEqual(new Set());
  });

  describe(".getByCondition", () => {
    type User = {
      id: string;
      email: string;
      group: string;
      name: string;
      active: boolean;
    };

    const specs: IndexSpec<User>[] = [
      { field: "id", kind: "primary" },
      { field: "email", kind: "unique" },
      { field: "group", kind: "shared" },
    ];

    it("should find documents by primary key", () => {
      const manager = new IndexManager<User>(specs);

      const user = { id: "u1", email: "a@example.com", group: "staff", name: "Alice", active: true };
      manager.insert(user);

      const results = manager.getByCondition({ id: "u1" }).all();
      expect(results).toHaveLength(1);
      expect(results[0]).toEqual(user);
    });

    it("should find documents by unique index", () => {
      const manager = new IndexManager<User>(specs);

      const user = { id: "u1", email: "a@example.com", group: "staff", name: "Alice", active: true };
      manager.insert(user);

      const results = manager.getByCondition({ email: "a@example.com" }).all();
      expect(results).toHaveLength(1);
      expect(results[0]).toEqual(user);
    });

    it("should find documents by shared index", () => {
      const manager = new IndexManager<User>(specs);

      const user1 = { id: "u1", email: "a@example.com", group: "staff", name: "Alice", active: true };
      const user2 = { id: "u2", email: "b@example.com", group: "staff", name: "Bob", active: false };
      const user3 = { id: "u3", email: "c@example.com", group: "admin", name: "Carol", active: true };

      manager.insert(user1);
      manager.insert(user2);
      manager.insert(user3);

      const staff = manager.getByCondition({ group: "staff" }).all();
      expect(staff).toHaveLength(2);
      expect(staff).toContainEqual(user1);
      expect(staff).toContainEqual(user2);

      const admin = manager.getByCondition({ group: "admin" }).all();
      expect(admin).toHaveLength(1);
      expect(admin[0]).toEqual(user3);
    });

    it("should handle multiple fields with intersection", () => {
      const manager = new IndexManager<User>(specs);

      const user1 = { id: "u1", email: "a@example.com", group: "staff", name: "Alice", active: true };
      const user2 = { id: "u2", email: "b@example.com", group: "staff", name: "Bob", active: false };

      manager.insert(user1);
      manager.insert(user2);

      // Lookup by shared + non-indexed field

      const results = manager.getByCondition({ group: "staff", active: true }).all();

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual(user1);
    });

    it("should return empty array if no match", () => {
      const manager = new IndexManager<User>(specs);

      const user = { id: "u1", email: "a@example.com", group: "staff", name: "Alice", active: true };
      manager.insert(user);

      const results = manager.getByCondition({ group: "admin" }).all();
      expect(results).toEqual([]);

      const results2 = manager.getByCondition({ email: "nonexistent@example.com" }).all();
      expect(results2).toEqual([]);
    });
  });
});

describe("IndexManager Performance", () => {
  type User = {
    id: string;
    email: string;
    group: string;
    name: string;
  };

  const NUM_RECORDS = 100_000;

  const specs: IndexSpec<User>[] = [
    { field: "id", kind: "primary" },
    { field: "email", kind: "unique" },
    { field: "group", kind: "shared" },
  ];

  it("should insert and query thousands of records efficiently", () => {
    const manager = new IndexManager<User>(specs);

    const groups = ["staff", "admin", "guest", "manager"];

    console.time("Insert 100k records");
    for (let i = 0; i < NUM_RECORDS; i++) {
      const user: User = {
        id: `user_${i}`,
        email: `user_${i}@example.com`,
        group: groups[i % groups.length],
        name: `User ${i}`,
      };
      manager.insert(user);
    }
    console.timeEnd("Insert 100k records");

    // Check total number of records
    expect(manager.getByPrimary("user_0")?.name).toEqual("User 0");
    expect(manager.getByPrimary(`user_${NUM_RECORDS - 1}`)?.name).toEqual(`User ${NUM_RECORDS - 1}`);

    // Unique lookup
    console.time("Unique lookup 10k");
    for (let i = 0; i < 10_000; i++) {
      const user = manager.getByUnique("email", `user_${i}@example.com`);
      expect(user?.id).toEqual(`user_${i}`);
    }
    console.timeEnd("Unique lookup 10k");

    // Shared lookup
    console.time("Shared lookup");
    for (const group of groups) {
      const users = manager.getByIndex("group", group);
      expect(users.length).toBeGreaterThan(0);
    }
    console.timeEnd("Shared lookup");

    // Update some users
    console.time("Update 10k records");
    for (let i = 0; i < 10_000; i++) {
      const user = manager.getByPrimary(`user_${i}`);
      if (!user) {
        continue;
      }
      const updated = { ...user, group: groups[(i + 1) % groups.length] };
      manager.update(updated);
    }
    console.timeEnd("Update 10k records");

    // Remove some users
    console.time("Remove 10k records");
    for (let i = 0; i < 10_000; i++) {
      const user = manager.getByPrimary(`user_${i}`);
      if (user) {
        manager.remove(user);
      }
    }
    console.timeEnd("Remove 10k records");

    // Spot check
    expect(manager.getByPrimary("user_0")).toBeUndefined();
    expect(manager.getByPrimary(`user_${10_001}`)).not.toBeUndefined();
  });
});
