import assert from "node:assert";

import { afterEach, beforeEach, describe, it } from "@std/testing/bdd";
import { expect } from "expect";
import z from "zod";

(globalThis as any).assert = assert;

import { Collection } from "../src/collection.ts";
import { MemoryStorage } from "../src/databases/memory/storage.ts";

const schema = {
  id: z.string(),
  name: z.string().optional(),
  emails: z.array(z.email()),
  friends: z.array(
    z.object({
      id: z.string(),
      type: z.union([z.literal("family"), z.literal("close")]),
    }),
  ),
  age: z.number(),
};

type UserSchema = typeof schema;

describe("Collection", () => {
  let collection: Collection<{
    name: string;
    storage: MemoryStorage;
    schema: UserSchema;
    indexes: [{ field: "id"; kind: "primary" }];
  }>;

  beforeEach(() => {
    collection = new Collection({
      name: "test",
      storage: new MemoryStorage("test", [
        {
          field: "id",
          kind: "primary",
        },
      ]),
      schema: {
        id: z.string(),
        name: z.string().optional(),
        fullName: z.string().optional(),
        emails: z.array(z.email()),
        friends: z.array(
          z.object({
            id: z.string(),
            type: z.union([z.literal("family"), z.literal("close")]),
          }),
        ),
        age: z.number(),
      },
      indexes: [
        {
          field: "id",
          kind: "primary",
        },
      ],
    });
  });

  afterEach(async () => {
    await collection.flush();
  });

  describe("Constructor and Properties", () => {
    it("should initialize with correct name", () => {
      expect(collection.name).toBe("test");
    });

    it("should have correct schema", () => {
      expect(collection.schema).toBeDefined();
      expect(collection.schema.id).toBeDefined();
    });

    it("should have correct storage", () => {
      expect(collection.storage).toBeDefined();
    });

    it("should identify primary key correctly", () => {
      expect(collection.primaryKey).toBe("id");
    });

    it("should throw error when primary key is missing", () => {
      expect(() => {
        new Collection({
          name: "invalid",
          storage: new MemoryStorage("invalid", []),
          schema: { id: z.string() },
          indexes: [],
        });
      }).toThrow("missing required primary key assignment");
    });
  });

  describe("Utilities", () => {
    describe("getPrimaryKeyValue", () => {
      it("should return primary key value from document", () => {
        const doc = { id: "123", name: "Test" };
        expect(collection.getPrimaryKeyValue(doc)).toBe("123");
      });

      it("should throw error when primary key is missing", () => {
        const doc = { name: "Test" };
        expect(() => collection.getPrimaryKeyValue(doc)).toThrow("Missing primary key");
      });

      it("should throw error when primary key is not a string", () => {
        const doc = { id: 123, name: "Test" };
        expect(() => collection.getPrimaryKeyValue(doc)).toThrow("Missing primary key");
      });
    });
  });

  describe("Insert Operations", () => {
    it("should insert a single document", async () => {
      await collection.insert([
        {
          id: "1",
          name: "Alice",
          emails: ["alice@test.com"],
          friends: [],
          age: 25,
        },
      ]);

      const doc = await collection.findOne({ id: "1" });
      expect(doc).toBeDefined();
      expect(doc?.name).toBe("Alice");
    });

    it("should insert multiple documents", async () => {
      await collection.insert([
        {
          id: "1",
          name: "Alice",
          emails: ["alice@test.com"],
          friends: [],
          age: 25,
        },
        {
          id: "2",
          name: "Bob",
          emails: ["bob@test.com"],
          friends: [],
          age: 30,
        },
      ]);

      const count = await collection.count();
      expect(count).toBe(2);
    });

    it("should validate documents against schema on insert", async () => {
      await expect(
        collection.insert([
          {
            id: "1",
            name: "Invalid",
            emails: ["not-an-email"],
            friends: [],
            age: 25,
          } as any,
        ]),
      ).rejects.toThrow();
    });
  });

  describe("Query Operations", () => {
    beforeEach(async () => {
      await collection.insert([
        {
          id: "1",
          name: "Alice",
          emails: ["alice@test.com"],
          friends: [{ id: "2", type: "close" }],
          age: 25,
        },
        {
          id: "2",
          name: "Bob",
          emails: ["bob@test.com"],
          friends: [{ id: "1", type: "family" }],
          age: 30,
        },
        {
          id: "3",
          name: "Charlie",
          emails: ["charlie@test.com"],
          friends: [],
          age: 35,
        },
      ]);
    });

    describe("findOne", () => {
      it("should find document by id", async () => {
        const doc = await collection.findOne({ id: "1" });
        expect(doc).toBeDefined();
        expect(doc?.name).toBe("Alice");
      });

      it("should return undefined when no match found", async () => {
        const doc = await collection.findOne({ id: "999" });
        expect(doc).toBeUndefined();
      });

      it("should find document by field value", async () => {
        const doc = await collection.findOne({ name: "Bob" });
        expect(doc).toBeDefined();
        expect(doc?.id).toBe("2");
      });

      it("should support comparison operators", async () => {
        const doc = await collection.findOne({ age: { $gte: 30 } });
        expect(doc).toBeDefined();
        expect(doc?.age).toBeGreaterThanOrEqual(30);
      });

      it("should support empty condition", async () => {
        const doc = await collection.findOne();
        expect(doc).toBeDefined();
      });
    });

    describe("findMany", () => {
      it("should find all documents", async () => {
        const docs = await collection.findMany();
        expect(docs).toHaveLength(3);
      });

      it("should find documents by condition", async () => {
        const docs = await collection.findMany({ age: { $gte: 30 } });
        expect(docs).toHaveLength(2);
        expect(docs.every((d) => d.age >= 30)).toBe(true);
      });

      it("should support limit option", async () => {
        const docs = await collection.findMany({}, { limit: 2 });
        expect(docs).toHaveLength(2);
      });

      it("should support skip option", async () => {
        const docs = await collection.findMany({}, { skip: 1 });
        expect(docs).toHaveLength(2);
      });

      it("should support sort option", async () => {
        const docs = await collection.findMany({}, { sort: { age: -1 } });
        expect(docs[0].age).toBe(35);
        expect(docs[2].age).toBe(25);
      });

      it("should support complex queries with $and", async () => {
        const docs = await collection.findMany({
          $and: [{ age: { $gte: 25 } }, { age: { $lte: 30 } }],
        });
        expect(docs).toHaveLength(2);
      });

      it("should support complex queries with $or", async () => {
        const docs = await collection.findMany({
          $or: [{ name: "Alice" }, { name: "Charlie" }],
        });
        expect(docs).toHaveLength(2);
      });

      it("should support array queries", async () => {
        const docs = await collection.findMany({
          emails: { $in: ["alice@test.com"] },
        });
        expect(docs).toHaveLength(1);
        expect(docs[0].name).toBe("Alice");
      });

      it("should support nested object queries", async () => {
        const docs = await collection.findMany({
          "friends.type": "close",
        });
        expect(docs).toHaveLength(1);
        expect(docs[0].name).toBe("Alice");
      });
    });

    describe("count", () => {
      it("should count all documents", async () => {
        const count = await collection.count();
        expect(count).toBe(3);
      });

      it("should count documents matching condition", async () => {
        const count = await collection.count({ age: { $gte: 30 } });
        expect(count).toBe(2);
      });

      it("should return 0 when no matches", async () => {
        const count = await collection.count({ age: { $gte: 100 } });
        expect(count).toBe(0);
      });
    });
  });

  describe("Update Operations", () => {
    beforeEach(async () => {
      await collection.insert([
        {
          id: "100",
          name: "John Doe",
          emails: ["john.doe@fixture.none"],
          friends: [{ id: "201", type: "close" }],
          age: 22,
        },
        {
          id: "200",
          name: "Jane Doe",
          emails: ["jane.doe@fixture.none"],
          friends: [],
          age: 28,
        },
      ]);
    });

    describe("$set operator", () => {
      it("should set top level fields", async () => {
        const result = await collection.update(
          { id: "100" },
          {
            $set: {
              age: 32,
              emails: ["john.doe@test.none"],
            },
          },
        );

        expect(result).toEqual({ matchedCount: 1, modifiedCount: 1 });

        const doc = await collection.findOne({ id: "100" });
        expect(doc?.age).toBe(32);
        expect(doc?.emails).toEqual(["john.doe@test.none"]);
      });

      it("should set nested fields", async () => {
        await collection.update(
          { id: "100" },
          {
            $set: {
              "friends.0.type": "family",
            },
          },
        );

        const doc = await collection.findOne({ id: "100" });
        expect(doc?.friends[0].type).toBe("family");
      });

      it("should update multiple documents", async () => {
        const result = await collection.update(
          { age: { $gte: 20 } },
          {
            $set: { age: 50 },
          },
        );

        expect(result.matchedCount).toBe(2);
        expect(result.modifiedCount).toBe(2);
      });
    });

    describe("$unset operator", () => {
      it("should remove fields", async () => {
        await collection.update(
          { id: "100" },
          {
            $unset: { name: "" },
          },
        );

        const doc = await collection.findOne({ id: "100" });
        expect(doc?.name).toBeUndefined();
      });
    });

    describe("$inc operator", () => {
      it("should increment numeric fields", async () => {
        await collection.update(
          { id: "100" },
          {
            $inc: { age: 5 },
          },
        );

        const doc = await collection.findOne({ id: "100" });
        expect(doc?.age).toBe(27);
      });

      it("should decrement with negative values", async () => {
        await collection.update(
          { id: "100" },
          {
            $inc: { age: -2 },
          },
        );

        const doc = await collection.findOne({ id: "100" });
        expect(doc?.age).toBe(20);
      });
    });

    describe("$mul operator", () => {
      it("should multiply numeric fields", async () => {
        await collection.update(
          { id: "100" },
          {
            $mul: { age: 2 },
          },
        );

        const doc = await collection.findOne({ id: "100" });
        expect(doc?.age).toBe(44);
      });
    });

    describe("$min and $max operators", () => {
      it("should update only if new value is smaller ($min)", async () => {
        await collection.update(
          { id: "100" },
          {
            $min: { age: 20 },
          },
        );

        const doc = await collection.findOne({ id: "100" });
        expect(doc?.age).toBe(20);
      });

      it("should not update if current value is smaller ($min)", async () => {
        await collection.update(
          { id: "100" },
          {
            $min: { age: 30 },
          },
        );

        const doc = await collection.findOne({ id: "100" });
        expect(doc?.age).toBe(22);
      });

      it("should update only if new value is larger ($max)", async () => {
        await collection.update(
          { id: "100" },
          {
            $max: { age: 30 },
          },
        );

        const doc = await collection.findOne({ id: "100" });
        expect(doc?.age).toBe(30);
      });
    });

    // TODO: TypeError: Cannot destructure property 'node' of 'params[key]' as it is undefined.
    // describe("$rename operator", () => {
    //   it("should rename fields", async () => {
    //     await collection.update(
    //       { id: "100" },
    //       {
    //         $rename: { name: "fullName" },
    //       },
    //     );

    //     const doc = await collection.findOne({ id: "100" });
    //     expect(doc?.fullName).toBe("John Doe");
    //     expect(doc?.name).toBeUndefined();
    //   });
    // });

    describe("Array update operators", () => {
      describe("$push", () => {
        it("should push item to array", async () => {
          await collection.update(
            { id: "100" },
            {
              $push: { emails: "new@test.com" },
            },
          );

          const doc = await collection.findOne({ id: "100" });
          expect(doc?.emails).toContain("new@test.com");
          expect(doc?.emails).toHaveLength(2);
        });

        it("should push object to array", async () => {
          await collection.update(
            { id: "100" },
            {
              $push: { friends: { id: "300", type: "family" } },
            },
          );

          const doc = await collection.findOne({ id: "100" });
          expect(doc?.friends).toHaveLength(2);
          expect(doc?.friends[1].id).toBe("300");
        });
      });

      describe("$pull", () => {
        it("should pull item from array", async () => {
          await collection.update(
            { id: "100" },
            {
              $pull: { emails: "john.doe@fixture.none" },
            },
          );

          const doc = await collection.findOne({ id: "100" });
          expect(doc?.emails).toHaveLength(0);
        });

        it("should pull object from array by condition", async () => {
          await collection.update(
            { id: "100" },
            {
              $pull: { friends: { id: "201" } },
            },
          );

          const doc = await collection.findOne({ id: "100" });
          expect(doc?.friends).toHaveLength(0);
        });
      });

      describe("$addToSet", () => {
        it("should add unique item to array", async () => {
          await collection.update(
            { id: "100" },
            {
              $addToSet: { emails: "new@test.com" },
            },
          );

          const doc = await collection.findOne({ id: "100" });
          expect(doc?.emails).toContain("new@test.com");
        });

        it("should not add duplicate item to array", async () => {
          await collection.update(
            { id: "100" },
            {
              $addToSet: { emails: "john.doe@fixture.none" },
            },
          );

          const doc = await collection.findOne({ id: "100" });
          expect(doc?.emails).toHaveLength(1);
        });
      });

      describe("$pop", () => {
        it("should remove last element with 1", async () => {
          await collection.update(
            { id: "100" },
            {
              $push: { emails: "extra@test.com" },
            },
          );

          await collection.update(
            { id: "100" },
            {
              $pop: { emails: 1 },
            },
          );

          const doc = await collection.findOne({ id: "100" });
          expect(doc?.emails[0]).toBe("john.doe@fixture.none");
        });

        it("should remove first element with -1", async () => {
          await collection.update(
            { id: "100" },
            {
              $push: { emails: "extra@test.com" },
            },
          );

          await collection.update(
            { id: "100" },
            {
              $pop: { emails: -1 },
            },
          );

          const doc = await collection.findOne({ id: "100" });
          expect(doc?.emails[0]).toBe("extra@test.com");
        });
      });
    });

    it("should return matched and modified counts", async () => {
      const result = await collection.update(
        { age: { $gte: 25 } },
        {
          $set: { age: 40 },
        },
      );

      expect(result.matchedCount).toBeGreaterThan(0);
      expect(result.modifiedCount).toBe(result.matchedCount);
    });

    it("should return 0 modified when no changes made", async () => {
      const result = await collection.update(
        { id: "100" },
        {
          $set: { age: 22 },
        },
      );

      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(0);
    });
  });

  describe("Remove Operations", () => {
    beforeEach(async () => {
      await collection.insert([
        {
          id: "1",
          name: "Alice",
          emails: ["alice@test.com"],
          friends: [],
          age: 25,
        },
        {
          id: "2",
          name: "Bob",
          emails: ["bob@test.com"],
          friends: [],
          age: 30,
        },
        {
          id: "3",
          name: "Charlie",
          emails: ["charlie@test.com"],
          friends: [],
          age: 35,
        },
      ]);
    });

    it("should remove single document", async () => {
      const removed = await collection.remove({ id: "1" });
      expect(removed).toBe(1);

      const doc = await collection.findOne({ id: "1" });
      expect(doc).toBeUndefined();
    });

    it("should remove multiple documents", async () => {
      const removed = await collection.remove({ age: { $gte: 30 } });
      expect(removed).toBe(2);

      const count = await collection.count();
      expect(count).toBe(1);
    });

    it("should return 0 when no documents match", async () => {
      const removed = await collection.remove({ id: "999" });
      expect(removed).toBe(0);
    });

    it("should remove all documents with empty condition", async () => {
      const removed = await collection.remove({});
      expect(removed).toBe(3);

      const count = await collection.count();
      expect(count).toBe(0);
    });
  });

  describe("Subscribe Operations", () => {
    beforeEach(async () => {
      await collection.insert([
        {
          id: "1",
          name: "Alice",
          emails: ["alice@test.com"],
          friends: [],
          age: 25,
        },
      ]);
    });

    it("should subscribe to single document changes", async () => {
      let subscription: any;

      try {
        const promise = new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            subscription?.unsubscribe();
            reject(new Error("Timeout waiting for subscription"));
          }, 1000);

          subscription = collection.subscribe({ id: "1" }, { limit: 1 }, (doc) => {
            if (doc && doc.age === 30) {
              clearTimeout(timeout);
              expect(doc.name).toBe("Alice");
              subscription.unsubscribe();
              resolve();
            }
          });

          setTimeout(() => {
            collection.update({ id: "1" }, { $set: { age: 30 } });
          }, 10);
        });

        await promise;
      } finally {
        subscription?.unsubscribe();
      }
    });

    it("should subscribe to multiple document changes", async () => {
      let subscription: any;

      try {
        const promise = new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            subscription?.unsubscribe();
            reject(new Error("Timeout waiting for subscription"));
          }, 1000);

          subscription = collection.subscribe({}, {}, (docs) => {
            if (docs.length > 1) {
              clearTimeout(timeout);
              expect(docs.length).toBeGreaterThan(1);
              subscription.unsubscribe();
              resolve();
            }
          });

          setTimeout(() => {
            collection.insert([
              {
                id: "2",
                name: "Bob",
                emails: ["bob@test.com"],
                friends: [],
                age: 30,
              },
            ]);
          }, 10);
        });

        await promise;
      } finally {
        subscription?.unsubscribe();
      }
    });

    it("should unsubscribe successfully", async () => {
      let callCount = 0;
      const subscription = collection.subscribe({}, {}, () => {
        callCount++;
      });

      subscription.unsubscribe();

      await collection.insert([
        {
          id: "2",
          name: "Bob",
          emails: ["bob@test.com"],
          friends: [],
          age: 30,
        },
      ]);

      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(callCount).toBe(1);
    });
  });

  describe("Event Handlers", () => {
    it("should trigger onChange on insert", async () => {
      let subscription: any;

      try {
        const promise = new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            subscription?.unsubscribe();
            reject(new Error("Timeout waiting for onChange"));
          }, 1000);

          subscription = collection.onChange((event) => {
            clearTimeout(timeout);
            expect(event.type).toBe("insert");
            subscription.unsubscribe();
            resolve();
          });

          collection.insert([
            {
              id: "1",
              name: "Alice",
              emails: ["alice@test.com"],
              friends: [],
              age: 25,
            },
          ]);
        });

        await promise;
      } finally {
        subscription?.unsubscribe();
      }
    });

    it("should trigger onChange on update", async () => {
      await collection.insert([
        {
          id: "1",
          name: "Alice",
          emails: ["alice@test.com"],
          friends: [],
          age: 25,
        },
      ]);

      let subscription: any;

      try {
        const promise = new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            subscription?.unsubscribe();
            reject(new Error("Timeout waiting for onChange"));
          }, 1000);

          subscription = collection.onChange((event) => {
            if (event.type === "update") {
              clearTimeout(timeout);
              subscription.unsubscribe();
              resolve();
            }
          });

          collection.update({ id: "1" }, { $set: { age: 30 } });
        });

        await promise;
      } finally {
        subscription?.unsubscribe();
      }
    });

    it("should trigger onChange on remove", async () => {
      await collection.insert([
        {
          id: "1",
          name: "Alice",
          emails: ["alice@test.com"],
          friends: [],
          age: 25,
        },
      ]);

      let subscription: any;

      try {
        const promise = new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            subscription?.unsubscribe();
            reject(new Error("Timeout waiting for onChange"));
          }, 1000);

          subscription = collection.onChange((event) => {
            if (event.type === "remove") {
              clearTimeout(timeout);
              subscription.unsubscribe();
              resolve();
            }
          });

          collection.remove({ id: "1" });
        });

        await promise;
      } finally {
        subscription?.unsubscribe();
      }
    });

    it("should trigger onFlush when flush is called", async () => {
      let subscription: any;

      try {
        const promise = new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            subscription?.unsubscribe();
            reject(new Error("Timeout waiting for onFlush"));
          }, 1000);

          subscription = collection.onFlush(() => {
            clearTimeout(timeout);
            subscription.unsubscribe();
            resolve();
          });

          collection.flush();
        });

        await promise;
      } finally {
        subscription?.unsubscribe();
      }
    });
  });

  describe("Flush Operation", () => {
    it("should clear all documents", async () => {
      await collection.insert([
        {
          id: "1",
          name: "Alice",
          emails: ["alice@test.com"],
          friends: [],
          age: 25,
        },
        {
          id: "2",
          name: "Bob",
          emails: ["bob@test.com"],
          friends: [],
          age: 30,
        },
      ]);

      await collection.flush();

      await new Promise((resolve) => setTimeout(resolve, 50));

      const count = await collection.count();
      expect(count).toBe(0);
    });
  });
});
