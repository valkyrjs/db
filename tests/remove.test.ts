import { describe, it } from "@std/testing/bdd";
import { expect } from "expect";

import { Collection } from "../src/collection.ts";
import { MemoryStorage } from "../src/databases/memory/storage.ts";
import { RemoveResult } from "../src/storage/operators/remove.ts";
import { getUsers } from "./users.mock.ts";

/*
 |--------------------------------------------------------------------------------
 | Unit Tests
 |--------------------------------------------------------------------------------
 */

describe("Storage Remove", () => {
  it("should successfully delete document", async () => {
    const collection = new Collection("users", new MemoryStorage("users"));
    const users = getUsers();
    await collection.insertMany(users);
    expect(await collection.remove({ id: "user-1" })).toEqual(new RemoveResult(1));
    collection.storage.destroy();
  });
});
