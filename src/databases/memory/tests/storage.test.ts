import { describe, it } from "@std/testing/bdd";
import { expect } from "expect";

import { MemoryStorage } from "../storage.ts";

/*
 |--------------------------------------------------------------------------------
 | Unit Tests
 |--------------------------------------------------------------------------------
 */

describe("Memory Storage", () => {
  it("should insert new records", async () => {
    const storage = new MemoryStorage("test", [["id", { primary: true }]]);

    const documents = [
      {
        id: "abc",
        foo: "bar",
      },
    ];

    await storage.insert(documents);

    console.log(storage);

    expect(storage.documents).toContain(documents);
  });
});
