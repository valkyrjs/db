import { describe, it } from "@std/testing/bdd";
import { expect } from "expect";

import { hashCodeQuery } from "../src/hash.ts";
import { Options } from "../src/mod.ts";

describe("hashCodeQuery", () => {
  const filter = { name: { $eq: "Document 1" } };
  const options: Options = { sort: { name: 1 } };

  it("return correct hash code", () => {
    const hashCode = hashCodeQuery(filter, options);
    expect(typeof hashCode).toBe("number");
  });
});
