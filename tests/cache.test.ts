import { afterEach, beforeEach, describe, it } from "@std/testing/bdd";
import { expect } from "expect";

import { IndexedDbCache } from "../src/databases/indexeddb/cache.ts";
import { Options } from "../src/storage/storage.ts";
import type { WithId } from "../src/types.ts";

describe("IndexedDbCache", () => {
  let cache: IndexedDbCache;

  beforeEach(() => {
    cache = new IndexedDbCache();
  });

  afterEach(() => {
    cache.flush();
  });

  const sampleDocuments: WithId<{ name: string }>[] = [
    { id: "doc1", name: "Document 1" },
    { id: "doc2", name: "Document 2" },
  ];

  const sampleCriteria = { name: { $eq: "Document 1" } };
  const sampleOptions: Options = { sort: { name: 1 } };

  it("hash", () => {
    const hashCode = cache.hash(sampleCriteria, sampleOptions);
    expect(typeof hashCode).toBe("number");
  });

  it("set and get", () => {
    const hashCode = cache.hash(sampleCriteria, sampleOptions);
    cache.set(hashCode, sampleDocuments);
    const result = cache.get(hashCode);
    expect(result).toEqual(sampleDocuments);
  });

  it("get undefined", () => {
    const hashCode = cache.hash(sampleCriteria, sampleOptions);
    const result = cache.get(hashCode);
    expect(result).toBeUndefined();
  });

  it("flush", () => {
    const hashCode = cache.hash(sampleCriteria, sampleOptions);
    cache.set(hashCode, sampleDocuments);
    cache.flush();
    const result = cache.get(hashCode);
    expect(result).toBeUndefined();
  });
});
