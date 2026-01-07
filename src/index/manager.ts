import { Query } from "mingo";
import type { Cursor } from "mingo/cursor";
import type { Criteria } from "mingo/types";

import type { AnyDocument, QueryCriteria, StringKeyOf } from "../types.ts";
import { PrimaryIndex, type PrimaryKey } from "./primary.ts";
import { SharedIndex } from "./shared.ts";
import { UniqueIndex } from "./unique.ts";

const OBJECT_PROTOTYPE = Object.getPrototypeOf({});
const OBJECT_TAG = "[object Object]";
const EMPTY_SET: ReadonlySet<PrimaryKey> = Object.freeze(new Set<PrimaryKey>());

export class IndexManager<TSchema extends AnyDocument> {
  readonly primary: PrimaryIndex<TSchema>;

  readonly unique: Map<StringKeyOf<TSchema>, UniqueIndex> = new Map<StringKeyOf<TSchema>, UniqueIndex>();
  readonly shared: Map<StringKeyOf<TSchema>, SharedIndex> = new Map<StringKeyOf<TSchema>, SharedIndex>();

  readonly specs: IndexSpec<TSchema>[];

  constructor(specs: IndexSpec<TSchema>[]) {
    const primary = specs.find((spec) => spec.kind === "primary");
    if (primary === undefined) {
      throw new Error("Primary index is required");
    }
    this.primary = new PrimaryIndex(primary.field);
    for (const spec of specs) {
      switch (spec.kind) {
        case "unique": {
          this.unique.set(spec.field, new UniqueIndex());
          break;
        }
        case "shared": {
          this.shared.set(spec.field, new SharedIndex());
          break;
        }
      }
    }
    this.specs = specs;
  }

  #isPrimaryIndex(key: string): boolean {
    for (const { field, kind } of this.specs) {
      if (key === field && kind === "primary") {
        return true;
      }
    }
    return false;
  }

  #isUniqueIndex(key: string): boolean {
    for (const { field, kind } of this.specs) {
      if (key === field && kind === "unique") {
        return true;
      }
    }
    return false;
  }

  #isSharedIndex(key: string): boolean {
    for (const { field, kind } of this.specs) {
      if (key === field && kind === "shared") {
        return true;
      }
    }
    return false;
  }

  #getOptimalIndex(keys: string[]): string {
    let best: string | undefined;

    for (const key of keys) {
      if (this.#isPrimaryIndex(key)) {
        return key; // cannot beat primary
      }

      if (this.#isUniqueIndex(key)) {
        best ??= key;
        continue;
      }

      if (best === undefined && this.#isSharedIndex(key)) {
        best = key;
      }
    }

    return best ?? keys[0];
  }

  /**
   * Atomic insert of the document into the index pools. If any part
   * of the operation fails all changes are rolled back to their original
   * states.
   *
   * @param document - Document to insert.
   */
  insert(document: TSchema): void {
    const pk = document[this.primary.key];

    const insertedUniques: [StringKeyOf<TSchema>, any][] = [];
    const insertedShared: [StringKeyOf<TSchema>, any][] = [];

    try {
      for (const [field, index] of this.unique) {
        const value = document[field] as any;
        if (value !== undefined) {
          if (Array.isArray(value)) {
            for (const innerValue of value) {
              index.insert(innerValue, pk);
              insertedUniques.push([field, innerValue]);
            }
          } else {
            index.insert(value, pk);
            insertedUniques.push([field, value]);
          }
        }
      }
      for (const [field, index] of this.shared) {
        const value = document[field] as any;
        if (value !== undefined) {
          if (Array.isArray(value)) {
            for (const innerValue of value) {
              index.insert(innerValue, pk);
              insertedShared.push([field, innerValue]);
            }
          } else {
            index.insert(value, pk);
            insertedShared.push([field, value]);
          }
        }
      }
      this.primary.insert(pk, document);
    } catch (err) {
      for (const [field, value] of insertedUniques) {
        this.unique.get(field)?.delete(value);
      }
      for (const [field, value] of insertedShared) {
        this.shared.get(field)?.delete(value, pk);
      }
      throw err;
    }
  }

  getByCondition(condition: Criteria<TSchema> = {}): Cursor<TSchema> {
    const indexes = resolveIndexesFromCondition(condition, this.specs);

    if (indexes === undefined) {
      return new Query(condition, {}).find<TSchema>(this.primary.documents);
    }

    const index = this.#getOptimalIndex(Object.keys(indexes));
    const value = indexes[index];

    if (Array.isArray(value)) {
      const results: TSchema[] = [];
      for (const innerValue of value) {
        const records = this.getByIndex(index as any, innerValue);
        results.push(...records);
      }
      const unique = new Map<any, TSchema>();
      for (const doc of results) {
        unique.set(doc[this.primary.key], doc);
      }
      return new Query(condition, {}).find<TSchema>(Array.from(unique.values()));
    }

    return new Query(condition, {}).find<TSchema>(this.getByIndex(index as any, value));
  }

  /**
   * Get all primary keys found for given field => value pair.
   *
   * @param field - Field to lookup.
   * @param value - Value to lookup.
   */
  getPrimaryKeysByIndex(field: StringKeyOf<TSchema>, value: any): ReadonlySet<PrimaryKey> {
    if (field === this.primary.key) {
      if (this.primary.has(value)) {
        return new Set([value]);
      }
      return EMPTY_SET;
    }
    if (this.unique.has(field)) {
      const pk = this.unique.get(field)?.lookup(value);
      if (pk === undefined) {
        return EMPTY_SET;
      }
      return new Set([pk]);
    }
    return this.shared.get(field)?.lookup(value) ?? EMPTY_SET;
  }

  /**
   * Get document by primary key.
   *
   * @param pk - Primary key to fetch document for.
   */
  getByPrimary(pk: string): TSchema | undefined {
    return this.primary.get(pk);
  }

  /**
   * Get a document found for given field => value pair.
   *
   * @param field - Field to lookup.
   * @param value - Value to lookup.
   */
  getByUnique(field: StringKeyOf<TSchema>, value: any): TSchema | undefined {
    const pk = this.unique.get(field)?.lookup(value);
    if (pk !== undefined) {
      return this.primary.get(pk);
    }
  }

  /**
   * Get all documents found for given field => value pair.
   *
   * @note This method may clean up stale index entries during reads.
   *
   * @param field - Field to lookup.
   * @param value - Value to lookup.
   */
  getByIndex(field: StringKeyOf<TSchema>, value: any): TSchema[] {
    if (field === this.primary.key) {
      const document = this.getByPrimary(value);
      if (document === undefined) {
        return [];
      }
      return [document];
    }

    if (this.unique.has(field)) {
      const document = this.getByUnique(field, value);
      if (document === undefined) {
        this.unique.get(field)?.delete(value);
        return [];
      }
      return [document];
    }

    const pks = this.shared.get(field)?.lookup(value);
    if (pks === undefined) {
      return [];
    }

    const documents: TSchema[] = [];
    for (const pk of pks) {
      const document = this.primary.get(pk);
      if (document === undefined) {
        this.shared.get(field)?.delete(value, pk);
      } else {
        documents.push(document);
      }
    }
    return documents;
  }

  /**
   * Update indexes for given document.
   *
   * @note If the document does not exist it will be inserted.
   *
   * @param document - Document to update against current index.
   */
  update(document: TSchema): void {
    const pk = document[this.primary.key];
    const current = this.primary.get(pk);

    if (current === undefined) {
      this.insert(document);
      return;
    }

    const revertedUniques: [StringKeyOf<TSchema>, any][] = [];
    const revertedShared: [StringKeyOf<TSchema>, any][] = [];

    try {
      for (const [field, index] of this.unique) {
        if (current[field] !== document[field]) {
          index.delete(current[field]);
          index.insert(document[field], pk);
          revertedUniques.push([field, current[field]]);
        }
      }
      for (const [field, index] of this.shared) {
        if (current[field] !== document[field]) {
          index.delete(current[field], pk);
          index.insert(document[field], pk);
          revertedShared.push([field, current[field]]);
        }
      }
      this.primary.replace(pk, document);
    } catch (err) {
      for (const [field, value] of revertedUniques) {
        this.unique.get(field)?.insert(value, pk);
        this.unique.get(field)?.delete(document[field]);
      }
      for (const [field, value] of revertedShared) {
        this.shared.get(field)?.insert(value, pk);
        this.shared.get(field)?.delete(document[field], pk);
      }
      throw err;
    }
  }

  /**
   * Remove all indexes related to given document.
   *
   * @param document - Document to remove.
   */
  remove(document: TSchema) {
    const pk = document[this.primary.key];
    const current = this.primary.get(pk);
    if (current === undefined) {
      return;
    }
    for (const [field, index] of this.unique) {
      index.delete(current[field]);
    }
    for (const [field, index] of this.shared) {
      index.delete(current[field], pk);
    }
    this.primary.delete(pk);
  }

  flush() {
    this.primary.flush();
    this.unique.clear();
    this.shared.clear();
    for (const spec of this.specs) {
      switch (spec.kind) {
        case "unique": {
          this.unique.set(spec.field, new UniqueIndex());
          break;
        }
        case "shared": {
          this.shared.set(spec.field, new SharedIndex());
          break;
        }
      }
    }
  }
}

/*
 |--------------------------------------------------------------------------------
 | Utils
 |--------------------------------------------------------------------------------
 */

function resolveIndexesFromCondition<TSchema extends AnyDocument>(
  condition: QueryCriteria<TSchema>,
  indexes: IndexSpec<TSchema>[],
): Record<StringKeyOf<TSchema>, any> | undefined {
  const indexNames = indexes.map(({ field }) => field);

  const index: any = {};

  for (const key in condition) {
    if (indexNames.includes(key as any) === true) {
      let val: any;
      if (isObject(condition[key]) === true) {
        if ((condition as any)[key].$in !== undefined) {
          val = (condition as any)[key].$in;
        }
      } else {
        val = condition[key];
      }
      if (val !== undefined) {
        index[key] = val;
      }
    }
  }

  if (Object.keys(index).length > 0) {
    return index;
  }
}

function isObject(v: any): v is object {
  if (!v) {
    return false;
  }
  const proto = Object.getPrototypeOf(v);
  return (proto === OBJECT_PROTOTYPE || proto === null) && OBJECT_TAG === Object.prototype.toString.call(v);
}

/*
 |--------------------------------------------------------------------------------
 | Types
 |--------------------------------------------------------------------------------
 */

export type IndexSpec<TSchema extends AnyDocument> = {
  field: StringKeyOf<TSchema>;
  kind: IndexKind;
};

type IndexKind = "primary" | "unique" | "shared";
