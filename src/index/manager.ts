import type { Criteria } from "mingo/types";

import type { AnyDocument, StringKeyOf } from "../types.ts";
import { PrimaryIndex, type PrimaryKey } from "./primary.ts";
import { SharedIndex } from "./shared.ts";
import { UniqueIndex } from "./unique.ts";

const EMPTY_SET: ReadonlySet<PrimaryKey> = Object.freeze(new Set<PrimaryKey>());

export class IndexManager<TSchema extends AnyDocument> {
  readonly primary: PrimaryIndex<TSchema>;

  readonly unique: Map<StringKeyOf<TSchema>, UniqueIndex> = new Map<StringKeyOf<TSchema>, UniqueIndex>();
  readonly shared: Map<StringKeyOf<TSchema>, SharedIndex> = new Map<StringKeyOf<TSchema>, SharedIndex>();

  constructor(readonly specs: IndexSpec<TSchema>[]) {
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
        index.insert(document[field], pk);
        insertedUniques.push([field, document[field]]);
      }
      for (const [field, index] of this.shared) {
        index.insert(document[field], pk);
        insertedShared.push([field, document[field]]);
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

  getByCondition(condition: Criteria<TSchema>): TSchema[] {
    const indexedKeys = Array.from(
      new Set([this.primary.key as StringKeyOf<TSchema>, ...this.unique.keys(), ...this.shared.keys()]),
    );

    const candidatePKs: PrimaryKey[] = [];

    // ### Primary Keys
    // Collect primary keys for indexed equality conditions

    const pkSets: ReadonlySet<PrimaryKey>[] = [];

    for (const key of indexedKeys) {
      const value = (condition as any)[key];
      if (value !== undefined) {
        // Use index if available
        const pks = this.getPrimaryKeysByIndex(key, value);
        pkSets.push(pks);
      }
    }

    // ### Intersect
    // Intersect all sets to find candidates

    if (pkSets.length > 0) {
      const sortedSets = pkSets.sort((a, b) => a.size - b.size);
      const intersection = new Set(sortedSets[0]);
      for (let i = 1; i < sortedSets.length; i++) {
        for (const pk of intersection) {
          if (!sortedSets[i].has(pk)) {
            intersection.delete(pk);
          }
        }
      }
      candidatePKs.push(...intersection);
    } else {
      candidatePKs.push(...this.primary.keys()); // no indexed fields → scan all primary keys
    }

    // ### Filter
    // Filter candidates by remaining condition

    const results: TSchema[] = [];
    for (const pk of candidatePKs) {
      const doc = this.primary.get(pk);
      if (doc === undefined) {
        continue;
      }
      let match = true;
      for (const [field, expected] of Object.entries(condition)) {
        if ((doc as any)[field] !== expected) {
          match = false;
          break;
        }
      }
      if (match) {
        results.push(doc);
      }
    }

    return results;
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

export type IndexSpec<TSchema extends AnyDocument> = {
  field: StringKeyOf<TSchema>;
  kind: IndexKind;
};

type IndexKind = "primary" | "unique" | "shared";
