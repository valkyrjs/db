import type { Criteria } from "mingo/types";

import type { AnyDocument } from "../types.ts";
import { PrimaryIndex } from "./primary.ts";
import { SharedIndex } from "./shared.ts";
import { UniqueIndex } from "./unique.ts";

export class IndexManager<TSchema extends AnyDocument> {
  readonly primary: PrimaryIndex<TSchema>;

  readonly unique = new Map<keyof TSchema, UniqueIndex>();
  readonly shared = new Map<keyof TSchema, SharedIndex>();

  constructor(specs: IndexSpec[]) {
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

  insert(document: TSchema) {
    const pk = document[this.primary.key];
    for (const [field, index] of this.unique) {
      index.insert(document[field], pk);
    }
    for (const [field, index] of this.shared) {
      index.insert(document[field], pk);
    }
    this.primary.insert(pk, document);
  }

  getByCondition(condition: Criteria<TSchema>): TSchema[] | undefined {
    // const pks = new Set<any>();
    // for (const key in condition) {
    //   if (this.indexes.includes(key)) {
    //     if (key === this.primaryKey) {
    //       pks.add(condition[key]);
    //     } else {
    //       const
    //     }
    //   }
    // }
    return [];
  }

  getByPrimary(pk: string): TSchema | undefined {
    return this.primary.get(pk);
  }

  getByUnique(field: keyof TSchema, value: any): TSchema | undefined {
    const pk = this.unique.get(field)?.lookup(value);
    if (pk !== undefined) {
      return this.primary.get(pk);
    }
  }

  getByIndex(field: keyof TSchema, value: any): TSchema[] {
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

  remove(pk: string) {
    const document = this.primary.get(pk);
    if (document === undefined) {
      return;
    }
    for (const [field, index] of this.unique) {
      index.delete(document[field]);
    }
    for (const [field, index] of this.shared) {
      index.delete(document[field], pk);
    }
    this.primary.delete(pk);
  }
}

export type IndexSpec = {
  field: string;
  kind: IndexKind;
};

type IndexKind = "primary" | "unique" | "shared";
