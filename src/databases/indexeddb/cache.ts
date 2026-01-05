import type { Criteria } from "mingo/types";

import { hashCodeQuery } from "../../hash.ts";
import type { QueryOptions } from "../../storage.ts";
import type { AnyDocument } from "../../types.ts";

export class IndexedDBCache<TSchema extends AnyDocument = AnyDocument> {
  readonly #cache = new Map<number, string[]>();
  readonly #documents = new Map<string, TSchema>();

  hash(condition: Criteria<TSchema>, options: QueryOptions = {}): number {
    return hashCodeQuery(condition, options);
  }

  set(hashCode: number, documents: TSchema[]) {
    this.#cache.set(
      hashCode,
      documents.map((document) => document.id),
    );
    for (const document of documents) {
      this.#documents.set(document.id, document);
    }
  }

  get(hashCode: number): TSchema[] | undefined {
    const ids = this.#cache.get(hashCode);
    if (ids !== undefined) {
      return ids.map((id) => this.#documents.get(id)).filter((document) => document !== undefined);
    }
  }

  flush() {
    this.#cache.clear();
    this.#documents.clear();
  }
}
