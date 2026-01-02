import { hashCodeQuery } from "../../hash.ts";
import type { QueryOptions } from "../../storage/mod.ts";
import type { Document, Filter } from "../../types.ts";

export class IndexedDBCache<TSchema extends Document = Document> {
  readonly #cache = new Map<number, string[]>();
  readonly #documents = new Map<string, TSchema>();

  hash(filter: Filter<TSchema>, options: QueryOptions): number {
    return hashCodeQuery(filter, options);
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
      return ids.map((id) => this.#documents.get(id) as TSchema);
    }
  }

  flush() {
    this.#cache.clear();
    this.#documents.clear();
  }
}
