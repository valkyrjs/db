import type { AnyDocument } from "../types.ts";

export type PrimaryKey = string;

export class PrimaryIndex<TSchema extends AnyDocument> {
  readonly #index = new Map<PrimaryKey, TSchema>();

  constructor(readonly key: string) {}

  get documents() {
    return Array.from(this.#index.values());
  }

  keys() {
    return Array.from(this.#index.keys());
  }

  has(pk: PrimaryKey): boolean {
    return this.#index.has(pk);
  }

  insert(pk: PrimaryKey, document: TSchema) {
    if (this.#index.has(pk)) {
      throw new Error(`Duplicate primary key: ${pk}`);
    }
    this.#index.set(pk, document);
  }

  get(pk: PrimaryKey): TSchema | undefined {
    return this.#index.get(pk);
  }

  replace(pk: PrimaryKey, document: TSchema) {
    this.#index.set(pk, document);
  }

  delete(pk: PrimaryKey) {
    this.#index.delete(pk);
  }

  flush() {
    this.#index.clear();
  }
}
