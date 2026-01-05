import type { AnyDocument } from "../types.ts";

export type PrimaryKey = string;

export class PrimaryIndex<TSchema extends AnyDocument> {
  readonly #index = new Map<PrimaryKey, TSchema>();

  constructor(readonly key: string) {}

  insert(pk: PrimaryKey, document: TSchema) {
    if (this.#index.has(pk)) {
      throw new Error(`Duplicate primary key: ${pk}`);
    }
    this.#index.set(pk, document);
  }

  get(pk: PrimaryKey): TSchema | undefined {
    return this.#index.get(pk);
  }

  delete(pk: PrimaryKey) {
    this.#index.delete(pk);
  }
}
