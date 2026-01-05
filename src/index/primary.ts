import type { AnyDocument } from "../types.ts";

export type PrimaryKey = string;

export class PrimaryIndex<TSchema extends AnyDocument> {
  readonly #index = new Map<PrimaryKey, TSchema>();

  constructor(readonly key: string) {}

  get documents(): TSchema[] {
    return Array.from(this.#index.values());
  }

  keys(): string[] {
    return Array.from(this.#index.keys());
  }

  has(pk: PrimaryKey): boolean {
    return this.#index.has(pk);
  }

  insert(pk: PrimaryKey, document: TSchema): void {
    if (this.#index.has(pk)) {
      throw new Error(`Duplicate primary key: ${pk}`);
    }
    this.#index.set(pk, document);
  }

  get(pk: PrimaryKey): TSchema | undefined {
    return this.#index.get(pk);
  }

  replace(pk: PrimaryKey, document: TSchema): void {
    this.#index.set(pk, document);
  }

  delete(pk: PrimaryKey): void {
    this.#index.delete(pk);
  }

  flush(): void {
    this.#index.clear();
  }
}
