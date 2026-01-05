import type { PrimaryKey } from "./primary.ts";

export class UniqueIndex {
  readonly #index = new Map<string, PrimaryKey>();

  insert(value: any, pk: PrimaryKey) {
    if (this.#index.has(value)) {
      throw new Error(`Unique constraint violation: ${value}`);
    }
    this.#index.set(value, pk);
  }

  lookup(value: any): PrimaryKey | undefined {
    return this.#index.get(value);
  }

  delete(value: any) {
    this.#index.delete(value);
  }
}
