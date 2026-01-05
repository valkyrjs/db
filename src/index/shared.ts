import type { PrimaryKey } from "./primary.ts";

export class SharedIndex {
  readonly #index = new Map<string, Set<PrimaryKey>>();

  /**
   * Add a value to a shared primary key index.
   *
   * @param value - Value to map the primary key to.
   * @param pk    - Primary key to add to the value set.
   */
  insert(value: any, pk: PrimaryKey) {
    let set = this.#index.get(value);
    if (set === undefined) {
      set = new Set();
      this.#index.set(value, set);
    }
    set.add(pk);
  }

  /**
   * Find a indexed primary key for the given value.
   *
   * @param value - Value to lookup a primary key for.
   */
  lookup(value: any): Set<PrimaryKey> {
    return this.#index.get(value) ?? new Set();
  }

  /**
   * Delete a primary key from a indexed value.
   *
   * @param value - Value to remove primary key from.
   * @param pk    - Primary key to remove.
   */
  delete(value: any, pk: PrimaryKey) {
    const set = this.#index.get(value);
    if (set === undefined) {
      return;
    }
    set.delete(pk);
    if (set.size === 0) {
      this.#index.delete(value);
    }
  }
}
