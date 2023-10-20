import { Collection } from "../Collection.js";
import { Document } from "../Types.js";
import { MemoryStorage } from "./MemoryDb.Storage.js";
import { Registrars } from "./Registrars.js";

type Options = {
  name: string;
  registrars: Registrars[];
};

export class MemoryDatabase<T extends Record<string, Document>> {
  readonly name: string;
  readonly #collections = new Map<keyof T, Collection<T[keyof T]>>();

  constructor(readonly options: Options) {
    this.name = options.name;
    for (const { name } of options.registrars) {
      this.#collections.set(name, new Collection(name, new MemoryStorage(name)));
    }
  }

  /*
   |--------------------------------------------------------------------------------
   | Fetchers
   |--------------------------------------------------------------------------------
   */

  collection<Name extends keyof T>(name: Name): Collection<T[Name]> {
    const collection = this.#collections.get(name);
    if (collection === undefined) {
      throw new Error(`Collection '${name as string}' not found`);
    }
    return collection as any;
  }

  /*
   |--------------------------------------------------------------------------------
   | Utilities
   |--------------------------------------------------------------------------------
   */

  async flush() {
    for (const collection of this.#collections.values()) {
      collection.flush();
    }
  }
}
