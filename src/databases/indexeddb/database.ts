import { type IDBPDatabase, openDB } from "idb";

import { Collection } from "../../collection.ts";
import type { DBLogger } from "../../logger.ts";
import type { Index, Registrars } from "../../registrars.ts";
import { IndexedDBStorage } from "./storage.ts";

export class IndexedDB<TOptions extends IndexedDBOptions> {
  readonly #collections = new Map<string, Collection>();
  readonly #db: Promise<IDBPDatabase<unknown>>;

  constructor(readonly options: TOptions) {
    this.#db = openDB(options.name, options.version ?? 1, {
      upgrade: (db: IDBPDatabase) => {
        for (const { name, indexes = [] } of options.registrars) {
          const store = db.createObjectStore(name);
          for (const [keyPath, options] of indexes) {
            store.createIndex(keyPath, keyPath, options);
          }
        }
      },
    });
    for (const { name, schema, indexes } of options.registrars) {
      this.#collections.set(
        name,
        new Collection({
          name,
          storage: new IndexedDBStorage(name, indexes, this.#db, options.log),
          schema,
          indexes,
        }),
      );
    }
  }

  /*
   |--------------------------------------------------------------------------------
   | Fetchers
   |--------------------------------------------------------------------------------
   */

  collection<
    TName extends TOptions["registrars"][number]["name"],
    TSchema = Extract<TOptions["registrars"][number], { name: TName }>["schema"],
  >(
    name: TName,
  ): Collection<{
    name: TName;
    storage: IndexedDBStorage;
    schema: TSchema;
    indexes: Index[];
  }> {
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

  async export(name: string, options?: { offset?: string; limit?: number }): Promise<any[]> {
    return (await this.#db).getAll(name, options?.offset, options?.limit) ?? [];
  }

  async flush() {
    for (const collection of this.#collections.values()) {
      collection.flush();
    }
  }

  close() {
    this.#db.then((db) => db.close());
  }
}

type IndexedDBOptions<TRegistrars extends Array<Registrars> = Array<any>> = {
  name: string;
  registrars: TRegistrars;
  version?: number;
  log?: DBLogger;
};
