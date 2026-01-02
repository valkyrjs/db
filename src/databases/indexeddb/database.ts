import { type IDBPDatabase, openDB } from "idb";

import { Collection } from "../../collection.ts";
import type { DBLogger } from "../../logger.ts";
import type { Document } from "../../types.ts";
import type { Registrars } from "../registrars.ts";
import { IndexedDBStorage } from "./storage.ts";

export class IndexedDB<TCollections extends StringRecord<Document>> {
  readonly #collections = new Map<keyof TCollections, Collection<TCollections[keyof TCollections]>>();
  readonly #db: Promise<IDBPDatabase<unknown>>;

  constructor(readonly options: Options) {
    this.#db = openDB(options.name, options.version ?? 1, {
      upgrade: (db: IDBPDatabase) => {
        for (const { name, primaryKey = "id", indexes = [] } of options.registrars) {
          const store = db.createObjectStore(name as string, { keyPath: primaryKey });
          store.createIndex(primaryKey, primaryKey, { unique: true });
          for (const [keyPath, options] of indexes) {
            store.createIndex(keyPath, keyPath, options);
          }
        }
      },
    });
    for (const { name, primaryKey = "id" } of options.registrars) {
      this.#collections.set(
        name,
        new Collection(name, new IndexedDBStorage(name, primaryKey, this.#db, options.log ?? log)),
      );
    }
  }

  /*
   |--------------------------------------------------------------------------------
   | Fetchers
   |--------------------------------------------------------------------------------
   */

  collection<Name extends keyof TCollections = keyof TCollections>(name: Name) {
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

function log() {}

type StringRecord<TCollections> = { [x: string]: TCollections };

type Options = {
  name: string;
  version?: number;
  registrars: Registrars[];
  log?: DBLogger;
};
