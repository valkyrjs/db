import { IDBPDatabase, openDB } from "idb";

import { Collection } from "../../collection.ts";
import { DBLogger } from "../../logger.ts";
import { Document } from "../../types.ts";
import { Registrars } from "../registrars.ts";
import { IndexedDbStorage } from "./storage.ts";

export class IndexedDatabase<TCollections extends StringRecord<Document>> {
  readonly #collections = new Map<keyof TCollections, Collection<TCollections[keyof TCollections]>>();
  readonly #db: Promise<IDBPDatabase<unknown>>;

  constructor(readonly options: Options) {
    this.#db = openDB(options.name, options.version ?? 1, {
      upgrade: (db: IDBPDatabase) => {
        for (const { name, indexes = [] } of options.registrars) {
          const store = db.createObjectStore(name as string, { keyPath: "id" });
          store.createIndex("id", "id", { unique: true });
          for (const [keyPath, options] of indexes) {
            store.createIndex(keyPath, keyPath, options);
          }
        }
      },
    });
    for (const { name } of options.registrars) {
      this.#collections.set(name, new Collection(name, new IndexedDbStorage(name, this.#db, options.log ?? log)));
    }
  }

  /*
   |--------------------------------------------------------------------------------
   | Fetchers
   |--------------------------------------------------------------------------------
   */

  collection<TSchema extends TCollections[Name], Name extends keyof TCollections = keyof TCollections>(
    name: Name,
  ): Collection<TSchema> {
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
