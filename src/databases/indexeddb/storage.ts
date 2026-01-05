import type { IDBPDatabase } from "idb";
import { Query, update } from "mingo";
import type { Criteria } from "mingo/types";
import type { Modifier } from "mingo/updater";

import type { IndexSpec } from "../../index/manager.ts";
import { type DBLogger, InsertLog, QueryLog, RemoveLog, UpdateLog } from "../../logger.ts";
import { addOptions, type QueryOptions, Storage, type UpdateResult } from "../../storage.ts";
import type { AnyDocument } from "../../types.ts";
import { IndexedDBCache } from "./cache.ts";

const OBJECT_PROTOTYPE = Object.getPrototypeOf({});
const OBJECT_TAG = "[object Object]";

export class IndexedDBStorage<TSchema extends AnyDocument = AnyDocument> extends Storage<TSchema> {
  readonly #cache = new IndexedDBCache<TSchema>();

  readonly #promise: Promise<IDBPDatabase>;

  #db?: IDBPDatabase;

  constructor(
    name: string,
    indexes: IndexSpec<TSchema>[],
    promise: Promise<IDBPDatabase>,
    readonly log: DBLogger = function log() {},
  ) {
    super(name, indexes);
    this.#promise = promise;
  }

  get db() {
    if (this.#db === undefined) {
      throw new Error("Database not initialized");
    }
    return this.#db;
  }

  async resolve() {
    if (this.#db === undefined) {
      this.#db = await this.#promise;
    }
    return this;
  }

  /*
   |--------------------------------------------------------------------------------
   | Insert
   |--------------------------------------------------------------------------------
   */

  async insert(documents: TSchema[]): Promise<void> {
    const logger = new InsertLog(this.name);

    const tx = this.db.transaction(this.name, "readwrite", { durability: "relaxed" });
    await Promise.all(documents.map((document) => tx.store.add(document)));
    await tx.done;

    this.broadcast("insert", documents);
    this.#cache.flush();

    this.log(logger.result());
  }

  /*
   |--------------------------------------------------------------------------------
   | Read
   |--------------------------------------------------------------------------------
   */

  async getByIndex(index: string, value: string): Promise<TSchema[]> {
    return this.db.getAllFromIndex(this.name, index, value);
  }

  async find(condition: Criteria<TSchema> = {}, options?: QueryOptions): Promise<TSchema[]> {
    const logger = new QueryLog(this.name, { condition, options });

    const hashCode = this.#cache.hash(condition, options);
    const cached = this.#cache.get(hashCode);
    if (cached !== undefined) {
      this.log(logger.result({ cached: true }));
      return cached;
    }

    const indexes = this.#resolveIndexes(condition);
    let cursor = new Query(condition).find<TSchema>(await this.#getAll({ ...options, ...indexes }));
    if (options !== undefined) {
      cursor = addOptions(cursor, options);
    }

    const documents = cursor.all();
    this.#cache.set(this.#cache.hash(condition, options), documents);

    this.log(logger.result());

    return documents;
  }

  /**
   * TODO: Prototype! Needs to cover more mongodb query cases and investigation around
   * nested indexing in indexeddb.
   */
  #resolveIndexes(filter: any): { index?: { [key: string]: any } } {
    const indexNames = this.db.transaction(this.name, "readonly").store.indexNames;
    const index: { [key: string]: any } = {};
    for (const key in filter) {
      if (indexNames.contains(key) === true) {
        let val: any;
        if (isObject(filter[key]) === true) {
          if ((filter as any)[key].$in !== undefined) {
            val = (filter as any)[key].$in;
          }
        } else {
          val = filter[key];
        }
        if (val !== undefined) {
          index[key] = val;
        }
      }
    }
    if (Object.keys(index).length > 0) {
      return { index };
    }
    return {};
  }

  async #getAll({ offset, range, limit }: QueryOptions) {
    if (range !== undefined) {
      return this.db.getAll(this.name, IDBKeyRange.bound(range.from, range.to));
    }
    if (offset !== undefined) {
      return this.#getAllByOffset(offset.value, offset.direction, limit);
    }
    return this.db.getAll(this.name, undefined, limit);
  }

  async #getAllByOffset(value: string, direction: 1 | -1, limit?: number) {
    if (direction === 1) {
      return this.db.getAll(this.name, IDBKeyRange.lowerBound(value), limit);
    }
    return this.#getAllByDescOffset(value, limit);
  }

  async #getAllByDescOffset(value: string, limit?: number) {
    if (limit === undefined) {
      return this.db.getAll(this.name, IDBKeyRange.upperBound(value));
    }
    const result = [];
    let cursor = await this.db
      .transaction(this.name, "readonly")
      .store.openCursor(IDBKeyRange.upperBound(value), "prev");
    for (let i = 0; i < limit; i++) {
      if (cursor === null) {
        break;
      }
      result.push(cursor.value);
      cursor = await cursor.continue();
    }
    return result.reverse();
  }

  /*
   |--------------------------------------------------------------------------------
   | Update
   |--------------------------------------------------------------------------------
   */

  async update(
    condition: Criteria<TSchema>,
    modifier: Modifier<TSchema>,
    arrayFilters?: TSchema[],
  ): Promise<UpdateResult> {
    const logger = new UpdateLog(this.name, { condition, modifier, arrayFilters });

    const ids = await this.find(condition).then((data) => data.map((d) => d.id));

    const documents: TSchema[] = [];
    let modifiedCount = 0;

    const tx = this.db.transaction(this.name, "readwrite", { durability: "relaxed" });
    await Promise.all(
      ids.map((id) =>
        tx.store.get(id).then((current) => {
          if (current === undefined) {
            return;
          }
          const modified = update(current, modifier, arrayFilters, condition, { cloneMode: "deep" });
          if (modified.length > 0) {
            modifiedCount += 1;
            documents.push(current);
            return tx.store.put(current);
          }
        }),
      ),
    );

    await tx.done;

    this.broadcast("update", documents);
    this.#cache.flush();

    this.log(logger.result());

    return { matchedCount: ids.length, modifiedCount };
  }

  /*
   |--------------------------------------------------------------------------------
   | Remove
   |--------------------------------------------------------------------------------
   */

  async remove(condition: Criteria<TSchema>): Promise<number> {
    const logger = new RemoveLog(this.name, { condition });

    const documents = await this.find(condition);
    const tx = this.db.transaction(this.name, "readwrite");

    await Promise.all(documents.map((data) => tx.store.delete(data.id)));
    await tx.done;

    this.broadcast("remove", documents);
    this.#cache.flush();

    this.log(logger.result({ count: documents.length }));

    return documents.length;
  }

  /*
   |--------------------------------------------------------------------------------
   | Count
   |--------------------------------------------------------------------------------
   */

  async count(condition: Criteria<TSchema>): Promise<number> {
    if (condition !== undefined) {
      return (await this.find(condition)).length;
    }
    return this.db.count(this.name);
  }

  /*
   |--------------------------------------------------------------------------------
   | Flush
   |--------------------------------------------------------------------------------
   */

  async flush(): Promise<void> {
    await this.db.clear(this.name);
  }
}

/*
 |--------------------------------------------------------------------------------
 | Utils
 |--------------------------------------------------------------------------------
 */

export function isObject(v: any): v is object {
  if (!v) {
    return false;
  }
  const proto = Object.getPrototypeOf(v);
  return (proto === OBJECT_PROTOTYPE || proto === null) && OBJECT_TAG === Object.prototype.toString.call(v);
}
