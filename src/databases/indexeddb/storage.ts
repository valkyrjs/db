import type { IDBPDatabase } from "idb";
import { Query, update } from "mingo";
import type { Criteria, Options } from "mingo/types";
import type { CloneMode, Modifier } from "mingo/updater";

import { type DBLogger, InsertLog, QueryLog, RemoveLog, ReplaceLog, UpdateLog } from "../../logger.ts";
import { getDocumentWithPrimaryKey } from "../../primary-key.ts";
import { DuplicateDocumentError } from "../../storage/errors.ts";
import {
  getInsertManyResult,
  getInsertOneResult,
  type InsertManyResult,
  type InsertOneResult,
} from "../../storage/operators/insert.ts";
import { RemoveResult } from "../../storage/operators/remove.ts";
import { UpdateResult } from "../../storage/operators/update.ts";
import { addOptions, type Index, type QueryOptions, Storage } from "../../storage/storage.ts";
import type { Document, Filter } from "../../types.ts";
import { IndexedDBCache } from "./cache.ts";

const OBJECT_PROTOTYPE = Object.getPrototypeOf({});
const OBJECT_TAG = "[object Object]";

export class IndexedDBStorage<TPrimaryKey extends string, TSchema extends Document = Document> extends Storage<
  TPrimaryKey,
  TSchema
> {
  readonly #cache = new IndexedDBCache<TSchema>();

  readonly #promise: Promise<IDBPDatabase>;

  #db?: IDBPDatabase;

  constructor(
    name: string,
    primaryKey: TPrimaryKey,
    promise: Promise<IDBPDatabase>,
    readonly log: DBLogger,
  ) {
    super(name, primaryKey);
    this.#promise = promise;
  }

  async resolve() {
    if (this.#db === undefined) {
      this.#db = await this.#promise;
    }
    return this;
  }

  async has(id: string): Promise<boolean> {
    const document = await this.db.getFromIndex(this.name, "id", id);
    if (document !== undefined) {
      return true;
    }
    return false;
  }

  get db() {
    if (this.#db === undefined) {
      throw new Error("Database not initialized");
    }
    return this.#db;
  }

  /*
   |--------------------------------------------------------------------------------
   | Insert
   |--------------------------------------------------------------------------------
   */

  async insertOne(values: TSchema | Omit<TSchema, TPrimaryKey>): Promise<InsertOneResult> {
    const logger = new InsertLog(this.name);

    const document = getDocumentWithPrimaryKey(this.primaryKey, values);

    if (await this.has(document[this.primaryKey])) {
      throw new DuplicateDocumentError(document, this as any);
    }
    await this.db.transaction(this.name, "readwrite", { durability: "relaxed" }).store.add(document);

    this.broadcast("insertOne", document);
    this.#cache.flush();

    this.log(logger.result());

    return getInsertOneResult(document);
  }

  async insertMany(values: (TSchema | Omit<TSchema, TPrimaryKey>)[]): Promise<InsertManyResult> {
    const logger = new InsertLog(this.name);

    const documents: TSchema[] = [];

    const tx = this.db.transaction(this.name, "readwrite", { durability: "relaxed" });
    await Promise.all(
      values.map((values) => {
        const document = getDocumentWithPrimaryKey(this.primaryKey, values);
        documents.push(document);
        return tx.store.add(document);
      }),
    );
    await tx.done;

    this.broadcast("insertMany", documents);
    this.#cache.flush();

    this.log(logger.result());

    return getInsertManyResult(documents);
  }

  /*
   |--------------------------------------------------------------------------------
   | Read
   |--------------------------------------------------------------------------------
   */

  async findById(id: string): Promise<TSchema | undefined> {
    return this.db.getFromIndex(this.name, "id", id);
  }

  async find(filter: Filter<TSchema>, options: QueryOptions = {}): Promise<TSchema[]> {
    const logger = new QueryLog(this.name, { filter, options });

    const hashCode = this.#cache.hash(filter, options);
    const cached = this.#cache.get(hashCode);
    if (cached !== undefined) {
      this.log(logger.result({ cached: true }));
      return cached;
    }

    const indexes = this.#resolveIndexes(filter);
    let cursor = new Query(filter).find<TSchema>(await this.#getAll({ ...options, ...indexes }));
    if (options !== undefined) {
      cursor = addOptions(cursor, options);
    }

    const documents = cursor.all() as TSchema[];
    this.#cache.set(this.#cache.hash(filter, options), documents);

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

  async #getAll({ index, offset, range, limit }: QueryOptions) {
    if (index !== undefined) {
      return this.#getAllByIndex(index);
    }
    if (range !== undefined) {
      return this.db.getAll(this.name, IDBKeyRange.bound(range.from, range.to));
    }
    if (offset !== undefined) {
      return this.#getAllByOffset(offset.value, offset.direction, limit);
    }
    return this.db.getAll(this.name, undefined, limit);
  }

  async #getAllByIndex(index: Index) {
    let result = new Set();
    for (const key in index) {
      const value = index[key];
      if (Array.isArray(value)) {
        for (const idx of value) {
          const values = await this.db.getAllFromIndex(this.name, key, idx);
          result = new Set([...result, ...values]);
        }
      } else {
        const values = await this.db.getAllFromIndex(this.name, key, value);
        result = new Set([...result, ...values]);
      }
    }
    return Array.from(result);
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

  async updateOne(
    filter: Filter<TSchema>,
    modifier: Modifier<TSchema>,
    arrayFilters?: Filter<TSchema>[],
    condition?: Criteria<TSchema>,
    options: { cloneMode?: CloneMode; queryOptions?: Partial<Options> } = { cloneMode: "deep" },
  ): Promise<UpdateResult> {
    if (typeof filter.id === "string") {
      return this.#update(filter.id, modifier, arrayFilters, condition, options);
    }
    const documents = await this.find(filter);
    if (documents.length > 0) {
      return this.#update(documents[0].id, modifier, arrayFilters, condition, options);
    }
    return new UpdateResult(0, 0);
  }

  async updateMany(
    filter: Filter<TSchema>,
    modifier: Modifier<TSchema>,
    arrayFilters?: Filter<TSchema>[],
    condition?: Criteria<TSchema>,
    options: { cloneMode?: CloneMode; queryOptions?: Partial<Options> } = { cloneMode: "deep" },
  ): Promise<UpdateResult> {
    const logger = new UpdateLog(this.name, { filter, modifier, arrayFilters, condition, options });

    const ids = await this.find(filter).then((data) => data.map((d) => d.id));

    const documents: TSchema[] = [];
    let modifiedCount = 0;

    const tx = this.db.transaction(this.name, "readwrite", { durability: "relaxed" });
    await Promise.all(
      ids.map((id) =>
        tx.store.get(id).then((current) => {
          if (current === undefined) {
            return;
          }
          const modified = update(current, modifier, arrayFilters, condition, options);
          if (modified.length > 0) {
            modifiedCount += 1;
            documents.push(current);
            return tx.store.put(current);
          }
        }),
      ),
    );

    await tx.done;

    this.broadcast("updateMany", documents);
    this.#cache.flush();

    this.log(logger.result());

    return new UpdateResult(ids.length, modifiedCount);
  }

  async replace(filter: Filter<TSchema>, document: TSchema): Promise<UpdateResult> {
    const logger = new ReplaceLog(this.name, document);

    const ids = await this.find(filter).then((data) => data.map((d) => d.id));

    const documents: TSchema[] = [];
    const count = ids.length;

    const tx = this.db.transaction(this.name, "readwrite", { durability: "relaxed" });
    await Promise.all(
      ids.map((id) => {
        const next = { ...document, id };
        documents.push(next);
        return tx.store.put(next);
      }),
    );
    await tx.done;

    this.broadcast("updateMany", documents);
    this.#cache.flush();

    this.log(logger.result({ count }));

    return new UpdateResult(count, count);
  }

  async #update(
    id: string | number,
    modifier: Modifier<TSchema>,
    arrayFilters?: Filter<TSchema>[],
    condition?: Criteria<TSchema>,
    options: { cloneMode?: CloneMode; queryOptions?: Partial<Options> } = { cloneMode: "deep" },
  ): Promise<UpdateResult> {
    const logger = new UpdateLog(this.name, { id, modifier });

    const tx = this.db.transaction(this.name, "readwrite", { durability: "relaxed" });

    const current = await tx.store.get(id);
    if (current === undefined) {
      await tx.done;
      return new UpdateResult(0, 0);
    }

    const modified = await update(current, modifier, arrayFilters, condition, options);
    if (modified.length > 0) {
      await tx.store.put(current);
    }
    await tx.done;

    if (modified.length > 0) {
      this.broadcast("updateOne", current);
      this.log(logger.result());
      this.#cache.flush();
      return new UpdateResult(1, 1);
    }

    return new UpdateResult(1);
  }

  /*
   |--------------------------------------------------------------------------------
   | Remove
   |--------------------------------------------------------------------------------
   */

  async remove(filter: Filter<TSchema>): Promise<RemoveResult> {
    const logger = new RemoveLog(this.name, { filter });

    const documents = await this.find(filter);
    const tx = this.db.transaction(this.name, "readwrite");

    await Promise.all(documents.map((data) => tx.store.delete(data.id)));
    await tx.done;

    this.broadcast("remove", documents);
    this.#cache.flush();

    this.log(logger.result({ count: documents.length }));

    return new RemoveResult(documents.length);
  }

  /*
   |--------------------------------------------------------------------------------
   | Count
   |--------------------------------------------------------------------------------
   */

  async count(filter?: Filter<TSchema>): Promise<number> {
    if (filter !== undefined) {
      return (await this.find(filter)).length;
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
