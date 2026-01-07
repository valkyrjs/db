import type { IDBPDatabase } from "idb";
import { update } from "mingo";
import type { Criteria } from "mingo/types";
import type { Modifier } from "mingo/updater";

import { IndexManager, type IndexSpec } from "../../index/manager.ts";
import { type DBLogger, InsertLog, QueryLog, RemoveLog, UpdateLog } from "../../logger.ts";
import { addOptions, type QueryOptions, Storage, type UpdateResult } from "../../storage.ts";
import type { AnyDocument, StringKeyOf } from "../../types.ts";

const OBJECT_PROTOTYPE = Object.getPrototypeOf({});
const OBJECT_TAG = "[object Object]";

export class IndexedDBStorage<TSchema extends AnyDocument = AnyDocument> extends Storage<TSchema> {
  readonly pkey: string;
  readonly log: DBLogger;

  readonly #index: IndexManager<TSchema>;

  readonly #promise: Promise<void>;

  #db?: IDBPDatabase;

  constructor(name: string, indexes: IndexSpec<TSchema>[], promise: Promise<IDBPDatabase>, log?: DBLogger) {
    super(name, indexes);
    const index = this.indexes.find((index) => index.kind === "primary");
    if (index === undefined) {
      throw new Error("missing required primary key index");
    }
    this.pkey = index.field;
    this.log = log ?? function log() {};
    this.#index = new IndexManager(indexes);
    this.#promise = this.#preload(promise);
  }

  async #preload(promise: Promise<IDBPDatabase>): Promise<void> {
    this.#db = await promise;
    const records = await this.#db.getAll(this.name);
    for (const record of records) {
      await this.#index.insert(record);
    }
  }

  get db(): IDBPDatabase {
    if (this.#db === undefined) {
      throw new Error("Database not initialized");
    }
    return this.#db;
  }

  get documents(): TSchema[] {
    return this.#index.primary.documents;
  }

  async resolve(): Promise<this> {
    await this.#promise;
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

    await Promise.all(
      documents.map(async (document) => {
        const existing = await tx.store.get(document[this.pkey]); // Assuming 'id' is your key
        if (existing === undefined) {
          await tx.store.add(document);
        }
      }),
    );

    await tx.done;

    this.broadcast("insert", documents);
    for (const document of documents) {
      this.#index.insert(document);
    }

    this.log(logger.result());
  }

  /*
   |--------------------------------------------------------------------------------
   | Read
   |--------------------------------------------------------------------------------
   */

  async getByIndex(field: StringKeyOf<TSchema>, value: string): Promise<TSchema[]> {
    return this.#index.getByIndex(field, value);
  }

  async find(condition: Criteria<TSchema> = {}, options?: QueryOptions): Promise<TSchema[]> {
    const logger = new QueryLog(this.name, { condition, options });

    const cursor = this.#index.getByCondition(condition);
    if (options !== undefined) {
      addOptions(cursor, options);
    }

    const documents = await cursor.all();

    this.log(logger.result());

    return documents;
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
    for (const document of documents) {
      this.#index.update(document);
    }

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
    for (const document of documents) {
      this.#index.remove(document);
    }

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
    this.#index.flush();
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
