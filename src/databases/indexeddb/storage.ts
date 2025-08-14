import { IDBPDatabase } from "idb";
import { createUpdater, Query } from "mingo";
import { UpdateOptions } from "mingo/core";
import { UpdateExpression } from "mingo/updater";

import { DBLogger, InsertLog, QueryLog, RemoveLog, ReplaceLog, UpdateLog } from "../../logger.ts";
import { DuplicateDocumentError } from "../../storage/errors.ts";
import {
  getInsertManyResult,
  getInsertOneResult,
  type InsertManyResult,
  type InsertOneResult,
} from "../../storage/operators/insert.ts";
import { RemoveResult } from "../../storage/operators/remove.ts";
import { UpdateResult } from "../../storage/operators/update.ts";
import { addOptions, Options, Storage } from "../../storage/storage.ts";
import type { Document, Filter, WithId } from "../../types.ts";
import { IndexedDbCache } from "./cache.ts";

const update = createUpdater({ cloneMode: "deep" });

export class IndexedDbStorage<TSchema extends Document = Document> extends Storage<TSchema> {
  readonly #cache = new IndexedDbCache<TSchema>();
  readonly #documents = new Map<string, WithId<TSchema>>();
  readonly #promise: Promise<IDBPDatabase>;

  #db?: IDBPDatabase;

  constructor(
    name: string,
    promise: Promise<IDBPDatabase>,
    readonly log: DBLogger,
  ) {
    super(name);
    this.#promise = promise;
  }

  async resolve() {
    if (this.#db === undefined) {
      this.#db = await this.#promise;
    }
    const documents = await this.db.getAll(this.name);
    for (const document of documents) {
      this.#documents.set(document.id, document);
    }
    return this;
  }

  async has(id: string): Promise<boolean> {
    return this.#documents.has(id);
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

  async insertOne(data: Partial<TSchema>): Promise<InsertOneResult> {
    const logger = new InsertLog(this.name);

    const document = { ...data, id: data.id ?? crypto.randomUUID() } as WithId<TSchema>;
    if (await this.has(document.id)) {
      throw new DuplicateDocumentError(document, this as any);
    }
    this.#documents.set(document.id, document);

    this.broadcast("insertOne", document);
    this.#cache.flush();

    this.log(logger.result());

    return getInsertOneResult(document);
  }

  async insertMany(documents: Partial<TSchema>[]): Promise<InsertManyResult> {
    const logger = new InsertLog(this.name);

    const result: TSchema[] = [];
    for (const data of documents) {
      const document = { ...data, id: data.id ?? crypto.randomUUID() } as WithId<TSchema>;
      result.push(document);
      this.#documents.set(document.id, document);
    }

    this.broadcast("insertMany", result);
    this.#cache.flush();

    this.log(logger.result());

    return getInsertManyResult(result);
  }

  /*
   |--------------------------------------------------------------------------------
   | Read
   |--------------------------------------------------------------------------------
   */

  async findById(id: string): Promise<WithId<TSchema> | undefined> {
    return this.#documents.get(id);
  }

  async find(filter: Filter<WithId<TSchema>>, options: Options = {}): Promise<WithId<TSchema>[]> {
    const logger = new QueryLog(this.name, { filter, options });

    const hashCode = this.#cache.hash(filter, options);
    const cached = this.#cache.get(hashCode);
    if (cached !== undefined) {
      this.log(logger.result({ cached: true }));
      return cached;
    }

    let cursor = new Query(filter ?? {}).find<TSchema>(Array.from(this.#documents.values()));
    if (options !== undefined) {
      cursor = addOptions(cursor, options);
    }
    return cursor.all() as WithId<TSchema>[];
  }

  /*
   |--------------------------------------------------------------------------------
   | Update
   |--------------------------------------------------------------------------------
   */

  async updateOne(
    filter: Filter<WithId<TSchema>>,
    expr: UpdateExpression,
    arrayFilters?: Filter<WithId<TSchema>>[],
    condition?: Filter<WithId<TSchema>>,
    options?: UpdateOptions,
  ): Promise<UpdateResult> {
    const query = new Query(filter);
    for (const document of Array.from(this.#documents.values())) {
      if (query.test(document) === true) {
        const modified = update(document, expr, arrayFilters, condition, options);
        if (modified.length > 0) {
          this.#documents.set(document.id, document);
          this.broadcast("updateOne", document);
          return new UpdateResult(1, 1);
        }
        return new UpdateResult(1, 0);
      }
    }
    return new UpdateResult(0, 0);
  }

  async updateMany(
    filter: Filter<WithId<TSchema>>,
    expr: UpdateExpression,
    arrayFilters?: Filter<WithId<TSchema>>[],
    condition?: Filter<WithId<TSchema>>,
    options?: UpdateOptions,
  ): Promise<UpdateResult> {
    const logger = new UpdateLog(this.name, { filter, expr, arrayFilters, condition, options });
    const query = new Query(filter);

    const documents: WithId<TSchema>[] = [];

    let matchedCount = 0;
    let modifiedCount = 0;

    for (const document of Array.from(this.#documents.values())) {
      if (query.test(document) === true) {
        matchedCount += 1;
        const modified = update(document, expr, arrayFilters, condition, options);
        if (modified.length > 0) {
          modifiedCount += 1;
          documents.push(document);
          this.#documents.set(document.id, document);
        }
      }
    }

    this.broadcast("updateMany", documents);
    this.#cache.flush();

    this.log(logger.result());

    return new UpdateResult(matchedCount, modifiedCount);
  }

  async replace(filter: Filter<WithId<TSchema>>, document: WithId<TSchema>): Promise<UpdateResult> {
    const logger = new ReplaceLog(this.name, document);

    const query = new Query(filter);

    const documents: WithId<TSchema>[] = [];

    let matchedCount = 0;
    let modifiedCount = 0;

    for (const current of Array.from(this.#documents.values())) {
      if (query.test(current) === true) {
        matchedCount += 1;
        modifiedCount += 1;
        documents.push(document);
        this.#documents.set(document.id, document);
      }
    }

    this.broadcast("updateMany", documents);
    this.#cache.flush();

    this.log(logger.result({ count: matchedCount }));

    return new UpdateResult(matchedCount, modifiedCount);
  }

  /*
   |--------------------------------------------------------------------------------
   | Remove
   |--------------------------------------------------------------------------------
   */

  async remove(filter: Filter<WithId<TSchema>>): Promise<RemoveResult> {
    const logger = new RemoveLog(this.name, { filter });
    const documents = Array.from(this.#documents.values());
    const query = new Query(filter);
    let count = 0;
    for (const document of documents) {
      if (query.test(document) === true) {
        this.#documents.delete(document.id);
        this.broadcast("remove", document);
        count += 1;
      }
    }
    this.#cache.flush();
    this.log(logger.result({ count: documents.length }));
    return new RemoveResult(count);
  }

  /*
   |--------------------------------------------------------------------------------
   | Count
   |--------------------------------------------------------------------------------
   */

  async count(filter?: Filter<WithId<TSchema>>): Promise<number> {
    return new Query(filter ?? {}).find(Array.from(this.#documents.values())).count();
  }

  /*
   |--------------------------------------------------------------------------------
   | Flush
   |--------------------------------------------------------------------------------
   */

  async flush(): Promise<void> {
    await this.db.clear(this.name);
    this.#documents.clear();
  }

  /*
   |--------------------------------------------------------------------------------
   | Save
   |--------------------------------------------------------------------------------
   */

  async save(): Promise<void> {
    // this.db.
  }
}

/*
const logger = new InsertLog(this.name);

    const document = { ...data, id: data.id ?? crypto.randomUUID() } as any;
    if (await this.has(document.id)) {
      throw new DuplicateDocumentError(document, this as any);
    }
    await this.db.transaction(this.name, "readwrite", { durability: "relaxed" }).store.add(document);

    this.broadcast("insertOne", document);
    this.#cache.flush();

    this.log(logger.result());

    return getInsertOneResult(document);
*/
