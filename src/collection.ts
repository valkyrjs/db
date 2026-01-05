import type { Subscription } from "@valkyr/event-emitter";
import type { AnyObject, Criteria } from "mingo/types";
import type { Modifier } from "mingo/updater";
import type { ZodObject, ZodRawShape } from "zod";
import z from "zod";

import type { IndexSpec } from "./index/manager.ts";
import { observe, observeOne } from "./observe/mod.ts";
import type { ChangeEvent, QueryOptions, Storage, UpdateResult } from "./storage.ts";
import type { AnyDocument, QueryCriteria } from "./types.ts";

/*
 |--------------------------------------------------------------------------------
 | Collection
 |--------------------------------------------------------------------------------
 */

export class Collection<
  TOptions extends AnyCollectionOptions = AnyCollectionOptions,
  TStorage extends Storage = TOptions["storage"],
  TSchema extends AnyDocument = z.output<ZodObject<TOptions["schema"]>>,
> {
  declare readonly $schema: TSchema;

  readonly #schema: ZodObject<TOptions["schema"]>;
  readonly #pkey: string | number;

  constructor(readonly options: TOptions) {
    this.#schema = z.strictObject(options.schema);
    this.#pkey = this.primaryKey;
  }

  get name(): string {
    return this.options.name;
  }

  get storage(): TStorage {
    return this.options.storage;
  }

  get schema(): TOptions["schema"] {
    return this.options.schema;
  }

  get primaryKey(): string {
    for (const index of this.options.indexes ?? []) {
      if (index.kind === "primary") {
        return index.field;
      }
    }
    throw new Error(`Collection '${this.name}' is missing required primary key assignment.`);
  }

  /*
 |--------------------------------------------------------------------------------
 | Utilities
 |--------------------------------------------------------------------------------
 */

  getPrimaryKeyValue(document: AnyDocument): string {
    const id = document[this.#pkey];
    if (id === undefined || typeof id !== "string") {
      throw new Error(
        `Primary Key: Missing primary key '${this.#pkey}' on given document: ${JSON.stringify(document, null, 2)}`,
      );
    }
    return id;
  }

  /*
   |--------------------------------------------------------------------------------
   | Mutators
   |--------------------------------------------------------------------------------
   */

  async insert(documents: TSchema[]): Promise<void> {
    return this.storage
      .resolve()
      .then((storage) => storage.insert(documents.map((document) => this.#schema.parse(document))));
  }

  async update(
    condition: Criteria<TSchema>,
    modifier: Modifier<TSchema>,
    arrayFilters?: AnyObject[],
  ): Promise<UpdateResult> {
    return this.storage.resolve().then((storage) => storage.update(condition, modifier, arrayFilters));
  }

  async remove(condition: Criteria<TSchema>): Promise<number> {
    return this.storage.resolve().then((storage) => storage.remove(condition));
  }

  /*
   |--------------------------------------------------------------------------------
   | Observers
   |--------------------------------------------------------------------------------
   */

  subscribe(
    condition?: Criteria<TSchema>,
    options?: SubscribeToSingle,
    next?: (document: TSchema | undefined) => void,
  ): Subscription;
  subscribe(
    condition?: Criteria<TSchema>,
    options?: SubscribeToMany,
    next?: (documents: TSchema[], changed: TSchema[], type: ChangeEvent["type"]) => void,
  ): Subscription;
  subscribe(condition: Criteria<TSchema> = {}, options?: QueryOptions, next?: (...args: any[]) => void): Subscription {
    if (options?.limit === 1) {
      return observeOne(this as any, condition, (values) => next?.(values as any));
    }
    return observe(this as any, condition, options, (values, changed, type) => next?.(values, changed, type));
  }

  /*
   |--------------------------------------------------------------------------------
   | Queries
   |--------------------------------------------------------------------------------
   */

  /**
   * Performs a mingo filter search over the collection data and returns
   * a single document if one was found matching the filter and options.
   */
  async findOne(condition: QueryCriteria<TSchema> = {}, options: QueryOptions = {}): Promise<TSchema | undefined> {
    return this.findMany(condition, { ...options, limit: 1 }).then(([document]) => document);
  }

  /**
   * Performs a mingo filter search over the collection data and returns any
   * documents matching the provided filter and options.
   */
  async findMany(condition: QueryCriteria<TSchema> = {}, options?: QueryOptions): Promise<TSchema[]> {
    return this.storage
      .resolve()
      .then((storage) =>
        storage
          .find(condition, options)
          .then((documents) => documents.map((document) => this.#schema.parse(document) as TSchema)),
      );
  }

  /**
   * Performs a mingo filter search over the collection data and returns
   * the count of all documents found matching the filter and options.
   */
  async count(condition: Criteria<TSchema> = {}): Promise<number> {
    return this.storage.resolve().then((storage) => storage.count(condition));
  }

  /**
   * Removes all documents from the storage instance.
   */
  async flush(): Promise<void> {
    await this.storage.resolve().then(async (storage) => {
      await storage.flush();
      storage.broadcast("flush");
    });
  }

  /*
   |--------------------------------------------------------------------------------
   | Event Handlers
   |--------------------------------------------------------------------------------
   */

  onFlush(cb: () => void) {
    return this.storage.event.subscribe("flush", cb);
  }

  onChange(cb: (event: ChangeEvent<TSchema>) => void) {
    return this.storage.event.subscribe("change", cb);
  }
}

/*
 |--------------------------------------------------------------------------------
 | Types
 |--------------------------------------------------------------------------------
 */

export type SubscriptionOptions = {
  sort?: QueryOptions["sort"];
  skip?: QueryOptions["skip"];
  range?: QueryOptions["range"];
  offset?: QueryOptions["offset"];
  limit?: QueryOptions["limit"];
};

export type SubscribeToSingle = QueryOptions & {
  limit: 1;
};

export type SubscribeToMany = QueryOptions & {
  limit?: number;
};

type AnyCollectionOptions = CollectionOptions<any, any, any>;

type CollectionOptions<TName extends string, TStorage extends Storage, TSchema extends ZodRawShape> = {
  /**
   * Name of the collection.
   */
  name: TName;

  /**
   * Storage adapter used to persist the collection documents.
   */
  storage: TStorage;

  /**
   * Schema definition of the document stored for the collection.
   */
  schema: TSchema;

  /**
   * List of custom indexes for the collection.
   */
  indexes: IndexSpec<TSchema>[];
};
