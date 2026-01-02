import type { AnyObject, Criteria } from "mingo/types";
import type { Modifier } from "mingo/updater";
import { Observable, type Subject, type Subscription } from "rxjs";
import type z from "zod";
import type { ZodObject, ZodRawShape } from "zod";

import { observe, observeOne } from "./observe/mod.ts";
import type { ChangeEvent, InsertResult, QueryOptions, Storage, UpdateResult } from "./storage/mod.ts";
import type { AnyDocument } from "./types.ts";

/*
 |--------------------------------------------------------------------------------
 | Collection
 |--------------------------------------------------------------------------------
 */

export class Collection<
  TOptions extends AnyCollectionOptions = AnyCollectionOptions,
  TAdapter extends Storage = TOptions["adapter"],
  TPrimaryKey extends string = TOptions["primaryKey"],
  TSchema extends AnyDocument = z.output<ZodObject<TOptions["schema"]>>,
> {
  declare readonly $schema: TSchema;

  constructor(readonly options: TOptions) {}

  get observable(): {
    change: Subject<ChangeEvent>;
    flush: Subject<void>;
  } {
    return this.storage.observable;
  }

  get storage(): TAdapter {
    return this.options.adapter;
  }

  /*
   |--------------------------------------------------------------------------------
   | Mutators
   |--------------------------------------------------------------------------------
   */

  async insertOne(values: TSchema | Omit<TSchema, TPrimaryKey>): Promise<InsertResult> {
    return this.storage.resolve().then((storage) =>
      storage.insertOne({
        collection: this.options.name,
        pkey: this.options.primaryKey,
        values,
      }),
    );
  }

  async insertMany(values: (TSchema | Omit<TSchema, TPrimaryKey>)[]): Promise<InsertResult> {
    return this.storage.resolve().then((storage) =>
      storage.insertMany({
        collection: this.options.name,
        pkey: this.options.primaryKey,
        values,
      }),
    );
  }

  async updateOne(
    condition: Criteria<TSchema>,
    modifier: Modifier<TSchema>,
    arrayFilters?: AnyObject[],
  ): Promise<UpdateResult> {
    return this.storage.resolve().then((storage) =>
      storage.updateOne({
        collection: this.options.name,
        pkey: this.options.primaryKey,
        condition,
        modifier,
        arrayFilters,
      }),
    );
  }

  async updateMany(
    condition: Criteria<TSchema>,
    modifier: Modifier<TSchema>,
    arrayFilters?: AnyObject[],
  ): Promise<UpdateResult> {
    return this.storage.resolve().then((storage) =>
      storage.updateMany({
        collection: this.options.name,
        pkey: this.options.primaryKey,
        condition,
        modifier,
        arrayFilters,
      }),
    );
  }

  async replaceOne(condition: Criteria<TSchema>, document: TSchema): Promise<UpdateResult> {
    return this.storage.resolve().then((storage) =>
      storage.replace({
        collection: this.options.name,
        pkey: this.options.primaryKey,
        condition,
        document,
      }),
    );
  }

  async remove(condition: Criteria<TSchema>): Promise<number> {
    return this.storage
      .resolve()
      .then((storage) => storage.remove({ collection: this.options.name, pkey: this.options.primaryKey, condition }));
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
      return this.#observeOne(condition).subscribe({ next });
    }
    return this.#observe(condition, options).subscribe({
      next: (value: [TSchema[], TSchema[], ChangeEvent["type"]]) => next?.(...value),
    });
  }

  #observe(
    filter: Criteria<TSchema> = {},
    options?: QueryOptions,
  ): Observable<[TSchema[], TSchema[], ChangeEvent["type"]]> {
    return new Observable<[TSchema[], TSchema[], ChangeEvent["type"]]>((subscriber) => {
      return observe(this as any, filter, options, (values, changed, type) =>
        subscriber.next([values, changed, type] as any),
      );
    });
  }

  #observeOne(filter: Criteria<TSchema> = {}): Observable<TSchema | undefined> {
    return new Observable<TSchema | undefined>((subscriber) => {
      return observeOne(this as any, filter, (values) => subscriber.next(values as any));
    });
  }

  /*
   |--------------------------------------------------------------------------------
   | Queries
   |--------------------------------------------------------------------------------
   */

  /**
   * Retrieve a record by the document 'id' key.
   */
  async findById(id: string): Promise<TSchema | undefined> {
    return this.storage.resolve().then((storage) => storage.findById({ collection: this.options.name, id }));
  }

  /**
   * Performs a mingo filter search over the collection data and returns
   * a single document if one was found matching the filter and options.
   */
  async findOne(condition: Criteria<TSchema> = {}, options?: QueryOptions): Promise<TSchema | undefined> {
    return this.find(condition, options).then(([document]) => document);
  }

  /**
   * Performs a mingo filter search over the collection data and returns any
   * documents matching the provided filter and options.
   */
  async find(condition: Criteria<TSchema> = {}, options?: QueryOptions): Promise<TSchema[]> {
    return this.storage
      .resolve()
      .then((storage) => storage.find({ collection: this.options.name, condition, options }));
  }

  /**
   * Performs a mingo filter search over the collection data and returns
   * the count of all documents found matching the filter and options.
   */
  async count(condition?: Criteria<TSchema>): Promise<number> {
    return this.storage.resolve().then((storage) => storage.count({ collection: this.options.name, condition }));
  }

  /**
   * Removes all documents from the storage instance.
   */
  flush(): void {
    this.storage.resolve().then((storage) => {
      storage.broadcast("flush");
      storage.flush();
    });
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
  index?: QueryOptions["index"];
};

export type SubscribeToSingle = QueryOptions & {
  limit: 1;
};

export type SubscribeToMany = QueryOptions & {
  limit?: number;
};

type AnyCollectionOptions = CollectionOptions<any, any, any, any>;

type CollectionOptions<
  TName extends string,
  TAdapter extends Storage,
  TPrimaryKey extends string | number | symbol,
  TSchema extends ZodRawShape,
> = {
  name: TName;
  adapter: TAdapter;
  primaryKey: TPrimaryKey;
  schema: TSchema;
};
