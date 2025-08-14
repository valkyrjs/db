import { UpdateOptions } from "mingo/core";
import { Cursor } from "mingo/cursor";
import { UpdateExpression } from "mingo/updater";
import { Subject } from "rxjs";

import { BroadcastChannel, StorageBroadcast } from "../broadcast.ts";
import { Document, Filter, WithId } from "../types.ts";
import { InsertManyResult, InsertOneResult } from "./operators/insert.ts";
import { RemoveResult } from "./operators/remove.ts";
import { UpdateResult } from "./operators/update.ts";

export abstract class Storage<TSchema extends Document = Document> {
  readonly observable: {
    change: Subject<ChangeEvent<TSchema>>;
    flush: Subject<void>;
  } = {
    change: new Subject<ChangeEvent<TSchema>>(),
    flush: new Subject<void>(),
  };

  status: Status = "loading";

  readonly #channel: BroadcastChannel;

  constructor(
    readonly name: string,
    readonly id: string = crypto.randomUUID(),
  ) {
    this.#channel = new BroadcastChannel(`valkyr:db:${name}`);
    this.#channel.onmessage = ({ data }: MessageEvent<StorageBroadcast<TSchema>>) => {
      if (data.name !== this.name) {
        return;
      }
      switch (data.type) {
        case "flush": {
          this.observable.flush.next();
          break;
        }
        default: {
          this.observable.change.next(data);
          break;
        }
      }
    };
  }

  /*
   |--------------------------------------------------------------------------------
   | Resolver
   |--------------------------------------------------------------------------------
   */

  abstract resolve(): Promise<this>;

  /*
   |--------------------------------------------------------------------------------
   | Status
   |--------------------------------------------------------------------------------
   */

  is(status: Status): boolean {
    return this.status === status;
  }

  /*
   |--------------------------------------------------------------------------------
   | Broadcaster
   |--------------------------------------------------------------------------------
   |
   | Broadcast local changes with any change listeners in the current and other
   | browser tabs and window.
   |
   */

  broadcast(type: StorageBroadcast<TSchema>["type"], data?: TSchema | TSchema[]): void {
    switch (type) {
      case "flush": {
        this.observable.flush.next();
        break;
      }
      default: {
        this.observable.change.next({ type, data: data as any });
        break;
      }
    }
    this.#channel.postMessage({ name: this.name, type, data });
  }

  /*
   |--------------------------------------------------------------------------------
   | Operations
   |--------------------------------------------------------------------------------
   */

  abstract has(id: string): Promise<boolean>;

  abstract insertOne(document: Partial<WithId<TSchema>>): Promise<InsertOneResult>;

  abstract insertMany(documents: Partial<WithId<TSchema>>[]): Promise<InsertManyResult>;

  abstract findById(id: string): Promise<WithId<TSchema> | undefined>;

  abstract find(filter?: Filter<WithId<TSchema>>, options?: Options): Promise<WithId<TSchema>[]>;

  abstract updateOne(
    filter: Filter<WithId<TSchema>>,
    expr: UpdateExpression,
    arrayFilters?: Filter<WithId<TSchema>>[],
    condition?: Filter<WithId<TSchema>>,
    options?: UpdateOptions,
  ): Promise<UpdateResult>;

  abstract updateMany(
    filter: Filter<WithId<TSchema>>,
    expr: UpdateExpression,
    arrayFilters?: Filter<WithId<TSchema>>[],
    condition?: Filter<WithId<TSchema>>,
    options?: UpdateOptions,
  ): Promise<UpdateResult>;

  abstract replace(filter: Filter<WithId<TSchema>>, document: TSchema): Promise<UpdateResult>;

  abstract remove(filter: Filter<WithId<TSchema>>): Promise<RemoveResult>;

  abstract count(filter?: Filter<WithId<TSchema>>): Promise<number>;

  abstract flush(): Promise<void>;

  /*
   |--------------------------------------------------------------------------------
   | Destructor
   |--------------------------------------------------------------------------------
   */

  destroy() {
    this.#channel.close();
  }
}

/*
 |--------------------------------------------------------------------------------
 | Utilities
 |--------------------------------------------------------------------------------
 */

export function addOptions<TSchema extends Document = Document>(
  cursor: Cursor<TSchema>,
  options: Options,
): Cursor<TSchema> {
  if (options.sort) {
    cursor.sort(options.sort);
  }
  if (options.skip !== undefined) {
    cursor.skip(options.skip);
  }
  if (options.limit !== undefined) {
    cursor.limit(options.limit);
  }
  return cursor;
}

/*
 |--------------------------------------------------------------------------------
 | Types
 |--------------------------------------------------------------------------------
 */

type Status = "loading" | "ready";

export type ChangeEvent<TSchema extends Document = Document> =
  | {
      type: "insertOne" | "updateOne";
      data: WithId<TSchema>;
    }
  | {
      type: "insertMany" | "updateMany" | "remove";
      data: WithId<TSchema>[];
    };

export type Options = {
  sort?: {
    [key: string]: 1 | -1;
  };
  skip?: number;
  range?: {
    from: string;
    to: string;
  };
  offset?: {
    value: string;
    direction: 1 | -1;
  };
  limit?: number;
  index?: Index;
};

export type Index = {
  [index: string]: any;
};
