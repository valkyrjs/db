import type { Cursor } from "mingo/cursor";
import type { AnyObject, Criteria } from "mingo/types";
import type { Modifier } from "mingo/updater";
import { Subject } from "rxjs";

import { BroadcastChannel, type StorageBroadcast } from "../broadcast.ts";
import type { Prettify } from "../types.ts";
import type { InsertResult } from "./operators/insert.ts";
import type { UpdateResult } from "./operators/update.ts";

export abstract class Storage {
  readonly observable: {
    change: Subject<ChangeEvent>;
    flush: Subject<void>;
  } = {
    change: new Subject<ChangeEvent>(),
    flush: new Subject<void>(),
  };

  status: Status = "loading";

  readonly #channel: BroadcastChannel;

  constructor(readonly name: string) {
    this.#channel = new BroadcastChannel(`valkyr:db:${name}`);
    this.#channel.onmessage = ({ data }: MessageEvent<StorageBroadcast>) => {
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

  broadcast(type: StorageBroadcast["type"], data?: AnyObject | AnyObject[]): void {
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

  abstract insertOne(payload: InsertOnePayload): Promise<InsertResult>;

  abstract insertMany(payload: InsertManyPayload): Promise<InsertResult>;

  abstract findById(payload: FindByIdPayload): Promise<AnyObject | undefined>;

  abstract find(payload: FindPayload): Promise<AnyObject[]>;

  abstract updateOne(payload: UpdatePayload): Promise<UpdateResult>;

  abstract updateMany(payload: UpdatePayload): Promise<UpdateResult>;

  abstract replace(payload: ReplacePayload): Promise<UpdateResult>;

  abstract remove(payload: RemovePayload): Promise<number>;

  abstract count(payload: CountPayload): Promise<number>;

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

export function addOptions<TSchema extends AnyObject = AnyObject>(
  cursor: Cursor<TSchema>,
  options: QueryOptions,
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

export type ChangeEvent =
  | {
      type: "insertOne" | "updateOne";
      data: AnyObject;
    }
  | {
      type: "insertMany" | "updateMany" | "remove";
      data: AnyObject[];
    };

export type QueryOptions = {
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

export type InsertOnePayload = Prettify<
  CollectionPayload &
    PrimaryKeyPayload & {
      values: AnyObject;
    }
>;

export type InsertManyPayload = Prettify<
  CollectionPayload &
    PrimaryKeyPayload & {
      values: AnyObject[];
    }
>;

export type FindByIdPayload = Prettify<
  CollectionPayload & {
    id: string;
  }
>;

export type FindPayload = Prettify<
  CollectionPayload & {
    condition?: Criteria<AnyObject>;
    options?: QueryOptions;
  }
>;

export type UpdatePayload = Prettify<
  CollectionPayload &
    PrimaryKeyPayload & {
      condition: Criteria<AnyObject>;
      modifier: Modifier<AnyObject>;
      arrayFilters?: AnyObject[];
    }
>;

export type ReplacePayload = Prettify<
  CollectionPayload &
    PrimaryKeyPayload & {
      condition: Criteria<AnyObject>;
      document: AnyObject;
    }
>;

export type RemovePayload = Prettify<
  CollectionPayload &
    PrimaryKeyPayload & {
      condition: Criteria<AnyObject>;
    }
>;

export type CountPayload = Prettify<
  CollectionPayload & {
    condition?: Criteria<AnyObject>;
  }
>;

type CollectionPayload = {
  collection: string;
};

type PrimaryKeyPayload = {
  pkey: string;
};
