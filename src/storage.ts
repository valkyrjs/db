import { EventEmitter } from "@valkyr/event-emitter";
import type { Cursor } from "mingo/cursor";
import type { Criteria } from "mingo/types";
import type { Modifier } from "mingo/updater";

import { BroadcastChannel, type StorageBroadcast } from "./broadcast.ts";
import type { IndexSpec } from "./index/manager.ts";
import type { AnyDocument } from "./types.ts";

type StorageEvent = "change" | "flush";

export abstract class Storage<TSchema extends AnyDocument = AnyDocument> {
  readonly event: EventEmitter<StorageEvent> = new EventEmitter<StorageEvent>();

  status: Status = "loading";

  readonly #channel: BroadcastChannel;

  constructor(
    /**
     * Name of the collection the storage is holding documents for.
     */
    readonly name: string,

    /**
     * List of indexes to optimize storage lookups.
     */
    readonly indexes: IndexSpec<TSchema>[],
  ) {
    if (primaryIndexCount(indexes) !== 1) {
      throw new Error("missing required primary key assignment");
    }
    this.#channel = new BroadcastChannel(`@valkyr/db:${name}`);
    this.#channel.onmessage = ({ data }: MessageEvent<StorageBroadcast>) => {
      if (data.name !== this.name) {
        return;
      }
      switch (data.type) {
        case "flush": {
          this.event.emit("flush");
          break;
        }
        default: {
          this.event.emit("change", data);
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

  broadcast(type: "flush"): void;
  broadcast(type: "insert" | "update" | "remove", data: TSchema[]): void;
  broadcast(type: StorageBroadcast["type"], data?: TSchema[]): void {
    switch (type) {
      case "flush": {
        this.event.emit("flush");
        break;
      }
      default: {
        this.event.emit("change", { type, data });
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

  /**
   * Add list of documents to the storage engine.
   *
   * @param documents - Documents to add.
   */
  abstract insert(documents: TSchema[]): Promise<void>;

  /**
   * Retrieve a list of documents by a index value.
   *
   * @param index - Index path to lookup.
   * @param value - Value to match against the path.
   */
  abstract getByIndex(index: string, value: string): Promise<TSchema[]>;

  /**
   * Retrieve a list of documents from the storage engine.
   *
   * @param condition - Mingo criteria to filter documents against.
   * @param options   - Additional query options.
   */
  abstract find(condition?: Criteria<TSchema>, options?: QueryOptions): Promise<TSchema[]>;

  /**
   * Update documents matching the given condition.
   *
   * @param condition    - Mingo criteria to filter documents to update.
   * @param modifier     - Modifications to apply to the filtered documents.
   * @param arrayFilters - Custom filter.
   */
  abstract update(
    condition: Criteria<TSchema>,
    modifier: Modifier<TSchema>,
    arrayFilters?: TSchema[],
  ): Promise<UpdateResult>;

  /**
   * Remove documents matching the given condition.
   *
   * @param condition - Mingo criteria to filter documents to remove.
   */
  abstract remove(condition: Criteria<TSchema>): Promise<number>;

  /**
   * Get document count matching given condition.
   *
   * @param condition - Mingo criteria to count document against.
   */
  abstract count(condition: Criteria<TSchema>): Promise<number>;

  /**
   * Remove all documents in the storage.
   */
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

export function addOptions<TSchema extends AnyDocument = AnyDocument>(
  cursor: Cursor<TSchema>,
  options: QueryOptions,
): Cursor<TSchema> {
  if (options.sort !== undefined) {
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

function primaryIndexCount<TSchema extends AnyDocument = AnyDocument>(indexes: IndexSpec<TSchema>[]): number {
  let count = 0;
  for (const { kind } of indexes) {
    if (kind === "primary") {
      count += 1;
    }
  }
  return count;
}

/*
 |--------------------------------------------------------------------------------
 | Types
 |--------------------------------------------------------------------------------
 */

type Status = "loading" | "ready";

export type ChangeEvent<TSchema extends AnyDocument = AnyDocument> = {
  type: "insert" | "update" | "remove";
  data: TSchema[];
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
};

export type UpdateResult = {
  matchedCount: number;
  modifiedCount: number;
};
