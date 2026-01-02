import type { AnyObject } from "mingo/types";

import { CollectionNotFoundError } from "./errors.ts";

export class Collections {
  #collections = new Map<string, Documents>();

  has(name: string): boolean {
    return this.#collections.has(name);
  }

  documents(name: string): AnyObject[] {
    return Array.from(this.get(name).values());
  }

  get(name: string): Documents {
    const collection = this.#collections.get(name);
    if (collection === undefined) {
      throw new CollectionNotFoundError(name);
    }
    return collection;
  }

  delete(name: string): boolean {
    return this.#collections.delete(name);
  }

  flush() {
    this.#collections.clear();
  }
}

type Documents = Map<string, AnyObject>;
