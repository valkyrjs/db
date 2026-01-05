import { Query, update } from "mingo";
import type { Criteria } from "mingo/types";
import type { Modifier } from "mingo/updater";

import { IndexManager, type IndexSpec } from "../../index/manager.ts";
import type { UpdateResult } from "../../storage.ts";
import { addOptions, type QueryOptions, Storage } from "../../storage.ts";
import type { AnyDocument } from "../../types.ts";

export class MemoryStorage<TSchema extends AnyDocument = AnyDocument> extends Storage<TSchema> {
  readonly index: IndexManager<TSchema>;

  constructor(name: string, indexes: IndexSpec[]) {
    super(name, indexes);
    this.index = new IndexManager(indexes);
  }

  get documents() {
    return this.index.primary.tree;
  }

  async resolve() {
    return this;
  }

  async insert(documents: TSchema[]): Promise<void> {
    for (const document of documents) {
      this.index.insert(document);
    }
    this.broadcast("insert", documents);
  }

  async getByIndex(index: string, value: string): Promise<TSchema[]> {
    return this.index.get(index)?.get(value) ?? [];
  }

  async find(condition: Criteria<TSchema> = {}, options?: QueryOptions): Promise<TSchema[]> {
    let cursor = new Query(condition).find<TSchema>(this.documents);
    if (options !== undefined) {
      cursor = addOptions(cursor, options);
    }
    return cursor.all();
  }

  async update(
    condition: Criteria<TSchema>,
    modifier: Modifier<TSchema>,
    arrayFilters?: TSchema[],
  ): Promise<UpdateResult> {
    const documents: TSchema[] = [];

    let matchedCount = 0;
    let modifiedCount = 0;

    for (const document of await this.find(condition)) {
      matchedCount += 1;
      const modified = update(document, modifier, arrayFilters, undefined, { cloneMode: "deep" });
      if (modified.length > 0) {
        modifiedCount += 1;
        documents.push(document);
        this.documents.add(document);
      }
    }

    if (modifiedCount > 0) {
      this.broadcast("update", documents);
    }

    return { matchedCount, modifiedCount };
  }

  async remove(condition: Criteria<TSchema>): Promise<number> {
    const documents = await this.find(condition);
    for (const document of documents) {
      this.documents.delete(document);
    }
    this.broadcast("remove", documents);
    return documents.length;
  }

  async count(condition: Criteria<TSchema>): Promise<number> {
    return new Query(condition).find(this.documents).all().length;
  }

  async flush(): Promise<void> {
    this.documents.clear();
  }
}
