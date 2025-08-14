import { createUpdater, Query } from "mingo";
import { UpdateOptions } from "mingo/core";
import { UpdateExpression } from "mingo/updater";

import { DuplicateDocumentError } from "../../storage/errors.ts";
import {
  getInsertManyResult,
  getInsertOneResult,
  InsertManyResult,
  InsertOneResult,
} from "../../storage/operators/insert.ts";
import { RemoveResult } from "../../storage/operators/remove.ts";
import { UpdateResult } from "../../storage/operators/update.ts";
import { addOptions, Options, Storage } from "../../storage/storage.ts";
import { Document, Filter, WithId } from "../../types.ts";

const update = createUpdater({ cloneMode: "deep" });

export class ObserverStorage<TSchema extends Document = Document> extends Storage<TSchema> {
  readonly #documents = new Map<string, WithId<TSchema>>();

  async resolve() {
    return this;
  }

  async has(id: string): Promise<boolean> {
    return this.#documents.has(id);
  }

  async insertOne(data: Partial<TSchema>): Promise<InsertOneResult> {
    const document = { ...data, id: data.id ?? crypto.randomUUID() } as WithId<TSchema>;
    if (await this.has(document.id)) {
      throw new DuplicateDocumentError(document, this as any);
    }
    this.#documents.set(document.id, document);
    return getInsertOneResult(document);
  }

  async insertMany(documents: Partial<TSchema>[]): Promise<InsertManyResult> {
    const result: TSchema[] = [];
    for (const data of documents) {
      const document = { ...data, id: data.id ?? crypto.randomUUID() } as WithId<TSchema>;
      result.push(document);
      this.#documents.set(document.id, document);
    }
    return getInsertManyResult(result);
  }

  async findById(id: string): Promise<WithId<TSchema> | undefined> {
    return this.#documents.get(id);
  }

  async find(filter?: Filter<WithId<TSchema>>, options?: Options): Promise<WithId<TSchema>[]> {
    let cursor = new Query(filter ?? {}).find<TSchema>(Array.from(this.#documents.values()));
    if (options !== undefined) {
      cursor = addOptions(cursor, options);
    }
    return cursor.all() as WithId<TSchema>[];
  }

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
    const query = new Query(filter);

    const documents: WithId<TSchema>[] = [];

    let matchedCount = 0;
    let modifiedCount = 0;

    for (const document of Array.from(this.#documents.values())) {
      if (query.test(document) === true) {
        matchedCount += 1;
        const modified = update(filter, expr, arrayFilters, condition, options);
        if (modified.length > 0) {
          modifiedCount += 1;
          documents.push(document);
          this.#documents.set(document.id, document);
        }
      }
    }

    this.broadcast("updateMany", documents);

    return new UpdateResult(matchedCount, modifiedCount);
  }

  async replace(filter: Filter<WithId<TSchema>>, document: WithId<TSchema>): Promise<UpdateResult> {
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

    return new UpdateResult(matchedCount, modifiedCount);
  }

  async remove(filter: Filter<WithId<TSchema>>): Promise<RemoveResult> {
    const documents = Array.from(this.#documents.values());
    const query = new Query(filter);
    let count = 0;
    for (const document of documents) {
      if (query.test(document) === true) {
        this.#documents.delete(document.id);
        count += 1;
      }
    }
    return new RemoveResult(count);
  }

  async count(filter?: Filter<WithId<TSchema>>): Promise<number> {
    return new Query(filter ?? {}).find(Array.from(this.#documents.values())).count();
  }

  async flush(): Promise<void> {
    this.#documents.clear();
  }
}
