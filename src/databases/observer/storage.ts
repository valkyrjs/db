import { Query, update } from "mingo";
import type { Criteria, Options } from "mingo/types";
import type { CloneMode, Modifier } from "mingo/updater";

import { getDocumentWithPrimaryKey } from "../../primary-key.ts";
import { DuplicateDocumentError } from "../../storage/errors.ts";
import type { InsertResult } from "../../storage/operators/insert.ts";
import { UpdateResult } from "../../storage/operators/update.ts";
import { addOptions, type QueryOptions, Storage } from "../../storage/storage.ts";
import type { AnyDocument } from "../../types.ts";

export class ObserverStorage extends Storage {
  readonly #documents = new Map<string, AnyDocument>();

  async resolve() {
    return this;
  }

  async has(id: string): Promise<boolean> {
    return this.#documents.has(id);
  }

  async insertOne(values: AnyDocument): Promise<InsertResult> {
    const document = getDocumentWithPrimaryKey(this.primaryKey, values);
    if (await this.has(document[this.primaryKey])) {
      throw new DuplicateDocumentError(document, this as any);
    }
    this.#documents.set(document[this.primaryKey], document);
    return getInsertOneResult(document);
  }

  async insertMany(list: TSchema[]): Promise<InsertResult> {
    const result: TSchema[] = [];
    for (const values of list) {
      const document = getDocumentWithPrimaryKey(this.primaryKey, values);
      result.push(document);
      this.#documents.set(document.id, document);
    }
    return getInsertManyResult(result);
  }

  async findById(id: string): Promise<TSchema | undefined> {
    return this.#documents.get(id);
  }

  async find(filter?: Filter<TSchema>, options?: QueryOptions): Promise<TSchema[]> {
    let cursor = new Query(filter ?? {}).find<TSchema>(Array.from(this.#documents.values()));
    if (options !== undefined) {
      cursor = addOptions(cursor, options);
    }
    return cursor.all();
  }

  async updateOne(
    filter: Filter<TSchema>,
    modifier: Modifier<TSchema>,
    arrayFilters?: Filter<TSchema>[],
    condition?: Criteria<TSchema>,
    options: { cloneMode?: CloneMode; queryOptions?: Partial<Options> } = { cloneMode: "deep" },
  ): Promise<UpdateResult> {
    const query = new Query(filter);
    for (const document of Array.from(this.#documents.values())) {
      if (query.test(document) === true) {
        const modified = update(document, modifier, arrayFilters, condition, options);
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
    filter: Filter<TSchema>,
    modifier: Modifier<TSchema>,
    arrayFilters?: Filter<TSchema>[],
    condition?: Criteria<TSchema>,
    options: { cloneMode?: CloneMode; queryOptions?: Partial<Options> } = { cloneMode: "deep" },
  ): Promise<UpdateResult> {
    const query = new Query(filter);

    const documents: TSchema[] = [];

    let matchedCount = 0;
    let modifiedCount = 0;

    for (const document of Array.from(this.#documents.values())) {
      if (query.test(document) === true) {
        matchedCount += 1;
        const modified = update(document, modifier, arrayFilters, condition, options);
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

  async replace(filter: Filter<TSchema>, document: TSchema): Promise<UpdateResult> {
    const query = new Query(filter);

    const documents: TSchema[] = [];

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

  async remove(filter: Filter<TSchema>): Promise<RemoveResult> {
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

  async count(filter?: Filter<TSchema>): Promise<number> {
    return new Query(filter ?? {}).find(Array.from(this.#documents.values())).all().length;
  }

  async flush(): Promise<void> {
    this.#documents.clear();
  }
}
