import { Query, update } from "mingo";
import type { AnyObject } from "mingo/types";

import { getDocumentWithPrimaryKey } from "../../primary-key.ts";
import { Collections } from "../../storage/collections.ts";
import type { UpdatePayload } from "../../storage/mod.ts";
import type { InsertResult } from "../../storage/operators/insert.ts";
import type { UpdateResult } from "../../storage/operators/update.ts";
import {
  addOptions,
  type CountPayload,
  type FindByIdPayload,
  type FindPayload,
  type InsertManyPayload,
  type InsertOnePayload,
  type RemovePayload,
  type ReplacePayload,
  Storage,
} from "../../storage/storage.ts";
import type { AnyDocument } from "../../types.ts";

export class MemoryStorage extends Storage {
  readonly #collections = new Collections();

  async resolve() {
    return this;
  }

  async insertOne({ pkey, values, ...payload }: InsertOnePayload): Promise<InsertResult> {
    const collection = this.#collections.get(payload.collection);

    const document = getDocumentWithPrimaryKey(pkey, values);
    if (collection.has(document[pkey])) {
      return { insertCount: 0, insertIds: [] };
    }

    collection.set(document[pkey], document);
    this.broadcast("insertOne", document);

    return { insertCount: 1, insertIds: [document[pkey]] };
  }

  async insertMany({ pkey, values, ...payload }: InsertManyPayload): Promise<InsertResult> {
    const collection = this.#collections.get(payload.collection);

    const documents: AnyDocument[] = [];
    for (const insert of values) {
      const document = getDocumentWithPrimaryKey(pkey, insert);
      if (collection.has(document[pkey])) {
        continue;
      }
      collection.set(document[pkey], document);
      documents.push(document);
    }

    if (documents.length > 0) {
      this.broadcast("insertMany", documents);
    }

    return { insertCount: documents.length, insertIds: documents.map((document) => document[pkey]) };
  }

  async findById({ collection, id }: FindByIdPayload): Promise<AnyObject | undefined> {
    return this.#collections.get(collection).get(id);
  }

  async find({ condition = {}, options, ...payload }: FindPayload): Promise<AnyDocument[]> {
    let cursor = new Query(condition).find<AnyDocument>(this.#collections.documents(payload.collection));
    if (options !== undefined) {
      cursor = addOptions(cursor, options);
    }
    return cursor.all();
  }

  async updateOne({ pkey, condition, modifier, arrayFilters, ...payload }: UpdatePayload): Promise<UpdateResult> {
    const collection = this.#collections.get(payload.collection);

    let matchedCount = 0;
    let modifiedCount = 0;

    for (const document of await this.find({ collection: payload.collection, condition, options: { limit: 1 } })) {
      const modified = update(document, modifier, arrayFilters, undefined, { cloneMode: "deep" });
      if (modified.length > 0) {
        collection.set(document[pkey], document);
        this.broadcast("updateOne", document);
        modifiedCount += 1;
      }
      matchedCount += 1;
    }
    return { matchedCount, modifiedCount };
  }

  async updateMany({ pkey, condition, modifier, arrayFilters, ...payload }: UpdatePayload): Promise<UpdateResult> {
    const collection = this.#collections.get(payload.collection);

    const documents: AnyDocument[] = [];

    let matchedCount = 0;
    let modifiedCount = 0;

    for (const document of await this.find({ collection: payload.collection, condition })) {
      matchedCount += 1;
      const modified = update(document, modifier, arrayFilters, undefined, { cloneMode: "deep" });
      if (modified.length > 0) {
        modifiedCount += 1;
        documents.push(document);
        collection.set(document[pkey], document);
      }
    }

    this.broadcast("updateMany", documents);

    return { matchedCount, modifiedCount };
  }

  async replace({ pkey, condition, document, ...payload }: ReplacePayload): Promise<UpdateResult> {
    const collection = this.#collections.get(payload.collection);

    let matchedCount = 0;
    let modifiedCount = 0;

    const documents: AnyDocument[] = [];
    for (const current of await this.find({ collection: payload.collection, condition })) {
      matchedCount += 1;
      modifiedCount += 1;
      documents.push(document);
      collection.set(current[pkey], document);
    }

    this.broadcast("updateMany", documents);

    return { matchedCount, modifiedCount };
  }

  async remove({ pkey, condition, ...payload }: RemovePayload): Promise<number> {
    const collection = this.#collections.get(payload.collection);

    const documents = await this.find({ collection: payload.collection, condition });
    for (const document of documents) {
      collection.delete(document[pkey]);
    }

    this.broadcast("remove", documents);

    return documents.length;
  }

  async count({ collection, condition = {} }: CountPayload): Promise<number> {
    return new Query(condition).find(this.#collections.documents(collection)).all().length;
  }

  async flush(): Promise<void> {
    this.#collections.flush();
  }
}
