import { ObserverStorage } from "../databases/observer/storage.ts";
import type { Storage } from "../storage/mod.ts";
import type { AnyDocument } from "../types.ts";
import { isMatch } from "./is-match.ts";

export class Store {
  private constructor(private storage: Storage) {}

  static create() {
    return new Store(new ObserverStorage(`observer[${crypto.randomUUID()}]`));
  }

  get destroy() {
    return this.storage.destroy.bind(this.storage);
  }

  async resolve(documents: AnyDocument[]): Promise<AnyDocument[]> {
    await this.storage.insertMany(documents);
    return this.getDocuments();
  }

  async getDocuments(): Promise<AnyDocument[]> {
    return this.storage.find();
  }

  async insertMany(documents: AnyDocument[], filter: Filter<AnyDocument>): Promise<AnyDocument[]> {
    const matched = [];
    for (const document of documents) {
      matched.push(...(await this.insertOne(document, filter)));
    }
    return matched;
  }

  async insertOne(document: AnyDocument, filter: Filter<AnyDocument>): Promise<AnyDocument[]> {
    if (isMatch<AnyDocument>(document, filter)) {
      await this.storage.insertOne(document);
      return [document];
    }
    return [];
  }

  async updateMany(documents: AnyDocument[], filter: Filter<AnyDocument>): Promise<AnyDocument[]> {
    const matched = [];
    for (const document of documents) {
      matched.push(...(await this.updateOne(document, filter)));
    }
    return matched;
  }

  async updateOne(document: AnyDocument, filter: Filter<AnyDocument>): Promise<AnyDocument[]> {
    if (await this.storage.has(document.id)) {
      await this.#updateOrRemove(document, filter);
      return [document];
    } else if (isMatch<AnyDocument>(document, filter)) {
      await this.storage.insertOne(document);
      return [document];
    }
    return [];
  }

  async remove(documents: AnyDocument[]): Promise<AnyDocument[]> {
    const matched = [];
    for (const document of documents) {
      if (isMatch<AnyDocument>(document, { id: document.id } as AnyDocument)) {
        await this.storage.remove({ id: document.id } as AnyDocument);
        matched.push(document);
      }
    }
    return matched;
  }

  async #updateOrRemove(document: AnyDocument, filter: Filter<AnyDocument>): Promise<void> {
    if (isMatch<AnyDocument>(document, filter)) {
      await this.storage.replace({ id: document.id } as AnyDocument, document);
    } else {
      await this.storage.remove({ id: document.id } as AnyDocument);
    }
  }

  flush() {
    this.storage.flush();
  }
}
