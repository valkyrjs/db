import { Document } from "../../types.ts";
import { IndexedDbStorage } from "./storage.ts";

export class Pending<TSchema extends Document = Document> {
  readonly #upsert: any[] = [];
  readonly #remove: string[] = [];

  readonly #chunkSize = 500;

  #saving: Promise<void> | null = null;

  #storage: IndexedDbStorage<TSchema>;

  constructor(storage: IndexedDbStorage<TSchema>) {
    this.#storage = storage;
  }

  get isSaving() {
    return this.#saving !== null;
  }

  upsert(document: any): void {
    this.#upsert.push(document);
    this.save();
  }

  remove(id: any): void {
    this.#remove.push(id);
    this.save();
  }

  async save() {
    if (this.#saving) {
      return;
    }

    this.#saving = (async () => {
      try {
        while (this.#upsert.length > 0 || this.#remove.length > 0) {
          const tx = this.#storage.db.transaction(this.#storage.name, "readwrite", { durability: "relaxed" });

          if (this.#remove.length > 0) {
            const removals = this.#remove.splice(0, this.#chunkSize);
            await Promise.all(removals.map((id) => tx.store.delete(id)));
          }

          if (this.#upsert.length > 0) {
            const upserts = this.#upsert.splice(0, this.#chunkSize);
            await Promise.all(upserts.map((doc) => tx.store.put(doc)));
          }

          await tx.done;
        }
      } finally {
        this.#saving = null;
      }
    })();

    await this.#saving;
  }
}
