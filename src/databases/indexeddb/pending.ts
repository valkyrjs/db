import { Document } from "../../types.ts";
import { IndexedDbStorage } from "./storage.ts";

export class Pending<TSchema extends Document = Document> {
  #storage: IndexedDbStorage<TSchema>;

  readonly #upsert = new Map<string, TSchema>();
  readonly #remove = new Set<string>();

  #chunkSize = 500;
  #saveScheduled = false;
  #saving: Promise<void> | null = null;

  constructor(storage: IndexedDbStorage<TSchema>) {
    this.#storage = storage;
  }

  get isSaving() {
    return this.#saving !== null;
  }

  upsert(document: any): void {
    this.#remove.delete(document.id);
    this.#upsert.set(document.id, document);
    this.#schedule();
  }

  remove(id: any): void {
    this.#upsert.delete(id);
    this.#remove.add(id);
    this.#schedule();
  }

  #schedule() {
    if (!this.#saveScheduled) {
      this.#saveScheduled = true;
      queueMicrotask(() => {
        this.#saveScheduled = false;
        void this.save();
      });
    }
  }

  async save() {
    if (this.#saving) return;

    this.#saving = (async () => {
      try {
        while (this.#upsert.size > 0 || this.#remove.size > 0) {
          const tx = this.#storage.db.transaction(this.#storage.name, "readwrite", { durability: "relaxed" });
          const store = tx.store;

          // Process removals
          if (this.#remove.size > 0) {
            const removals = Array.from(this.#remove).slice(0, this.#chunkSize);
            removals.forEach((id) => this.#remove.delete(id));
            await Promise.all(removals.map((id) => store.delete(id)));
          }

          // Process upserts
          if (this.#upsert.size > 0) {
            const upserts = Array.from(this.#upsert.values()).slice(0, this.#chunkSize);
            upserts.forEach((doc) => this.#upsert.delete(doc.id));
            await Promise.all(upserts.map((doc) => store.put(doc)));
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
