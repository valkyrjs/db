import { Collection } from "../../collection.ts";
import type { Index, Registrars } from "../../registrars.ts";
import { MemoryStorage } from "./storage.ts";

export class MemoryDatabase<TOptions extends MemoryDatabaseOptions> {
  readonly #collections = new Map<string, Collection>();

  constructor(readonly options: TOptions) {
    for (const { name, schema, indexes } of options.registrars) {
      this.#collections.set(
        name,
        new Collection({
          name,
          storage: new MemoryStorage(name, indexes),
          schema,
          indexes,
        }),
      );
    }
  }

  get name() {
    return this.options.name;
  }

  get registrars() {
    return this.options.registrars;
  }

  /*
   |--------------------------------------------------------------------------------
   | Fetchers
   |--------------------------------------------------------------------------------
   */

  collection<
    TName extends TOptions["registrars"][number]["name"],
    TSchema = Extract<TOptions["registrars"][number], { name: TName }>["schema"],
  >(
    name: TName,
  ): Collection<{
    name: TName;
    storage: MemoryStorage;
    schema: TSchema;
    indexes: Index[];
  }> {
    const collection = this.#collections.get(name);
    if (collection === undefined) {
      throw new Error(`Collection '${name as string}' not found`);
    }
    return collection as any;
  }

  /*
   |--------------------------------------------------------------------------------
   | Utilities
   |--------------------------------------------------------------------------------
   */

  async flush() {
    for (const collection of this.#collections.values()) {
      collection.flush();
    }
  }
}

type MemoryDatabaseOptions<TRegistrars extends Array<Registrars> = Array<any>> = {
  name: string;
  registrars: TRegistrars;
};
