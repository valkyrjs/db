import type { AnyDocument } from "@valkyr/db";
import type { ZodRawShape } from "zod";

import type { IndexSpec } from "./index/manager.ts";

export type Registrars<TSchema extends AnyDocument = ZodRawShape> = {
  /**
   * Name of the collection.
   */
  name: string;

  /**
   * Schema definition of the documents stored in the collection.
   */
  schema: TSchema;

  /**
   * List of custom indexes for the collection.
   */
  indexes: IndexSpec<TSchema>[];
};
