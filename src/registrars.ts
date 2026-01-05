import type { ZodRawShape } from "zod";

export type Registrars<TSchema extends ZodRawShape = ZodRawShape> = {
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
  indexes: IndexSpec[];
};
