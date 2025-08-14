import { Query } from "mingo";

import type { Document, Filter, WithId } from "../types.ts";

export function isMatch<TSchema extends Document = Document>(
  document: WithId<TSchema>,
  filter?: Filter<WithId<TSchema>>,
): boolean {
  return !filter || new Query(filter).test(document);
}
