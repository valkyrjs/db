import { Query } from "mingo";
import type { Criteria } from "mingo/types";

import type { AnyDocument } from "../types.ts";

export function isMatch<TSchema extends AnyDocument = AnyDocument>(
  document: TSchema,
  condition?: Criteria<TSchema>,
): boolean {
  return condition === undefined || new Query(condition).test(document);
}
