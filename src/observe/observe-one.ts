import type { Subscription } from "@valkyr/event-emitter";
import type { Criteria } from "mingo/types";

import type { Collection } from "../collection.ts";
import type { AnyDocument } from "../types.ts";
import { isMatch } from "./is-match.ts";

export function observeOne<TCollection extends Collection>(
  collection: TCollection,
  condition: Criteria<AnyDocument>,
  onChange: (document: AnyDocument | undefined) => void,
): Subscription {
  collection.findOne(condition).then((document) => onChange(document));
  return collection.onChange(({ type, data }) => {
    switch (type) {
      case "insert":
      case "update": {
        for (const document of data) {
          if (isMatch(document, condition) === true) {
            onChange(document);
            break;
          }
        }
        break;
      }
      case "remove": {
        for (const document of data) {
          if (isMatch(document, condition) === true) {
            onChange(undefined);
            break;
          }
        }
        break;
      }
    }
  });
}
