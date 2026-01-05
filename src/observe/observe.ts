import type { Subscription } from "@valkyr/event-emitter";
import { Query } from "mingo";
import type { Criteria } from "mingo/types";

import type { Collection } from "../collection.ts";
import { addOptions, type ChangeEvent, type QueryOptions } from "../storage.ts";
import type { AnyDocument } from "../types.ts";
import { isMatch } from "./is-match.ts";

export function observe<TCollection extends Collection>(
  collection: TCollection,
  condition: Criteria<AnyDocument>,
  options: QueryOptions | undefined,
  onChange: (documents: AnyDocument[], changed: AnyDocument[], type: ChangeEvent["type"]) => void,
): Subscription {
  const documents = new Map<string | number, AnyDocument>();

  let debounce: any;

  // ### Init
  // Find the initial documents and send them to the change listener.

  collection.findMany(condition, options).then(async (documents) => {
    onChange(documents, documents, "insert");
  });

  // ### Subscriptions

  const subscriptions = [
    collection.onFlush(() => {
      clearTimeout(debounce);
      onChange([], [], "remove");
    }),
    collection.onChange(async ({ type, data }) => {
      const changed: AnyDocument[] = [];
      switch (type) {
        case "insert": {
          for (const document of data) {
            if (isMatch(document, condition)) {
              documents.set(collection.getPrimaryKeyValue(document), document);
              changed.push(document);
            }
          }
          break;
        }
        case "update": {
          for (const document of data) {
            const id = collection.getPrimaryKeyValue(document);
            if (documents.has(id)) {
              if (isMatch(document, condition)) {
                documents.set(id, document);
              } else {
                documents.delete(id);
              }
              changed.push(document);
            } else if (isMatch(document, condition)) {
              documents.set(id, document);
              changed.push(document);
            }
          }
          break;
        }
        case "remove": {
          for (const document of data) {
            if (isMatch(document, condition)) {
              documents.delete(collection.getPrimaryKeyValue(document));
              changed.push(document);
            }
          }
          break;
        }
      }
      if (changed.length > 0) {
        clearTimeout(debounce);
        debounce = setTimeout(() => {
          onChange(applyQueryOptions(Array.from(documents.values()), options), changed, type);
        }, 0);
      }
    }),
  ];

  return {
    unsubscribe: () => {
      for (const subscription of subscriptions) {
        subscription.unsubscribe();
      }
    },
  };
}

function applyQueryOptions(documents: AnyDocument[], options?: QueryOptions): AnyDocument[] {
  if (options !== undefined) {
    return addOptions<AnyDocument>(new Query({}).find<AnyDocument>(documents), options).all();
  }
  return documents;
}
