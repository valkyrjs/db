import { Query } from "mingo";
import type { AnyObject, Criteria } from "mingo/types";

import type { Collection } from "../collection.ts";
import { addOptions, type ChangeEvent, type QueryOptions } from "../storage/mod.ts";
import type { AnyDocument } from "../types.ts";
import { Store } from "./store.ts";

export function observe<TCollection extends Collection, TSchema extends AnyObject = TCollection["$schema"]>(
  collection: TCollection,
  condition: Criteria<TSchema>,
  options: QueryOptions | undefined,
  onChange: (documents: TSchema[], changed: TSchema[], type: ChangeEvent["type"]) => void,
): {
  unsubscribe: () => void;
} {
  const store = Store.create();

  let debounce: any;

  collection.find(condition, options).then(async (documents) => {
    const resolved = await store.resolve(documents);
    onChange(resolved, resolved, "insertMany");
  });

  const subscriptions = [
    collection.observable.flush.subscribe(() => {
      clearTimeout(debounce);
      store.flush();
      onChange([], [], "remove");
    }),
    collection.observable.change.subscribe(async ({ type, data }) => {
      let changed: AnyObject[] = [];
      switch (type) {
        case "insertOne":
        case "updateOne": {
          changed = await store[type](data, condition);
          break;
        }
        case "insertMany":
        case "updateMany":
        case "remove": {
          changed = await store[type](data, condition);
          break;
        }
      }
      if (changed.length > 0) {
        clearTimeout(debounce);
        debounce = setTimeout(() => {
          store.getDocuments().then((documents) => {
            onChange(applyQueryOptions(documents, options), changed, type);
          });
        }, 0);
      }
    }),
  ];

  return {
    unsubscribe: () => {
      for (const subscription of subscriptions) {
        subscription.unsubscribe();
      }
      store.destroy();
    },
  };
}

function applyQueryOptions(documents: AnyDocument[], options?: QueryOptions): AnyDocument[] {
  if (options !== undefined) {
    return addOptions(new Query({}).find<AnyDocument>(documents), options).all();
  }
  return documents;
}
