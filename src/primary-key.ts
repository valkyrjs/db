import type { AnyDocument } from "./types.ts";

export function getDocumentWithPrimaryKey<TPKey extends string>(pkey: TPKey, document: AnyDocument): AnyDocument {
  if (Object.hasOwn(document, pkey) === true) {
    return document;
  }
  return { [pkey]: crypto.randomUUID(), ...document };
}
