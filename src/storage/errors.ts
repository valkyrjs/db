import type { AnyObject } from "mingo/types";

export class DuplicateDocumentError extends Error {
  readonly type = "DuplicateDocumentError";

  constructor(
    readonly collection: string,
    readonly document: AnyObject,
  ) {
    super(`Collection Insert Violation: Document '${document.id}' already exists in '${collection}' collection`);
  }
}

export class CollectionNotFoundError extends Error {
  readonly type = "CollectionNotFoundError";

  constructor(readonly collection: string) {
    super(`Collection Retrieve Violation: Collection '${collection}' does not exist`);
  }
}

export class DocumentNotFoundError extends Error {
  readonly type = "DocumentNotFoundError";

  constructor(readonly criteria: AnyObject) {
    super(`Collection Update Violation: Document matching criteria does not exists`);
  }
}

export class PullUpdateArrayError extends Error {
  readonly type = "PullUpdateArrayError";

  constructor(document: string, key: string) {
    super(`Collection Update Violation: Document '${document}' $pull operation failed, '${key}' is not an array`);
  }
}
