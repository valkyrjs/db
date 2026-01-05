import type { Criteria } from "mingo/types";

/**
 * Represents an unknown document with global support.
 */
export type AnyDocument = {
  [key: string]: any;
};

export type StringKeyOf<T> = Extract<keyof T, string>;

/**
 * Simplifies a complex type.
 */
export type Prettify<T> = { [K in keyof T]: T[K] } & {};

/**
 * Extended Criteria type that includes MongoDB logical and comparison operators
 */
export type QueryCriteria<T> = Criteria<T> & {
  $and?: QueryCriteria<T>[];
  $or?: QueryCriteria<T>[];
  $nor?: QueryCriteria<T>[];
  $not?: QueryCriteria<T>;

  $exists?: boolean;
  $type?: string | number;

  [key: string]: any;
};
