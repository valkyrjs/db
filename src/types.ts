/**
 * Represents an unknown document with global support.
 */
export type AnyDocument = {
  [key: string]: any;
};

/**
 * Simplifies a complex type.
 */
export type Prettify<T> = { [K in keyof T]: T[K] } & {};
