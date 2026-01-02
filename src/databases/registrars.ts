export type Registrars = {
  /**
   * Name of the collection.
   */
  name: string;

  /**
   * Set the primary key of the collection.
   * Default: "id"
   */
  primaryKey?: string;

  /**
   * List of custom indexes for the collection.
   */
  indexes?: Index[];
};

type Index = [IndexKey, IndexOptions?];

type IndexKey = string;

type IndexOptions = { unique: boolean };
