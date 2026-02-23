<p align="center">
  <img src="https://user-images.githubusercontent.com/1998130/229430454-ca0f2811-d874-4314-b13d-c558de8eec7e.svg" />
</p>

# Valkyr Database

A practical, MongoDB-flavored data storage solution for client-side applications. Designed to be framework-agnostic, it works in browsers (via IndexedDB or in-memory), and hybrid mobile environments. It provides native reactivity through subscriptions, removing the need for external state management utilities.

## Table of Contents

- [Installation](#installation)
- [Core Concepts](#core-concepts)
- [Defining a Collection](#defining-a-collection)
- [Setting Up a Database](#setting-up-a-database)
  - [MemoryDatabase](#memorydatabase)
  - [IndexedDB](#indexeddb)
- [CRUD Operations](#crud-operations)
  - [insert](#insert)
  - [findOne](#findone)
  - [findMany](#findmany)
  - [update](#update)
  - [remove](#remove)
  - [count](#count)
  - [flush](#flush)
- [Querying with Mingo](#querying-with-mingo)
  - [Query Criteria](#query-criteria)
  - [Query Options (sort, skip, limit)](#query-options)
- [Subscriptions & Reactivity](#subscriptions--reactivity)
  - [subscribe (many)](#subscribe-many)
  - [subscribe (one)](#subscribe-one)
  - [onChange](#onchange)
  - [onFlush](#onflush)
- [Indexes](#indexes)
  - [primary](#primary-index)
  - [unique](#unique-index)
  - [shared](#shared-index)
- [Logging (IndexedDB)](#logging-indexeddb)
- [Export & Close (IndexedDB)](#export--close-indexeddb)
- [Cross-Tab Sync](#cross-tab-sync)
- [TypeScript Usage](#typescript-usage)
- [Full Example](#full-example)

---

## Installation

**JSR (Deno):**
```bash
deno add jsr:@valkyr/db
```

**npm:**
```bash
npx jsr add @valkyr/db
```

**Peer dependencies:** `zod`, `mingo`, `idb` (for IndexedDB adapter), `@valkyr/event-emitter`

---

## Core Concepts

| Concept | Description |
|---|---|
| **Database** | Top-level container (`MemoryDatabase` or `IndexedDB`). Holds a set of named collections. |
| **Collection** | A named group of documents with a defined Zod schema and indexes. |
| **Storage** | The underlying adapter (`MemoryStorage` or `IndexedDBStorage`). Abstracted away — you interact through the Collection API. |
| **Registrar** | The configuration object `{ name, schema, indexes }` that defines a collection before it is registered with a database. |
| **Index** | A declaration on a field that speeds up reads. Every collection requires exactly one `primary` index. |

---

## Defining a Collection

Collections are defined via a **registrar** object: a plain record with a `name`, a `schema` (Zod shape), and an `indexes` array. Registrars are passed to the database constructor and do not need to be instantiated directly.

```ts
import z from "zod";

const usersRegistrar = {
  name: "users",
  schema: {
    id:    z.string(),
    name:  z.string(),
    email: z.string().email(),
    role:  z.enum(["admin", "member"]),
  },
  indexes: [
    { field: "id",    kind: "primary" },
    { field: "email", kind: "unique"  },
    { field: "role",  kind: "shared"  },
  ],
};
```

> **Schema validation** is enforced on every `insert`. Documents that fail Zod parsing will throw.

---

## Setting Up a Database

### MemoryDatabase

Stores all data in memory. Data is lost on page refresh. Ideal for testing, ephemeral state, or server-side rendering.

```ts
import { MemoryDatabase } from "@valkyr/db";

const db = new MemoryDatabase({
  name: "my-app",
  registrars: [usersRegistrar, postsRegistrar],
});

const users = db.collection("users");
const posts = db.collection("posts");
```

### IndexedDB

Persists data using the browser's IndexedDB API via the `idb` library. Data survives page refreshes and is scoped to the browser origin.

```ts
import { IndexedDB } from "@valkyr/db";

const db = new IndexedDB({
  name: "my-app",        // IndexedDB database name
  version: 1,            // Increment when schema changes
  registrars: [usersRegistrar, postsRegistrar],
  log: (event) => {      // Optional: see Logging section
    console.log(`[${event.type}] ${event.collection} — ${event.performance.duration}ms`);
  },
});

const users = db.collection("users");
```

> **Important:** Incrementing `version` triggers the IndexedDB `upgrade` callback, which recreates all object stores and indexes. Existing data is **not** automatically migrated — handle this in the `upgrade` hook if needed.

---

## CRUD Operations

All operations are async and return Promises.

### insert

Inserts one or more documents. Each document is validated against the collection's Zod schema before insertion. Duplicate primary keys are silently ignored.

```ts
await users.insert([
  { id: "u1", name: "Alice", email: "alice@example.com", role: "admin" },
  { id: "u2", name: "Bob",   email: "bob@example.com",   role: "member" },
]);
```

### findOne

Returns the first document matching the condition, or `undefined` if none is found.

```ts
const user = await users.findOne({ id: "u1" });
// { id: "u1", name: "Alice", email: "alice@example.com", role: "admin" }

const admin = await users.findOne({ role: "admin" });
```

### findMany

Returns all documents matching the condition. Returns an empty array if none match.

```ts
const members = await users.findMany({ role: "member" });

// With sort + limit
const latest = await posts.findMany(
  { authorId: "u1" },
  { sort: { createdAt: -1 }, limit: 10 }
);
```

### update

Updates all documents matching the condition using MongoDB-style update operators (`$set`, `$unset`, `$push`, `$pull`, etc.) powered by `mingo`.

Returns an `UpdateResult`: `{ matchedCount: number, modifiedCount: number }`.

```ts
// $set — update specific fields
const result = await users.update(
  { id: "u1" },
  { $set: { name: "Alice Smith" } }
);
// { matchedCount: 1, modifiedCount: 1 }

// $push — append to an array field
await posts.update(
  { id: "p1" },
  { $push: { tags: "typescript" } }
);

// $inc — increment a numeric field
await posts.update(
  { id: "p1" },
  { $inc: { views: 1 } }
);

// Update multiple documents at once
await users.update({ role: "member" }, { $set: { active: true } });
```

**Array filters** (optional third argument) can be used for fine-grained updates within nested arrays, following the MongoDB `arrayFilters` convention.

### remove

Removes all documents matching the condition. Returns the number of documents deleted.

```ts
const deletedCount = await users.remove({ id: "u2" });
// 1

await posts.remove({ status: "draft" });
```

### count

Returns the count of documents matching the condition.

```ts
const total    = await users.count();
const admins   = await users.count({ role: "admin" });
```

### flush

Removes **all** documents from the collection and broadcasts a `flush` event to all subscribers and other browser tabs.

```ts
await users.flush();
```

---

## Querying with Mingo

Valkyr DB uses [mingo](https://github.com/kofrasa/mingo) to evaluate query conditions, giving you a MongoDB-compatible query syntax on the client side.

### Query Criteria

```ts
// Equality
{ id: "u1" }

// Comparison operators
{ age: { $gt: 18 } }
{ age: { $gte: 18, $lte: 65 } }

// $in / $nin
{ role: { $in: ["admin", "moderator"] } }
{ status: { $nin: ["banned", "deleted"] } }

// Logical operators
{ $or:  [{ role: "admin" }, { role: "moderator" }] }
{ $and: [{ active: true }, { verified: true }] }
{ $nor: [{ role: "banned" }] }

// Existence check
{ avatar: { $exists: true } }

// Nested field access (dot notation)
{ "address.city": "Oslo" }

// Array contains element
{ tags: "typescript" }

// Empty condition — matches all documents
{}
```

### Query Options

```ts
type QueryOptions = {
  sort?:   { [field: string]: 1 | -1 };  // 1 = ascending, -1 = descending
  skip?:   number;                        // offset from the start
  limit?:  number;                        // max documents to return
  range?:  { from: string; to: string }; // key-range cursor (IndexedDB)
  offset?: { value: string; direction: 1 | -1 }; // keyset pagination
};
```

```ts
// Sorted, paginated query
const page2 = await posts.findMany(
  { status: "published" },
  { sort: { createdAt: -1 }, skip: 20, limit: 10 }
);
```

---

## Subscriptions & Reactivity

Collections support live subscriptions. When data changes (insert, update, remove, flush), subscribers are notified — including changes made in other browser tabs via the BroadcastChannel API.

All subscription methods return a `Subscription` object with an `unsubscribe()` method.

### subscribe (many)

Subscribes to a list of documents matching the condition. The callback fires immediately with the current results, then again on every change.

```ts
const sub = users.subscribe(
  { role: "member" },              // condition (optional, defaults to {})
  { sort: { name: 1 }, limit: 50 }, // options (optional)
  (documents, changed, type) => {
    // documents — the full current result set
    // changed   — only the documents that were affected by this event
    // type      — "insert" | "update" | "remove"
    console.log("Current members:", documents);
  }
);

// Later, clean up:
sub.unsubscribe();
```

### subscribe (one)

When `limit: 1` is passed in the options, the callback receives a single document (or `undefined`).

```ts
const sub = users.subscribe(
  { id: "u1" },
  { limit: 1 },
  (user) => {
    // user is TSchema | undefined
    console.log("User updated:", user);
  }
);
```

### onChange

Low-level event listener that fires on any insert, update, or remove in the collection. Useful for building custom reactive logic.

```ts
const sub = users.onChange(({ type, data }) => {
  // type — "insert" | "update" | "remove"
  // data — array of affected documents
  if (type === "insert") {
    console.log("New users:", data);
  }
});

sub.unsubscribe();
```

### onFlush

Fires when the collection is flushed (all documents removed).

```ts
const sub = users.onFlush(() => {
  console.log("Collection cleared!");
});

sub.unsubscribe();
```

---

## Indexes

Every collection requires exactly **one** primary index. Additional indexes are optional but recommended for fields used frequently in query conditions, as they significantly improve lookup performance in the in-memory index manager.

### Primary Index

Uniquely identifies each document. The field value must be a `string`.

```ts
{ field: "id", kind: "primary" }
```

Querying by the primary key is the fastest possible lookup (O(1) map access).

### Unique Index

Enforces uniqueness on a field. Inserting a document with a duplicate value throws a `"Unique constraint violation"` error.

```ts
{ field: "email", kind: "unique" }
```

Querying by a unique-indexed field uses direct map lookup (O(1)).

### Shared Index

Multiple documents can share the same value. Ideal for foreign keys, enums, tags, and other non-unique fields.

```ts
{ field: "role",     kind: "shared" }
{ field: "authorId", kind: "shared" }
```

Querying by a shared-indexed field retrieves a set of matching primary keys, then fetches each document.

**Index selection:** When a query condition involves multiple indexed fields, the index manager automatically picks the most selective one — preferring primary > unique > shared.

---

## Logging (IndexedDB)

The `IndexedDB` adapter accepts an optional `log` callback that receives structured performance data for every storage operation.

```ts
import type { DBLogEvent } from "@valkyr/db";

const db = new IndexedDB({
  name: "my-app",
  version: 1,
  registrars: [...],
  log: (event: DBLogEvent) => {
    console.log(`[${event.type.toUpperCase()}] ${event.collection} completed in ${event.performance.duration}ms`);
  },
});
```

`DBLogEvent` has these properties:

| Property | Type | Description |
|---|---|---|
| `type` | `"insert" \| "update" \| "remove" \| "query"` | Operation type |
| `collection` | `string` | Name of the collection |
| `performance.duration` | `number` | Elapsed time in milliseconds |
| `performance.startedAt` | `number` | `performance.now()` at start |
| `performance.endedAt` | `number` | `performance.now()` at end |

---

## Export & Close (IndexedDB)

### export

Reads raw records directly from IndexedDB (bypassing the in-memory index), useful for data backup or migration.

```ts
// Export all records from a store
const allUsers = await db.export("users");

// Export with pagination
const batch = await db.export("users", { limit: 100, offset: "last-seen-id" });
```

### close

Closes the underlying IDBDatabase connection. Call this when the app is shutting down or before deleting the database.

```ts
await db.close();
```

---

## Cross-Tab Sync

Valkyr DB uses the browser's `BroadcastChannel` API to propagate change events between tabs and windows sharing the same origin. This means:

- An `insert` in Tab A triggers `onChange` subscribers in Tab B automatically.
- A `flush` in one tab notifies `onFlush` subscribers everywhere.

No additional configuration is required. In environments where `BroadcastChannel` is unavailable (e.g. Node.js, older browsers), it silently falls back to a no-op mock so the library still works — changes just won't cross tabs.

---

## TypeScript Usage

The library is written in TypeScript and provides full type inference end-to-end.

```ts
import z from "zod";
import { MemoryDatabase } from "@valkyr/db";

// 1. Define your schema shape
const userSchema = {
  id:    z.string(),
  name:  z.string(),
  email: z.string().email(),
  role:  z.enum(["admin", "member"]),
};

// 2. Create the registrar
const usersRegistrar = {
  name: "users" as const,  // 'as const' enables collection name inference
  schema: userSchema,
  indexes: [
    { field: "id" as const,    kind: "primary" as const },
    { field: "email" as const, kind: "unique"  as const },
    { field: "role" as const,  kind: "shared"  as const },
  ],
};

// 3. Create the database
const db = new MemoryDatabase({
  name: "my-app",
  registrars: [usersRegistrar],
});

// 4. db.collection() is fully typed — the return type is Collection<...> with
//    the inferred Zod output as the document type.
const users = db.collection("users");

// users.findOne() returns Promise<{ id: string; name: string; email: string; role: "admin" | "member" } | undefined>
const user = await users.findOne({ id: "u1" });

// users.insert() validates against the Zod schema at runtime
await users.insert([{ id: "u1", name: "Alice", email: "alice@example.com", role: "admin" }]);
```

You can access the inferred document type directly from a collection instance:

```ts
type User = typeof users["$schema"];
// { id: string; name: string; email: string; role: "admin" | "member" }
```

---

## Full Example

```ts
import z from "zod";
import { IndexedDB } from "@valkyr/db";

// --- Schema Definitions ---

const postsRegistrar = {
  name: "posts" as const,
  schema: {
    id:        z.string(),
    title:     z.string(),
    body:      z.string(),
    authorId:  z.string(),
    status:    z.enum(["draft", "published", "archived"]),
    tags:      z.array(z.string()).default([]),
    createdAt: z.string(),
  },
  indexes: [
    { field: "id"       as const, kind: "primary" as const },
    { field: "authorId" as const, kind: "shared"  as const },
    { field: "status"   as const, kind: "shared"  as const },
  ],
};

// --- Database Setup ---

const db = new IndexedDB({
  name: "blog-app",
  version: 1,
  registrars: [postsRegistrar],
  log: (e) => console.debug(`[DB:${e.type}] ${e.collection} ${e.performance.duration}ms`),
});

const posts = db.collection("posts");

// --- Insert ---

await posts.insert([
  {
    id: "p1",
    title: "Hello World",
    body: "My first post.",
    authorId: "u1",
    status: "published",
    tags: ["intro"],
    createdAt: new Date().toISOString(),
  },
  {
    id: "p2",
    title: "Draft Post",
    body: "Work in progress.",
    authorId: "u1",
    status: "draft",
    tags: [],
    createdAt: new Date().toISOString(),
  },
]);

// --- Query ---

const published = await posts.findMany(
  { status: "published" },
  { sort: { createdAt: -1 }, limit: 20 }
);

const byAuthor = await posts.findMany({ authorId: "u1" });
const single   = await posts.findOne({ id: "p1" });
const total    = await posts.count({ status: "published" });

// --- Update ---

await posts.update(
  { id: "p2" },
  { $set: { status: "published" }, $push: { tags: "update" } }
);

// --- Subscribe (reactive list) ---

const sub = posts.subscribe(
  { status: "published", authorId: "u1" },
  { sort: { createdAt: -1 } },
  (allPosts, changedPosts, changeType) => {
    console.log(`${changeType}: now ${allPosts.length} posts total`);
  }
);

// --- Subscribe (single document) ---

const singleSub = posts.subscribe(
  { id: "p1" },
  { limit: 1 },
  (post) => {
    if (post) console.log("Post updated:", post.title);
    else      console.log("Post deleted.");
  }
);

// --- Remove ---

await posts.remove({ status: "archived" });

// --- Clean up ---

sub.unsubscribe();
singleSub.unsubscribe();
await db.close();
```
