import type { Document, WithId } from "./types.ts";

export const BroadcastChannel =
  globalThis.BroadcastChannel ??
  class BroadcastChannelMock {
    onmessage?: any;
    postMessage() {}
    close() {}
  };

export type StorageBroadcast<TSchema extends Document = Document> =
  | {
      name: string;
      type: "insertOne" | "updateOne";
      data: WithId<TSchema>;
    }
  | {
      name: string;
      type: "insertMany" | "updateMany" | "remove";
      data: WithId<TSchema>[];
    }
  | {
      name: string;
      type: "flush";
    };
