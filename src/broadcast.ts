import type { AnyObject } from "mingo/types";

export const BroadcastChannel =
  globalThis.BroadcastChannel ??
  class BroadcastChannelMock {
    onmessage?: any;
    postMessage() {}
    close() {}
  };

export type StorageBroadcast =
  | {
      name: string;
      type: "insertOne" | "updateOne";
      data: AnyObject;
    }
  | {
      name: string;
      type: "insertMany" | "updateMany" | "remove";
      data: AnyObject[];
    }
  | {
      name: string;
      type: "flush";
    };
