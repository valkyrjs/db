import type { AnyDocument } from "./types.ts";

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
      type: "insert" | "update" | "remove";
      data: AnyDocument[];
    }
  | {
      name: string;
      type: "flush";
    };
