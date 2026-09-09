import type { OpenClawConfig } from "../../config/types.openclaw.js";
import type { ChannelHeartbeatDeps } from "./types.core.js";

export type ChannelTypingRequest = {
  cfg: OpenClawConfig;
  to: string;
  accountId?: string | null;
  threadId?: string | number | null;
  deps?: ChannelHeartbeatDeps;
};

/** Versioned ephemeral activity request; queued work must retain its live owner. */
export type ChannelTypingRequestV2 = ChannelTypingRequest & {
  signal: AbortSignal;
  /** Recheck synchronously after transport waits and immediately before provider I/O. */
  assertPlatformSendAuthorized: () => void;
};
