import type { ReplyPayload } from "../types.js";
import { shouldBridgeCliPreambleEvents, type InternalGetReplyOptions } from "./get-reply.types.js";

/** Ordinary CLI preambles have independent progress-preview and durable block lanes. */
export function createCliCommentaryHandler(params: {
  options?: InternalGetReplyOptions;
  blockStreamingEnabled: boolean;
  onBlockReply?: (payload: ReplyPayload) => Promise<void>;
}): ((payload: { text: string; itemId?: string }) => Promise<void>) | undefined {
  const progress =
    Boolean(params.options?.onItemEvent) && shouldBridgeCliPreambleEvents(params.options);
  const durable =
    Boolean(params.onBlockReply) &&
    (params.blockStreamingEnabled || params.options?.commentaryPayloadsEnabled === true);
  if (!progress && !durable) {
    return undefined;
  }
  return async (payload) => {
    const deliveries: unknown[] = [];
    if (progress) {
      deliveries.push(
        params.options?.onItemEvent?.({
          itemId: payload.itemId,
          kind: "preamble",
          progressText: payload.text,
          ...(durable ? { suppressDurableProgress: true } : {}),
        }),
      );
    }
    if (durable) {
      deliveries.push(
        params.onBlockReply?.({
          text: payload.text,
          ...(params.blockStreamingEnabled ? {} : { isCommentary: true }),
        }),
      );
    }
    await Promise.all(deliveries);
  };
}
