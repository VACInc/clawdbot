import type { RunCliAgentParams } from "../cli-runner/types.js";
import { createCommandChannelReplyPresentation } from "./channel-reply-callbacks.js";

/** CLI events use the ordinary channel bridges; ACP uses its native projector instead. */
export async function withCommandChannelReplyEvents<T>(
  params: Parameters<typeof createCommandChannelReplyPresentation>[0],
  run: (
    presentation?: Pick<RunCliAgentParams, "emitCommentaryText" | "onExecutionStarted">,
  ) => Promise<T>,
): Promise<T> {
  const options = params.opts.channelReply?.options;
  if (!options) {
    return run();
  }
  const [
    { createAgentReplyEventBridges, createCliToolSummaryTracker },
    { createCliCommentaryHandler },
  ] = await Promise.all([
    import("../../auto-reply/reply/agent-runner-cli-dispatch.js"),
    import("../../auto-reply/reply/agent-runner-cli-commentary.js"),
  ]);
  const presentation = await createCommandChannelReplyPresentation(params);
  if (!presentation) {
    return run();
  }
  const { callbacks } = presentation;
  const summary = createCliToolSummaryTracker({
    detailMode: callbacks.toolProgressDetail,
    commandDetailsVisible: params.resolvedVerboseLevel === "full",
    shouldEmitToolResult: callbacks.shouldEmitToolResult ?? (() => false),
    shouldEmitToolOutput: callbacks.shouldEmitToolOutput ?? (() => false),
    deliver: async (payload) => {
      await callbacks.onToolResult?.(payload);
    },
  });
  const onCommentaryText = createCliCommentaryHandler({
    options,
    blockStreamingEnabled: presentation.blockStreamingEnabled,
    onBlockReply: callbacks.onBlockReply
      ? async (payload) => {
          await callbacks.onBlockReply?.(payload);
        }
      : undefined,
  });
  const bridges = createAgentReplyEventBridges({
    runId: params.runId,
    preserveProgressCallbackStartOrder: options.preserveProgressCallbackStartOrder,
    onAssistantText: async (text) => await callbacks.onPartialReply?.({ text }),
    onReasoningText: async (payload) => {
      await callbacks.onReasoningStream?.({ ...payload, requiresReasoningProgressOptIn: true });
    },
    onReasoningProgress: async (payload) => {
      await options.onReasoningProgress?.(payload);
    },
    onToolEvent: async (payload) => {
      // Result events often omit arguments. Retain the start event's classification.
      const summaryPromise = summary.noteToolEvent(payload);
      if (payload.phase === "result") {
        const commandBearing = await summaryPromise;
        await callbacks.onAgentEvent?.({ stream: "tool", data: { ...payload, commandBearing } });
      } else {
        await Promise.all([
          summaryPromise,
          callbacks.onAgentEvent?.({ stream: "tool", data: payload }),
        ]);
      }
    },
    onPlanUpdate: options.onPlanUpdate,
    onCompactionStart: options.onCompactionStart,
    onCompactionEnd: options.onCompactionEnd,
    onCommentaryText,
  });
  try {
    return await run({
      emitCommentaryText: Boolean(onCommentaryText),
      onExecutionStarted: callbacks.onExecutionStarted,
    });
  } finally {
    bridges.unsubscribe();
    await bridges.drain();
  }
}
