import type { ReasoningLevel, ThinkLevel, VerboseLevel } from "../../auto-reply/thinking.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import type { RunEmbeddedAgentInternalParams } from "../embedded-agent-runner/run/internal-params.js";
import type { AgentCommandOpts } from "./types.js";

/** Load ordinary presentation only for commands with an admitted channel presenter. */
export async function createCommandChannelReplyPresentation(params: {
  opts: AgentCommandOpts;
  cfg: OpenClawConfig;
  workspaceDir: string;
  conversationContext?: string;
  sessionKey?: string;
  storePath?: string;
  runId: string;
  provider: string;
  model: string;
  resolvedVerboseLevel: VerboseLevel;
  thinkLevel?: ThinkLevel;
  reasoningLevel?: ReasoningLevel;
}): Promise<
  | {
      callbacks: Partial<RunEmbeddedAgentInternalParams>;
      blockStreamingEnabled: boolean;
    }
  | undefined
> {
  const channelReply = params.opts.channelReply;
  const options = channelReply?.options;
  if (!channelReply || !options) {
    return undefined;
  }
  const [
    { createAgentRunEventHandler },
    { createShouldEmitToolOutput, createShouldEmitToolResult },
    { createAgentTurnPresentation },
    { createReplyMediaContext },
    { resolveBlockStreamingEnabled, resolveBlockStreamingChunking },
  ] = await Promise.all([
    import("../../auto-reply/reply/agent-runner-event-handler.js"),
    import("../../auto-reply/reply/agent-runner-helpers.js"),
    import("../../auto-reply/reply/agent-runner-presentation.js"),
    import("../../auto-reply/reply/reply-media-paths.js"),
    import("../../auto-reply/reply/block-streaming.js"),
  ]);
  let started = false;
  let compactionCount = 0;
  const notifyAgentRunStart = () => {
    if (started) {
      return;
    }
    started = true;
    options.onAgentRunStart?.(params.runId);
    options.onModelSelected?.({
      provider: params.provider,
      model: params.model,
      thinkLevel: params.thinkLevel ?? "off",
    });
  };
  const signalTyping = async () => {
    await options.onReplyStart?.();
  };
  const toolProgressDetail = params.cfg.agents?.defaults?.toolProgressDetail ?? "explain";
  const blockStreamingEnabled = resolveBlockStreamingEnabled(params.cfg.agents?.defaults, options);
  const replyMediaContext = createReplyMediaContext({
    cfg: params.cfg,
    sessionKey: params.sessionKey,
    workspaceDir: params.workspaceDir,
    messageProvider: params.opts.channel,
    accountId: params.opts.accountId,
  });
  const presentation = createAgentTurnPresentation({
    turn: {
      opts: options,
      followupRun: { run: {} },
      isHeartbeat: false,
      sessionCtx: { agentText: params.conversationContext ?? params.opts.message },
      replyOperation: options.replyOperation,
      applyReplyToMode: (payload) => payload,
      typingSignals: { signalTextDelta: signalTyping },
      blockStreamingEnabled,
      blockReplyPipeline: null,
    },
    replyMediaContext,
    // Local block bookkeeping only; the caller's core finalizer owns terminal accounting.
    directlySentBlockKeys: new Set(),
    directlySentBlockPayloads: [],
    heartbeatState: { didLogStrip: false },
  });
  const eventHandler = createAgentRunEventHandler({
    turn: {
      opts: options,
      sessionKey: params.sessionKey,
      toolProgressDetail,
      replyOperation: options.replyOperation,
      sessionCtx: {},
      typingSignals: { signalToolStart: signalTyping },
      applyReplyToMode: (payload) => payload,
    },
    notifyAgentRunStart,
    sourceRepliesAreToolOnly: params.opts.sourceReplyDeliveryMode === "message_tool_only",
    provider: params.provider,
    model: params.model,
    runId: params.runId,
    notifyUserAboutCompaction: false,
    onCompactionCompleted: () => ++compactionCount,
    messageToolDeliveryState: { toolCallIds: new Set(), completed: false },
  });
  const verbosity = {
    sessionKey: params.sessionKey,
    storePath: params.storePath,
    resolvedVerboseLevel: params.resolvedVerboseLevel,
  };
  options.onRunVerbosityResolved?.({ resolvedVerboseLevel: params.resolvedVerboseLevel });
  const callbacks: Partial<RunEmbeddedAgentInternalParams> = {
    blockReplyBreak:
      !blockStreamingEnabled || params.cfg.agents?.defaults?.blockStreamingBreak === "message_end"
        ? "message_end"
        : "text_end",
    blockReplyChunking: blockStreamingEnabled
      ? resolveBlockStreamingChunking(params.cfg, params.opts.channel, params.opts.accountId)
      : undefined,
    replyOperation: options.replyOperation,
    toolProgressDetail,
    reasoningLevel: params.reasoningLevel,
    toolResultFormat: "markdown",
    shouldEmitToolResult: createShouldEmitToolResult(verbosity),
    shouldEmitToolOutput: createShouldEmitToolOutput(verbosity),
    onExecutionStarted: notifyAgentRunStart,
    onAgentEvent: eventHandler,
    onPartialReply: async (payload) => {
      notifyAgentRunStart();
      const normalized = presentation.normalizeStreamingText(payload);
      if (normalized.skip) {
        return false;
      }
      return await presentation.presentWithTyping(signalTyping(), () =>
        options.onPartialReply?.({ ...payload, text: normalized.text }),
      );
    },
    onAssistantMessageStart: async () => {
      notifyAgentRunStart();
      await presentation.presentWithTyping(signalTyping(), () =>
        options.onAssistantMessageStart?.(),
      );
    },
    onReasoningStream: async (payload) => {
      notifyAgentRunStart();
      await options.onReasoningStream?.(payload);
    },
    onReasoningEnd: options.onReasoningEnd
      ? async () => {
          await options.onReasoningEnd?.();
        }
      : undefined,
    streamReasoningInNonStreamModes: options.streamReasoningInNonStreamModes,
    onBlockReply: presentation.blockReplyHandler,
    onToolResult: options.onToolResult
      ? async (payload) => {
          await options.onToolResult?.(payload);
        }
      : undefined,
  };
  return {
    callbacks,
    blockStreamingEnabled,
  };
}
