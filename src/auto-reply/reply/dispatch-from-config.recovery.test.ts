import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import type { AgentCommandGatewayIngressOpts } from "../../agents/command/types.js";
import type { ChannelStreamingAdapter } from "../../channels/plugins/types.core.js";
import type { SessionEntry } from "../../config/sessions/types.js";
import { PlatformMessageNotDispatchedError } from "../../infra/outbound/deliver-types.js";
import { setActivePluginRegistry } from "../../plugins/runtime.js";
import { createDeferredCore } from "../../shared/deferred.js";
import { createSessionConversationTestRegistry } from "../../test-utils/session-conversation-registry.js";
import {
  messageAuditMocks,
  sessionStoreMocks,
} from "./dispatch-from-config.shared.test-harness.js";
import {
  automaticGroupReplyConfig,
  globalBeforeAll0,
  describe0BeforeEach0,
  setNoAbort,
} from "./dispatch-from-config.test-harness.js";
import type { ReplyDispatcher, ReplyDispatchRuntimeInfo } from "./reply-dispatcher.types.js";
import { buildTestCtx } from "./test-ctx.js";

type ChannelRecoveryReplyContext = Parameters<
  NonNullable<ChannelStreamingAdapter["dispatchRecoveryReply"]>
>[0];

let createReplyDispatcher: typeof import("./reply-dispatcher.js").createReplyDispatcher;
let createCommandChannelReplyPresentation: typeof import("../../agents/command/channel-reply-callbacks.js").createCommandChannelReplyPresentation;
let runAgentWithRecoveryChannelReply: typeof import("../../gateway/agent-turn/agent-recovery-channel-reply.js").runAgentWithRecoveryChannelReply;
beforeAll(async () => {
  await globalBeforeAll0();
  ({ createReplyDispatcher } = await import("./reply-dispatcher.js"));
  ({ createCommandChannelReplyPresentation } =
    await import("../../agents/command/channel-reply-callbacks.js"));
  ({ runAgentWithRecoveryChannelReply } =
    await import("../../gateway/agent-turn/agent-recovery-channel-reply.js"));
});
beforeEach(describe0BeforeEach0);

const opts: AgentCommandGatewayIngressOpts = {
  message: "Continue the interrupted response.",
  allowModelOverride: false,
  agentId: "main",
  sessionId: "session",
  sessionKey: "agent:main:telegram:group:-100123:topic:42",
  runId: "recovered-run",
  lifecycleGeneration: "test-generation",
  mainRestartRecoveryAdmitted: true,
  channel: "telegram",
  accountId: "default",
  to: "-100123",
  threadId: 42,
  deliver: true,
  sourceReplyDeliveryMode: "automatic",
};

function setRecoveryEntry(patch: Partial<SessionEntry> = {}) {
  sessionStoreMocks.currentEntry = {
    sessionId: "session",
    updatedAt: 1,
    status: "running",
    restartRecoveryDeliveryRunId: "recovered-run",
    restartRecoveryDeliveryContext: {
      channel: "telegram",
      accountId: "default",
      to: "-100123",
      threadId: 42,
    },
    ...patch,
  };
}

function createDispatcher() {
  const dispatcher = createReplyDispatcher({ deliver: async () => ({ visibleReplySent: true }) });
  return {
    ...dispatcher,
    sendToolResult: vi.fn(dispatcher.sendToolResult),
    sendBlockReply: vi.fn(dispatcher.sendBlockReply),
    sendFinalReply: vi.fn(dispatcher.sendFinalReply),
  };
}

function installRecoveryPresenter(present: (params: ChannelRecoveryReplyContext) => Promise<void>) {
  const registry = createSessionConversationTestRegistry();
  const telegram = registry.channels.find((entry) => entry.plugin.id === "telegram");
  if (!telegram) {
    throw new Error("Missing Telegram test plugin");
  }
  telegram.plugin.streaming = { dispatchRecoveryReply: present };
  setActivePluginRegistry(registry);
}

function recoveryDispatch(
  params: ChannelRecoveryReplyContext,
  dispatcher: ReplyDispatcher = createDispatcher(),
) {
  return params.dispatchReplyFromConfig({
    cfg: params.cfg,
    dispatcher,
    ctx: buildTestCtx({
      Provider: "telegram",
      Surface: "telegram",
      From: params.to,
      To: params.to,
      SessionKey: params.sessionKey,
      ChatType: "group",
    }),
  });
}

describe("recovered channel reply dispatch", () => {
  it("does not audit a recovered presentation as a new inbound message", async () => {
    setNoAbort();
    setRecoveryEntry();
    messageAuditMocks.enabled = true;
    installRecoveryPresenter(async (params) => {
      await recoveryDispatch(params);
    });
    await runAgentWithRecoveryChannelReply({
      assertCurrent: () => {},
      opts,
      cfg: automaticGroupReplyConfig,
      run: async (admitted) => await admitted.channelReply!.deliverFinal([{ text: "Finished" }]),
    });
    expect(messageAuditMocks.emitTrustedMessageAuditEvent).not.toHaveBeenCalled();
  });
  it.each(["suppressed", "failed-before-send", "ambiguous", "partial"] as const)(
    "preserves exact %s final outcomes",
    async (mode) => {
      setNoAbort();
      setRecoveryEntry();
      let attempted = 0;
      const dispatcher = createReplyDispatcher({
        beforeDeliver: mode === "suppressed" ? () => null : undefined,
        deliver: async () => {
          attempted++;
          if (mode === "failed-before-send" || (mode === "partial" && attempted === 2)) {
            throw new PlatformMessageNotDispatchedError("test transport unavailable", {
              cause: new Error("not sent"),
            });
          }
          return mode === "ambiguous"
            ? { visibleReplySent: false, suppression: { reason: "adapter_returned_no_identity" } }
            : { visibleReplySent: true };
        },
      });
      installRecoveryPresenter(async (params) => {
        await recoveryDispatch(params, dispatcher);
      });
      const status = await runAgentWithRecoveryChannelReply({
        assertCurrent: () => {},
        opts,
        cfg: automaticGroupReplyConfig,
        run: async (admitted) =>
          await admitted.channelReply!.deliverFinal(
            mode === "partial" ? [{ text: "First" }, { text: "Second" }] : [{ text: "Finished" }],
          ),
      });
      expect(status.status).toBe(
        mode === "partial" ? "partial_failed" : mode === "suppressed" ? "suppressed" : "failed",
      );
      expect(status.succeeded).toBe(mode === "partial" ? "partial" : mode === "suppressed");
      expect(status.payloadOutcomes?.map((outcome) => outcome.status)).toEqual(
        mode === "partial" ? ["sent", "failed"] : [mode === "suppressed" ? "suppressed" : "failed"],
      );
      if (mode === "ambiguous") {
        expect(status.payloadOutcomes?.[0]).toMatchObject({ sentBeforeError: true });
      }
      if (mode === "suppressed") {
        expect(attempted).toBe(0);
      }
    },
  );

  it("retains recovery until delayed provider finalization settles", async () => {
    setNoAbort();
    setRecoveryEntry();
    const started = createDeferredCore();
    const finalization = createDeferredCore<{ visibleReplySent: boolean }>();
    const dispatcher = createReplyDispatcher({
      deliver: async () => {
        started.resolve();
        return { visibleReplySent: false, finalization: finalization.promise };
      },
    });
    installRecoveryPresenter(async (params) => {
      await recoveryDispatch(params, dispatcher);
    });
    let settled = false;
    const pending = runAgentWithRecoveryChannelReply({
      assertCurrent: () => {},
      opts,
      cfg: automaticGroupReplyConfig,
      run: async (admitted) => await admitted.channelReply!.deliverFinal([{ text: "Finished" }]),
    });
    void pending.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    try {
      await started.promise;
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
      expect(settled).toBe(false);
    } finally {
      finalization.resolve({ visibleReplySent: true });
    }
    expect(await pending).toMatchObject({ status: "sent", succeeded: true });
  });

  it.each([
    { name: "completion", patch: { status: "done" } },
    { name: "replacement", patch: { sessionId: "replacement" } },
    { name: "claim loss", patch: { restartRecoveryDeliveryRunId: "another-run" } },
    { name: "send denial", patch: { sendPolicy: "deny" } },
  ] satisfies Array<{ name: string; patch: Partial<SessionEntry> }>)(
    "rejects final I/O after $name",
    async ({ patch }) => {
      setNoAbort();
      setRecoveryEntry();
      const entered = createDeferredCore();
      const release = createDeferredCore();
      const visible = vi.fn();
      const dispatcher = createReplyDispatcher({
        deliver: async (payload, info) => {
          entered.resolve();
          await release.promise;
          await info.onPlatformSendDispatch!();
          info.assertPlatformSendAuthorized!();
          visible(payload.text);
          return { visibleReplySent: true };
        },
      });
      installRecoveryPresenter(async (params) => {
        await recoveryDispatch(params, dispatcher);
      });
      const pending = runAgentWithRecoveryChannelReply({
        assertCurrent: () => {},
        opts,
        cfg: automaticGroupReplyConfig,
        run: async (admitted) => await admitted.channelReply!.deliverFinal([{ text: "Finished" }]),
      });
      await entered.promise;
      setRecoveryEntry(patch);
      release.resolve();
      const status = await pending;
      expect(visible).not.toHaveBeenCalled();
      expect(status.succeeded).toBe(false);
    },
  );

  it("settles final text already delivered as a block without sending it again", async () => {
    setNoAbort();
    setRecoveryEntry();
    const visible = vi.fn(async () => ({ visibleReplySent: true }));
    const dispatcher = createReplyDispatcher({ deliver: visible });
    installRecoveryPresenter(async (params) => {
      await recoveryDispatch(params, dispatcher);
    });
    const status = await runAgentWithRecoveryChannelReply({
      assertCurrent: () => {},
      opts,
      cfg: automaticGroupReplyConfig,
      run: async (admitted) => {
        await admitted.channelReply!.options!.onBlockReply?.({ text: "Finished" });
        return await admitted.channelReply!.deliverFinal([{ text: "Finished" }]);
      },
    });
    expect(visible).toHaveBeenCalledOnce();
    expect(status).toMatchObject({ status: "sent", succeeded: true, resultCount: 1 });
  });

  it("binds transport custody and fences retained send callbacks at presentation exit", async () => {
    setNoAbort();
    setRecoveryEntry();
    let retained: ReplyDispatchRuntimeInfo | undefined;
    const visible = vi.fn();
    const dispatcher = createReplyDispatcher({
      deliver: async (payload, info) => {
        retained = info;
        expect(info.onPlatformSendDispatch).toBeTypeOf("function");
        expect(info.assertPlatformSendAuthorized).toBeTypeOf("function");
        await info.onPlatformSendDispatch!();
        info.assertPlatformSendAuthorized!();
        visible(payload.text);
        return { visibleReplySent: true };
      },
    });
    installRecoveryPresenter(async (params) => {
      await recoveryDispatch(params, dispatcher);
    });
    await runAgentWithRecoveryChannelReply({
      assertCurrent: () => {},
      opts,
      cfg: automaticGroupReplyConfig,
      run: async (admitted) => await admitted.channelReply!.deliverFinal([{ text: "Finished" }]),
    });
    expect(visible).toHaveBeenCalledExactlyOnceWith("Finished");
    expect(() => retained!.assertPlatformSendAuthorized!()).toThrow("no longer active");
    await expect(retained!.onPlatformSendDispatch!()).rejects.toThrow("no longer active");
  });
  it("joins command cleanup before surfacing a presentation failure", async () => {
    setNoAbort();
    setRecoveryEntry();
    const cleanupStarted = createDeferredCore();
    const releaseCleanup = createDeferredCore();
    const failure = new Error("presenter cleanup failed");
    installRecoveryPresenter(async (params) => {
      await recoveryDispatch(params);
      throw failure;
    });
    let settled = false;
    const pending = runAgentWithRecoveryChannelReply({
      assertCurrent: () => {},
      opts,
      cfg: automaticGroupReplyConfig,
      run: async (admitted) => {
        await admitted.channelReply!.deliverFinal([{ text: "Finished" }]);
        cleanupStarted.resolve();
        await releaseCleanup.promise;
        return "cleaned";
      },
    });
    const observed = pending.then(
      () => {
        settled = true;
      },
      (error: unknown) => {
        settled = true;
        return error;
      },
    );
    try {
      await cleanupStarted.promise;
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
      expect(settled).toBe(false);
    } finally {
      releaseCleanup.resolve();
      expect(await observed).toBe(failure);
    }
  });

  it("rejects a retained presenter callback after its owner has exited", async () => {
    setNoAbort();
    setRecoveryEntry();
    let retained: ChannelRecoveryReplyContext | undefined;
    installRecoveryPresenter(async (params) => {
      retained = params;
    });
    const run = vi.fn(async () => "ran");
    await expect(
      runAgentWithRecoveryChannelReply({
        assertCurrent: () => {},
        opts,
        cfg: automaticGroupReplyConfig,
        run,
      }),
    ).rejects.toThrow("did not admit");
    expect(retained).toBeDefined();
    await expect(recoveryDispatch(retained!)).rejects.toThrow("no longer active");
    expect(run).not.toHaveBeenCalled();
  });
  it.each(["off", "on", "full"] as const)(
    "uses normal progress policy and settles final delivery under verbose %s",
    async (verboseLevel) => {
      setNoAbort();
      setRecoveryEntry({ verboseLevel });
      const dispatcher = createDispatcher();
      const onPartialReply = vi.fn(async () => true);
      const onToolStart = vi.fn();
      const onCompactionStart = vi.fn();
      const registry = createSessionConversationTestRegistry();
      const telegram = registry.channels.find((entry) => entry.plugin.id === "telegram");
      if (!telegram) {
        throw new Error("Missing Telegram test plugin");
      }
      telegram.plugin.streaming = {
        dispatchRecoveryReply: async (params) => {
          expect({ to: params.to, threadId: params.threadId, accountId: params.accountId }).toEqual(
            { to: "-100123", threadId: 42, accountId: "default" },
          );
          await params.dispatchReplyFromConfig({
            cfg: params.cfg,
            dispatcher,
            ctx: buildTestCtx({
              Provider: "telegram",
              Surface: "telegram",
              From: params.to,
              To: params.to,
              SessionKey: params.sessionKey,
              ChatType: "group",
            }),
            replyOptions: { onPartialReply, onToolStart, onCompactionStart },
          });
        },
      };
      setActivePluginRegistry(registry);
      const run = vi.fn(async (admitted: AgentCommandGatewayIngressOpts) => {
        const bundle = await createCommandChannelReplyPresentation({
          workspaceDir: process.cwd(),
          opts: admitted,
          cfg: automaticGroupReplyConfig,
          sessionKey: admitted.sessionKey,
          runId: "recovered-run",
          provider: "openai",
          model: "gpt-5.4",
          resolvedVerboseLevel: verboseLevel,
        });
        if (!bundle) {
          throw new Error("Missing recovered presentation bundle");
        }
        const { callbacks } = bundle;
        await callbacks.onPartialReply?.({ text: "Checking the result" });
        await callbacks.onAgentEvent?.({
          stream: "tool",
          data: { phase: "start", name: "read", toolCallId: "tool-1", args: { path: "README.md" } },
        });
        await callbacks.onAgentEvent?.({ stream: "compaction", data: { phase: "start" } });
        if (callbacks.shouldEmitToolResult?.()) {
          await callbacks.onToolResult?.({ text: "Read: README.md" });
        }
        if (callbacks.shouldEmitToolOutput?.()) {
          await callbacks.onToolResult?.({ text: "File output" });
        }
        if (!admitted.channelReply) {
          throw new Error("Recovery bypassed channel presentation");
        }
        const status = await admitted.channelReply.deliverFinal([{ text: "Finished" }]);
        expect(status.status).toBe("sent");
        expect(status.succeeded).toBe(true);
        return "settled";
      });
      await expect(
        runAgentWithRecoveryChannelReply({
          assertCurrent: () => {},
          opts,
          cfg: automaticGroupReplyConfig,
          run,
        }),
      ).resolves.toBe("settled");
      expect(run).toHaveBeenCalledOnce();
      expect(onPartialReply).toHaveBeenCalled();
      expect(onCompactionStart).toHaveBeenCalledTimes(verboseLevel === "off" ? 0 : 1);
      expect(onToolStart).toHaveBeenCalledTimes(verboseLevel === "off" ? 0 : 1);
      expect(dispatcher.sendToolResult).toHaveBeenCalledTimes(
        verboseLevel === "off" ? 0 : verboseLevel === "full" ? 2 : 1,
      );
      expect(dispatcher.sendFinalReply).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ text: "Finished" }),
      );
    },
  );

  it.each([
    { mainRestartRecoveryAdmitted: false },
    { deliver: false },
    { sourceReplyDeliveryMode: "message_tool_only" as const },
  ])("preserves deliberate non-channel execution %j", async (override) => {
    const run = vi.fn(async (admitted: AgentCommandGatewayIngressOpts) => {
      expect(admitted.channelReply).toBeUndefined();
      return "settled";
    });
    await runAgentWithRecoveryChannelReply({
      assertCurrent: () => {},
      opts: { ...opts, ...override },
      cfg: automaticGroupReplyConfig,
      run,
    });
    expect(run).toHaveBeenCalledOnce();
  });
});
