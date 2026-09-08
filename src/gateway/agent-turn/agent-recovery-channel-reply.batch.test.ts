import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { deliverAgentCommandResult } from "../../agents/command/delivery.js";
import type { AgentCommandGatewayIngressOpts } from "../../agents/command/types.js";
import { persistPendingFinalDeliveryMarker } from "../../agents/pending-final-delivery-marker.js";
import {
  getReplyPayloadMetadata,
  setReplyPayloadMetadata,
} from "../../auto-reply/reply-payload.js";
import { createReplyDispatcher } from "../../auto-reply/reply/reply-dispatcher.js";
import { buildTestCtx } from "../../auto-reply/reply/test-ctx.js";
import { sendDurableMessageBatchCore } from "../../channels/message/send.js";
import type { ChannelStreamingAdapter } from "../../channels/plugins/types.core.js";
import type { ChannelOutboundAdapter } from "../../channels/plugins/types.public.js";
import { loadSessionEntry, replaceSessionEntry } from "../../config/sessions/session-accessor.js";
import type { InternalSessionEntry } from "../../config/sessions/types.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { getAgentEventLifecycleGeneration } from "../../infra/agent-events.js";
import { PlatformMessageNotDispatchedError } from "../../infra/outbound/deliver-types.js";
import { deliverOutboundPayloads } from "../../infra/outbound/deliver.js";
import { drainPendingDeliveriesCore } from "../../infra/outbound/delivery-queue-recovery.js";
import {
  loadPendingDeliveries,
  createRecoveryLog,
} from "../../infra/outbound/delivery-queue.test-helpers.js";
import { setActivePluginRegistry } from "../../plugins/runtime.js";
import { createDeferredCore } from "../../shared/deferred.js";
import { closeOpenClawAgentDatabasesForTest } from "../../state/openclaw-agent-db.js";
import { closeOpenClawStateDatabaseForTest } from "../../state/openclaw-state-db.js";
import { createOutboundTestPlugin, createTestRegistry } from "../../test-utils/channel-plugins.js";
import { runAgentWithRecoveryChannelReply } from "./agent-recovery-channel-reply.js";

// Real marker persistence, command normalization, dispatcher, finalizer, and SQLite.
// Only the channel transport/presenter is substituted; no lifecycle/storage mocks.
type ChannelRecoveryReplyContext = Parameters<
  NonNullable<ChannelStreamingAdapter["dispatchRecoveryReply"]>
>[0];

describe("recovery command batch custody", () => {
  let stateDir: string;
  let storePath: string;
  let cfg: OpenClawConfig;
  const sessionKey = "agent:main:telegram:direct:123";
  const sessionId = "batch-session";
  const runId = "batch-run";
  const context = { channel: "telegram", to: "123", accountId: "default" };
  const readEntry = () => loadSessionEntry({ storePath, sessionKey })!;
  const opts = (): AgentCommandGatewayIngressOpts => ({
    message: "Continue",
    allowModelOverride: false,
    agentId: "main",
    sessionId,
    sessionKey,
    runId,
    lifecycleGeneration: getAgentEventLifecycleGeneration(),
    mainRestartRecoveryAdmitted: true,
    deliver: true,
    ...context,
    sourceReplyDeliveryMode: "automatic",
  });
  const plugin = createOutboundTestPlugin({
    id: "telegram",
    outbound: {
      deliveryMode: "direct",
      sendText: async () => {
        throw new Error("Unexpected fallback");
      },
    },
  });
  function installPresenter(
    present: (params: ChannelRecoveryReplyContext) => Promise<void>,
    outbound = plugin.outbound,
  ) {
    setActivePluginRegistry(
      createTestRegistry([
        {
          pluginId: "telegram",
          source: "test",
          plugin: { ...plugin, outbound, streaming: { dispatchRecoveryReply: present } },
        },
      ]),
    );
  }
  async function command(admitted: AgentCommandGatewayIngressOpts) {
    const payloads = [{ text: "First distinct final" }, { text: "Second distinct final" }];
    for (const payload of payloads) {
      setReplyPayloadMetadata(payload, {
        sessionWriterDeliveryAuthority: {
          expectedSessionId: sessionId,
          expectedWriterRunId: runId,
          sessionKey,
          storePath,
        },
      });
    }
    const entry = readEntry();
    const marker = await persistPendingFinalDeliveryMarker({
      deliver: true,
      sessionStore: { [sessionKey]: entry },
      sessionKey,
      sessionEntry: entry,
      storePath,
      suppressVisibleSessionEffects: false,
      sessionReboundDuringRun: false,
      payloads,
      deliveryContext: context,
      runOwnedSessionId: sessionId,
    });
    expect(marker.pendingFinalDeliveryMarkerPersisted).toBe(true);
    expect(readEntry().pendingFinalDelivery?.deliveries).toHaveLength(1);
    return await deliverAgentCommandResult({
      cfg,
      deps: {},
      runtime: { log: () => {}, error: () => {}, exit: () => {} },
      opts: admitted,
      outboundSession: { key: sessionKey, agentId: "main" },
      sessionEntry: marker.sessionEntry,
      result: { payloads, meta: { durationMs: 1 } },
      payloads,
      preparedPlugin: plugin,
    });
  }
  beforeEach(async () => {
    stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-batch-custody-"));
    vi.stubEnv("OPENCLAW_STATE_DIR", stateDir);
    storePath = path.join(stateDir, "sessions.json");
    cfg = {
      session: { store: storePath },
      agents: { defaults: { workspace: stateDir } },
      tts: { auto: "off" },
    };
    await replaceSessionEntry(
      { storePath, sessionKey },
      {
        sessionId,
        updatedAt: Date.now(),
        status: "running",
        activeWriterRunId: runId,
        restartRecoveryDeliveryRunId: runId,
        restartRecoveryDeliveryContext: context,
      },
    );
  });
  afterEach(async () => {
    setActivePluginRegistry(createTestRegistry([]));
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
    vi.unstubAllEnvs();
    await fs.rm(stateDir, { recursive: true, force: true });
  });
  it("sends both distinct finals once and holds the batch and command through presenter teardown", async () => {
    const cleanupStarted = createDeferredCore();
    const releaseCleanup = createDeferredCore();
    const observed: Array<{ text?: string; state?: string; completion?: unknown }> = [];
    let commandComplete = false;
    installPresenter(async (params) => {
      const dispatcher = createReplyDispatcher({
        deliver: async (payload, info) => {
          await info.onPlatformSendDispatch!();
          info.assertPlatformSendAuthorized!();
          observed.push({
            text: payload.text,
            state: readEntry().pendingFinalDelivery?.deliveries?.[0]?.state,
            completion: getReplyPayloadMetadata(payload)?.pendingFinalDeliveryCompletion,
          });
          return { visibleReplySent: true };
        },
      });
      await params.dispatchReplyFromConfig({
        cfg,
        dispatcher,
        ctx: buildTestCtx({
          Body: "Continue",
          Provider: "telegram",
          Surface: "telegram",
          From: "123",
          To: "123",
          SessionKey: sessionKey,
          ChatType: "direct",
        }),
      });
      cleanupStarted.resolve();
      await releaseCleanup.promise;
      params.assertCurrent();
    });
    const pending = runAgentWithRecoveryChannelReply({
      cfg,
      opts: opts(),
      assertCurrent: () => {},
      run: async (admitted) => {
        const result = await command(admitted);
        commandComplete = true;
        return result;
      },
    });
    try {
      await Promise.race([
        cleanupStarted.promise,
        pending.then(() => {
          throw new Error("Presenter did not start");
        }),
      ]);
      expect(commandComplete).toBe(false);
      expect(readEntry()).toMatchObject({ status: "running", restartRecoveryDeliveryRunId: runId });
      expect(observed).toEqual([
        { text: "First distinct final", state: "unknown", completion: undefined },
        { text: "Second distinct final", state: "unknown", completion: undefined },
      ]);
    } finally {
      releaseCleanup.resolve();
      await pending;
    }
    expect(await pending).toMatchObject({
      deliverySucceeded: true,
      deliveryStatus: {
        status: "sent",
        resultCount: 2,
        payloadOutcomes: [
          { index: 0, status: "sent" },
          { index: 1, status: "sent" },
        ],
      },
    });
    expect(readEntry().pendingFinalDelivery?.deliveries?.[0]?.state).toBe("delivered");
  });

  it.each([
    "second-no-send",
    "ambiguous",
    "finalization-failure",
    "all-no-send",
    "suppressed",
  ] as const)("aggregates %s without false success or replaying a sent sibling", async (mode) => {
    const visible: string[] = [];
    let attempts = 0;
    installPresenter(async (params) => {
      const dispatcher = createReplyDispatcher({
        beforeDeliver: mode === "suppressed" ? () => null : undefined,
        deliver: async (payload, info) => {
          attempts++;
          if (mode === "all-no-send" || (mode === "second-no-send" && attempts === 2)) {
            throw new PlatformMessageNotDispatchedError("Transport unavailable", {
              cause: new Error("No send"),
            });
          }
          await info.onPlatformSendDispatch!();
          info.assertPlatformSendAuthorized!();
          if (attempts === 2 && mode === "ambiguous") {
            return {
              visibleReplySent: false,
              suppression: { reason: "adapter_returned_no_identity" },
            };
          }
          if (attempts === 2 && mode === "finalization-failure") {
            return {
              visibleReplySent: false,
              finalization: Promise.reject(new Error("Unknown transport result")),
            };
          }
          visible.push(payload.text!);
          return { visibleReplySent: true };
        },
      });
      await params.dispatchReplyFromConfig({
        cfg,
        dispatcher,
        ctx: buildTestCtx({
          Body: "Continue",
          Provider: "telegram",
          Surface: "telegram",
          From: "123",
          To: "123",
          SessionKey: sessionKey,
          ChatType: "direct",
        }),
      });
    });
    const result = await runAgentWithRecoveryChannelReply({
      cfg,
      opts: opts(),
      assertCurrent: () => {},
      run: command,
    });
    const suppressed = mode === "suppressed";
    const noSend = mode === "all-no-send";
    expect(result.deliverySucceeded).toBe(suppressed);
    expect(result.deliveryStatus).toMatchObject({
      status: suppressed ? "suppressed" : noSend ? "failed" : "partial_failed",
      succeeded: suppressed ? true : noSend ? false : "partial",
      resultCount: suppressed || noSend ? 0 : 1,
    });
    expect(result.deliveryStatus?.payloadOutcomes?.map((outcome) => outcome.status)).toEqual(
      suppressed
        ? ["suppressed", "suppressed"]
        : noSend
          ? ["failed", "failed"]
          : ["sent", "failed"],
    );
    expect(visible).toEqual(suppressed || noSend ? [] : ["First distinct final"]);
    const entry = readEntry();
    expect(entry.pendingFinalDelivery?.deliveries?.[0]?.state).toBe(
      suppressed ? "suppressed" : noSend ? "prepared" : "unknown",
    );
    expect(entry.restartRecoveryDeliveryRunId).toBe(runId);
    // Ordinary dispatch also attempts its generic failure notice when no final sent.
    expect(attempts).toBe(suppressed ? 0 : noSend ? 3 : 2);
  });

  it.each(["retryable-no-send", "ambiguous"] as const)(
    "keeps independent durable %s custody without attaching the batch receipt or resending its first final",
    async (mode) => {
      const visible: string[] = [];
      const queueIds: string[] = [];
      const statesAtIo: Array<string | undefined> = [];
      let failSecond = true;
      const outbound: ChannelOutboundAdapter = {
        deliveryMode: "direct",
        sendText: async ({ text, onPlatformSendDispatch }) => {
          await onPlatformSendDispatch?.();
          statesAtIo.push(readEntry().pendingFinalDelivery?.deliveries?.[0]?.state);
          if (text === "Second distinct final" && failSecond) {
            if (mode === "retryable-no-send") {
              throw new PlatformMessageNotDispatchedError("Offline", {
                cause: new Error("No send"),
              });
            }
            throw new Error("Connection lost after dispatch");
          }
          visible.push(text);
          return { channel: "telegram", messageId: "message-" + visible.length };
        },
      };
      installPresenter(async (params) => {
        const dispatcher = createReplyDispatcher({
          deliver: async (payload, info) => {
            const send = await sendDurableMessageBatchCore({
              cfg,
              channel: "telegram",
              to: "123",
              accountId: "default",
              payloads: [payload],
              session: { key: sessionKey, agentId: "main" },
              durability: "required",
              onPlatformSendDispatch: info.onPlatformSendDispatch,
              assertDirectAdapterHandoff: info.assertPlatformSendAuthorized,
              onDeliveryIntent: (intent) => {
                queueIds.push(intent.id);
              },
            });
            if (send.status === "failed" || send.status === "partial_failed") {
              throw send.error;
            }
            return { visibleReplySent: send.status === "sent" };
          },
        });
        await params.dispatchReplyFromConfig({
          cfg,
          dispatcher,
          ctx: buildTestCtx({
            Body: "Continue",
            Provider: "telegram",
            Surface: "telegram",
            From: "123",
            To: "123",
            SessionKey: sessionKey,
            ChatType: "direct",
          }),
        });
      }, outbound);
      const result = await runAgentWithRecoveryChannelReply({
        cfg,
        opts: opts(),
        assertCurrent: () => {},
        run: command,
      });
      expect(result.deliverySucceeded).toBe(false);
      expect(result.deliveryStatus).toMatchObject({
        status: "partial_failed",
        resultCount: 1,
        payloadOutcomes: [{ status: "sent" }, { status: "failed", sentBeforeError: true }],
      });
      expect(visible).toEqual(["First distinct final"]);
      expect(statesAtIo).toEqual(["unknown", "unknown"]);
      const marker = readEntry().pendingFinalDelivery!;
      expect(marker.deliveries?.[0]?.state).toBe("unknown");
      expect(new Set(queueIds).size).toBe(2);
      expect(queueIds).not.toContain(marker.deliveries?.[0]?.id);
      const queued = await loadPendingDeliveries(stateDir);
      expect(queued).toHaveLength(1);
      expect(queued[0]?.deliveryCompletion).toBeUndefined();
      // Close/reopen real stores and let the real queue owner perform reconciliation.
      closeOpenClawAgentDatabasesForTest();
      closeOpenClawStateDatabaseForTest();
      failSecond = false;
      await drainPendingDeliveriesCore({
        cfg,
        stateDir,
        drainKey: stateDir,
        logLabel: "batch custody test",
        log: createRecoveryLog(),
        selectEntry: () => ({ match: true, bypassBackoff: true }),
        deliver: deliverOutboundPayloads,
      });
      expect(visible).toEqual(
        mode === "retryable-no-send"
          ? ["First distinct final", "Second distinct final"]
          : ["First distinct final"],
      );
      expect(readEntry().pendingFinalDelivery?.deliveries?.[0]?.state).toBe("unknown");
    },
  );

  it.each([
    { name: "session completion", patch: { status: "done" } },
    { name: "recovery claim loss", patch: { restartRecoveryDeliveryRunId: "replacement" } },
    { name: "writer claim loss", patch: { activeWriterRunId: "replacement" } },
  ] satisfies Array<{ name: string; patch: Partial<InternalSessionEntry> }>)(
    "fences the second physical send after $name",
    async ({ patch }) => {
      const visible: string[] = [];
      installPresenter(async (params) => {
        const dispatcher = createReplyDispatcher({
          deliver: async (payload, info) => {
            if (visible.length === 1) {
              await replaceSessionEntry({ storePath, sessionKey }, { ...readEntry(), ...patch });
            }
            await info.onPlatformSendDispatch!();
            info.assertPlatformSendAuthorized!();
            visible.push(payload.text!);
            return { visibleReplySent: true };
          },
        });
        await params.dispatchReplyFromConfig({
          cfg,
          dispatcher,
          ctx: buildTestCtx({
            Body: "Continue",
            Provider: "telegram",
            Surface: "telegram",
            From: "123",
            To: "123",
            SessionKey: sessionKey,
            ChatType: "direct",
          }),
        });
      });
      const result = await runAgentWithRecoveryChannelReply({
        cfg,
        opts: opts(),
        assertCurrent: () => {},
        run: command,
      });
      expect(visible).toEqual(["First distinct final"]);
      expect(result.deliverySucceeded).toBe(false);
      expect(result.deliveryStatus?.status).toBe("partial_failed");
      expect(readEntry().pendingFinalDelivery?.deliveries?.[0]?.state).toBe("unknown");
    },
  );
});
