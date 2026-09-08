import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { ChannelOutboundContext } from "../../channels/plugins/types.adapters.js";
import type { SessionEntry } from "../../config/sessions.js";
import { replaceSessionEntry } from "../../config/sessions/session-accessor.js";
import { createGatewayInstanceRuntime } from "../../gateway/server-instance-runtime.js";
import type { GatewayRequestContext } from "../../gateway/server-methods/types.js";
import { getAgentEventLifecycleGeneration } from "../../infra/agent-events.js";
import { PlatformMessageNotDispatchedError } from "../../infra/outbound/deliver-types.js";
import { deliverOutboundPayloads } from "../../infra/outbound/deliver.js";
import { drainPendingDeliveriesCore } from "../../infra/outbound/delivery-queue-recovery.js";
import { findDeliveryIntentOwner } from "../../infra/outbound/delivery-queue-storage.js";
import {
  captureActivePluginRegistrySnapshot,
  restoreActivePluginRegistrySnapshot,
  stageActivePluginRegistry,
} from "../../plugins/runtime.js";
import { createDeferredCore } from "../../shared/deferred.js";
import { createOutboundTestPlugin, createTestRegistry } from "../../test-utils/channel-plugins.js";
import { withOpenClawTestState } from "../../test-utils/openclaw-test-state.js";
import { announceRestartRecoveryResumption } from "./main-session-restart-recovery-delivery.js";

const deliveryContext = {
  channel: "signal",
  to: "+15551234567",
  accountId: "work",
  threadId: "thread-1",
};
const sessionKey = "agent:main:signal:direct:15551234567";
const recoveryRunId = "recovery-run";
const resumptionId = "main-session-restart-recovery:recovery-run:resumed-notice";

async function withNoticeTransport(
  run: (fixture: {
    announce: () => Promise<void>;
    retire: (patch: Partial<SessionEntry>) => Promise<void>;
    drain: () => Promise<void>;
    sendText: ReturnType<
      typeof vi.fn<(ctx: ChannelOutboundContext) => Promise<{ channel: string; messageId: string }>>
    >;
    visibleSend: ReturnType<typeof vi.fn<(text: string) => void>>;
    runtime: ReturnType<typeof createGatewayInstanceRuntime>;
  }) => Promise<void>,
) {
  await withOpenClawTestState({ prefix: "resumption-notice-" }, async (state) => {
    const cfg = { channels: { signal: { enabled: true } } };
    const target = { sessionKey, storePath: path.join(state.sessionsDir(), "sessions.json") };
    const entry: SessionEntry = {
      sessionId: "session-1",
      updatedAt: Date.now(),
      status: "running",
      restartRecoveryDeliveryRunId: recoveryRunId,
      restartRecoveryDeliveryContext: deliveryContext,
    };
    await replaceSessionEntry(target, entry);
    const visibleSend = vi.fn<(text: string) => void>();
    const sendText = vi.fn(async (ctx: ChannelOutboundContext) => {
      await ctx.onPlatformSendDispatch?.();
      visibleSend(ctx.text);
      return { channel: "signal", messageId: "notice-1" };
    });
    const plugin = createOutboundTestPlugin({
      id: "signal",
      outbound: {
        deliveryMode: "direct",
        resolveTarget: ({ to }) => ({ ok: true, to: to ?? "" }),
        sendText,
      },
    });
    plugin.config = {
      listAccountIds: () => ["work"],
      resolveAccount: () => ({}),
      isConfigured: () => true,
    };
    const snapshot = captureActivePluginRegistrySnapshot();
    stageActivePluginRegistry(
      createTestRegistry([{ pluginId: "signal", source: "test", plugin }]),
      null,
      "default",
    );
    const runtime = createGatewayInstanceRuntime({
      getContext: () => ({ deps: {}, getRuntimeConfig: () => cfg }) as GatewayRequestContext,
      getMethodRegistry: () => {
        throw new Error("notice must not use RPC");
      },
      isDispatchAvailable: () => true,
    });
    try {
      await run({
        announce: () =>
          announceRestartRecoveryResumption({
            ...target,
            sessionId: entry.sessionId,
            recoveryRunId,
            lifecycleGeneration: getAgentEventLifecycleGeneration(),
            deliveryContext,
            cfg,
            gatewayRuntime: runtime.recovery,
          }),
        retire: async (patch) => {
          await replaceSessionEntry(target, { ...entry, ...patch });
        },
        drain: () =>
          drainPendingDeliveriesCore({
            drainKey: "resumption-notice",
            logLabel: "resumption notice test",
            cfg,
            log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
            stateDir: state.stateDir,
            deliver: deliverOutboundPayloads,
            selectEntry: () => ({ match: true, bypassBackoff: true }),
          }),
        sendText,
        visibleSend,
        runtime,
      });
    } finally {
      runtime.close();
      restoreActivePluginRegistrySnapshot(snapshot);
    }
  });
}

const notDispatched = () =>
  new PlatformMessageNotDispatchedError("temporary transport failure", {
    cause: new Error("connection unavailable"),
  });

const retirements = [
  { name: "completion", patch: { status: "done" } },
  { name: "replacement", patch: { sessionId: "session-2" } },
  { name: "send denial", patch: { sendPolicy: "deny" } },
] satisfies { name: string; patch: Partial<SessionEntry> }[];

describe("restart resumption notice lifetime", () => {
  it.each(retirements)(
    "does not replay a failed resumption notice after $name",
    async ({ patch }) => {
      await withNoticeTransport(async ({ announce, retire, drain, sendText, visibleSend }) => {
        sendText.mockRejectedValueOnce(notDispatched());
        await announce();
        expect(sendText).toHaveBeenCalledOnce();
        expect(visibleSend).not.toHaveBeenCalled();
        await retire(patch);
        await drain();
        expect(visibleSend).not.toHaveBeenCalled();
        expect(findDeliveryIntentOwner(resumptionId)).toBeNull();
      });
    },
  );

  it.each(retirements)(
    "fences a held notice after $name without queuing a retry",
    async ({ patch }) => {
      await withNoticeTransport(async ({ announce, retire, drain, sendText, visibleSend }) => {
        const hold = createDeferredCore();
        sendText.mockImplementationOnce(async (ctx) => {
          await hold.promise;
          await ctx.onPlatformSendDispatch?.();
          visibleSend(ctx.text);
          return { channel: "signal", messageId: "held-notice" };
        });
        const pending = announce();
        try {
          await vi.waitFor(() => expect(sendText).toHaveBeenCalledOnce());
          await retire(patch);
        } finally {
          hold.resolve();
          await pending;
        }
        expect(visibleSend).not.toHaveBeenCalled();
        expect(findDeliveryIntentOwner(resumptionId)).toBeNull();
        await drain();
        expect(visibleSend).not.toHaveBeenCalled();
      });
    },
  );

  it("does not leave a failed resumption for the next Gateway instance", async () => {
    await withNoticeTransport(async ({ announce, drain, sendText, visibleSend, runtime }) => {
      sendText.mockRejectedValueOnce(notDispatched());
      await announce();
      runtime.close();
      await drain();
      expect(visibleSend).not.toHaveBeenCalled();
      expect(findDeliveryIntentOwner(resumptionId)).toBeNull();
    });
  });

  it("delivers an active resumption without leaving replayable custody", async () => {
    await withNoticeTransport(async ({ announce, drain, sendText, visibleSend }) => {
      await announce();
      expect(visibleSend).toHaveBeenCalledOnce();
      expect(sendText).toHaveBeenCalledWith(
        expect.objectContaining({
          to: deliveryContext.to,
          accountId: deliveryContext.accountId,
          threadId: deliveryContext.threadId,
        }),
      );
      expect(findDeliveryIntentOwner(resumptionId)).toBeNull();
      await drain();
      expect(visibleSend).toHaveBeenCalledOnce();
    });
  });

  it("preserves durable tombstone retry and completed-notice deduplication", async () => {
    await withNoticeTransport(async ({ runtime, drain, sendText, visibleSend }) => {
      const idempotencyKey = "main-session-restart-recovery:source-run:failed-notice";
      const notice = { ...deliveryContext, text: "Session recovery failed", idempotencyKey };
      sendText.mockRejectedValueOnce(notDispatched());
      await expect(runtime.recovery.sendRecoveryNotice(notice)).rejects.toThrow(
        "temporary transport failure",
      );
      expect(findDeliveryIntentOwner(idempotencyKey)).toMatchObject({ status: "pending" });
      await drain();
      expect(visibleSend).toHaveBeenCalledExactlyOnceWith(notice.text);
      expect(findDeliveryIntentOwner(idempotencyKey)).toMatchObject({ status: "completed" });
      await runtime.recovery.sendRecoveryNotice(notice);
      expect(visibleSend).toHaveBeenCalledOnce();
    });
  });
});
