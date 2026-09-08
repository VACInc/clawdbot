// Root-owned integration combines the public Telegram plugin with host command/runtime ownership.
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Api } from "grammy";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { telegramPlugin } from "../extensions/telegram/api.js";
import { agentCommandFromGatewayIngress } from "../src/agents/agent-command.js";
import type { AgentCommandGatewayIngressOpts } from "../src/agents/command/types.js";
import { MAIN_SESSION_RECOVERY_WORK_ADMISSION_OWNER } from "../src/agents/main-session-recovery/main-session-recovery-admission.js";
import { commitMainSessionRecovery } from "../src/agents/main-session-recovery/main-session-recovery-store.js";
import { createAgentRunRestartAbortError } from "../src/agents/run-termination.js";
import { readAgentRunTerminalOutcome } from "../src/channels/turn/agent-run-terminal-outcome.js";
import {
  clearRuntimeConfigSnapshot,
  setRuntimeConfigSnapshot,
} from "../src/config/runtime-snapshot.js";
import { loadSessionEntry, replaceSessionEntry } from "../src/config/sessions/session-accessor.js";
import type { OpenClawConfig } from "../src/config/types.openclaw.js";
import { runAgentWithRecoveryChannelReply } from "../src/gateway/agent-turn/agent-recovery-channel-reply.js";
import {
  getAgentEventLifecycleGeneration,
  rotateAgentEventLifecycleGeneration,
} from "../src/infra/agent-events.js";
import { getAgentRunContext } from "../src/infra/agent-run-registry.js";
import { resolvePluginMetadataSnapshot } from "../src/plugins/plugin-metadata-snapshot.js";
import { setActivePluginRegistry } from "../src/plugins/runtime.js";
import { clearSecretsRuntimeSnapshot } from "../src/secrets/runtime.js";
import { beginSessionWorkAdmission } from "../src/sessions/session-lifecycle-admission.js";
import { createDeferredCore } from "../src/shared/deferred.js";
import { closeOpenClawAgentDatabasesForTest } from "../src/state/openclaw-agent-db.js";
import { closeOpenClawStateDatabaseForTest } from "../src/state/openclaw-state-db.js";
import { createTestRegistry } from "../src/test-utils/channel-plugins.js";

// Substitute only Telegram's remote API. Command preparation, admission, CLI process,
// transcript, Telegram presentation, dispatcher and recovery cleanup remain real.
const transport = vi.hoisted(() => ({
  api: undefined as Api | undefined,
  cleanup: undefined as (() => Promise<void>) | undefined,
}));
vi.mock("../extensions/telegram/src/send-context.js", async (importOriginal) => {
  const original =
    await importOriginal<typeof import("../extensions/telegram/src/send-context.js")>();
  return {
    ...original,
    withTelegramApiContext: async <T>(
      opts: Parameters<typeof original.withTelegramApiContext>[0],
      operation: Parameters<typeof original.withTelegramApiContext<T>>[1],
    ) => {
      try {
        return await original.withTelegramApiContext({ ...opts, api: transport.api }, operation);
      } finally {
        await transport.cleanup?.();
      }
    },
  };
});

describe("admitted recovery through real command and Telegram", () => {
  let root: string;
  let cfg: OpenClawConfig;
  let storePath: string;
  let registry: ReturnType<typeof createTestRegistry>;
  const sessionKey = "agent:main:telegram:direct:123";
  const sessionId = "handshake-session";
  const runId = "handshake-recovery";
  const context = { channel: "telegram", to: "123", accountId: "default" };
  const visible = new Map<number, string>();
  const requests: string[] = [];
  const readEntry = () => loadSessionEntry({ storePath, sessionKey })!;
  const opts = (): AgentCommandGatewayIngressOpts => ({
    message: "Continue the interrupted response.",
    agentId: "main",
    sessionKey,
    sessionId,
    runId,
    allowModelOverride: false,
    lifecycleGeneration: getAgentEventLifecycleGeneration(),
    mainRestartRecoveryAdmitted: true,
    deliver: true,
    ...context,
    sourceReplyDeliveryMode: "automatic",
    inputProvenance: { kind: "internal_system", sourceTool: "main-session-restart-recovery" },
  });
  async function run(options = opts()) {
    // The Gateway names this lease before admitting recovery. Keep that actual
    // process-local owner as well as the SQLite reservation/admission below.
    const admission = options.mainRestartRecoveryAdmitted
      ? await beginSessionWorkAdmission({
          scope: storePath,
          identities: [sessionKey, sessionId],
          owner: MAIN_SESSION_RECOVERY_WORK_ADMISSION_OWNER,
          signal: options.abortSignal,
          assertAllowed: () => {
            options.abortSignal?.throwIfAborted();
            if (readEntry().sessionId !== sessionId) {
              throw new Error("recovery session changed");
            }
          },
        })
      : undefined;
    try {
      return await runAgentWithRecoveryChannelReply({
        cfg,
        opts: options,
        assertCurrent: () => {
          options.abortSignal?.throwIfAborted();
          if (admission && !admission.isActive()) {
            throw new Error("Gateway admission closed");
          }
        },
        run: (admitted) =>
          agentCommandFromGatewayIngress(
            admitted,
            { log: () => {}, error: () => {}, exit: () => {} },
            {},
            {},
            {
              config: cfg,
              pluginGeneration: {
                pluginMetadataSnapshot: resolvePluginMetadataSnapshot({
                  config: cfg,
                  workspaceDir: root,
                }),
                pluginRegistry: registry,
                inlineProviderModels: [],
                configuredCatalogEntries: [],
              },
            },
          ),
      });
    } finally {
      admission?.release();
    }
  }
  async function admitRecovery() {
    const target = { storePath, sessionKey, agentId: "main" };
    const lifecycleGeneration = getAgentEventLifecycleGeneration();
    const observed = await commitMainSessionRecovery({
      target,
      command: {
        kind: "observe",
        cycleId: "handshake-cycle",
        lifecycleGeneration,
        sessionKey,
      },
    });
    if (
      observed.transition.kind !== "observed" ||
      observed.transition.view.status !== "recoverable"
    ) {
      throw new Error("Fixture did not discover interrupted recovery");
    }
    const reserved = await commitMainSessionRecovery({
      target,
      command: {
        kind: "prepare_attempt",
        observation: observed.transition.view.observation,
        attempt: observed.transition.view.nextAttempt,
        lifecycleGeneration,
        runId,
        now: Date.now(),
        executionIdentity: { state: "disabled" },
      },
    });
    expect(reserved.transition.kind).toBe("reserved");
    const admitted = await commitMainSessionRecovery({
      target,
      command: {
        kind: "admit_recovery",
        lifecycleGeneration,
        runId,
        sessionId,
        now: Date.now(),
      },
    });
    expect(admitted.transition.kind).toBe("admitted_recovery");
  }
  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-real-recovery-"));
    vi.stubEnv("OPENCLAW_STATE_DIR", root);
    vi.stubEnv("OPENCLAW_CONFIG_PATH", path.join(root, "openclaw.json"));
    vi.stubEnv("HOME", root);
    vi.stubEnv("OPENCLAW_AGENT_DIR", path.join(root, "agents/main/agent"));
    storePath = path.join(root, "sessions.json");
    cfg = {
      session: { store: storePath },
      agents: {
        defaults: {
          workspace: root,
          skipBootstrap: true,
          model: "handshake-cli/fixture",
          models: { "handshake-cli/fixture": {} },
          thinkingDefault: "off",
        },
      },
      channels: {
        telegram: { botToken: "123:synthetic-test-only", streaming: { mode: "partial" } },
      },
      plugins: { enabled: false },
      tts: { auto: "off" },
    };
    await fs.writeFile(path.join(root, "openclaw.json"), JSON.stringify(cfg));
    await fs.writeFile(path.join(root, "producer.sh"), 'printf "executed\\n" >> "$1"\ncat "$2"\n');
    await fs.writeFile(path.join(root, "output"), "NO_REPLY");
    registry = createTestRegistry([
      { pluginId: "telegram", source: "test", plugin: telegramPlugin },
    ]);
    registry.cliBackends.push({
      pluginId: "handshake-producer",
      source: "test",
      backend: {
        id: "handshake-cli",
        config: {
          command: "/bin/sh",
          args: [
            path.join(root, "producer.sh"),
            path.join(root, "executed"),
            path.join(root, "output"),
          ],
          input: "stdin",
          output: "text",
          sessionMode: "none",
          systemPromptWhen: "never",
        },
      },
    });
    setActivePluginRegistry(registry);
    setRuntimeConfigSnapshot(cfg, cfg);
    visible.clear();
    requests.length = 0;
    const api = new Api("123:synthetic-test-only");
    api.config.use(async () => {
      throw new Error("Unexpected live Telegram request");
    });
    const message = (id: number, text: string) => ({
      message_id: id,
      date: 1,
      chat: { id: 123, type: "private" as const, first_name: "Fixture" },
      text,
    });
    vi.spyOn(api, "sendMessage").mockImplementation(async (_chat, text) => {
      requests.push("sendMessage");
      const id = visible.size + 1;
      visible.set(id, text);
      return message(id, text);
    });
    vi.spyOn(api, "editMessageText").mockImplementation(async (_chat, id, text) => {
      if (typeof text !== "string") {
        throw new Error("Unexpected rich-message edit");
      }
      requests.push("editMessageText");
      visible.set(id, text);
      return { ...message(id, text), edit_date: 2 };
    });
    vi.spyOn(api, "deleteMessage").mockImplementation(async (_chat, id) => {
      requests.push("deleteMessage");
      visible.delete(id);
      return true;
    });
    vi.spyOn(api, "sendChatAction").mockResolvedValue(true);
    vi.spyOn(api, "sendPhoto").mockImplementation(async (_chat, _photo, options) => {
      requests.push("sendPhoto");
      const id = visible.size + 1;
      visible.set(id, options?.caption ?? "[photo]");
      return {
        ...message(id, ""),
        photo: [{ file_id: "photo", file_unique_id: "photo", width: 1, height: 1 }],
      };
    });
    transport.api = api;
    transport.cleanup = undefined;
    await replaceSessionEntry(
      { storePath, sessionKey },
      {
        sessionId,
        updatedAt: Date.now(),
        status: "running",
        abortedLastRun: true,
        restartRecoveryDeliveryRunId: runId,
        restartRecoveryDeliverySourceRunId: "interrupted-source",
        restartRecoveryDeliveryContext: context,
        skillsSnapshot: { prompt: "", skills: [], version: 0 },
      },
    );
    await admitRecovery();
  });
  afterEach(async () => {
    transport.cleanup = undefined;
    transport.api = undefined;
    clearSecretsRuntimeSnapshot();
    clearRuntimeConfigSnapshot();
    setActivePluginRegistry(createTestRegistry([]));
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
    vi.unstubAllEnvs();
    await fs.rm(root, { recursive: true, force: true });
  });
  it.each(["NO_REPLY", "constrained"])(
    "retains %s command ownership until Telegram cleanup without manufacturing a DM error",
    async (output) => {
      await fs.writeFile(
        path.join(root, "output"),
        output === "constrained" ? "Already delivered text" : output,
      );
      if (output === "constrained") {
        await replaceSessionEntry(
          { storePath, sessionKey },
          {
            ...readEntry(),
            restartRecoveryDeliveryMediaUrls: [],
            restartRecoverySuppressTextDelivery: true,
            restartRecoveryDisableMessageTool: true,
            restartRecoveryForceSafeTools: true,
            restartRecoverySourceReplyDeliveryMode: "automatic",
          },
        );
      }
      const cleanupEntered = createDeferredCore();
      const releaseCleanup = createDeferredCore();
      transport.cleanup = async () => {
        cleanupEntered.resolve();
        await releaseCleanup.promise;
      };
      const pending = run();
      try {
        await Promise.race([
          cleanupEntered.promise,
          pending.then(() => {
            throw new Error("missed cleanup");
          }),
        ]);
        expect(await fs.readFile(path.join(root, "executed"), "utf8")).toBe("executed\n");
        expect([...visible.values()]).toEqual([]);
        expect(readEntry().restartRecoveryDeliveryRunId).toBe(runId);
        expect(getAgentRunContext(runId)).toBeDefined();
      } finally {
        releaseCleanup.resolve();
        await pending;
      }
      expect(await pending).toMatchObject({
        deliverySucceeded: true,
        deliveryStatus: { status: "suppressed" },
      });
      expect(readEntry().restartRecoveryDeliveryRunId).toBeUndefined();
      expect(getAgentRunContext(runId)).toBeUndefined();
      expect(readAgentRunTerminalOutcome(await pending)).toBe("completed");
    },
    60_000,
  );

  it("keeps one real Telegram final and its batch owner through teardown, then admits a user follow-up", async () => {
    await fs.writeFile(path.join(root, "output"), "The interrupted work is finished.");
    const cleanupEntered = createDeferredCore();
    const releaseCleanup = createDeferredCore();
    transport.cleanup = async () => {
      cleanupEntered.resolve();
      await releaseCleanup.promise;
    };
    const pending = run();
    try {
      await Promise.race([
        cleanupEntered.promise,
        pending.then(() => {
          throw new Error("missed cleanup");
        }),
      ]);
      expect([...visible.values()]).toEqual(["The interrupted work is finished."]);
      expect(readEntry()).toMatchObject({ status: "running", restartRecoveryDeliveryRunId: runId });
      expect(readEntry().pendingFinalDelivery?.deliveries).toEqual([
        expect.objectContaining({ state: "unknown" }),
      ]);
      expect(getAgentRunContext(runId)).toBeDefined();
    } finally {
      releaseCleanup.resolve();
      await pending;
    }
    expect(await pending).toMatchObject({
      deliverySucceeded: true,
      deliveryStatus: { status: "sent", resultCount: 1 },
    });
    expect(readEntry().pendingFinalDelivery).toBeUndefined();
    expect(readEntry().restartRecoveryDeliveryRunId).toBeUndefined();
    expect(readEntry().mainRestartRecovery).toBeUndefined();
    transport.cleanup = undefined;
    await fs.writeFile(path.join(root, "output"), "The follow-up is finished.");
    const followup = await run({
      ...opts(),
      runId: "human-followup",
      mainRestartRecoveryAdmitted: false,
      message: "Thanks. Now do the follow-up.",
      inputProvenance: undefined,
    });
    expect(readAgentRunTerminalOutcome(followup)).toBe("completed");
    expect([...visible.values()]).toEqual([
      "The interrupted work is finished.",
      "The follow-up is finished.",
    ]);
    expect(await fs.readFile(path.join(root, "executed"), "utf8")).toBe("executed\nexecuted\n");
  }, 60_000);

  it.each(["empty", "error"])(
    "preserves a real CLI %s failure rather than reporting successful silence",
    async (mode) => {
      if (mode === "error") {
        await fs.writeFile(
          path.join(root, "producer.sh"),
          'echo "fixture runtime failed" >&2; exit 1',
        );
      } else {
        await fs.writeFile(path.join(root, "output"), "");
      }
      const cleanupEntered = createDeferredCore();
      const releaseCleanup = createDeferredCore();
      transport.cleanup = async () => {
        cleanupEntered.resolve();
        await releaseCleanup.promise;
      };
      const pending = run();
      const rejection = expect(pending).rejects.toThrow(
        mode === "error" ? "fixture runtime failed" : "empty response",
      );
      try {
        await Promise.race([cleanupEntered.promise, pending]);
        expect(readEntry().restartRecoveryDeliveryRunId).toBe(runId);
        expect(getAgentRunContext(runId)).toBeDefined();
      } finally {
        releaseCleanup.resolve();
        await rejection;
      }
      expect([...visible.values()].join(" ")).toMatch(/failed|error|try again/i);
      expect(getAgentRunContext(runId)).toBeUndefined();
    },
    60_000,
  );

  it("sends only retained media without replaying model text or model-selected attachments", async () => {
    const media = path.join(root, "retained.png");
    await fs.writeFile(
      media,
      Buffer.from(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aT1sAAAAASUVORK5CYII=",
        "base64",
      ),
    );
    await fs.writeFile(
      path.join(root, "output"),
      "Already delivered caption\nMEDIA:https://example.invalid/unrelated.png",
    );
    await replaceSessionEntry(
      { storePath, sessionKey },
      {
        ...readEntry(),
        restartRecoveryDeliveryMediaUrls: [media],
        restartRecoverySuppressTextDelivery: true,
        restartRecoveryDisableMessageTool: true,
        restartRecoveryForceSafeTools: true,
        restartRecoverySourceReplyDeliveryMode: "automatic",
      },
    );
    const result = await run();
    expect(result).toMatchObject({
      deliverySucceeded: true,
      deliveryStatus: { status: "sent", resultCount: 1 },
    });
    expect([...visible.values()]).toEqual(["[photo]"]);
    expect(requests).toEqual(["sendPhoto"]);
    expect(readEntry().pendingFinalDelivery).toBeUndefined();
    expect(readEntry().restartRecoveryDeliveryRunId).toBeUndefined();
  }, 60_000);

  it("uses real command delivery when the channel has no recovery presenter", async () => {
    const registration = registry.channels[0];
    if (!registration) {
      throw new Error("Missing registered Telegram plugin");
    }
    registry.channels[0] = {
      ...registration,
      plugin: {
        ...registration.plugin,
        streaming: { ...registration.plugin.streaming, dispatchRecoveryReply: undefined },
      },
    };
    setActivePluginRegistry(registry);
    await fs.writeFile(path.join(root, "output"), "Final via ordinary command delivery");
    const result = await run();
    expect(result).toMatchObject({ deliverySucceeded: true, deliveryStatus: { status: "sent" } });
    expect([...visible.values()]).toEqual(["Final via ordinary command delivery"]);
    expect(readEntry().restartRecoveryDeliveryRunId).toBeUndefined();
    expect(getAgentRunContext(runId)).toBeUndefined();
  }, 60_000);

  it("joins failed presenter teardown without replaying the visible final", async () => {
    await fs.writeFile(path.join(root, "output"), "Completed before transport teardown");
    transport.cleanup = async () => {
      throw new Error("fixture teardown failed");
    };
    await expect(run()).rejects.toThrow("fixture teardown failed");
    expect([...visible.values()]).toEqual(["Completed before transport teardown"]);
    expect(getAgentRunContext(runId)).toBeUndefined();
    expect(readEntry().restartRecoveryDeliveryRunId).toBeUndefined();
    expect(readEntry().pendingFinalDelivery).toBeUndefined();
  }, 60_000);

  it.each(["cancel", "restart"])(
    "joins %s during terminal presentation before releasing command ownership",
    async (reason) => {
      const abort = new AbortController();
      const cleanupEntered = createDeferredCore();
      const releaseCleanup = createDeferredCore();
      transport.cleanup = async () => {
        cleanupEntered.resolve();
        await releaseCleanup.promise;
      };
      const pending = run({ ...opts(), abortSignal: abort.signal });
      try {
        await Promise.race([
          cleanupEntered.promise,
          pending.then(() => {
            throw new Error("missed cleanup");
          }),
        ]);
        expect(readEntry().restartRecoveryDeliveryRunId).toBe(runId);
        if (reason === "restart") {
          await commitMainSessionRecovery({
            target: { storePath, sessionKey, agentId: "main" },
            command: {
              kind: "mark_interrupted",
              cycleId: "second-restart",
              now: Date.now(),
              runs: [{ runId, lifecycleGeneration: getAgentEventLifecycleGeneration() }],
            },
          });
        }
        abort.abort(
          reason === "restart"
            ? createAgentRunRestartAbortError()
            : new DOMException("cancelled", "AbortError"),
        );
        expect(getAgentRunContext(runId)).toBeDefined();
      } finally {
        releaseCleanup.resolve();
      }
      const result = await pending;
      expect(readAgentRunTerminalOutcome(result)).toBe("failed");
      expect([...visible.values()]).toEqual([]);
      expect(getAgentRunContext(runId)).toBeUndefined();
      if (reason === "restart") {
        expect(readEntry()).toMatchObject({
          abortedLastRun: true,
          restartRecoveryDeliveryRunId: runId,
        });
        transport.cleanup = undefined;
        rotateAgentEventLifecycleGeneration();
        await admitRecovery();
        await fs.writeFile(path.join(root, "output"), "Resumed after the second restart");
        const resumed = await run();
        expect(readAgentRunTerminalOutcome(resumed)).toBe("completed");
        expect([...visible.values()]).toEqual(["Resumed after the second restart"]);
        expect(readEntry().restartRecoveryDeliveryRunId).toBeUndefined();
        expect(readEntry().mainRestartRecovery).toBeUndefined();
      }
    },
    60_000,
  );
});
