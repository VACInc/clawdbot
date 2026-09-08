// Root-owned integration combines the public Telegram plugin with host command/runtime ownership.
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Api, GrammyError, HttpError } from "grammy";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { buildTelegramRoutingTarget, telegramPlugin } from "../extensions/telegram/api.js";
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
import {
  initializeGlobalHookRunner,
  resetGlobalHookRunner,
} from "../src/plugins/hook-runner-global.js";
import { resolvePluginMetadataSnapshot } from "../src/plugins/plugin-metadata-snapshot.js";
import { setActivePluginRegistry } from "../src/plugins/runtime.js";
import { withPluginRuntimeGatewayRequestScope } from "../src/plugins/runtime/gateway-request-scope.js";
import type { PluginHookRegistration, SpeechProviderPlugin } from "../src/plugins/types.js";
import { clearSecretsRuntimeSnapshot } from "../src/secrets/runtime.js";
import { MAIN_SESSION_RESTART_RECOVERY_SOURCE_TOOL } from "../src/sessions/input-provenance.js";
import { beginSessionWorkAdmission } from "../src/sessions/session-lifecycle-admission.js";
import { createDeferredCore } from "../src/shared/deferred.js";
import { closeOpenClawAgentDatabasesForTest } from "../src/state/openclaw-agent-db.js";
import { closeOpenClawStateDatabaseForTest } from "../src/state/openclaw-state-db.js";
import { createTestRegistry } from "../src/test-utils/channel-plugins.js";

// Substitute only Telegram's remote API. Command preparation, admission, CLI process,
// transcript, Telegram presentation, dispatcher and recovery cleanup remain real.
const transport = vi.hoisted(() => ({
  api: undefined as Api | undefined,
  accountIds: [] as string[],
  speech: undefined as SpeechProviderPlugin | undefined,
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
        return await original.withTelegramApiContext(
          { ...opts, api: transport.api },
          async (context) => {
            transport.accountIds.push(context.account.accountId);
            return await operation(context);
          },
        );
      } finally {
        await transport.cleanup?.();
      }
    },
  };
});

// Only the external speech catalog/producer is substituted. Core synthesis,
// TTS policy, media persistence and finalization remain real; no live fallback.
vi.mock("../src/tts/provider-registry.js", async (importOriginal) => {
  const original = await importOriginal<typeof import("../src/tts/provider-registry.js")>();
  return {
    ...original,
    getSpeechProvider: (id?: string) =>
      id === transport.speech?.id ? transport.speech : undefined,
    listSpeechProviders: () => (transport.speech ? [transport.speech] : []),
    canonicalizeSpeechProviderId: (id?: string) => id?.trim().toLowerCase(),
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
  async function admitRecovery(admit = true) {
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
    if (!admit) {
      return;
    }
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
  beforeEach(async ({ task }) => {
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
    transport.accountIds.length = 0;
    transport.speech = undefined;
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
    const gatewayCase = task.name.includes("[gateway-acceptance]");
    const persistedContext = gatewayCase
      ? { ...context, to: buildTelegramRoutingTarget(123) }
      : context;
    await replaceSessionEntry(
      { storePath, sessionKey },
      {
        sessionId,
        updatedAt: Date.now(),
        status: "running",
        abortedLastRun: true,
        restartRecoveryDeliveryRunId: runId,
        restartRecoveryDeliverySourceRunId: "interrupted-source",
        restartRecoveryDeliveryContext: persistedContext,
        ...(gatewayCase
          ? {
              restartRecoverySourceIngress: "channel" as const,
              restartRecoverySourceReplyDeliveryMode: "automatic" as const,
              restartRecoveryRequesterAccountId: "default",
              restartRecoveryRequesterSenderId: "123",
            }
          : {}),
        skillsSnapshot: { prompt: "", skills: [], version: 0 },
      },
    );
    await admitRecovery(!task.name.includes("[gateway-acceptance]"));
  });
  afterEach(async () => {
    transport.cleanup = undefined;
    transport.api = undefined;
    resetGlobalHookRunner();
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

  // Acceptance gap: prior ambiguous-delivery proof supplied the command itself.
  // These errors cross the real CLI, post-run, Telegram API and cleanup owners.
  it.each(["rejected", "ambiguous"] as const)(
    "[acceptance] preserves physical %s send evidence through the full command",
    async (mode) => {
      const finalText = "Physical delivery outcome probe";
      await fs.writeFile(path.join(root, "output"), finalText);
      cfg.channels!.telegram!.streaming = { mode: "off" };
      await fs.writeFile(path.join(root, "openclaw.json"), JSON.stringify(cfg));
      setRuntimeConfigSnapshot(cfg, cfg);
      const api = transport.api!;
      const ordinarySend = vi.spyOn(api, "sendMessage").getMockImplementation()!;
      let finalAttempts = 0;
      vi.spyOn(api, "sendMessage").mockImplementation(async (...args) => {
        if (args[1] !== finalText) {
          return ordinarySend(...args);
        }
        finalAttempts++;
        if (mode === "rejected") {
          throw new GrammyError(
            "Forbidden",
            {
              ok: false,
              error_code: 403,
              description: "Forbidden: bot was blocked by the user",
            },
            "sendMessage",
            {},
          );
        }
        visible.set(900 + finalAttempts, finalText);
        throw new HttpError(
          "Network request for 'sendMessage' failed!",
          new Error("Connection lost after acceptance"),
        );
      });
      const result = await run();
      expect(result.deliverySucceeded).toBe(false);
      expect(result.deliveryStatus).toMatchObject({ status: "failed", succeeded: false });
      // Execution completed; failed transport is a separate durable outcome.
      expect(readAgentRunTerminalOutcome(result)).toBe("completed");
      expect(finalAttempts).toBe(1);
      expect([...visible.values()].filter((text) => text === finalText)).toHaveLength(
        mode === "ambiguous" ? 1 : 0,
      );
      expect(readEntry().pendingFinalDelivery).toMatchObject({ text: finalText });
      expect(readEntry().restartRecoveryDeliveryRunId).toBeUndefined();
      expect(readEntry().restartRecoveryTerminalDeliveryEvidence).toContainEqual(
        expect.objectContaining({
          runId: "interrupted-source",
          deliveryStatus: expect.objectContaining({ status: "failed" }),
        }),
      );
      expect(getAgentRunContext(runId)).toBeUndefined();
      if (mode === "ambiguous") {
        expect(readEntry().pendingFinalDelivery?.deliveries?.[0]?.state).toBe("unknown");
        expect(result.deliveryStatus?.payloadOutcomes?.[0]).toMatchObject({
          status: "failed",
          sentBeforeError: true,
        });
      }
      expect(await fs.readFile(path.join(root, "executed"), "utf8")).toBe("executed\n");
    },
    60_000,
  );

  it("[acceptance] sends a named-account Direct Messages topic final through the real presenter", async () => {
    const to = "-100123:direct-topic:77";
    cfg.channels!.telegram!.accounts = {
      work: { botToken: "456:synthetic-work-only", streaming: { mode: "partial" } },
    };
    await fs.writeFile(path.join(root, "openclaw.json"), JSON.stringify(cfg));
    setRuntimeConfigSnapshot(cfg, cfg);
    await replaceSessionEntry(
      { storePath, sessionKey },
      {
        ...readEntry(),
        restartRecoveryDeliveryContext: { channel: "telegram", to, accountId: "work" },
      },
    );
    const finalText = "Named inbox topic final";
    await fs.writeFile(path.join(root, "output"), finalText);
    const api = transport.api!;
    vi.spyOn(api, "sendMessage").mockImplementation(async (chatId, text, options) => {
      expect(chatId).toBe(-100123);
      expect(options).toMatchObject({ direct_messages_topic_id: 77 });
      expect(options?.message_thread_id).toBeUndefined();
      visible.set(901, text);
      return {
        message_id: 901,
        date: 1,
        text,
        chat: { id: -100123, type: "supergroup", title: "Fixture inbox", is_direct_messages: true },
        direct_messages_topic: {
          topic_id: 77,
          user: { id: 123, is_bot: false, first_name: "Fixture" },
        },
      };
    });
    const result = await run({ ...opts(), to, accountId: "work" });
    expect(result).toMatchObject({
      deliverySucceeded: true,
      deliveryStatus: { status: "sent", resultCount: 1 },
    });
    expect([...visible.values()]).toEqual([finalText]);
    expect(transport.accountIds).toEqual(["work"]);
    expect(vi.spyOn(api, "sendChatAction")).not.toHaveBeenCalled();
    expect(readEntry().pendingFinalDelivery).toBeUndefined();
    expect(readEntry().restartRecoveryDeliveryRunId).toBeUndefined();
  }, 60_000);

  it("[acceptance] joins native ACP output with real Telegram finalization and cleanup", async () => {
    const { registerAcpRuntimeBackend, unregisterAcpRuntimeBackend } =
      await import("../src/acp/runtime/registry.js");
    const { getAcpSessionManager, testing } = await import("../src/acp/control-plane/manager.js");
    const { disposeAcpSessionManagerInstance } =
      await import("../src/acp/control-plane/manager.lifecycle.js");
    const backend = "handshake-acp";
    const backendTurns: string[] = [];
    registerAcpRuntimeBackend({
      id: backend,
      runtime: {
        ownerAwareSessions: 1,
        ensureSession: async (input) => ({
          backend,
          agentId: input.agentId,
          sessionKey: input.sessionKey,
          runtimeSessionName: input.sessionKey,
        }),
        cancel: async () => {},
        close: async () => {},
        async *runTurn(input) {
          backendTurns.push(input.text);
          yield {
            type: "text_delta",
            stream: "thought",
            text: "Private ACP reasoning",
            tag: "agent_thought_chunk",
          };
          yield {
            type: "text_delta",
            stream: "output",
            text: "ACP recovered final",
            tag: "agent_message_chunk",
          };
          yield { type: "done", status: "completed", stopReason: "end_turn" };
        },
      },
    });
    testing.resetAcpSessionManagerForTests();
    const manager = getAcpSessionManager();
    cfg.acp = { enabled: true, backend, allowedAgents: ["main"] };
    await fs.writeFile(path.join(root, "openclaw.json"), JSON.stringify(cfg));
    setRuntimeConfigSnapshot(cfg, cfg);
    const cleanupEntered = createDeferredCore();
    const releaseCleanup = createDeferredCore();
    try {
      await manager.initializeSession({
        cfg,
        sessionKey,
        agentId: "main",
        agent: "main",
        mode: "persistent",
        cwd: root,
      });
      transport.cleanup = async () => {
        cleanupEntered.resolve();
        await releaseCleanup.promise;
      };
      const pending = run();
      try {
        await Promise.race([
          cleanupEntered.promise,
          pending.then(() => {
            throw new Error("ACP missed presenter cleanup");
          }),
        ]);
        expect(backendTurns).toHaveLength(1);
        expect([...visible.values()]).toEqual(["ACP recovered final"]);
        expect(readEntry().restartRecoveryDeliveryRunId).toBe(runId);
        expect(getAgentRunContext(runId)).toBeDefined();
      } finally {
        releaseCleanup.resolve();
      }
      const result = await pending;
      expect(result).toMatchObject({
        deliverySucceeded: true,
        deliveryStatus: { status: "sent", resultCount: 1 },
      });
      expect([...visible.values()].join(" ")).not.toContain("Private ACP reasoning");
      expect(readEntry().restartRecoveryDeliveryRunId).toBeUndefined();
      expect(readEntry().pendingFinalDelivery).toBeUndefined();
      expect(getAgentRunContext(runId)).toBeUndefined();
    } finally {
      releaseCleanup.resolve();
      await disposeAcpSessionManagerInstance(manager, "test-complete");
      testing.resetAcpSessionManagerForTests();
      unregisterAcpRuntimeBackend(backend);
    }
  }, 60_000);

  it("[acceptance] keeps recovery custody through delayed TTS and outbound hooks", async () => {
    const synthesisEntered = createDeferredCore();
    const releaseSynthesis = createDeferredCore();
    const cleanupEntered = createDeferredCore();
    const releaseCleanup = createDeferredCore();
    const synthesized: string[] = [];
    const finalHooks: string[] = [];
    const sentHooks: unknown[] = [];
    const inboundHooks: unknown[] = [];
    transport.speech = {
      id: "acceptance-speech",
      label: "Acceptance speech",
      isConfigured: () => true,
      synthesize: async ({ text }) => {
        synthesized.push(text);
        synthesisEntered.resolve();
        await releaseSynthesis.promise;
        return {
          audioBuffer: Buffer.from("OggS" + "\0".repeat(24) + "OpusHead" + "\0".repeat(32)),
          fileExtension: ".ogg",
          outputFormat: "ogg",
          voiceCompatible: true,
        };
      },
    };
    const outboundHook: PluginHookRegistration<"reply_payload_sending"> = {
      pluginId: "acceptance-observer",
      source: "test",
      hookName: "reply_payload_sending",
      handler: (event) => {
        if (event.kind === "final") {
          finalHooks.push(event.payload.text ?? "");
          return { payload: { ...event.payload, text: "Hook-approved final" } };
        }
        return undefined;
      },
    };
    const sentHook: PluginHookRegistration<"message_sent"> = {
      pluginId: "acceptance-observer",
      source: "test",
      hookName: "message_sent",
      handler: (event) => {
        sentHooks.push(event);
      },
    };
    const inboundHook: PluginHookRegistration<"message_received"> = {
      pluginId: "acceptance-observer",
      source: "test",
      hookName: "message_received",
      handler: (event) => {
        inboundHooks.push(event);
      },
    };
    registry.typedHooks.push(outboundHook, sentHook, inboundHook);
    initializeGlobalHookRunner(registry);
    cfg = {
      ...cfg,
      plugins: undefined,
      tts: {
        auto: "always",
        provider: "acceptance-speech",
        providers: { "acceptance-speech": {} },
        persona: "fixture-only",
        personas: {
          "fixture-only": {
            provider: "acceptance-speech",
            fallbackPolicy: "fail",
            providers: { "acceptance-speech": { voice: "fixture" } },
          },
        },
      },
    };
    setActivePluginRegistry(registry);
    await fs.writeFile(path.join(root, "openclaw.json"), JSON.stringify(cfg));
    setRuntimeConfigSnapshot(cfg, cfg);
    await replaceSessionEntry({ storePath, sessionKey }, { ...readEntry(), ttsAuto: "always" });
    await fs.writeFile(path.join(root, "output"), "Unapproved original final");
    const voiceSends: string[] = [];
    vi.spyOn(transport.api!, "sendVoice").mockImplementation(async (_chat, _voice, options) => {
      voiceSends.push(options?.caption ?? "");
      return {
        message_id: 902,
        date: 1,
        chat: { id: 123, type: "private", first_name: "Fixture" },
        voice: { file_id: "fixture-voice", file_unique_id: "fixture-voice", duration: 1 },
      };
    });
    transport.cleanup = async () => {
      cleanupEntered.resolve();
      await releaseCleanup.promise;
    };
    const pending = withPluginRuntimeGatewayRequestScope(
      { isWebchatConnect: () => false, pluginRegistry: registry },
      () => run(),
    );
    try {
      await Promise.race([
        synthesisEntered.promise,
        cleanupEntered.promise.then(async () => {
          const { getLastTtsAttempt } = await import("../src/tts/tts.js");
          throw new Error(
            "TTS was bypassed before cleanup: " +
              JSON.stringify({ hooks: finalHooks, attempt: getLastTtsAttempt() }),
          );
        }),
        pending.then(() => {
          throw new Error("TTS was bypassed");
        }),
      ]);
      expect(voiceSends).toEqual([]);
      expect(readEntry().restartRecoveryDeliveryRunId).toBe(runId);
      releaseSynthesis.resolve();
      await Promise.race([
        cleanupEntered.promise,
        pending.then(() => {
          throw new Error("TTS missed cleanup");
        }),
      ]);
      expect(voiceSends).toHaveLength(1);
      expect(readEntry().restartRecoveryDeliveryRunId).toBe(runId);
    } finally {
      releaseSynthesis.resolve();
      releaseCleanup.resolve();
    }
    const result = await pending;
    expect(result.deliverySucceeded).toBe(true);
    expect(finalHooks).toEqual(["Unapproved original final"]);
    expect(synthesized).toHaveLength(1);
    expect(sentHooks).toHaveLength(1);
    expect(inboundHooks).toEqual([]);
    expect([...visible.values()].join(" ")).not.toContain("Unapproved original final");
    expect(voiceSends[0]).toContain("Hook-approved final");
    expect(readEntry().pendingFinalDelivery).toBeUndefined();
    expect(readEntry().restartRecoveryDeliveryRunId).toBeUndefined();
  }, 60_000);

  it("[acceptance] presents parsed CLI events and a mid-run verbosity change through Telegram", async () => {
    const quietVisible = createDeferredCore();
    const continueVerbose = createDeferredCore();
    const seen: string[] = [];
    const finalText = "CLI eventful final";
    const api = transport.api!;
    const send = vi.spyOn(api, "sendMessage").getMockImplementation()!;
    const edit = vi.spyOn(api, "editMessageText").getMockImplementation()!;
    const observe = (text: string) => {
      seen.push(text);
      if (text.includes("Quiet checkpoint complete")) {
        quietVisible.resolve();
      }
    };
    vi.spyOn(api, "sendMessage").mockImplementation(async (...args) => {
      observe(args[1]);
      return send(...args);
    });
    vi.spyOn(api, "editMessageText").mockImplementation(async (...args) => {
      if (typeof args[2] === "string") {
        observe(args[2]);
      }
      return edit(...args);
    });
    const backend = registry.cliBackends[0]!.backend;
    backend.config = { ...backend.config, output: "jsonl", jsonlDialect: "claude-stream-json" };
    backend.parseJsonlLifecycleEvent = (line) => {
      const event = JSON.parse(line) as { type?: string; completed?: boolean };
      return event.type === "fixture_compaction"
        ? event.completed === undefined
          ? { kind: "compaction", phase: "start" }
          : { kind: "compaction", phase: "end", completed: event.completed }
        : undefined;
    };
    backend.prepareExecution = async () => ({
      async *execute(execution) {
        execution.assertCurrent?.();
        yield {
          type: "assistant",
          message: {
            content: [
              {
                type: "tool_use",
                id: "quiet-read",
                name: "read",
                input: { path: "QUIET_ACCEPTANCE.md" },
              },
            ],
          },
        };
        yield {
          type: "user",
          message: {
            content: [{ type: "tool_result", tool_use_id: "quiet-read", content: "quiet result" }],
          },
        };
        yield {
          type: "stream_event",
          event: { type: "message_start", message: { id: "quiet-text" } },
        };
        yield {
          type: "stream_event",
          event: {
            type: "content_block_start",
            index: 0,
            content_block: { type: "text", text: "" },
          },
        };
        yield {
          type: "stream_event",
          event: {
            type: "content_block_delta",
            index: 0,
            delta: { type: "text_delta", text: "Quiet checkpoint complete for the recovery" },
          },
        };
        yield { type: "stream_event", event: { type: "content_block_stop", index: 0 } };
        yield { type: "stream_event", event: { type: "message_stop" } };
        await continueVerbose.promise;
        execution.assertCurrent?.();
        yield { type: "fixture_compaction" };
        yield { type: "fixture_compaction", completed: true };
        yield {
          type: "assistant",
          message: { content: [{ type: "thinking", thinking: "PRIVATE_REASONING_ACCEPTANCE" }] },
        };
        yield {
          type: "assistant",
          message: {
            content: [
              {
                type: "tool_use",
                id: "visible-read",
                name: "read",
                input: { path: "VERBOSE_ACCEPTANCE.md" },
              },
            ],
          },
        };
        yield {
          type: "user",
          message: {
            content: [
              {
                type: "tool_result",
                tool_use_id: "visible-read",
                content: "Visible tool result",
                is_error: true,
              },
            ],
          },
        };
        yield { type: "result", result: finalText };
      },
    });
    await replaceSessionEntry(
      { storePath, sessionKey },
      { ...readEntry(), verboseLevel: "off", reasoningLevel: "off" },
    );
    const deadline = AbortSignal.timeout(10_000);
    const pending = run({ ...opts(), abortSignal: deadline });
    let restoreClock: (() => void) | undefined;
    try {
      await Promise.race([
        quietVisible.promise,
        pending.then(() => {
          throw new Error("CLI partial output was not presented");
        }),
        new Promise<never>((_resolve, reject) => {
          deadline.addEventListener(
            "abort",
            () => reject(new Error("CLI presentation boundary deadline")),
            { once: true },
          );
        }),
      ]);
      // Ordinary verbose-off still permits a compact tool preview, not the full result.
      expect(seen.join(" ")).not.toContain("quiet result");
      await replaceSessionEntry(
        { storePath, sessionKey },
        { ...readEntry(), verboseLevel: "full" },
      );
      // Advance only the verbosity cache clock; no sleeping or owner/presenter mocks.
      const now = Date.now.bind(Date);
      const clock = vi.spyOn(Date, "now").mockImplementation(() => now() + 1_000);
      restoreClock = () => clock.mockRestore();
      continueVerbose.resolve();
      const result = await pending;
      expect(result.deliverySucceeded).toBe(true);
      expect(seen.join(" ")).toContain("Read");
      expect(seen.join(" ")).toContain("Visible tool result");
      expect(seen.join(" ")).not.toContain("PRIVATE_REASONING_ACCEPTANCE");
      expect([...visible.values()].filter((text) => text === finalText)).toHaveLength(1);
      expect(vi.spyOn(api, "sendChatAction")).toHaveBeenCalled();
      expect(getAgentRunContext(runId)).toBeUndefined();
      expect(readEntry().pendingFinalDelivery).toBeUndefined();
    } finally {
      continueVerbose.resolve();
      await pending.catch(() => undefined);
      restoreClock?.();
    }
  }, 60_000);

  it.each(["collect", "steer", "interrupt"] as const)(
    "[gateway-acceptance] preserves %s inbound during recovery and a normal turn after",
    async (queueMode) => {
      const { createRecoveryGatewayTransport } =
        await import("./helpers/telegram-recovery-gateway.js");
      const { refreshPreparedModelRuntimeSnapshots, markPreparedModelRuntimeSnapshotsStale } =
        await import("../src/agents/prepared-model-runtime.js");
      const { withLocalGatewayRequestScope } =
        await import("../src/gateway/local-request-context.js");
      const { getPluginRuntimeGatewayRequestScope } =
        await import("../src/plugins/runtime/gateway-request-scope.js");
      const { createSyntheticPluginRuntimeClient } =
        await import("../src/gateway/server-plugin-runtime-client.js");
      const { prepareAgentRequestPreflight } =
        await import("../src/gateway/agent-turn/agent-request-preflight.js");
      const { createAgentTurnService } =
        await import("../src/gateway/agent-turn/agent-turn-service.js");
      const { AsyncWorkScope } = await import("../src/shared/async-work-scope.js");
      const local = await createRecoveryGatewayTransport(root, cfg);
      cfg = {
        ...local.cfg,
        messages: {
          queue: { mode: queueMode, debounceMsByChannel: { telegram: 0 } },
          inbound: { debounceMs: 0 },
        },
      };
      transport.api = undefined;
      await fs.writeFile(path.join(root, "openclaw.json"), JSON.stringify(cfg));
      await fs.writeFile(path.join(root, "output"), "Gateway recovered answer");
      setRuntimeConfigSnapshot(cfg, cfg);
      const work = new AsyncWorkScope();
      const { createPluginRegistry } = await import("../src/plugins/registry.js");
      const { createPluginRecord } = await import("../src/plugins/loader-records.js");
      const { createPluginRuntime } = await import("../src/plugins/runtime/index.js");
      const { setTelegramRuntime } = await import("../extensions/telegram/runtime-setter-api.js");
      const builder = createPluginRegistry({
        runtime: createPluginRuntime(),
        logger: { info() {}, warn() {}, error() {}, debug() {} },
      });
      const telegramRecord = createPluginRecord({
        id: "telegram",
        source: path.resolve("extensions/telegram/index.ts"),
        rootDir: path.resolve("extensions/telegram"),
        origin: "bundled",
        enabled: true,
        configSchema: true,
        channelIds: ["telegram"],
      });
      const producerRecord = createPluginRecord({
        id: "acceptance-runtime",
        source: path.join(local.pluginDir, "index.js"),
        rootDir: local.pluginDir,
        origin: "workspace",
        enabled: true,
        configSchema: true,
      });
      builder.registry.plugins.push(telegramRecord, producerRecord);
      const telegramApi = builder.createApi(telegramRecord, { config: cfg });
      const registeredTelegram = registry.channels.find(
        (entry) => entry.plugin.id === "telegram",
      )?.plugin;
      if (!registeredTelegram) {
        throw new Error("Missing registered Telegram fixture");
      }
      telegramApi.registerChannel({ plugin: registeredTelegram });
      builder.registerCliBackend(producerRecord, local.backend);
      registry = builder.registry;
      setActivePluginRegistry(registry, undefined, "gateway-bindable", root);
      setTelegramRuntime(telegramApi.runtime);
      try {
        await refreshPreparedModelRuntimeSnapshots(cfg, {
          gatewayLifecycle: true,
          catalogMode: "static",
          defaultWorkspaceDir: root,
        });
        await withLocalGatewayRequestScope({ deps: {}, getRuntimeConfig: () => cfg }, async () => {
          const gateway = getPluginRuntimeGatewayRequestScope()?.context;
          if (!gateway) {
            throw new Error("Local Gateway context was not created");
          }
          gateway.trackExecution = (execute) => work.track(execute);
          const { loadTelegramGatewayCaptureFixture } =
            await import("../extensions/telegram/test-api.js");
          const { createTelegramBot } = await loadTelegramGatewayCaptureFixture();
          const accountAbort = new AbortController();
          const bot = createTelegramBot({
            token: "123:synthetic-test-only",
            accountId: "default",
            ownerAgentId: "main",
            config: cfg,
            botInfo: {
              id: 999,
              is_bot: true,
              first_name: "Fixture",
              username: "recovery_fixture_bot",
              can_join_groups: true,
              can_read_all_group_messages: false,
              supports_inline_queries: false,
              can_connect_to_business: false,
              has_main_web_app: false,
              has_topics_enabled: false,
              allows_users_to_create_topics: false,
              can_manage_bots: false,
              supports_join_request_queries: false,
            },
            accountAbortSignal: accountAbort.signal,
            buildContext: telegramApi.runtime.channel.inbound.buildContext,
            dispatchReplyFromConfig: telegramApi.runtime.channel.reply.dispatchReplyFromConfig,
          });
          const updateBase = ["collect", "steer", "interrupt"].indexOf(queueMode) * 1000;
          const update = (id: number, text: string) => ({
            update_id: updateBase + id,
            message: {
              message_id: updateBase + id,
              date: Math.floor(Date.now() / 1000),
              text,
              chat: { id: 123, type: "private" as const, first_name: "Fixture" },
              from: { id: 123, is_bot: false, first_name: "Fixture" },
            },
          });
          const principal = createSyntheticPluginRuntimeClient();
          const final =
            createDeferredCore<import("../src/gateway/agent-turn/types.js").AgentTurnFrame>();
          const owner =
            createDeferredCore<import("../src/gateway/chat-abort.js").ChatAbortControllerEntry>();
          const io: import("../src/gateway/agent-turn/types.js").AgentTurnIo = {
            emitAcceptance: (frame) => {
              if (!frame[0]) {
                final.resolve(frame);
              }
            },
            emitStartOwner: (_id, entry) => owner.resolve(entry),
            emitFinal: (frame) => final.resolve(frame),
          };
          const request = {
            message: "Continue the interrupted response.",
            agentId: "main",
            sessionKey,
            sessionId,
            expectedExistingSessionId: sessionId,
            idempotencyKey: runId,
            channel: "telegram",
            to: "123",
            accountId: "default",
            deliver: true,
            sourceReplyDeliveryMode: "automatic" as const,
            inputProvenance: {
              kind: "internal_system" as const,
              sourceTool: MAIN_SESSION_RESTART_RECOVERY_SOURCE_TOOL,
            },
          };
          const preflight = prepareAgentRequestPreflight({
            request,
            context: gateway,
            client: principal,
            io,
          });
          if (!preflight) {
            throw new Error("Gateway preflight rejected the fixture");
          }
          expect(preflight.isRestartRecoveryResumeRun).toBe(true);
          await createAgentTurnService({
            context: gateway,
            isWebchatConnect: () => false,
          }).startTurn({ preflight, principal, io });
          await Promise.race([
            local.producerStarted.promise,
            final.promise.then(async (frame) => {
              const entry = readEntry();
              const executed = await fs
                .readFile(path.join(root, "executed"), "utf8")
                .catch(() => "not-started");
              throw new Error(
                "Gateway ended before transport: " +
                  JSON.stringify({
                    frame,
                    executed,
                    calls: local.calls,
                    sessionId: entry.sessionId,
                    status: entry.status,
                    abortedLastRun: entry.abortedLastRun,
                    claim: entry.restartRecoveryDeliveryRunId,
                    claimContext: entry.restartRecoveryDeliveryContext,
                    pending: entry.pendingFinalDelivery,
                  }),
              );
            }),
          ]);
          const registered = await owner.promise;
          expect(gateway.chatAbortControllers.get(runId)).toBe(registered);
          expect(registered.operationalRunInstance).toBeDefined();
          expect(registered.agentRunDelegatedAuthority).toBeDefined();
          expect(readEntry().restartRecoveryDeliveryRunId).toBe(runId);
          const { getSessionWorkAdmissionOwnerRelease, collectActiveSessionWorkAdmissions } =
            await import("../src/sessions/session-lifecycle-admission.js");
          expect(
            getSessionWorkAdmissionOwnerRelease({
              scope: storePath,
              identities: [sessionKey, sessionId],
              owner: MAIN_SESSION_RECOVERY_WORK_ADMISSION_OWNER,
            }),
            JSON.stringify(
              [...collectActiveSessionWorkAdmissions()].map(([scope, ids]) => [scope, [...ids]]),
            ),
          ).toBeDefined();
          try {
            await fs.writeFile(path.join(root, "output"), "Queued inbound answer");
            const activityTarget = { channel: "telegram" as const, accountId: "default" };
            const beforeInbound =
              telegramApi.runtime.channel.activity.get(activityTarget).inboundAt;
            const during = work.track(() => bot.handleUpdate(update(101, "During recovery")));
            // Observe actual ingress arrival while the external producer is held.
            // Later real replies and the execution count establish preservation/order.
            await vi.waitUntil(
              () =>
                telegramApi.runtime.channel.activity.get(activityTarget).inboundAt !==
                beforeInbound,
              { timeout: 5_000, interval: 10 },
            );
            expect(registered.controller.signal.aborted).toBe(false);
            expect(gateway.chatAbortControllers.get(runId)).toBe(registered);
            expect(await fs.readFile(path.join(root, "executed"), "utf8")).toBe("executed\n");
            local.releaseProducer.resolve();
            await final.promise;
            await local.waitForMessage("Queued inbound answer");
            await during;
            await fs.writeFile(path.join(root, "output"), "Ordinary inbound answer");
            const after = work.track(() => bot.handleUpdate(update(102, "After recovery")));
            await local.waitForMessage("Ordinary inbound answer");
            await after;
            expect(local.messages.filter((text) => text === "Queued inbound answer")).toHaveLength(
              1,
            );
            expect(
              local.messages.filter((text) => text === "Ordinary inbound answer"),
            ).toHaveLength(1);
            expect(await fs.readFile(path.join(root, "executed"), "utf8")).toBe(
              "executed\nexecuted\nexecuted\n",
            );
            expect(readEntry().sessionId).toBe(sessionId);
          } finally {
            accountAbort.abort();
            if (bot.isRunning()) {
              await bot.stop();
            }
            local.releaseProducer.resolve();
          }
          local.releaseProducer.resolve();
          const frame = await final.promise;
          expect(frame[0]).toBe(true);
          expect(frame[1]).toMatchObject({ status: "ok", result: { deliverySucceeded: true } });
          expect(local.messages.filter((text) => text === "Gateway recovered answer")).toHaveLength(
            1,
          );
          await work.drain();
          expect(gateway.chatAbortControllers.get(runId)).toBeUndefined();
          expect(readEntry().pendingFinalDelivery).toBeUndefined();
        });
      } finally {
        local.releaseProducer.resolve();
        await work.drain();
        await local.close();
        markPreparedModelRuntimeSnapshotsStale("Gateway acceptance fixture closed");
      }
    },
    60_000,
  );

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
