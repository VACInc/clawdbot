// Root-owned proof of host recovery authority after the real Telegram account queue.
import fs from "node:fs/promises";
import { createServer, type Server, type ServerResponse } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { telegramOutbound, telegramPlugin } from "../extensions/telegram/api.js";
import { agentCommandFromGatewayIngress } from "../src/agents/agent-command.js";
import type { AgentCommandGatewayIngressOpts } from "../src/agents/command/types.js";
import { MAIN_SESSION_RECOVERY_WORK_ADMISSION_OWNER } from "../src/agents/main-session-recovery/main-session-recovery-admission.js";
import { commitMainSessionRecovery } from "../src/agents/main-session-recovery/main-session-recovery-store.js";
import { isRestartRecoveryDeliveryCurrent } from "../src/agents/main-session-recovery/main-session-restart-recovery-delivery.js";
import {
  clearRuntimeConfigSnapshot,
  setRuntimeConfigSnapshot,
} from "../src/config/runtime-snapshot.js";
import { loadSessionEntry, replaceSessionEntry } from "../src/config/sessions/session-accessor.js";
import type { OpenClawConfig } from "../src/config/types.openclaw.js";
import { runAgentWithRecoveryChannelReply } from "../src/gateway/agent-turn/agent-recovery-channel-reply.js";
import { getAgentEventLifecycleGeneration } from "../src/infra/agent-events.js";
import { deliverOutboundPayloads } from "../src/infra/outbound/deliver.js";
import { drainPendingDeliveriesCore } from "../src/infra/outbound/delivery-queue-recovery.js";
import { resolvePluginMetadataSnapshot } from "../src/plugins/plugin-metadata-snapshot.js";
import { setActivePluginRegistry } from "../src/plugins/runtime.js";
import { clearSecretsRuntimeSnapshot } from "../src/secrets/runtime.js";
import {
  beginSessionWorkAdmission,
  type SessionWorkAdmissionLease,
} from "../src/sessions/session-lifecycle-admission.js";
import { createDeferredCore } from "../src/shared/deferred.js";
import { closeOpenClawAgentDatabasesForTest } from "../src/state/openclaw-agent-db.js";
import { closeOpenClawStateDatabaseForTest } from "../src/state/openclaw-state-db.js";
import { createTestRegistry } from "../src/test-utils/channel-plugins.js";

// Only the remote Bot API and text producer are substituted. No Api override,
// transformer, presenter, dispatcher, throttler, or authorization mocks.
describe("recovery authorization inside the production Telegram throttle", () => {
  let root: string;
  let cfg: OpenClawConfig;
  let storePath: string;
  let registry: ReturnType<typeof createTestRegistry>;
  const sessionKey = "agent:main:telegram:direct:123";
  const sessionId = "handshake-session";
  const runId = "handshake-recovery";
  const context = { channel: "telegram", to: "123", accountId: "default" };
  let server: Server;
  let apiRoot: string;
  let admission: SessionWorkAdmissionLease | undefined;
  const sockets = new Set<Socket>();
  const requests: Array<{ method: string; text?: string }> = [];
  const visible: string[] = [];
  let firstRequest = createDeferredCore<ServerResponse>();
  let blockerRequest = createDeferredCore<ServerResponse>();
  let rejectFirst = true;
  const finalText = "Recovered answer";
  const blockerText = "Independent queue blocker";
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
    admission = options.mainRestartRecoveryAdmitted
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
    firstRequest = createDeferredCore<ServerResponse>();
    blockerRequest = createDeferredCore<ServerResponse>();
    rejectFirst = true;
    requests.length = 0;
    visible.length = 0;
    server = createServer((request, response) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        const fields = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { text?: string };
        const method = request.url?.split("/").at(-1) ?? "";
        requests.push({ method, text: fields.text });
        if (method !== "sendMessage") {
          response
            .writeHead(400)
            .end(JSON.stringify({ ok: false, error_code: 400, description: "Unexpected method" }));
        } else if (fields.text === finalText && rejectFirst) {
          rejectFirst = false;
          firstRequest.resolve(response);
        } else if (fields.text === blockerText) {
          blockerRequest.resolve(response);
        } else {
          accept(response, fields.text ?? "");
        }
      });
    });
    server.on("connection", (socket) => {
      sockets.add(socket);
      socket.on("close", () => sockets.delete(socket));
    });
    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });
    apiRoot = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    cfg = {
      session: { store: storePath },
      agents: {
        defaults: {
          workspace: root,
          skipBootstrap: true,
          model: "handshake-cli/fixture",
          models: { "handshake-cli/fixture": {} },
          thinkingDefault: "off",
          typingMode: "never",
        },
      },
      channels: {
        telegram: {
          botToken: `123:throttle-${path.basename(root)}`,
          apiRoot,
          streaming: { mode: "off" },
        },
      },
      plugins: { enabled: false },
      tts: { auto: "off" },
    };
    await fs.writeFile(path.join(root, "openclaw.json"), JSON.stringify(cfg));
    await fs.writeFile(path.join(root, "producer.sh"), 'printf "executed\\n" >> "$1"\ncat "$2"\n');
    await fs.writeFile(path.join(root, "output"), finalText);
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
    admission?.release();
    for (const socket of sockets) {
      socket.destroy();
    }
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    clearSecretsRuntimeSnapshot();
    clearRuntimeConfigSnapshot();
    setActivePluginRegistry(createTestRegistry([]));
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
    vi.unstubAllEnvs();
    await fs.rm(root, { recursive: true, force: true });
  });
  function accept(response: ServerResponse, text: string) {
    visible.push(text);
    response.setHeader("content-type", "application/json");
    response.end(
      JSON.stringify({
        ok: true,
        result: {
          message_id: visible.length,
          date: 1,
          chat: { id: 123, type: "private", first_name: "Fixture" },
          text,
        },
      }),
    );
  }

  it.each([
    "allowed",
    "revoked",
    "session replaced",
    "route replaced",
    "send policy denied",
  ] as const)("%s during recovery backoff after a same-chat throttle wait", async (state) => {
    const pending = run().then(
      (result) => ({ result }),
      (error: unknown) => ({ error }),
    );
    // Race against completion so a missing boundary fails rather than hanging.
    const first = await Promise.race([
      firstRequest.promise,
      pending.then(() => {
        throw new Error("Recovery did not reach HTTP");
      }),
    ]);
    const sendText = telegramOutbound.sendText;
    if (!sendText) {
      throw new Error("Telegram has no public text sender");
    }
    const blocker = sendText({
      cfg,
      to: context.to,
      text: blockerText,
      accountId: context.accountId,
    });
    first.setHeader("content-type", "application/json");
    first.writeHead(429).end(
      JSON.stringify({
        ok: false,
        error_code: 429,
        description: "Too Many Requests",
        parameters: { retry_after: 2 },
      }),
    );
    let held: ServerResponse | undefined;
    try {
      held = await Promise.race([
        blockerRequest.promise,
        blocker.then(() => {
          throw new Error("Blocker did not reach HTTP");
        }),
      ]);
      // The preceding same-chat request has now traversed the real account
      // throttle. Keep its HTTP response held while retiring recovery authority.
      expect(requests).toEqual([
        { method: "sendMessage", text: finalText },
        { method: "sendMessage", text: blockerText },
      ]);
      expect(visible).toEqual([]);
      if (state === "revoked") {
        expect(admission?.isActive()).toBe(true);
        admission?.release();
        expect(admission?.isActive()).toBe(false);
      }
      if (state === "session replaced") {
        await replaceSessionEntry(
          { storePath, sessionKey },
          { ...readEntry(), sessionId: "replacement-session" },
        );
      }
      if (state === "route replaced") {
        const entry = readEntry();
        const pendingFinalDelivery = entry.pendingFinalDelivery;
        if (!pendingFinalDelivery) {
          throw new Error("Recovery has no pending-final route to replace");
        }
        const replacement = { ...context, to: "456" };
        await replaceSessionEntry(
          { storePath, sessionKey },
          {
            ...entry,
            restartRecoveryDeliveryContext: replacement,
            pendingFinalDelivery: { ...pendingFinalDelivery, context: replacement },
          },
        );
      }
      if (state === "send policy denied") {
        await replaceSessionEntry(
          { storePath, sessionKey },
          { ...readEntry(), sendPolicy: "deny" },
        );
      }
      // Confirm the canonical host decision before allowing another HTTP send.
      expect(
        isRestartRecoveryDeliveryCurrent({
          storePath,
          sessionKey,
          sessionId,
          recoveryRunId: runId,
          lifecycleGeneration: getAgentEventLifecycleGeneration(),
          cfg,
          deliveryContext: context,
          shouldContinue: () => admission?.isActive() === true,
        }),
      ).toBe(state === "allowed");
      accept(held, blockerText);
      await blocker;
      const outcome = await pending;
      if (state === "allowed") {
        expect(outcome).toMatchObject({
          result: {
            deliverySucceeded: true,
            deliveryStatus: { status: "sent", resultCount: 1 },
          },
        });
        expect(visible).toEqual([blockerText, finalText]);
        expect(requests).toHaveLength(3);
      } else {
        expect.soft(requests).toHaveLength(2);
        expect.soft(visible).toEqual([blockerText]);
        expect.soft(outcome).not.toMatchObject({ result: { deliverySucceeded: true } });
      }
      expect(await fs.readFile(path.join(root, "executed"), "utf8")).toBe("executed\n");
      // A later durable drain must not resurrect delivery after the live owner closes.
      await drainPendingDeliveriesCore({
        drainKey: `recovery-transport:${root}`,
        logLabel: "recovery transport test",
        cfg,
        log: { info: () => {}, warn: () => {}, error: () => {} },
        stateDir: root,
        deliver: deliverOutboundPayloads,
        selectEntry: () => ({ match: true, bypassBackoff: true }),
      });
      // Neither an immediate fallback nor a queued retry may escape the denial.
      expect(requests.filter(({ text }) => text === finalText)).toHaveLength(
        state === "allowed" ? 2 : 1,
      );
    } finally {
      if (held && !held.writableEnded) {
        accept(held, blockerText);
      }
      await Promise.allSettled([blocker, pending]);
    }
  });

  it("rejects automatic recovery output under message_tool_only before HTTP", async () => {
    // Unexpected sends must fail an assertion, not wait on the 429 fixture.
    rejectFirst = false;
    await replaceSessionEntry(
      { storePath, sessionKey },
      { ...readEntry(), restartRecoverySourceReplyDeliveryMode: "message_tool_only" },
    );
    const result = await run({ ...opts(), sourceReplyDeliveryMode: "message_tool_only" });
    expect.soft(requests).toEqual([]);
    expect.soft(visible).toEqual([]);
    // The canonical deliver:false command returns its answer locally without a delivery receipt.
    expect.soft(result.deliveryStatus).toBeUndefined();
    expect.soft(result.deliverySucceeded).not.toBe(true);
    expect
      .soft(result.payloads)
      .toEqual(expect.arrayContaining([expect.objectContaining({ text: finalText })]));
    expect(await fs.readFile(path.join(root, "executed"), "utf8")).toBe("executed\n");
  });
});
