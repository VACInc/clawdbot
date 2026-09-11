import { beforeEach, describe, expect, it, vi } from "vitest";
import { FailoverError } from "../failover-error.js";
import { resetFallbackSkipCacheForTest } from "../fallback-skip-cache.test-support.js";
import { runEmbeddedAgentEntry } from "./run-entry.js";
import type { EmbeddedAgentRunResult } from "./types.js";

// This file deliberately runs the real `runWithModelFallback`. The committed-work
// contract that protects a delivered reply lives in the runner itself
// (`canFallbackAfterError` -> rethrow), so a mocked runner cannot prove that the
// cyber-escalation catch honors it.
vi.mock("../harness/runtime-plugin.js", () => ({
  ensureSelectedAgentHarnessPlugin: vi.fn(async () => undefined),
}));

vi.mock("../harness/selection.js", () => ({
  selectAgentHarness: vi.fn(() => ({ id: "openclaw", contextEngineHostCapabilities: [] })),
}));

function makeRefusalResult(params: { provider: string; model: string }): EmbeddedAgentRunResult {
  return {
    payloads: [{ text: "policy refusal", isError: true }],
    meta: {
      durationMs: 10,
      aborted: false,
      providerStarted: true,
      stopReason: "completed",
      agentMeta: {
        sessionId: "session-1",
        provider: params.provider,
        model: params.model,
        agentHarnessId: "openclaw",
        providerRefusal: { provider: "openai", category: "cyber" },
      },
    },
  };
}

describe("runEmbeddedAgentEntry cyber failover against the real fallback runner", () => {
  beforeEach(() => {
    resetFallbackSkipCacheForTest();
  });

  it("propagates a recognized provider error thrown after the Daybreak retry delivered", async () => {
    // `overloaded` is an ordinary failover-class reason, so error classification
    // alone would call this retry interchangeable with the refusal it replaced.
    // The reply already went out, so the runner rethrows and the escalation must
    // not substitute the initial refusal.
    const failure = new FailoverError("daybreak overloaded after delivering", {
      provider: "openai",
      model: "gpt-daybreak-blue-latest",
      reason: "overloaded",
    });
    let delivered = false;

    await expect(
      runEmbeddedAgentEntry({
        selection: { cfg: {}, provider: "openai", model: "gpt-5.6" },
        identity: { runId: "run-cyber-real-runner", agentId: "main", sessionId: "session-1" },
        harness: {
          workspaceDir: "/tmp/workspace",
          preparation: { kind: "direct" as const },
          resolveRuntimeOverride: () => undefined,
        },
        behavior: { kind: "command-rpc", hasCommittedSideEffect: () => delivered },
        sessionOverride: { kind: "preserve" },
        runCandidate: async (provider, model) => {
          if (model === "gpt-daybreak-blue-latest") {
            delivered = true;
            throw failure;
          }
          return makeRefusalResult({ provider, model });
        },
      }),
    ).rejects.toBe(failure);

    expect(delivered).toBe(true);
  });

  it("restores the original refusal when the Daybreak retry fails without committing work", async () => {
    const failure = new FailoverError("daybreak overloaded before any output", {
      provider: "openai",
      model: "gpt-daybreak-blue-latest",
      reason: "overloaded",
    });

    const result = await runEmbeddedAgentEntry({
      selection: { cfg: {}, provider: "openai", model: "gpt-5.6" },
      identity: { runId: "run-cyber-real-runner-clean", agentId: "main", sessionId: "session-1" },
      harness: {
        workspaceDir: "/tmp/workspace",
        preparation: { kind: "direct" as const },
        resolveRuntimeOverride: () => undefined,
      },
      behavior: { kind: "command-rpc", hasCommittedSideEffect: () => false },
      sessionOverride: { kind: "preserve" },
      runCandidate: async (provider, model) => {
        if (model === "gpt-daybreak-blue-latest") {
          throw failure;
        }
        return makeRefusalResult({ provider, model });
      },
    });

    expect(result.model).toBe("gpt-5.6");
    expect(result.result.payloads).toEqual([{ text: "policy refusal", isError: true }]);
  });
});
