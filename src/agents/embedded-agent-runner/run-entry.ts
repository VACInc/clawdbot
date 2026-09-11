import type { OpenClawConfig } from "../../config/types.openclaw.js";
import type { ContextEngineHostSupport } from "../../context-engine/host-compat.js";
import { requireActivePluginRegistry } from "../../plugins/runtime.js";
import {
  createAssistantErrorTranscript,
  type AssistantErrorTranscript,
} from "../assistant-error-transcript.js";
import { resolveModelFallbackError } from "../failover-error.js";
import {
  createContextEngineLogicalTurnLease,
  type ContextEngineLogicalTurnLease,
} from "../harness/context-engine-logical-turn.js";
import {
  discardContextEngineTurnAttemptIntent,
  finalizeAcceptedContextEngineTurn,
  type ContextEngineTurnAttemptFacts,
} from "../harness/context-engine-turn-attempt.js";
import { ensureSelectedAgentHarnessPlugin } from "../harness/runtime-plugin.js";
import { selectAgentHarness } from "../harness/selection.js";
import type { ModelFallbackResultClassification } from "../model-fallback-attempt.js";
import type { ModelFallbackStepFields } from "../model-fallback-observation.js";
import { runWithModelFallback } from "../model-fallback-runner.js";
import type {
  FallbackAttempt,
  ModelFallbackAttemptProvenance,
  ModelFallbackRouteResolution,
} from "../model-fallback.types.js";
import type { ModelManifestNormalizationContext } from "../model-ref-shared.js";
import { resolveAgentRunAbortLifecycleFields } from "../run-termination.js";
import {
  EMBEDDED_CYBER_FAILOVER_TRIGGER_CODE,
  isEmbeddedCyberFailoverTargetSkipped,
  isEmbeddedCyberFailoverTargetUsable,
  isSameEmbeddedCyberFailoverTarget,
  recordEmbeddedCyberFailoverTargetUnavailable,
  resolveEmbeddedCyberFailoverConfig,
  resolveEmbeddedCyberFailoverTarget,
} from "./embedded-cyber-failover.js";
import {
  classifyEmbeddedAgentRunResultForModelFallback,
  mergeEmbeddedAgentRunResultForModelFallbackExhaustion,
} from "./result-fallback-classifier.js";
import {
  buildRunEntryTerminal,
  canAdvanceContextEngineTurn,
  mergeRunEntryExecutionTrace,
  resolveRunEntryTerminalOutcome,
  type EmbeddedAgentRunEntryTerminal,
  type RunEntryTerminalBehavior,
} from "./run-entry-terminal.js";
import type { EmbeddedAgentRunResult } from "./types.js";

export type { EmbeddedAgentRunEntryTerminal } from "./run-entry-terminal.js";

type RunEntryCandidateOptions = {
  assistantErrorTranscript: AssistantErrorTranscript;
  classifyResult: (result: EmbeddedAgentRunResult) => ModelFallbackResultClassification;
  allowTransientCooldownProbe?: boolean;
  isFinalFallbackAttempt?: boolean;
  isFallbackRetry: boolean;
  modelRoutingProvenance: ModelFallbackAttemptProvenance;
  contextEngineLogicalTurnLease: ContextEngineLogicalTurnLease;
  onContextEngineTurnCandidate: (facts: ContextEngineTurnAttemptFacts) => void;
};

type RunEntryCandidate<T> = {
  result: T;
  classification?: ModelFallbackResultClassification;
  turnAttempt?: ContextEngineTurnAttemptFacts;
};

type RunEntryHarnessPreparation =
  | { kind: "direct" }
  | {
      kind: "measured";
      run: (prepare: () => Promise<void>) => Promise<void>;
    };

type RunEntryBehavior = RunEntryTerminalBehavior;

type RunEntrySessionOverride =
  | { kind: "preserve" }
  | {
      kind: "reconcile-completed";
      reconcile: (candidate: { provider: string; model: string }) => Promise<void>;
    };

type EmbeddedAgentRunEntryResult<T extends EmbeddedAgentRunResult> = {
  outcome: "completed" | "exhausted";
  result: T;
  provider: string;
  model: string;
  attempts: FallbackAttempt[];
  terminal: EmbeddedAgentRunEntryTerminal;
  settleSessionOverride: () => Promise<void>;
};

type EmbeddedAgentRunEntryParams<T extends EmbeddedAgentRunResult> = {
  selection: {
    cfg: OpenClawConfig;
    provider: string;
    model: string;
    requestedRouteResolution?: ModelFallbackRouteResolution;
    fallbacksOverride?: string[];
    agentDir?: string;
    userLockedAuthProfileId?: string;
  } & ModelManifestNormalizationContext;
  identity: {
    runId: string;
    agentId: string;
    sessionId: string;
    sessionKey?: string;
    lane?: string;
  };
  harness: {
    workspaceDir: string;
    sessionKey?: string;
    preparation: RunEntryHarnessPreparation;
    resolveRuntimeOverride: (provider: string, model: string) => string | undefined;
    resolveContextEngineHost?: (
      provider: string,
      model: string,
    ) => ContextEngineHostSupport | undefined;
  };
  behavior: RunEntryBehavior;
  sessionOverride: RunEntrySessionOverride;
  abortSignal?: AbortSignal;
  onFallbackStep?: (step: ModelFallbackStepFields) => void | Promise<void>;
  /** Runs once after the successful winner is accepted, before post-turn context commit. */
  onAcceptedTerminal?: () => void | (() => void) | Promise<void | (() => void)>;
  runCandidate: (provider: string, model: string, options: RunEntryCandidateOptions) => Promise<T>;
};

const PRESERVED_FOLLOWUP_RESULT_CODES = new Set([
  "empty_result",
  "reasoning_only_result",
  "planning_only_result",
]);

function preserveFollowupResultForDelivery(
  classification: ModelFallbackResultClassification,
): ModelFallbackResultClassification {
  if (
    !classification ||
    !("code" in classification) ||
    !classification.code ||
    !PRESERVED_FOLLOWUP_RESULT_CODES.has(classification.code)
  ) {
    return classification;
  }
  // Follow-up delivery owns its terminal fallback, so retain the classified
  // result for that layer instead of replacing it with a summary error.
  return {
    ...classification,
    preserveResultOnExhaustion: true,
    preserveResultPriority: -1,
  };
}

/** Runs one logical turn across model candidates and advances only the accepted winner. */
export async function runEmbeddedAgentEntry<T extends EmbeddedAgentRunResult>(
  params: EmbeddedAgentRunEntryParams<T>,
): Promise<EmbeddedAgentRunEntryResult<T>> {
  const contextEngineLogicalTurnLease = await createContextEngineLogicalTurnLease({
    identity: params.identity,
    config: params.selection.cfg,
    agentDir: params.selection.agentDir,
    workspaceDir: params.harness.workspaceDir,
  });
  const assistantErrorTranscript = createAssistantErrorTranscript({
    runId: params.identity.runId,
    config: params.selection.cfg,
  });
  let failed = true;
  let unsettledContextEngineTurnAttempt: ContextEngineTurnAttemptFacts | undefined;
  let candidateIndex = 0;
  const committedSideEffect =
    params.behavior.kind === "command-rpc" ? params.behavior.hasCommittedSideEffect : undefined;
  const readChannelDeliveryEvidence =
    params.behavior.kind === "channel-delivery" ? params.behavior.readDeliveryEvidence : undefined;
  const preparedHarnessRuntimes = new Set<string>();
  const prepareHarnessRuntime = async (candidate: {
    provider: string;
    model: string;
    agentHarnessRuntimeOverride?: string;
  }) => {
    assistantErrorTranscript.clear();
    const key = [
      candidate.provider,
      candidate.model,
      candidate.agentHarnessRuntimeOverride ?? "",
    ].join("\0");
    if (preparedHarnessRuntimes.has(key)) {
      return;
    }
    const prepare = () =>
      ensureSelectedAgentHarnessPlugin({
        config: params.selection.cfg,
        provider: candidate.provider,
        modelId: candidate.model,
        agentId: params.identity.agentId,
        sessionKey: params.harness.sessionKey,
        agentHarnessId: candidate.agentHarnessRuntimeOverride,
        agentHarnessRuntimeOverride: candidate.agentHarnessRuntimeOverride,
        workspaceDir: params.harness.workspaceDir,
        pluginRegistry: requireActivePluginRegistry(),
      });
    if (params.harness.preparation.kind === "measured") {
      await params.harness.preparation.run(prepare);
    } else {
      await prepare();
    }
    preparedHarnessRuntimes.add(key);
  };
  // Thrown candidate errors skip result classification, so without an error-path
  // backstop the loop advances to the next candidate even when the attempt already
  // delivered its reply, producing a duplicate visible answer (#113788). Consult the
  // same live delivery evidence the result classifier already uses so both exit
  // paths suppress fallback after a delivered reply.
  const canFallbackAfterError = committedSideEffect
    ? () => !committedSideEffect()
    : readChannelDeliveryEvidence
      ? () => {
          const evidence = readChannelDeliveryEvidence();
          return !evidence.hasDirectlySentBlockReply && !evidence.hasBlockReplyPipelineOutput;
        }
      : undefined;
  try {
    let capturedCyberRefusal: { provider: string; model: string } | undefined;
    const runFallbackSearch = (
      selection: EmbeddedAgentRunEntryParams<T>["selection"],
      runOptions: { captureCyberRefusal?: boolean; forceFallbackRetry?: boolean } = {},
    ) =>
      runWithModelFallback<RunEntryCandidate<T>>({
        ...selection,
        ...params.identity,
        abortSignal: params.abortSignal,
        resolveAgentHarnessRuntimeOverride: params.harness.resolveRuntimeOverride,
        prepareCandidateChain: async (candidates) => {
          for (const candidate of candidates) {
            try {
              const agentHarnessRuntimeOverride = params.harness.resolveRuntimeOverride(
                candidate.provider,
                candidate.model,
              );
              await prepareHarnessRuntime({
                provider: candidate.provider,
                model: candidate.model,
                ...(agentHarnessRuntimeOverride ? { agentHarnessRuntimeOverride } : {}),
              });
              const resolvedHost = params.harness.resolveContextEngineHost?.(
                candidate.provider,
                candidate.model,
              );
              const host =
                resolvedHost ??
                (() => {
                  const harness = selectAgentHarness({
                    provider: candidate.provider,
                    modelId: candidate.model,
                    config: params.selection.cfg,
                    agentId: params.identity.agentId,
                    sessionKey: params.harness.sessionKey,
                    agentHarnessRuntimeOverride,
                  });
                  return {
                    id: `agent-harness:${harness.id}`,
                    label: `agent harness "${harness.id}"`,
                    capabilities: harness.contextEngineHostCapabilities ?? [],
                  };
                })();
              contextEngineLogicalTurnLease.selectForHost({
                host,
                operation: "agent-run",
                requiresDurableCommit: false,
              });
            } catch {
              contextEngineLogicalTurnLease.degradeBeforeStart(
                "a model fallback candidate harness could not be validated before dispatch",
              );
              return;
            }
          }
        },
        prepareAgentHarnessRuntime: prepareHarnessRuntime,
        onFallbackStep: params.onFallbackStep,
        ...(params.behavior.kind === "maintenance"
          ? {}
          : {
              classifyResult: ({ result }: { result: RunEntryCandidate<T> }) =>
                result.result.meta.modelFallbackStopReason
                  ? { stopReason: result.result.meta.modelFallbackStopReason }
                  : result.classification,
            }),
        ...(canFallbackAfterError ? { canFallbackAfterError } : {}),
        ...(params.behavior.kind === "maintenance"
          ? {}
          : {
              mergeExhaustedResult: ({
                latestResult,
                preferredResult,
              }: {
                latestResult: RunEntryCandidate<T>;
                preferredResult: RunEntryCandidate<T>;
              }) => ({
                result: mergeEmbeddedAgentRunResultForModelFallbackExhaustion({
                  latestResult: latestResult.result,
                  preferredResult: preferredResult.result,
                }) as T,
                turnAttempt: latestResult.turnAttempt,
              }),
            }),
        run: async (provider, model, options) => {
          assistantErrorTranscript.clear();
          if (!options) {
            throw new Error("Model fallback attempt is missing routing provenance");
          }
          const isFallbackRetry = runOptions.forceFallbackRetry === true || candidateIndex > 0;
          candidateIndex += 1;
          let contextEngineTurnCandidate: ContextEngineTurnAttemptFacts | undefined;
          let classified:
            | { result: EmbeddedAgentRunResult; value: ModelFallbackResultClassification }
            | undefined;
          const classifyResult = (result: EmbeddedAgentRunResult) => {
            if (!classified || classified.result !== result) {
              const classification =
                params.behavior.kind === "maintenance"
                  ? undefined
                  : classifyEmbeddedAgentRunResultForModelFallback({
                      result,
                      provider,
                      model,
                      ...readChannelDeliveryEvidence?.(),
                    });
              const effectiveClassification =
                params.behavior.kind === "followup-delivery"
                  ? preserveFollowupResultForDelivery(classification)
                  : classification;
              // Keep pre-release acceptance for the exact result. Failed finalization
              // returns a replacement that must not inherit its predecessor's decision.
              const acceptedClassification =
                effectiveClassification && committedSideEffect?.()
                  ? undefined
                  : effectiveClassification;
              const cyberRefusal =
                acceptedClassification &&
                "code" in acceptedClassification &&
                acceptedClassification.code === EMBEDDED_CYBER_FAILOVER_TRIGGER_CODE;
              if (runOptions.captureCyberRefusal && cyberRefusal) {
                capturedCyberRefusal = { provider, model };
              }
              classified = {
                result,
                value:
                  runOptions.captureCyberRefusal && cyberRefusal
                    ? undefined
                    : acceptedClassification,
              };
            }
            return classified.value;
          };
          const result = await params.runCandidate(provider, model, {
            assistantErrorTranscript,
            classifyResult,
            allowTransientCooldownProbe: options?.allowTransientCooldownProbe,
            isFinalFallbackAttempt: options?.isFinalFallbackAttempt,
            isFallbackRetry,
            modelRoutingProvenance: runOptions.forceFallbackRetry
              ? {
                  ...options.modelRoutingProvenance,
                  stage: "fallback",
                  fallbackReason: "unknown",
                }
              : options.modelRoutingProvenance,
            contextEngineLogicalTurnLease,
            onContextEngineTurnCandidate: (facts) => {
              contextEngineTurnCandidate = facts;
              unsettledContextEngineTurnAttempt = facts;
            },
          });
          return {
            result,
            classification: classifyResult(result),
            turnAttempt: contextEngineTurnCandidate,
          };
        },
      });

    const originalFallbackResult = await runFallbackSearch(params.selection, {
      captureCyberRefusal: true,
    });
    const originalErrorTranscript = assistantErrorTranscript.snapshot();
    let fallbackResult = originalFallbackResult;
    let policyEscalated = false;
    const cyberFailover = resolveEmbeddedCyberFailoverConfig(params.selection.cfg);
    const target =
      capturedCyberRefusal && cyberFailover.mode === "auto"
        ? resolveEmbeddedCyberFailoverTarget({
            cfg: params.selection.cfg,
            agentId: params.identity.agentId,
            raw: cyberFailover.model,
            manifestPlugins: params.selection.manifestPlugins,
          })
        : null;
    const authScope = params.selection.userLockedAuthProfileId?.trim() || undefined;
    if (
      capturedCyberRefusal &&
      target &&
      !isSameEmbeddedCyberFailoverTarget(capturedCyberRefusal, target) &&
      !isEmbeddedCyberFailoverTargetSkipped({
        sessionId: params.identity.sessionId,
        target,
        authScope,
      })
    ) {
      if (originalFallbackResult.result.turnAttempt) {
        discardContextEngineTurnAttemptIntent({
          facts: originalFallbackResult.result.turnAttempt,
          lease: contextEngineLogicalTurnLease,
        });
        unsettledContextEngineTurnAttempt = undefined;
      }
      try {
        const targetFallbackResult = await runFallbackSearch(
          {
            ...params.selection,
            provider: target.provider,
            model: target.model,
            requestedRouteResolution: "resolved",
            fallbacksOverride: [],
          },
          { forceFallbackRetry: true },
        );
        if (
          targetFallbackResult.outcome === "completed" &&
          isEmbeddedCyberFailoverTargetUsable(targetFallbackResult.result.result)
        ) {
          policyEscalated = true;
          fallbackResult = {
            ...targetFallbackResult,
            attempts: [
              ...originalFallbackResult.attempts,
              {
                provider: capturedCyberRefusal.provider,
                model: capturedCyberRefusal.model,
                error: "OpenAI cyber policy refusal",
                reason: "unknown",
                code: EMBEDDED_CYBER_FAILOVER_TRIGGER_CODE,
              },
              ...targetFallbackResult.attempts,
            ],
          };
        } else {
          if (targetFallbackResult.result.turnAttempt) {
            discardContextEngineTurnAttemptIntent({
              facts: targetFallbackResult.result.turnAttempt,
              lease: contextEngineLogicalTurnLease,
            });
            unsettledContextEngineTurnAttempt = undefined;
          }
          recordEmbeddedCyberFailoverTargetUnavailable({
            sessionId: params.identity.sessionId,
            target,
            authScope,
            attempts: targetFallbackResult.attempts,
            cooloffMs: cyberFailover.cooloffMs,
          });
          assistantErrorTranscript.restore(originalErrorTranscript);
          fallbackResult = {
            ...originalFallbackResult,
            result: { ...originalFallbackResult.result, turnAttempt: undefined },
          };
        }
      } catch (error) {
        const resolution = resolveModelFallbackError(error, {
          provider: target.provider,
          model: target.model,
          sessionId: params.identity.sessionId,
          lane: params.identity.lane,
        });
        if (resolution.kind === "coordination") {
          throw error;
        }
        if (
          resolution.kind === "failover" &&
          (resolution.error.reason === "auth" || resolution.error.reason === "auth_permanent")
        ) {
          recordEmbeddedCyberFailoverTargetUnavailable({
            sessionId: params.identity.sessionId,
            target,
            authScope,
            attempts: [
              {
                provider: target.provider,
                model: target.model,
                error: resolution.error.message,
                reason: resolution.error.reason,
                code: resolution.error.code,
              },
            ],
            cooloffMs: cyberFailover.cooloffMs,
          });
        }
        assistantErrorTranscript.restore(originalErrorTranscript);
        fallbackResult = {
          ...originalFallbackResult,
          result: { ...originalFallbackResult.result, turnAttempt: undefined },
        };
      }
    }
    const abortFields =
      params.behavior.kind === "command-rpc"
        ? resolveAgentRunAbortLifecycleFields(params.abortSignal)
        : {};
    const candidateResult =
      abortFields.aborted === true
        ? ({
            ...fallbackResult.result.result,
            meta: {
              ...fallbackResult.result.result.meta,
              ...abortFields,
            },
          } as T)
        : fallbackResult.result.result;
    const outcome =
      fallbackResult.outcome === "exhausted" ? ("exhausted" as const) : ("completed" as const);
    // A completed fallback search can still return a failed or interrupted run.
    const terminalOutcome = resolveRunEntryTerminalOutcome({
      result: candidateResult,
      fallbackExhausted: outcome === "exhausted",
    });
    failed = terminalOutcome.status === "error";
    const result = mergeRunEntryExecutionTrace({
      result: candidateResult,
      terminalStatus: terminalOutcome.status,
      provider: fallbackResult.provider,
      model: fallbackResult.model,
      requestedProvider: params.selection.provider,
      requestedModel: params.selection.model,
      fallbackAttempts: fallbackResult.attempts,
    });
    const settledResult = {
      ...fallbackResult,
      outcome,
      result,
    };
    const terminal = buildRunEntryTerminal({
      result,
      outcome: terminalOutcome,
      behavior: params.behavior,
      runId: params.identity.runId,
      requested: { provider: params.selection.provider, model: params.selection.model },
      sessionId: params.identity.sessionId,
    });
    const acceptedTerminal =
      !params.abortSignal?.aborted &&
      canAdvanceContextEngineTurn({
        result,
        fallbackOutcome: settledResult.outcome,
        terminal,
      });
    let releaseAcceptedTerminalWork: (() => void) | undefined;
    if (acceptedTerminal) {
      const acceptedTerminalWork = await params.onAcceptedTerminal?.();
      if (typeof acceptedTerminalWork === "function") {
        releaseAcceptedTerminalWork = acceptedTerminalWork;
      }
    }
    try {
      if (fallbackResult.result.turnAttempt) {
        if (acceptedTerminal) {
          await finalizeAcceptedContextEngineTurn({
            facts: fallbackResult.result.turnAttempt,
            lease: contextEngineLogicalTurnLease,
          });
        } else {
          discardContextEngineTurnAttemptIntent({
            facts: fallbackResult.result.turnAttempt,
            lease: contextEngineLogicalTurnLease,
          });
        }
        unsettledContextEngineTurnAttempt = undefined;
      }
    } finally {
      releaseAcceptedTerminalWork?.();
    }
    let sessionOverrideSettled = false;
    const settleSessionOverride = async () => {
      if (sessionOverrideSettled) {
        return;
      }
      sessionOverrideSettled = true;
      if (
        !policyEscalated &&
        settledResult.outcome === "completed" &&
        params.sessionOverride.kind === "reconcile-completed"
      ) {
        await params.sessionOverride.reconcile({
          provider: settledResult.provider,
          model: settledResult.model,
        });
      }
    };
    return { ...settledResult, terminal, settleSessionOverride };
  } finally {
    if (unsettledContextEngineTurnAttempt) {
      discardContextEngineTurnAttemptIntent({
        facts: unsettledContextEngineTurnAttempt,
        lease: contextEngineLogicalTurnLease,
      });
    }
    try {
      await assistantErrorTranscript.settle(failed && !params.abortSignal?.aborted);
    } finally {
      await contextEngineLogicalTurnLease.dispose();
    }
  }
}
