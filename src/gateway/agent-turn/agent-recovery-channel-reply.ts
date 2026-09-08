import type { AgentCommandDeliveryStatus } from "../../agents/command/delivery.js";
import type { AgentCommandGatewayIngressOpts } from "../../agents/command/types.js";
import type { ReplyPayload, ReplyPayloadMetadata } from "../../auto-reply/reply-payload.js";
import type { InternalReplyResolverOptions } from "../../auto-reply/reply/dispatch-from-config.events.js";
import type { InternalGetReplyOptions } from "../../auto-reply/reply/get-reply.types.js";
import type { SerializedDurableMessagePayloadOutcome } from "../../channels/message/send.js";
import { getChannelPlugin } from "../../channels/plugins/registry.js";
import type { DirectPendingFinalBatchCustody } from "../../channels/turn/direct-delivery-custody.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { createDeferredCore } from "../../shared/deferred.js";

/** Execution retains its recovery claim until the ordinary channel dispatcher settles delivery. */
export async function runAgentWithRecoveryChannelReply<T>(params: {
  opts: AgentCommandGatewayIngressOpts;
  cfg: OpenClawConfig;
  /** Exact Gateway operational owner, independently of persisted recovery facts. */
  assertCurrent: () => void;
  run: (opts: AgentCommandGatewayIngressOpts) => Promise<T>;
}): Promise<T> {
  const { opts } = params;
  if (
    !opts.mainRestartRecoveryAdmitted ||
    !opts.deliver ||
    opts.sourceReplyDeliveryMode === "message_tool_only" ||
    !opts.channel ||
    !opts.to ||
    !opts.sessionKey ||
    !opts.sessionId ||
    !opts.agentId
  ) {
    return params.run(opts);
  }
  if (!opts.runId || !opts.lifecycleGeneration) {
    throw new Error("Restart continuation is missing its Gateway run owner");
  }
  const present = getChannelPlugin(opts.channel)?.streaming?.dispatchRecoveryReply;
  if (!present) {
    return params.run(opts);
  }
  const [
    { dispatchReplyFromConfig },
    { REPLY_OPERATION_RUN_STATE },
    { createDirectPendingFinalCustody, NO_PENDING_FINAL_CUSTODY },
    { resolveSessionStorePathCore },
    { getReplyPayloadMetadata, setReplyPayloadMetadata },
    { withReplyDispatcher },
    { isRestartRecoveryDeliveryCurrent },
  ] = await Promise.all([
    import("../../auto-reply/reply/dispatch-from-config.js"),
    import("../../auto-reply/reply/reply-operation-run-state.js"),
    import("../../channels/turn/direct-delivery-custody.js"),
    import("../../config/sessions/paths.js"),
    import("../../auto-reply/reply-payload.js"),
    import("../../auto-reply/dispatch-dispatcher.js"),
    import("../../agents/main-session-recovery/main-session-restart-recovery-delivery.js"),
  ]);
  const storePath = resolveSessionStorePathCore(params.cfg.session?.store, {
    agentId: opts.agentId,
  });
  const recoveryScope = {
    storePath,
    sessionKey: opts.sessionKey,
    sessionId: opts.sessionId,
    recoveryRunId: opts.runId,
    lifecycleGeneration: opts.lifecycleGeneration,
    cfg: params.cfg,
    deliveryContext: {
      channel: opts.channel,
      to: opts.to,
      accountId: opts.accountId,
      threadId: opts.threadId,
    },
  };
  const presentationAbort = new AbortController();
  const abortSignal = opts.abortSignal
    ? AbortSignal.any([opts.abortSignal, presentationAbort.signal])
    : presentationAbort.signal;
  let presentationState: "open" | "dispatching" | "closed" = "open";
  let presentationFailure: { error: unknown } | undefined;
  const assertCurrent = () => {
    abortSignal.throwIfAborted();
    if (presentationState === "closed") {
      throw new Error("Restart presentation is no longer active");
    }
    params.assertCurrent();
    if (!isRestartRecoveryDeliveryCurrent(recoveryScope)) {
      throw new Error("Restart recovery delivery is no longer current");
    }
  };
  const finalPayloads = createDeferredCore<ReplyPayload[]>();
  const finalDelivery = createDeferredCore<AgentCommandDeliveryStatus>();
  let execution: Promise<T> | undefined;
  const finalOutcomes: Array<
    Parameters<NonNullable<ReplyPayloadMetadata["onFinalDeliverySettled"]>> | undefined
  > = [];
  let offeredFinal = false;
  let emptyFinalFailed = false;
  let batchCustody: DirectPendingFinalBatchCustody | undefined;
  try {
    await present({
      cfg: params.cfg,
      agentId: opts.agentId,
      sessionKey: opts.sessionKey,
      sessionId: opts.sessionId,
      accountId: opts.accountId,
      to: opts.to,
      threadId: opts.threadId,
      abortSignal,
      assertCurrent,
      dispatchReplyFromConfig: async (dispatch) => {
        if (presentationState !== "open") {
          throw new Error("Restart presentation is no longer active or was already dispatched");
        }
        presentationState = "dispatching";
        assertCurrent();
        if (!dispatch.dispatcher.appendBeforeDeliver) {
          throw new Error("Restart presentation requires a guarded reply dispatcher");
        }
        // The ordinary inbound lifecycle installs this custody for provider-owned
        // sends. Recovery has no inbound event, so bind it at the host dispatcher.
        dispatch.dispatcher.appendBeforeDeliver((payload, info) => {
          assertCurrent();
          const custody =
            createDirectPendingFinalCustody(payload, storePath) ?? NO_PENDING_FINAL_CUSTODY;
          const finalBatch = info.kind === "final" ? batchCustody : undefined;
          Object.assign(info, {
            ...custody,
            onPlatformSendDispatch: async () => {
              assertCurrent();
              await custody.onPlatformSendDispatch();
              assertCurrent();
              await finalBatch?.onPlatformSendDispatch();
              assertCurrent();
            },
            assertPlatformSendAuthorized: () => {
              assertCurrent();
              custody.assertPlatformSendAuthorized();
              finalBatch?.assertPlatformSendAuthorized();
            },
          });
          return payload;
        });
        return await withReplyDispatcher({
          dispatcher: dispatch.dispatcher,
          run: () =>
            dispatchReplyFromConfig({
              ...dispatch,
              replyOptions: {
                ...dispatch.replyOptions,
                [REPLY_OPERATION_RUN_STATE]: { restartRecovery: true },
                runId: opts.runId,
                expectedExistingSessionId: opts.sessionId,
                pinExpectedExistingSession: true,
                abortSignal,
                sourceReplyDeliveryMode: opts.sourceReplyDeliveryMode,
              },
              replyResolver: async (
                _ctx,
                replyOptions: InternalGetReplyOptions & InternalReplyResolverOptions = {},
              ) => {
                abortSignal.throwIfAborted();
                if (presentationState !== "dispatching") {
                  throw new Error("Restart presentation is no longer active");
                }
                if (execution) {
                  throw new Error("Restart continuation was already dispatched");
                }
                execution = params.run({
                  ...opts,
                  // The dispatcher detaches its upstream listener on settlement,
                  // but the command still owns presentation teardown and cleanup.
                  abortSignal: replyOptions.abortSignal
                    ? AbortSignal.any([abortSignal, replyOptions.abortSignal])
                    : abortSignal,
                  onSessionIdChanged: (sessionId) => {
                    params.assertCurrent();
                    opts.onSessionIdChanged?.(sessionId);
                    recoveryScope.sessionId = sessionId;
                    replyOptions.replyOperation?.updateSessionId(sessionId);
                  },
                  channelReply: {
                    options: replyOptions,
                    onError: async (error) => {
                      if (!offeredFinal && presentationState === "dispatching") {
                        finalPayloads.reject(error);
                        await finalDelivery.promise;
                      }
                    },
                    deliverFinal: async (payloads, custody, deliberateSilentTerminalReply) => {
                      if (presentationState !== "dispatching") {
                        throw new Error("Restart presentation is no longer active");
                      }
                      if (offeredFinal) {
                        throw new Error("Restart final reply was already offered");
                      }
                      assertCurrent();
                      offeredFinal = true;
                      if (payloads.length === 0) {
                        emptyFinalFailed = deliberateSilentTerminalReply !== true;
                        if (deliberateSilentTerminalReply) {
                          replyOptions.onDeliberateSilentTerminalReply?.();
                        }
                      }
                      batchCustody = custody;
                      finalPayloads.resolve(
                        payloads.map((payload, index) => {
                          finalOutcomes.push(undefined);
                          const previous = getReplyPayloadMetadata(payload)?.onFinalDeliverySettled;
                          return setReplyPayloadMetadata(payload, {
                            onFinalDeliverySettled: (outcome, pending) => {
                              finalOutcomes[index] = [outcome, pending];
                              previous?.(outcome, pending);
                            },
                          });
                        }),
                      );
                      return finalDelivery.promise;
                    },
                  },
                });
                // The command pauses at its existing final-delivery checkpoint; the standard
                // dispatcher now owns TTS, hooks, source policy, previews, and delivery receipts.
                return await Promise.race([finalPayloads.promise, execution.then(() => undefined)]);
              },
            }),
        });
      },
    });
  } catch (error) {
    presentationFailure = { error };
  } finally {
    presentationState = "closed";
    if (execution && !offeredFinal) {
      presentationAbort.abort(
        presentationFailure?.error ?? new Error("Restart presentation ended"),
      );
    }
    const payloadOutcomes = finalOutcomes.map(
      (observed, index): SerializedDurableMessagePayloadOutcome => {
        const [outcome, pending] = observed ?? [];
        if (!pending && outcome === "delivered") {
          return { index, status: "sent", resultCount: 1 };
        }
        if (
          !pending &&
          (outcome === "cancelled" ||
            outcome === "delivered-not-visible" ||
            outcome === "channel-transform")
        ) {
          return { index, status: "suppressed", reason: "no_visible_result" };
        }
        return {
          index,
          status: "failed",
          error: outcome ?? "Final reply settlement unavailable",
          stage: "unknown",
          sentBeforeError: pending === true || outcome !== "failed-before-deliver",
        };
      },
    );
    const sent = payloadOutcomes.filter((outcome) => outcome.status === "sent").length;
    const failed =
      emptyFinalFailed || payloadOutcomes.some((outcome) => outcome.status === "failed");
    const settled = offeredFinal && !failed;
    finalDelivery.resolve({
      requested: true,
      attempted: sent > 0 || failed,
      status: settled ? (sent ? "sent" : "suppressed") : sent ? "partial_failed" : "failed",
      succeeded: settled ? true : sent ? "partial" : false,
      resultCount: sent,
      payloadOutcomes,
      ...(!settled ? { error: true, reason: "recovery_final_delivery_failed" } : {}),
    });
  }
  if (!execution) {
    if (presentationFailure) {
      throw presentationFailure.error;
    }
    throw new Error("Channel did not admit the restart continuation");
  }
  // Presentation may fail after the command pauses at delivery. Unblock it above,
  // then join its cleanup before the Gateway releases the recovery owner.
  let result: T;
  try {
    result = await execution;
  } catch (error) {
    throw presentationFailure ? presentationFailure.error : error;
  }
  if (presentationFailure) {
    throw presentationFailure.error;
  }
  return result;
}
