import {
  copyReplyPayloadMetadata,
  getReplyPayloadMetadata,
  setReplyPayloadMetadata,
  type ReplyPayload,
} from "../../auto-reply/reply-payload.js";
import { assertReplyPayloadSessionWriterDeliveryAuthorized } from "../../auto-reply/reply/session-writer-delivery-authority.js";
import { PlatformMessageNotDispatchedError } from "../../infra/outbound/deliver-types.js";
import { settlePendingFinalDelivery } from "../../infra/outbound/delivery-completion.js";
import type { ChannelDeliveryInfo } from "./types.js";

type DirectPendingFinalCustody = Pick<ChannelDeliveryInfo, "bindPendingFinalDelivery"> & {
  assertPlatformSendAuthorized: () => void;
  onPlatformSendDispatch: () => Promise<void>;
};

export const NO_PENDING_FINAL_CUSTODY: DirectPendingFinalCustody = {
  assertPlatformSendAuthorized: () => undefined,
  onPlatformSendDispatch: () => Promise.resolve(),
};

export function resolvePendingFinalCompletion(payload: ReplyPayload) {
  const identity = getReplyPayloadMetadata(payload)?.pendingFinalDeliveryCompletion;
  return identity ? { kind: "pending-final" as const, ...identity } : undefined;
}

export function createDirectPendingFinalCustody(
  payload: ReplyPayload,
  fallbackStorePath?: string,
): DirectPendingFinalCustody | undefined {
  const completion = resolvePendingFinalCompletion(payload);
  const hasWriterAuthority = Boolean(
    getReplyPayloadMetadata(payload)?.sessionWriterDeliveryAuthority,
  );
  if (!completion && !hasWriterAuthority) {
    return undefined;
  }
  const identity = completion ? (({ kind: _kind, ...value }) => value)(completion) : undefined;
  let firstDispatch = true;
  let admissionTail = Promise.resolve();
  return {
    bindPendingFinalDelivery: (nextPayload) =>
      identity
        ? setReplyPayloadMetadata(nextPayload, {
            pendingFinalDeliveryCompletion: identity,
          })
        : nextPayload,
    assertPlatformSendAuthorized: () =>
      assertReplyPayloadSessionWriterDeliveryAuthorized(payload, fallbackStorePath),
    onPlatformSendDispatch: () => {
      const expectedStates = firstDispatch
        ? (["prepared", "queued"] as const)
        : (["unknown"] as const);
      firstDispatch = false;
      const admission = admissionTail.then(async () => {
        assertReplyPayloadSessionWriterDeliveryAuthorized(payload, fallbackStorePath);
        if (!completion) {
          return;
        }
        const result = await settlePendingFinalDelivery(completion, "unknown", expectedStates);
        if (result.state !== "unknown") {
          throw new PlatformMessageNotDispatchedError(
            "Pending final delivery ownership changed before platform dispatch",
            { cause: new Error(`pending final delivery is ${result.state}`) },
          );
        }
      });
      // Every physical post must observe the state left by the prior post's check.
      admissionTail = admission.catch(() => undefined);
      return admission;
    },
  };
}

/** Send authority only: individual presentation payloads cannot settle the batch receipt. */
export type DirectPendingFinalBatchCustody = Pick<
  DirectPendingFinalCustody,
  "assertPlatformSendAuthorized" | "onPlatformSendDispatch"
>;

/** Transfer the command's single receipt to one owner before splitting presentation. */
export async function claimDirectPendingFinalBatch(payloads: ReplyPayload[]) {
  const ownerPayload = payloads.find((payload) => resolvePendingFinalCompletion(payload));
  if (!ownerPayload) {
    return undefined;
  }
  const completion = resolvePendingFinalCompletion(ownerPayload)!;
  const direct = createDirectPendingFinalCustody(ownerPayload)!;
  direct.assertPlatformSendAuthorized();
  const claim = await settlePendingFinalDelivery(completion, "queued", ["prepared"]);
  if (claim.state !== "queued") {
    throw new PlatformMessageNotDispatchedError("Pending final batch is no longer prepared", {
      cause: new Error("pending final delivery is " + claim.state),
    });
  }
  let active = true;
  const assertActive = () => {
    if (!active) {
      throw new PlatformMessageNotDispatchedError("Pending final batch custody is closed", {
        cause: new Error("Command presentation has settled"),
      });
    }
    direct.assertPlatformSendAuthorized();
  };
  const custody: DirectPendingFinalBatchCustody = {
    assertPlatformSendAuthorized: assertActive,
    onPlatformSendDispatch: async () => {
      assertActive();
      await direct.onPlatformSendDispatch();
      assertActive();
    },
  };
  return {
    custody,
    // This is an explicit transfer, not a data-only clone: keep writer authority,
    // hooks and other private metadata, but never lend the batch receipt to an
    // independently settling dispatcher or durable transport queue.
    payloads: payloads.map((payload) =>
      setReplyPayloadMetadata(copyReplyPayloadMetadata(payload, { ...payload }), {
        pendingFinalDeliveryCompletion: undefined,
      }),
    ),
    settle: async (state: "prepared" | "delivered" | "suppressed" | "unknown") => {
      active = false;
      // Receipt settlement deliberately does not clear the recovery claim. The
      // paused command owns cleanup after the presenter, including teardown.
      await settlePendingFinalDelivery(completion, state, ["queued", "unknown"]);
    },
  };
}

export function toCoreManagedDeliveryInfo(info: ChannelDeliveryInfo) {
  return {
    kind: info.kind,
    ...(info.participant ? { participant: info.participant } : {}),
    ...(info.assistantMessageIndex === undefined
      ? {}
      : { assistantMessageIndex: info.assistantMessageIndex }),
  };
}
