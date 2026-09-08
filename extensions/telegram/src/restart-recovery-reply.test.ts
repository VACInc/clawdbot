import { expect, it, vi } from "vitest";
import {
  describeTelegramDispatch,
  createBot,
  setupDraftStreams,
  createTelegramDraftStream,
  deliverReplies,
  mockCallArg,
} from "./bot-message-dispatch.test-harness.js";
import { dispatchTelegramRecoveryReply } from "./restart-recovery-reply.js";

vi.mock("./send-context.js", () => ({
  withTelegramApiContext: async (
    params: { cfg: unknown; accountId?: string },
    run: (ctx: unknown) => Promise<void>,
  ) =>
    run({
      api: createBot().api,
      account: {
        accountId: params.accountId ?? "default",
        token: "123:test",
        config: { streaming: { mode: "partial" } },
      },
    }),
}));

describeTelegramDispatch("restart recovery Telegram presentation", () => {
  it.each(
    [
      { to: "-100123", threadId: "42", expectedThread: 42 },
      { to: "123:topic:77", threadId: undefined, expectedThread: 77 },
    ].flatMap((route) =>
      [false, true].map((verbose) => ({
        to: route.to,
        threadId: route.threadId,
        expectedThread: route.expectedThread,
        verbose,
      })),
    ),
  )(
    "presents tools and final text in $to with verbose=$verbose",
    async ({ to, threadId, expectedThread, verbose }) => {
      const { answerDraftStream } = setupDraftStreams({ answerMessageId: 2001 });
      const dispatchReplyFromConfig = vi.fn<
        Parameters<typeof dispatchTelegramRecoveryReply>[0]["dispatchReplyFromConfig"]
      >(async ({ dispatcher, replyOptions }) => {
        if (!dispatcher.appendBeforeDeliver) {
          throw new Error("Expected the host-managed reply dispatcher");
        }
        dispatcher.appendBeforeDeliver((payload, info) => {
          Object.assign(info, {
            assertPlatformSendAuthorized: () => {},
            onPlatformSendDispatch: async () => {},
          });
          return payload;
        });
        replyOptions?.onVerboseProgressVisibility?.(() => verbose);
        await replyOptions?.onToolStart?.({
          name: "read",
          phase: "start",
          args: { path: "README.md" },
        });
        if (verbose) {
          dispatcher.sendToolResult({ text: "Reading README.md" });
        }
        await replyOptions?.onPartialReply?.({ text: "Working on the reply" });
        const queuedFinal = dispatcher.sendFinalReply({
          text: "Finished the interrupted response",
        });
        return { queuedFinal, counts: dispatcher.getQueuedCounts() };
      });
      await dispatchTelegramRecoveryReply({
        cfg: {},
        agentId: "default",
        accountId: "work",
        sessionKey: to.startsWith("-")
          ? "agent:default:telegram:group:-100123:topic:42"
          : "agent:default:telegram:direct:123:thread:77",
        sessionId: "session",
        to,
        threadId,
        assertCurrent: () => {},
        dispatchReplyFromConfig,
      });
      expect(dispatchReplyFromConfig).toHaveBeenCalledOnce();
      expect(mockCallArg(createTelegramDraftStream)).toMatchObject({
        chatId: Number(to.split(":")[0]),
      });
      // Verbose output is durable; ordinary Telegram intentionally avoids a duplicate tool preview.
      expect(answerDraftStream.updatePreview).toHaveBeenCalledTimes(verbose ? 0 : 1);
      expect(answerDraftStream.update).toHaveBeenCalledWith(
        "Finished the interrupted response",
        expect.objectContaining({
          assertPlatformSendAuthorized: expect.any(Function),
          onPlatformSendDispatch: expect.any(Function),
        }),
      );
      expect(deliverReplies).toHaveBeenCalledTimes(verbose ? 1 : 0);
      if (verbose) {
        expect(deliverReplies).toHaveBeenCalledWith(
          expect.objectContaining({
            replies: [expect.objectContaining({ text: "Reading README.md" })],
          }),
        );
      }
      const draft = mockCallArg(createTelegramDraftStream) as { thread?: { id?: number } };
      expect(draft.thread?.id).toBe(expectedThread);
    },
  );
  it("refuses an unguarded recovery final while retaining normal error feedback", async () => {
    const { answerDraftStream } = setupDraftStreams({ answerMessageId: 2001 });
    await dispatchTelegramRecoveryReply({
      cfg: {},
      agentId: "default",
      accountId: "work",
      sessionKey: "agent:default:telegram:direct:123",
      sessionId: "session",
      to: "123",
      assertCurrent: () => {},
      dispatchReplyFromConfig: async ({ dispatcher }) => {
        const queuedFinal = dispatcher.sendFinalReply({ text: "Unguarded recovery final" });
        await dispatcher.waitForIdle();
        return { queuedFinal, counts: dispatcher.getQueuedCounts() };
      },
    });
    expect(answerDraftStream.update).not.toHaveBeenCalled();
    // Refusing the logical final must not suppress Telegram's existing error notice.
    expect(deliverReplies).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        replies: [{ text: "No response generated. Please try again." }],
      }),
    );
  });
});
