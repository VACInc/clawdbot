import { createServer, type ServerResponse } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { describe, expect, it } from "vitest";
import { telegramOutbound, telegramPlugin } from "../extensions/telegram/api.js";
import type { OpenClawConfig } from "../src/config/types.openclaw.js";
import { createGatewayInstanceRuntime } from "../src/gateway/server-instance-runtime.js";
import type { GatewayRequestContext } from "../src/gateway/server-methods/types.js";
import {
  captureActivePluginRegistrySnapshot,
  restoreActivePluginRegistrySnapshot,
  stageActivePluginRegistry,
} from "../src/plugins/runtime.js";
import { createDeferredCore } from "../src/shared/deferred.js";
import { createTestRegistry } from "../src/test-utils/channel-plugins.js";
import { withOpenClawTestState } from "../src/test-utils/openclaw-test-state.js";

// Only external HTTP is substituted: Gateway ownership and Telegram's client/throttler remain real.
describe("recovery notice final transport fence", () => {
  it.each(["allowed", "gateway closed", "policy revoked"] as const)(
    "checks %s after a real account throttle wait",
    async (mode) => {
      await withOpenClawTestState({ prefix: "notice-http-" }, async (state) => {
        const blocked = createDeferredCore<ServerResponse>();
        const preDispatch = createDeferredCore();
        const requests: string[] = [];
        const visible: string[] = [];
        const sockets = new Set<Socket>();
        const blockerText = "Independent throttle predecessor";
        const noticeText = "Recovery resumed";
        const accept = (response: ServerResponse, text: string) => {
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
        };
        const server = createServer((request, response) => {
          const chunks: Buffer[] = [];
          request.on("data", (chunk: Buffer) => chunks.push(chunk));
          request.on("end", () => {
            const payload = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { text?: string };
            const text = payload.text ?? "";
            requests.push(text);
            if (text === blockerText) {
              blocked.resolve(response);
            } else {
              accept(response, text);
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
        const cfg: OpenClawConfig = {
          channels: {
            telegram: {
              botToken: "123:notice-" + state.stateDir.split("/").at(-1),
              apiRoot: "http://127.0.0.1:" + (server.address() as AddressInfo).port,
            },
          },
        };
        const snapshot = captureActivePluginRegistrySnapshot();
        stageActivePluginRegistry(
          createTestRegistry([{ pluginId: "telegram", source: "test", plugin: telegramPlugin }]),
          null,
          "default",
        );
        const runtime = createGatewayInstanceRuntime({
          getContext: () => ({ deps: {}, getRuntimeConfig: () => cfg }) as GatewayRequestContext,
          getMethodRegistry: () => {
            throw new Error("Notice must not use RPC");
          },
          isDispatchAvailable: () => true,
        });
        const sendText = telegramOutbound.sendText;
        if (!sendText) {
          throw new Error("Missing Telegram sender");
        }
        let policyCurrent = true;
        let checks = 0;
        let held: ServerResponse | undefined;
        const blocker = sendText({ cfg, to: "123", accountId: "default", text: blockerText });
        let notice: Promise<{ result?: { suppressed: boolean }; error?: unknown }> | undefined;
        try {
          held = await Promise.race([
            blocked.promise,
            blocker.then(() => {
              throw new Error("Predecessor did not reach HTTP");
            }),
          ]);
          notice = runtime.recovery
            .sendRecoveryNotice({
              channel: "telegram",
              to: "123",
              accountId: "default",
              text: noticeText,
              idempotencyKey: "notice-" + mode,
              isCurrent: () => {
                checks++;
                if (checks >= 2) {
                  preDispatch.resolve();
                }
                return policyCurrent;
              },
            })
            .then(
              (result) => ({ result }),
              (error: unknown) => ({ error }),
            );
          await Promise.race([
            preDispatch.promise,
            notice.then(() => {
              throw new Error("Notice did not reach its pre-dispatch check");
            }),
          ]);
          // Finish the synchronous pre-dispatch continuation before changing its owner.
          await new Promise<void>((resolve) => {
            setImmediate(resolve);
          });
          expect(requests).toEqual([blockerText]);
          if (mode === "gateway closed") {
            runtime.close();
          }
          if (mode === "policy revoked") {
            policyCurrent = false;
          }
          accept(held, blockerText);
          await blocker;
          const outcome = await notice;
          if (mode === "allowed") {
            expect(outcome).toEqual({ result: { suppressed: false } });
            expect(visible).toEqual([blockerText, noticeText]);
            expect(requests).toEqual([blockerText, noticeText]);
          } else {
            expect(outcome.error).toBeDefined();
            expect(visible).toEqual([blockerText]);
            expect(requests).toEqual([blockerText]);
          }
        } finally {
          if (held && !held.writableEnded) {
            accept(held, blockerText);
          }
          await Promise.allSettled([blocker, ...(notice ? [notice] : [])]);
          runtime.close();
          restoreActivePluginRegistrySnapshot(snapshot);
          for (const socket of sockets) {
            socket.destroy();
          }
          await new Promise<void>((resolve) => {
            server.close(() => resolve());
          });
        }
      });
    },
  );
});
