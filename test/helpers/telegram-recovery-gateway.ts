import fs from "node:fs/promises";
import http from "node:http";
import type { AddressInfo } from "node:net";
import path from "node:path";
import type { OpenClawConfig } from "../../src/config/types.openclaw.js";
import { toErrorObject } from "../../src/infra/errors.js";
import type { CliBackendPlugin } from "../../src/plugins/cli-backend.types.js";
import { createDeferredCore } from "../../src/shared/deferred.js";

/** Local Bot API transport and a declared external CLI producer. No core/presenter mocks. */
export async function createRecoveryGatewayTransport(root: string, original: OpenClawConfig) {
  const producerStarted = createDeferredCore();
  const releaseProducer = createDeferredCore();
  const messages: string[] = [];
  const calls: string[] = [];
  const waitingMessages = new Map<string, ReturnType<typeof createDeferredCore<void>>>();
  let firstProducer = true;
  let sequence = 0;
  const handleRequest = async (req: http.IncomingMessage, res: http.ServerResponse) => {
    try {
      res.setHeader("connection", "close");
      if (req.url === "/producer") {
        await fs.appendFile(path.join(root, "executed"), "executed\n");
        const output = await fs.readFile(path.join(root, "output"), "utf8");
        if (firstProducer) {
          firstProducer = false;
          producerStarted.resolve();
          await releaseProducer.promise;
        }
        res.setHeader("content-type", "text/plain");
        res.end(output);
        return;
      }
      const chunks: Buffer[] = [];
      for await (const chunk of req) {
        chunks.push(Buffer.from(chunk));
      }
      const payload = JSON.parse(Buffer.concat(chunks).toString() || "{}") as {
        text?: string;
        chat_id?: number | string;
        message_id?: number;
      };
      const method = req.url?.split("/").at(-1) ?? "";
      calls.push(method);
      let result: unknown;
      if (method === "getMe") {
        result = { id: 999, is_bot: true, first_name: "Fixture", username: "recovery_fixture_bot" };
      } else if (method === "sendMessage" || method === "editMessageText") {
        messages.push(payload.text ?? "");
        waitingMessages.get(payload.text ?? "")?.resolve();
        result = {
          message_id: payload.message_id ?? ++sequence,
          date: 1,
          chat: { id: Number(payload.chat_id ?? 123), type: "private", first_name: "Fixture" },
          text: payload.text ?? "",
        };
      } else if (
        [
          "sendChatAction",
          "deleteMessage",
          "sendMessageDraft",
          "setMyCommands",
          "deleteMyCommands",
          "deleteWebhook",
        ].includes(method)
      ) {
        result = true;
      } else {
        throw new Error("Unexpected fixture Bot API method: " + method);
      }
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ ok: true, result }));
    } catch (error) {
      res.statusCode = 400;
      res.end(JSON.stringify({ ok: false, error_code: 400, description: String(error) }));
    }
  };
  const server = http.createServer((req, res) => {
    void handleRequest(req, res).catch((error: unknown) => {
      res.destroy(error instanceof Error ? error : new Error(String(error)));
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const apiRoot = "http://127.0.0.1:" + (server.address() as AddressInfo).port;
  const pluginDir = path.join(root, "acceptance-runtime");
  await fs.mkdir(pluginDir, { recursive: true });
  await fs.writeFile(
    path.join(pluginDir, "package.json"),
    JSON.stringify({
      name: "acceptance-runtime",
      version: "0.0.0",
      type: "module",
      openclaw: { extensions: ["./index.js"] },
    }),
  );
  await fs.writeFile(
    path.join(pluginDir, "openclaw.plugin.json"),
    JSON.stringify({
      id: "acceptance-runtime",
      name: "Acceptance runtime",
      cliBackends: ["gateway-acceptance-cli"],
      configSchema: { type: "object", additionalProperties: false },
    }),
  );
  const producerFile = path.join(root, "gateway-producer.cjs");
  await fs.writeFile(
    producerFile,
    "const http = require('node:http'); process.stdin.resume(); http.get(" +
      JSON.stringify(apiRoot + "/producer") +
      ", res => res.pipe(process.stdout)).on('error', error => { console.error(error.message); process.exitCode = 1; });\n",
  );
  const backend: CliBackendPlugin = {
    id: "gateway-acceptance-cli",
    config: {
      command: process.execPath,
      args: [producerFile],
      input: "stdin",
      output: "text",
      sessionMode: "none",
      systemPromptWhen: "never",
    },
  };
  await fs.writeFile(
    path.join(pluginDir, "index.js"),
    "export default { id: 'acceptance-runtime', register(api) { api.registerCliBackend(" +
      JSON.stringify(backend) +
      "); } };\n",
  );
  const cfg: OpenClawConfig = {
    ...original,
    session: { ...original.session, dmScope: "per-channel-peer" },
    agents: {
      ...original.agents,
      defaults: {
        ...original.agents?.defaults,
        model: "gateway-acceptance-cli/fixture",
        models: { "gateway-acceptance-cli/fixture": {} },
      },
    },
    commands: { ...original.commands, native: false, nativeSkills: false },
    plugins: {
      enabled: true,
      allow: ["acceptance-runtime", "telegram"],
      load: { paths: [pluginDir] },
    },
    channels: {
      telegram: {
        botToken: "123:synthetic-test-only",
        apiRoot,
        dmPolicy: "open",
        allowFrom: ["*"],
        streaming: { mode: "off" },
      },
    },
  };
  return {
    cfg,
    backend,
    pluginDir,
    messages,
    calls,
    producerStarted,
    releaseProducer,
    waitForMessage: (text: string) => {
      if (messages.includes(text)) {
        return Promise.resolve();
      }
      const pending = createDeferredCore();
      waitingMessages.set(text, pending);
      return new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
          () =>
            reject(
              new Error(
                "Expected Bot API message was not observed: " +
                  text +
                  "; observed=" +
                  JSON.stringify(messages),
              ),
            ),
          10_000,
        );
        void pending.promise.then(
          () => {
            clearTimeout(timer);
            resolve();
          },
          (error: unknown) => {
            clearTimeout(timer);
            reject(error instanceof Error ? error : new Error(String(error)));
          },
        );
      });
    },
    close: async () => {
      releaseProducer.resolve();
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(toErrorObject(error, "Gateway fixture server close failed"));
          } else {
            resolve();
          }
        });
      });
    },
  };
}
