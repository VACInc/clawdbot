// Test-only public entrypoint keeps Gateway capture fixtures inside the Telegram plugin.
export const loadTelegramGatewayCaptureFixture = () => import("./src/bot.js");
