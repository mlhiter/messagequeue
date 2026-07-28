import { readFile } from "node:fs/promises";

const source = await readFile(new URL("./app.js", import.meta.url), "utf8");
const html = await readFile(new URL("./index.html", import.meta.url), "utf8");
const config = await readFile(new URL("./config.js", import.meta.url), "utf8");

const apiPrefix = source.match(/const API_PREFIX = "([^"]+)";/)?.[1];

if (apiPrefix !== "/v1/messagequeues") {
  throw new Error(`unexpected API prefix: ${apiPrefix || "missing"}`);
}
if (!source.includes("const LOCALES =") || !source.includes("switchLanguageLabel")) {
  throw new Error("i18n catalogue is missing");
}
if (!source.includes("window.MESSAGEQUEUE_LOCALE") || !source.includes("applyLanguage")) {
  throw new Error("language persistence is missing");
}
if (!html.includes('lang="zh-CN"') || !html.includes("MessageQueue | 消息队列") || !html.includes("language-button")) {
  throw new Error("Chinese-first shell is missing");
}
if (!config.includes("window.MESSAGEQUEUE_LOCALE")) {
  throw new Error("locale override hook is missing");
}
if (!source.includes('fetch(`${API_BASE}${path}`')) {
  throw new Error("request() no longer joins API_BASE with a relative path");
}
if (!source.includes("result.degraded") || !source.includes("metricsUnavailable")) {
  throw new Error("metrics degraded state is not rendered explicitly");
}

for (const apiBase of ["/api", "/messagequeue/api", "https://api.example.test"]) {
  const url = `${apiBase.replace(/\/$/, "")}${apiPrefix}`;
  if (url.includes("/api/api/")) {
    throw new Error(`API base was duplicated: ${url}`);
  }
}
