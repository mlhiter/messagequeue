import { readFile } from "node:fs/promises";

const source = await readFile(new URL("./app.js", import.meta.url), "utf8");
const apiPrefix = source.match(/const API_PREFIX = "([^"]+)";/)?.[1];

if (apiPrefix !== "/v1/messagequeues") {
  throw new Error(`unexpected API prefix: ${apiPrefix || "missing"}`);
}
if (!source.includes('fetch(`${API_BASE}${path}`')) {
  throw new Error("request() no longer joins API_BASE with a relative path");
}
if (!source.includes("result.degraded") || !source.includes("Metrics unavailable")) {
  throw new Error("metrics degraded state is not rendered explicitly");
}

for (const apiBase of ["/api", "/messagequeue/api", "https://api.example.test"]) {
  const url = `${apiBase.replace(/\/$/, "")}${apiPrefix}`;
  if (url.includes("/api/api/")) {
    throw new Error(`API base was duplicated: ${url}`);
  }
}
