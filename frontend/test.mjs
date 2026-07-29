import { readFile } from "node:fs/promises";

const source = await readFile(new URL("./app.js", import.meta.url), "utf8");
const html = await readFile(new URL("./index.html", import.meta.url), "utf8");
const config = await readFile(new URL("./config.js", import.meta.url), "utf8");

const apiPrefix = source.match(/const API_PREFIX = "([^"]+)";/)?.[1];

function functionBody(name) {
  const start = source.indexOf(`function ${name}`);
  if (start === -1) {
    throw new Error(`${name} is missing`);
  }
  const openBrace = source.indexOf("{", start);
  let depth = 0;
  for (let index = openBrace; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    if (source[index] === "}") depth -= 1;
    if (depth === 0) return source.slice(openBrace + 1, index);
  }
  throw new Error(`${name} body is unterminated`);
}

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
if (!html.includes('id="back-button"') || !html.includes("header-back-button")) {
  throw new Error("top-left detail back button is missing from the shell");
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
if (!source.includes("function commitRouteHash(target)") || !functionBody("commitRouteHash").includes("render();")) {
  throw new Error("route navigation must render immediately after changing the hash");
}
if (!functionBody("navigateToList").includes('commitRouteHash("#/clusters")')) {
  throw new Error("detail back navigation does not commit the list route synchronously");
}
if (!source.includes('backButton.setAttribute("aria-label", message("backToList"))')) {
  throw new Error("top-left back button is not localized for assistive labels");
}
if (source.includes('<div class="detail-actions"><button class="button button-secondary" type="button" data-action="back-to-list"')) {
  throw new Error("detail card should not duplicate the top-left back button");
}
if (/location\.hash\s*=\s*target;\s*return;/.test(functionBody("navigateToList")) || /location\.hash\s*=\s*target;\s*return;/.test(functionBody("navigateToCluster"))) {
  throw new Error("route navigation still depends on hashchange before rendering");
}
if (!source.includes("data-cluster-name") || !source.includes("navigateToCluster(row.dataset.clusterName)")) {
  throw new Error("cluster row clicks must use the same synchronous route commit as detail navigation");
}

for (const apiBase of ["/api", "/messagequeue/api", "https://api.example.test"]) {
  const url = `${apiBase.replace(/\/$/, "")}${apiPrefix}`;
  if (url.includes("/api/api/")) {
    throw new Error(`API base was duplicated: ${url}`);
  }
}
