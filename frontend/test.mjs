import { readFile } from "node:fs/promises";

const source = await readFile(new URL("./app.js", import.meta.url), "utf8");
const html = await readFile(new URL("./index.html", import.meta.url), "utf8");
const config = await readFile(new URL("./config.js", import.meta.url), "utf8");
const styles = await readFile(new URL("./styles.css", import.meta.url), "utf8");
const build = await readFile(new URL("./build.mjs", import.meta.url), "utf8");
const nginx = await readFile(new URL("./nginx.conf.template", import.meta.url), "utf8");

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
if (!source.includes("const LOCALES =")) {
  throw new Error("i18n catalogue is missing");
}
if (
  !source.includes('SEALOS_DESKTOP_CHANGE_I18N_EVENT = "change_i18n"') ||
  !source.includes('SEALOS_DESKTOP_LANGUAGE_API = "getLanguage"') ||
  !source.includes("requestSealosDesktopLanguage") ||
  !source.includes("setupSealosLanguageSync") ||
  !source.includes("event.source !== window.top") ||
  !source.includes("window.top === window || event.source !== window.top")
) {
  throw new Error("Sealos Desktop language sync is missing");
}
if (!html.includes('lang="zh-CN"') || !html.includes("MessageQueue | 消息队列")) {
  throw new Error("Chinese-first shell is missing");
}
if (html.includes("language-button") || html.includes("lang-button") || source.includes("language-button")) {
  throw new Error("language should follow Sealos Desktop without an explicit in-app toggle");
}
if (html.includes("api-indicator") || source.includes("renderApiIndicator") || styles.includes(".api-indicator")) {
  throw new Error("list toolbar must not show the API connection pill");
}
if (source.includes("cluster-meta") || styles.includes(".cluster-meta")) {
  throw new Error("cluster list rows must not show secondary namespace/generation meta under the name");
}
if (!html.includes('id="back-button"') || !html.includes("header-back-button")) {
  throw new Error("top-left detail back button is missing from the shell");
}
const backButtonMarkup = html.match(/<button[\s\S]*?id="back-button"[\s\S]*?<\/button>/)?.[0] || "";
if (!backButtonMarkup.includes("<svg") || />\s*实例列表\s*</.test(backButtonMarkup)) {
  throw new Error("top-left detail back button must be icon-only");
}
if (!html.includes('id="detail-header-actions"') || !html.includes('data-testid="messagequeue.detail.header-actions"')) {
  throw new Error("detail header actions slot is missing from the shell top bar");
}
if (
  !styles.includes('.app-shell[data-view="detail"] .brand') ||
  !styles.includes('.app-shell[data-view="detail"] .header-actions')
) {
  throw new Error("detail route should hide global brand and toolbar surfaces");
}
if (!config.includes("window.MESSAGEQUEUE_LOCALE")) {
  throw new Error("locale override hook is missing");
}
for (const path of ["/index.html", "(app|config)", "/styles.css"]) {
  if (!nginx.includes(path)) {
    throw new Error(`nginx cache policy is missing ${path}`);
  }
}
if (!nginx.includes('add_header Cache-Control "no-store" always')) {
  throw new Error("same-name frontend assets must be served with no-store");
}
if (!build.includes("createHash") || !build.includes('replaceAll(`"${asset}"`')) {
  throw new Error("build output must version same-name frontend assets");
}
if (!source.includes('fetch(`${API_BASE}${path}`')) {
  throw new Error("request() no longer joins API_BASE with a relative path");
}
if (!source.includes("result.degraded") || !source.includes("monitoringUnavailable") || !source.includes("loadMonitoring")) {
  throw new Error("monitoring degraded state is not rendered explicitly");
}
if (source.includes("CREATE_ENABLED") || source.includes("MESSAGEQUEUE_CREATE_ENABLED")) {
  throw new Error("write affordances must not depend on a create feature flag");
}
if (!source.includes("parseApiError") || !source.includes("describeApiError") || !source.includes("workspaceQuota") || !source.includes("loadWorkspaceQuota")) {
  throw new Error("create flow must normalize API errors and initialize workspace quota");
}
if (!source.includes("const CREATE_PROFILES") || !source.includes('cpu: "500m"') || !source.includes('memory: "1Gi"') || !source.includes("updateCreateSummary")) {
  throw new Error("create flow must expose development resource presets and a dynamic summary");
}
if (source.includes('resources: { cpu: "1", memory: "2Gi" }')) {
  throw new Error("create flow must not hardcode the old standard broker resources");
}
for (const testId of [
  "messagequeue.create.modal",
  "messagequeue.create.profile-option",
  "messagequeue.create.cpu-input",
  "messagequeue.create.memory-input",
  "messagequeue.create.summary",
  "messagequeue.create.quota-note",
  "messagequeue.create.submit-button"
]) {
  if (!html.includes(`data-testid="${testId}"`)) {
    throw new Error(`create semantic test id is missing: ${testId}`);
  }
}
for (const profile of ["development", "standard", "custom"]) {
  if (!html.includes(`data-qa-profile="${profile}"`)) {
    throw new Error(`create profile option is missing: ${profile}`);
  }
}
if (!html.includes('value="500m"') || !html.includes('value="1Gi"') || !html.includes('value="10"')) {
  throw new Error("development create defaults must match deploy/examples/messagequeue-dev.yaml");
}
if (!html.includes('id="storage-size"') || !html.includes('max="1024"') || !source.includes("capped at 8 CPU") || !source.includes("不超过 64Gi")) {
  throw new Error("create custom resource fields must expose the backend product limits");
}
if (!html.includes('id="quota-note"') || !source.includes("quotaReady") || !source.includes("quotaExceededCopy") || !source.includes("sessionRequiredCopy")) {
  throw new Error("create flow must expose quota-aware status and fixed error copy");
}
if (!source.includes("function writesEnabled()") || !source.includes('return state.apiState === "ready";')) {
  throw new Error("write affordances must require a ready API");
}
if (
  !source.includes('/client-config') ||
  !source.includes('/client-credentials') ||
  !source.includes("connectionModel") ||
  !source.includes("internalAddress") ||
  !source.includes("externalAddress") ||
  !source.includes("connectionString") ||
  !source.includes("environmentVariables") ||
  !source.includes("loadClientCredentials") ||
  !source.includes("copyClientTemplate") ||
  !source.includes("data-copy") ||
  !source.includes("copy-icon-button") ||
  !source.includes("showToast") ||
  !source.includes("!endpoint?.available") ||
  !source.includes('data-qa-state="${stateName}"') ||
  !source.includes("config.degraded") ||
  !source.includes('data-testid="messagequeue.detail.client-config-degraded"')
) {
  throw new Error("auth-aware internal/external client connection fields are missing");
}
if (
  source.includes("copyIconButton(credentials.password") ||
  source.includes('"copy-password"') ||
  source.includes("function copyPassword") ||
  source.includes('data-testid="messagequeue.detail.connection-auth"') ||
  source.includes("revealed") ||
  !source.includes('actionIconButton("copy-client-template"') ||
  !source.includes('const maskedSecret = "********";') ||
  !functionBody("connectionsHtml").includes("envTemplate(cluster, mergedConfig, model, false)")
) {
  throw new Error("connection tab must keep individual credentials out of the DOM and show masked environment variables");
}
if (
  !source.includes('/external-access') ||
  !source.includes('method: "PUT"') ||
  !source.includes('body: JSON.stringify({ enabled })') ||
  !source.includes('data-action="set-external-access"') ||
  !source.includes('data-qa-state="${externalAction.error ? "error" : enabling ? "enabling" : "off"}') ||
  !source.includes('externalAction.error ? "error" : "on"') ||
  !source.includes("status.externalEndpoints") ||
  !source.includes("status.externalEndpoint") ||
  !source.includes("config.externalBootstrapServers")
) {
  throw new Error("external access must expose off/enabling/error states and use the bounded PUT contract");
}
for (const removedCopy of [
  "credentialsStayServerSide",
  "credentialsProtected",
  "noSecretMaterial",
  "secretReference",
  "metricsFetchNote"
]) {
  if (source.includes(removedCopy)) {
    throw new Error(`detail UI still includes redundant descriptive copy: ${removedCopy}`);
  }
}
if (
  !source.includes('method: "DELETE"') ||
  !source.includes('data-action="delete-cluster"') ||
  !source.includes("function actionMenuHtml") ||
  !source.includes("function menuIcon") ||
  !source.includes("function tabIcon") ||
  !source.includes('data-icon="inline-start"') ||
  !source.includes('aria-orientation="vertical"') ||
  !source.includes("detail-layout") ||
  !source.includes("detail-surface") ||
  !source.includes("function rowActionsHtml") ||
  !source.includes("function detailActionsHtml") ||
  !source.includes('$("#detail-header-actions")?.addEventListener("click"') ||
  !source.includes("action-menu-content") ||
	  !styles.includes('[data-icon="inline-start"]') ||
	  !styles.includes(".detail-layout") ||
	  !styles.includes(".detail-surface") ||
	  !styles.includes('.app-shell[data-view="detail"]') ||
	  !styles.includes("#notice-region:empty") ||
	  !styles.includes(".tab-icon") ||
	  !styles.includes(".action-menu-content") ||
  !source.includes('["monitoring", message("monitoring")]') ||
  source.includes('["settings", message("settings")]') ||
  source.includes('message("settings")')
) {
  throw new Error("row lifecycle actions must stay in its menu and Settings tab must be absent");
}
if (
  styles.includes("min-height: 560px") ||
  !styles.includes("height: 100vh") ||
  !styles.includes("height: 100%;\n  min-height: 0;")
) {
  throw new Error("detail view must fill the viewport without fixed minimum height causing outer scroll");
}
const detailActionsBody = functionBody("detailActionsHtml");
if (
  detailActionsBody.includes("actionMenuHtml(") ||
  detailActionsBody.includes("refresh-detail") ||
  !detailActionsBody.includes("updateInstance") ||
  !detailActionsBody.includes("lifecycleLabel") ||
  !detailActionsBody.includes('data-action="delete-cluster"')
) {
  throw new Error("detail header must directly lay out status, update, pause/resume, and delete without refresh or a menu");
}
if (functionBody("renderDetail").includes("detailActionsHtml(")) {
  throw new Error("detail actions must render in the top bar, not inside the detail card");
}
if (!source.includes('data-action="dismiss-notice"') || !source.includes("noticeDismissed")) {
  throw new Error("dismissible degraded notice is missing a stateful handler");
}
for (const key of [
  "loadClientConfig",
  "clientConfigUnavailable",
  "internalAddress",
  "externalAddress",
  "enableExternalAccess",
  "disableExternalAccess",
  "externalAccessFailed",
  "connectionString",
  "monitoring",
  "monitoringUnavailable",
  "updateInstance",
  "pauseInstance",
  "resumeInstance",
  "moreActions",
  "deleteCluster",
  "deleteUnavailable",
  "deleteConfirmPrompt"
]) {
  if (!source.includes(key)) {
    throw new Error(`new i18n key is missing: ${key}`);
  }
}
if (
  html.includes('id="refresh-button"') ||
  source.includes('data-action="refresh-detail"') ||
  functionBody("logsHtml").includes('data-action="load-logs"') ||
  functionBody("monitoringHtml").includes('data-action="load-metrics"') ||
  functionBody("logsHtml").includes('message("refresh")') ||
  functionBody("monitoringHtml").includes('message("refresh")')
) {
  throw new Error("normal list, detail, logs, and monitoring refresh buttons must be removed");
}
const copyClientTemplateBody = functionBody("copyClientTemplate");
if (
  !copyClientTemplateBody.includes("await ensureClientCredentials()") ||
  !copyClientTemplateBody.includes("!credentials?.password || !credentials?.caCertificate") ||
  !copyClientTemplateBody.includes("envTemplate(cluster, config, model, true)") ||
  copyClientTemplateBody.includes("state.clientCredentials?.revealed")
) {
  throw new Error("environment copy must fetch real credentials on demand without reveal-state coupling");
}
if (
  !source.includes("const POLL_INTERVALS") ||
  !source.includes("clusters: 15000") ||
  !source.includes("logs: 10000") ||
  !source.includes("monitoring: 30000") ||
  !source.includes("function syncAutoPolling()") ||
  !source.includes('document.addEventListener("visibilitychange", syncAutoPolling)') ||
  !functionBody("loadClusters").includes("options.background === true") ||
  !functionBody("loadObservability").includes("options.background === true") ||
  !functionBody("loadMonitoring").includes("options.background === true")
) {
  throw new Error("bounded visibility-aware background polling is missing");
}
if (
  !source.includes('data-testid="messagequeue.detail.monitoring" data-qa-state="loading"') ||
  !source.includes('data-testid="messagequeue.detail.monitoring" data-qa-state="error"') ||
  !source.includes('${result.degraded ? "degraded" : "ready"}') ||
  !source.includes('class="detail-section monitoring-section"') ||
  !source.includes("formatMetricInstant(series, unit)") ||
  !source.includes("data-monitoring-key") ||
  !styles.includes(".metric-tooltip") ||
  !styles.includes(".metric-icon:hover .metric-tooltip") ||
  !styles.includes('.detail-body[data-tab="monitoring"]') ||
  !styles.includes(".monitoring-fill") ||
  !styles.includes(".monitoring-trend-grid") ||
  !styles.includes("flex: 0 0 auto;") ||
  !styles.includes("grid-template-rows: auto auto 42px") ||
  !styles.includes("width: 36px;") ||
  !styles.includes("height: 36px;")
) {
  throw new Error("monitoring tab must fill the detail surface without stretching metric cards or icons");
}
if (
  !styles.includes('.detail-body[data-tab="logs"]') ||
  !styles.includes("overflow-x: hidden") ||
  !styles.includes("overflow-wrap: anywhere") ||
  !source.includes('class="detail-section log-section"')
) {
  throw new Error("logs tab must fill the detail card and wrap long broker lines without horizontal scroll");
}
for (const forbidden of ["passwordInput", "privateKey", "kubeconfigText", "secretData", "saslPassword"]) {
  if (source.includes(forbidden)) {
    throw new Error(`client config UI should not introduce secret material field: ${forbidden}`);
  }
}
if (functionBody("loadClientConfig").includes("/client-credentials")) {
  throw new Error("ordinary client config loading must remain secret-free");
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
if (
  !source.includes("data-cluster-name") ||
  !source.includes("navigateToCluster(row.dataset.clusterName)") ||
  !source.includes('data-action="toggle-row-actions"') ||
  !source.includes('event.target.closest(".cluster-row-actions")') ||
  !source.includes("list-delete-error")
) {
  throw new Error("cluster row clicks and row action menus must stay isolated");
}

for (const apiBase of ["/api", "/messagequeue/api", "https://api.example.test"]) {
  const url = `${apiBase.replace(/\/$/, "")}${apiPrefix}`;
  if (url.includes("/api/api/")) {
    throw new Error(`API base was duplicated: ${url}`);
  }
}
