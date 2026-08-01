(() => {
  "use strict";

  const API_BASE = (window.MESSAGEQUEUE_API_BASE || "/api").replace(/\/$/, "");
  const API_PREFIX = "/v1/messagequeues";
  const DEFAULT_LANGUAGE = "zh";
  const SEALOS_DESKTOP_EVENT_API = "event-bus";
  const SEALOS_DESKTOP_LANGUAGE_API = "getLanguage";
  const SEALOS_DESKTOP_CHANGE_I18N_EVENT = "change_i18n";
  const SEALOS_DESKTOP_REQUEST_TIMEOUT_MS = 3000;

  const LOCALES = {
    zh: {
      title: "MessageQueue | 消息队列",
      description: "MessageQueue Kafka 管理控制台",
      brandSubtitle: "工作空间控制平面",
      navClusters: "实例",
      navOperations: "操作",
      workspace: "工作空间",
      workspaceOwner: "工作空间所有者",
      ownerRole: "Owner",
      breadcrumbWorkspace: "工作空间",
      breadcrumbClusters: "实例",
      sectionTag: "消息代理 / Kafka",
      pageTitle: "消息队列实例",
      pageDescription: "先看列表，再进入详情页查看连接、日志和指标。",
      detailPageTitle: "实例详情",
      detailPageDescription: "这里单独展示一个实例的状态、连接和可观测信息。",
      backToList: "实例列表",
      createCluster: "创建实例",
      newCluster: "新建",
      totalClusters: "实例总数",
      observedInWorkspace: "在当前工作空间中观测到",
      ready: "就绪",
      observedAndServing: "已观测且可服务",
      attention: "关注",
      provisioningOrDegraded: "正在创建或异常",
      engine: "引擎",
      kafka: "Kafka",
      strimziManaged: "Strimzi 托管",
      yourClusters: "你的实例",
      clusterResourcesReadOnly: (count) => `${count} 个实例 · 只读`,
      clusterResourcesInWorkspace: (count) => `${count} 个实例 · 当前工作空间`,
      clusterNameColumn: "名称",
      statusColumn: "状态",
      versionColumn: "版本",
      brokerColumn: "Broker",
      storageColumn: "存储",
      namespaceColumn: "命名空间",
      updatedColumn: "最后变更",
      openDetail: "查看详情",
      loadingClusters: "正在加载实例…",
      searchPlaceholder: "搜索实例",
      noKafkaClustersYet: "还没有 Kafka 实例",
      noMatchingClusters: "没有匹配的实例",
      createDevClusterHint: "创建一个开发实例开始发送消息。",
      createUnavailableHint: "API 接通后即可创建开发实例。",
      tryDifferentName: "换个名字试试，或者清空搜索。",
      selectACluster: "选择一个实例",
      selectAClusterHint: "从列表中选择一个实例查看它的观测状态。",
      apiUnavailableTitle: "管理 API 不可用",
      apiUnavailableCopy: (message) => message || "这里展示的是明确标注的演示数据。要改动资源，需要可用的 API。",
      permissionDeniedTitle: "权限不足",
      permissionDeniedCopy: "当前会话无法读取这个工作空间的 MessageQueue 资源。",
      demoDataTitle: "演示数据",
      demoDataCopy: "后端不可达时，会显示只读的演示数据。",
      observedState: "观测状态",
      generation: "代数",
      engineName: "Apache Kafka",
      kafkaVersion: "Kafka 版本",
      topology: "拓扑",
      storage: "存储",
      bootstrapEndpoint: "Bootstrap 端点",
      deletionPolicy: "删除策略",
      retainData: "保留数据",
      deleteWithCluster: "随实例删除",
      controllerDefault: "控制器默认",
      conditions: "条件",
      controllerReported: "控制器上报",
      recentEvents: "最近事件",
      latestFirst: "最新在前",
      detailTabsLabel: "实例详情选项卡",
      conditionPlaceholder: "控制器尚未上报条件。",
      eventPlaceholder: "暂无最近事件。",
      overview: "概览",
      controllerEvent: "控制器事件",
      connections: "连接",
      clientConnection: "客户端连接",
      credentialsStayServerSide: "凭据保留在服务端",
      credentialsProtected:
        "通过授权的服务端操作获取短期客户端配置。密码和私钥不会出现在浏览器日志或状态中。",
      loadClientConfig: "加载客户端配置",
      retryClientConfig: "重试配置",
      loadingClientConfig: "正在加载客户端配置…",
      clientConfigUnavailable: "客户端配置不可用",
      clientConfigFetchNote: (name) => `配置通过服务端获取，范围限定为 ${name}。`,
      clientConfigDegraded: "客户端配置尚未就绪，不会暴露 Secret 数据。",
      bootstrapServers: "Bootstrap 服务",
      username: "用户名",
      secretReference: "Secret 引用",
      noSecretMaterial: "这里仅显示连接元数据。密码、私钥和 kubeconfig 不会返回到浏览器。",
      authentication: "认证",
      transport: "传输",
      mechanism: "机制",
      kafkaUser: "Kafka 用户",
      access: "访问",
      tls: "TLS",
      scramSha512: "SCRAM-SHA-512",
      workspaceScoped: "工作空间级别",
      copy: "复制",
      liveLogs: "实时日志",
      loadLogs: "加载日志",
      retryLogs: "重试日志",
      brokerLogs: "Broker 日志",
      logsUnavailable: "日志不可用",
      loadingLogs: "正在加载 broker 日志…",
      logsOptional:
        "历史或实时日志是可选的平台依赖，不会阻塞 Kafka 操作。",
      logsFetchNote: (name) => `日志通过固定的服务端查询获取，范围限定为 ${name}。`,
      logs: "日志",
      loadMetrics: "加载指标",
      retryMetrics: "重试指标",
      brokerHealth: "Broker 健康",
      loadingMetrics: "正在加载 broker 指标…",
      metricsUnavailable: "指标不可用",
      metricsOptional:
        "VictoriaMetrics 可能不可用，或者还没有为这个工作空间配置。",
      metricsFetchNote:
        "指标使用固定的服务端查询。命名空间和实例选择器由后端注入。",
      metricsProviderMissing:
        "指标提供器未配置，不会影响 Kafka 操作。",
      metricsMessagesIn: "消息进入",
      metricsMessagesOut: "消息流出",
      metricsUnderReplicated: "未同步副本",
      metricsConsumerLag: "消费者堆积",
      metrics: "指标",
      perSecond: "每秒",
      partitions: "分区",
      messages: "条消息",
      refresh: "刷新",
      refreshData: "刷新数据",
      openHelp: "打开帮助",
      newResource: "新实例",
      createKafkaCluster: "创建 Kafka 实例",
      createDescription: "选择一个规格，控制器会在你的工作空间命名空间里创建 Strimzi 资源。",
      close: "关闭",
      basicSectionTitle: "基础信息",
      basicSectionCopy: "名称会成为 Kubernetes 资源名称。",
      clusterName: "实例名称",
      required: "必填",
      lowercaseHint: "仅支持小写字母、数字和连字符。",
      kafkaVersionLabel: "Kafka 版本",
      recommended: "推荐",
      profileLabel: "规格预设",
      profileHelp: "开发规格默认打开，资源不够再切自定义。",
      profileDevelopmentTitle: "开发",
      profileDevelopmentCopy: "单 Broker，适合验证和轻量测试。",
      profileDevelopmentMeta: "500m / 1Gi / 10Gi",
      profileStandardTitle: "标准",
      profileStandardCopy: "3 Broker，适合更接近生产的验证。",
      profileStandardMeta: "1 CPU / 2Gi / 20Gi",
      profileCustomTitle: "自定义",
      profileCustomCopy: "手动配置 broker、CPU、内存和存储。",
      profileCustomMeta: "可编辑",
      resourceSectionTitle: "规格细节",
      resourceSectionCopy: "CPU 和内存会写入每个 broker 的资源请求和限制。",
      brokerCount: "Broker 数量",
      brokerCountHint: "生产环境建议至少 3 个 broker。",
      cpuPerBroker: "每个 Broker 的 CPU",
      cpuHint: "支持 500m、1、1.5；每个 broker 不超过 8 CPU。",
      memoryPerBroker: "每个 Broker 的内存",
      memoryHint: "建议使用 Gi 或 Mi；每个 broker 不超过 64Gi。",
      storagePerBroker: "每个 Broker 的存储",
      storageHint: "每个 broker 支持 1 到 1024 Gi。",
      storageClass: "存储类",
      deletionPolicyLabel: "删除策略",
      storageClassPlaceholder: "默认存储类",
      resourceFootprint: "资源占用",
      resourceFootprintCopy: ({ profile, brokers, cpu, memory, storageGi, policy }) =>
        `${profile} · ${brokers} 个 broker · ${cpu} CPU / ${memory} 内存 · 每个 ${storageGi} Gi 存储 · ${policy}`,
      creationAsync: "创建是异步的。你可以在实例详情里跟踪观测状态。",
      lastTransition: "最后变更",
      cancel: "取消",
      create: "创建",
      formError: "请输入合法的实例名称，并检查各项配置。",
      formPermissionDenied: "权限不足：当前工作空间不能创建实例。",
      sessionRequiredTitle: "会话不可用",
      sessionRequiredCopy: "当前会话不可用，请刷新 Desktop 后再试。",
      quotaLoading: "正在读取当前工作空间的 CPU、内存和存储配额。",
      quotaUnavailable: "配额信息暂不可用，提交后仍会由后端再校验。",
      quotaReady: ({ cpu, memory, storage }) =>
        `工作空间配额：CPU ${cpu}，内存 ${memory}，存储 ${storage}。`,
      quotaDegraded: (message) => `配额暂未配置${message ? `：${message}` : ""}`,
      quotaExceededCopy: (message) => `工作空间配额不足${message ? `：${message}` : ""}`,
      resourceNotFoundCopy: "这个实例已经不存在了，请返回列表后重试。",
      formCreateFailed: (message) => `实例创建失败：${message}`,
      createButton: "创建实例",
      createButtonShort: "+ 新建",
      statusUnknown: "未知",
      statusReady: "就绪",
      statusProvisioning: "准备中",
      statusUpdating: "更新中",
      statusDegraded: "降级",
      statusFailed: "失败",
      statusSuspended: "已挂起",
      statusDeleting: "删除中",
      statusLabel: "状态",
      loadingDemoClusters: "正在加载观测到的实例…",
      stateReadyMeta: "已观测且可服务",
      stateAttentionMeta: "正在创建或异常",
      demoReadyEvent1: "Kafka 用户凭据已完成协调。",
      demoReadyEvent2: "3 个 broker 已全部就绪。",
      demoProvisioningEvent1: "MessageQueue 已被控制器接收。",
      demoProvisioningEvent2: "Kafka 资源正在通过 Strimzi 创建。",
      demoReadyCondition1: "Kafka Broker 正在接受连接。",
      demoReadyCondition2: "Broker 指标和日志已可用。",
      demoProvisioningCondition1: "等待 Kafka broker Pod 就绪。",
      demoProvisioningCondition2: "正在协调 Strimzi 资源。",
      noRecentEvents: "暂无最近事件。",
      noConditions: "控制器尚未上报条件。",
      clusterScopedAccess: "工作空间范围访问",
      credentialSafety: "密码和私钥不会出现在浏览器日志里。",
      topicPlaceholder: "示例：orders-dev",
      topbarHelp: "帮助",
      topbarRefresh: "刷新",
      managementApiUnavailable:
        "后端不可达。这里显示只读演示数据；请先接通 API 再创建实例。",
      clusterCreationUnavailable:
        "管理 API 不可用，暂时不能创建实例。",
      loadingState: "正在加载…",
      noHelp: "暂无帮助内容",
      operationsComingSoon: "运维功能正在完善中",
      settings: "设置",
      deleteCluster: "删除实例",
      deletingCluster: "正在删除…",
      dangerZone: "危险操作",
      deleteClusterDescription: "删除 MessageQueue 实例。实际 PVC 处理遵循创建时选择的删除策略。",
      deleteUnavailable: "管理 API 不可用，暂时不能删除实例。",
      deleteConfirmPrompt: (name) => `确认删除 ${name}？这个操作会提交到当前工作空间。`,
      deleteFailed: (message) => `删除失败：${message}`,
      deleteRetainImpact: "当前策略会保留数据卷。",
      deleteDeleteImpact: "当前策略会随实例删除数据卷。",
      readOnlySuffix: "只读",
      workspaceReady: "工作空间已就绪",
      workspacePending: "工作空间加载中"
    },
    en: {
      title: "MessageQueue | Message Brokers",
      description: "MessageQueue Kafka management console",
      brandSubtitle: "Workspace control plane",
      navClusters: "Instances",
      navOperations: "Operations",
      workspace: "Workspace",
      workspaceOwner: "Workspace owner",
      ownerRole: "Owner",
      breadcrumbWorkspace: "Workspace",
      breadcrumbClusters: "Instances",
      sectionTag: "Message brokers / Kafka",
      pageTitle: "Message queue instances",
      pageDescription: "Start with the list, then open a dedicated detail page for each instance.",
      detailPageTitle: "Instance details",
      detailPageDescription: "Inspect the selected instance's status, connections, logs, and metrics here.",
      backToList: "Instance list",
      createCluster: "Create instance",
      newCluster: "New",
      totalClusters: "Total instances",
      observedInWorkspace: "Observed in this workspace",
      ready: "Ready",
      observedAndServing: "Observed and serving",
      attention: "Attention",
      provisioningOrDegraded: "Provisioning or degraded",
      engine: "Engine",
      kafka: "Kafka",
      strimziManaged: "Strimzi managed",
      yourClusters: "Your instances",
      clusterResourcesReadOnly: (count) => `${count} instance${count === 1 ? "" : "s"} · read only`,
      clusterResourcesInWorkspace: (count) => `${count} instance${count === 1 ? "" : "s"} in your workspace`,
      clusterNameColumn: "Name",
      statusColumn: "Status",
      versionColumn: "Version",
      brokerColumn: "Broker",
      storageColumn: "Storage",
      namespaceColumn: "Namespace",
      updatedColumn: "Last change",
      openDetail: "Open detail",
      loadingClusters: "Loading instances…",
      searchPlaceholder: "Search instances",
      noKafkaClustersYet: "No Kafka instances yet",
      noMatchingClusters: "No matching instances",
      createDevClusterHint: "Create a development instance to start producing messages.",
      createUnavailableHint: "Connect the API to create a development instance.",
      tryDifferentName: "Try a different name or clear the search.",
      selectACluster: "Select an instance",
      selectAClusterHint: "Choose an instance from the list to inspect its observed status.",
      apiUnavailableTitle: "Management API unavailable",
      apiUnavailableCopy: (message) => message || "Showing clearly labelled demo data. Changes require an available API.",
      permissionDeniedTitle: "Permission denied",
      permissionDeniedCopy: "Your session cannot read MessageQueue resources in this workspace.",
      demoDataTitle: "Demo data",
      demoDataCopy: "When the backend cannot be reached, the app falls back to read-only demo data.",
      observedState: "Observed state",
      generation: "generation",
      engineName: "Apache Kafka",
      kafkaVersion: "Kafka version",
      topology: "Topology",
      storage: "Storage",
      bootstrapEndpoint: "Bootstrap endpoint",
      deletionPolicy: "Deletion policy",
      retainData: "Retain data",
      deleteWithCluster: "Delete with instance",
      controllerDefault: "Controller default",
      conditions: "Conditions",
      controllerReported: "controller reported",
      recentEvents: "Recent events",
      latestFirst: "latest first",
      detailTabsLabel: "Instance detail tabs",
      conditionPlaceholder: "The controller has not reported a condition yet.",
      eventPlaceholder: "No recent events reported.",
      overview: "Overview",
      controllerEvent: "Controller event",
      connections: "Connections",
      clientConnection: "Client connection",
      credentialsStayServerSide: "Credentials stay server-side",
      credentialsProtected:
        "Retrieve a short-lived client configuration through an authorized server operation. Passwords and private keys are never rendered in browser logs or status.",
      loadClientConfig: "Load client config",
      retryClientConfig: "Retry config",
      loadingClientConfig: "Loading client configuration…",
      clientConfigUnavailable: "Client configuration unavailable",
      clientConfigFetchNote: (name) => `Configuration is fetched through the server and scoped to ${name}.`,
      clientConfigDegraded: "Client configuration is not ready yet; Secret data is not exposed.",
      bootstrapServers: "Bootstrap servers",
      username: "Username",
      secretReference: "Secret reference",
      noSecretMaterial: "Only connection metadata is shown here. Passwords, private keys, and kubeconfigs are never returned to the browser.",
      authentication: "Authentication",
      transport: "Transport",
      mechanism: "Mechanism",
      kafkaUser: "Kafka user",
      access: "Access",
      tls: "TLS",
      scramSha512: "SCRAM-SHA-512",
      workspaceScoped: "Workspace scoped",
      copy: "Copy",
      liveLogs: "Live logs",
      loadLogs: "Load logs",
      retryLogs: "Retry logs",
      brokerLogs: "Broker logs",
      logsUnavailable: "Logs unavailable",
      loadingLogs: "Loading broker logs…",
      logsOptional: "Historical or live logs are an optional platform dependency and do not block Kafka operations.",
      logsFetchNote: (name) => `Logs are fetched through a fixed server-owned query scoped to ${name}.`,
      logs: "Logs",
      loadMetrics: "Load metrics",
      retryMetrics: "Retry metrics",
      brokerHealth: "Broker health",
      loadingMetrics: "Loading broker metrics…",
      metricsUnavailable: "Metrics unavailable",
      metricsOptional: "VictoriaMetrics may be unavailable or not configured for this workspace.",
      metricsFetchNote: "Metrics use a fixed server-owned query. Namespace and instance selectors are injected by the backend.",
      metricsProviderMissing: "The metrics provider is not configured; Kafka operations are unaffected.",
      metricsMessagesIn: "Messages in",
      metricsMessagesOut: "Messages out",
      metricsUnderReplicated: "Under-replicated",
      metricsConsumerLag: "Consumer lag",
      metrics: "Metrics",
      perSecond: "per second",
      partitions: "partitions",
      messages: "messages",
      refresh: "Refresh",
      refreshData: "Refresh data",
      openHelp: "Open help",
      newResource: "New instance",
      createKafkaCluster: "Create Kafka instance",
      createDescription: "Choose a profile and the controller will provision Strimzi resources in your workspace namespace.",
      close: "Close",
      basicSectionTitle: "Basic information",
      basicSectionCopy: "The name becomes the Kubernetes resource name.",
      clusterName: "Instance name",
      required: "required",
      lowercaseHint: "Lowercase letters, numbers, and hyphens only.",
      kafkaVersionLabel: "Kafka version",
      recommended: "recommended",
      profileLabel: "Spec profile",
      profileHelp: "Development is enabled by default; switch to custom when you need more resources.",
      profileDevelopmentTitle: "Development",
      profileDevelopmentCopy: "Single broker for validation and light tests.",
      profileDevelopmentMeta: "500m / 1Gi / 10Gi",
      profileStandardTitle: "Standard",
      profileStandardCopy: "Three brokers for production-like validation.",
      profileStandardMeta: "1 CPU / 2Gi / 20Gi",
      profileCustomTitle: "Custom",
      profileCustomCopy: "Configure brokers, CPU, memory, and storage manually.",
      profileCustomMeta: "Editable",
      resourceSectionTitle: "Spec details",
      resourceSectionCopy: "CPU and memory become each broker's resource requests and limits.",
      brokerCount: "Broker count",
      brokerCountHint: "Use 3+ brokers for production.",
      cpuPerBroker: "CPU per broker",
      cpuHint: "Use 500m, 1, or 1.5; each broker is capped at 8 CPU.",
      memoryPerBroker: "Memory per broker",
      memoryHint: "Use Gi or Mi values; each broker is capped at 64Gi.",
      storagePerBroker: "Storage per broker",
      storageHint: "Each broker supports 1 to 1024 Gi.",
      storageClass: "Storage class",
      deletionPolicyLabel: "Deletion policy",
      storageClassPlaceholder: "Default storage class",
      resourceFootprint: "Resource footprint",
      resourceFootprintCopy: ({ profile, brokers, cpu, memory, storageGi, policy }) =>
        `${profile} · ${brokers} broker${brokers === 1 ? "" : "s"} · ${cpu} CPU / ${memory} memory · ${storageGi} Gi each · ${policy}`,
      creationAsync: "Creation is asynchronous. You can follow the observed state from the instance detail.",
      lastTransition: "last transition",
      cancel: "Cancel",
      create: "Create",
      formError: "Enter a valid instance name and check the requested values.",
      formPermissionDenied: "Permission denied: your workspace cannot create instances.",
      sessionRequiredTitle: "Session unavailable",
      sessionRequiredCopy: "The current session is unavailable. Refresh Desktop and try again.",
      quotaLoading: "Reading the current workspace CPU, memory, and storage quota…",
      quotaUnavailable: "Quota details are unavailable for now; the backend will still recheck on submit.",
      quotaReady: ({ cpu, memory, storage }) =>
        `Workspace quota: CPU ${cpu}, memory ${memory}, storage ${storage}.`,
      quotaDegraded: (message) => `Quota is not configured${message ? `: ${message}` : ""}`,
      quotaExceededCopy: (message) => `Workspace quota is insufficient${message ? `: ${message}` : ""}`,
      resourceNotFoundCopy: "That instance no longer exists. Go back to the list and try again.",
      formCreateFailed: (message) => `Instance creation failed: ${message}`,
      createButton: "Create instance",
      createButtonShort: "+ New",
      statusUnknown: "Unknown",
      statusReady: "Ready",
      statusProvisioning: "Provisioning",
      statusUpdating: "Updating",
      statusDegraded: "Degraded",
      statusFailed: "Failed",
      statusSuspended: "Suspended",
      statusDeleting: "Deleting",
      statusLabel: "Status",
      loadingDemoClusters: "Loading observed instances…",
      stateReadyMeta: "Observed and serving",
      stateAttentionMeta: "Provisioning or degraded",
      demoReadyEvent1: "Kafka user credentials reconciled.",
      demoReadyEvent2: "All 3 brokers reported Ready.",
      demoProvisioningEvent1: "MessageQueue accepted by the controller.",
      demoProvisioningEvent2: "Kafka resource created through Strimzi.",
      demoReadyCondition1: "Kafka brokers are accepting connections.",
      demoReadyCondition2: "Broker metrics and logs are available.",
      demoProvisioningCondition1: "Waiting for the Kafka broker pod to become Ready.",
      demoProvisioningCondition2: "Reconciling Strimzi resources.",
      noRecentEvents: "No recent events reported.",
      noConditions: "The controller has not reported a condition yet.",
      clusterScopedAccess: "Workspace scoped access",
      credentialSafety: "Passwords and private keys are never rendered in browser logs.",
      topicPlaceholder: "Example: orders-dev",
      topbarHelp: "Help",
      topbarRefresh: "Refresh",
      managementApiUnavailable:
        "The backend could not be reached. Demo data is read only; connect the API before creating instances.",
      clusterCreationUnavailable: "The management API is unavailable, so instances cannot be created yet.",
      loadingState: "Loading…",
      noHelp: "No help content yet",
      operationsComingSoon: "Operations are still being polished",
      settings: "Settings",
      deleteCluster: "Delete instance",
      deletingCluster: "Deleting…",
      dangerZone: "Danger zone",
      deleteClusterDescription: "Delete the MessageQueue instance. PVC handling follows the deletion policy selected during creation.",
      deleteUnavailable: "The management API is unavailable, so instances cannot be deleted yet.",
      deleteConfirmPrompt: (name) => `Delete ${name}? This submits a write to the current workspace.`,
      deleteFailed: (message) => `Delete failed: ${message}`,
      deleteRetainImpact: "The current policy retains data volumes.",
      deleteDeleteImpact: "The current policy deletes data volumes with the instance.",
      readOnlySuffix: "read only",
      workspaceReady: "Workspace ready",
      workspacePending: "Workspace loading"
    }
  };

  const STATUS_MAP = {
    zh: {
      ready: ["就绪", "ready"],
      provisioning: ["准备中", "provisioning"],
      creating: ["准备中", "provisioning"],
      updating: ["更新中", "updating"],
      degraded: ["降级", "degraded"],
      failed: ["失败", "failed"],
      suspended: ["已挂起", "suspended"],
      deleting: ["删除中", "deleting"]
    },
    en: {
      ready: ["Ready", "ready"],
      provisioning: ["Provisioning", "provisioning"],
      creating: ["Provisioning", "provisioning"],
      updating: ["Updating", "updating"],
      degraded: ["Degraded", "degraded"],
      failed: ["Failed", "failed"],
      suspended: ["Suspended", "suspended"],
      deleting: ["Deleting", "deleting"]
    }
  };

  const CREATE_PROFILES = {
    development: {
      brokers: 1,
      cpu: "500m",
      memory: "1Gi",
      storageGi: 10,
      storageClass: "",
      deletionPolicy: "Retain",
      titleKey: "profileDevelopmentTitle"
    },
    standard: {
      brokers: 3,
      cpu: "1",
      memory: "2Gi",
      storageGi: 20,
      storageClass: "",
      deletionPolicy: "Retain",
      titleKey: "profileStandardTitle"
    }
  };

  const CUSTOM_RESOURCE_FIELD_IDS = [
    "broker-count",
    "broker-cpu",
    "broker-memory",
    "storage-size",
    "storage-class",
    "deletion-policy"
  ];

  function getCookie(name) {
    const match = document.cookie.match(new RegExp(`(?:^|; )${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}=([^;]*)`));
    return match ? decodeURIComponent(match[1]) : "";
  }

  function setCookie(name, value, days = 30) {
    const sameSite = location.protocol === "https:" ? "None" : "Lax";
    const secure = location.protocol === "https:" ? "; Secure" : "";
    document.cookie = `${name}=${encodeURIComponent(value)}; Path=/; Max-Age=${days * 24 * 60 * 60}; SameSite=${sameSite}${secure}`;
  }

  function detectLanguage() {
    const candidates = [
      window.MESSAGEQUEUE_LOCALE,
      getCookie("MESSAGEQUEUE_LOCALE"),
      getCookie("NEXT_LOCALE"),
      navigator.language,
      DEFAULT_LANGUAGE
    ];
    for (const candidate of candidates) {
      const lang = normalizeLanguage(candidate);
      if (lang) return lang;
    }
    return DEFAULT_LANGUAGE;
  }

  const state = {
    clusters: [],
    tab: "overview",
    loading: true,
    apiState: "loading",
    apiMessage: "",
    noticeDismissed: false,
    search: "",
    observability: {},
    clientConfig: {},
    createSubmitting: false,
    createProfile: "development",
    deleteSubmitting: false,
    deleteError: null,
    workspaceQuota: { status: "idle", data: null, message: "" },
    language: detectLanguage(),
    route: { view: "list", clusterName: "" }
  };

  const $ = (selector) => document.querySelector(selector);

  function parseRoute() {
    const parts = (location.hash || "").replace(/^#\/?/, "").split("/").filter(Boolean);
    if (parts[0] !== "clusters") {
      return { view: "list", clusterName: "" };
    }
    if (parts.length < 2) {
      return { view: "list", clusterName: "" };
    }
    const encodedName = parts.slice(1).join("/");
    let clusterName = encodedName;
    try {
      clusterName = decodeURIComponent(encodedName);
    } catch {
      clusterName = encodedName;
    }
    return {
      view: "detail",
      clusterName
    };
  }

  function syncRouteFromLocation() {
    state.route = parseRoute();
  }

  function commitRouteHash(target) {
    if (location.hash !== target) {
      location.hash = target;
    }
    syncRouteFromLocation();
    window.scrollTo({ top: 0, behavior: "auto" });
    render();
  }

  function navigateToList() {
    commitRouteHash("#/clusters");
  }

  function navigateToCluster(name) {
    if (!name) return;
    state.tab = "overview";
    state.observability = {};
    commitRouteHash(`#/clusters/${encodeURIComponent(name)}`);
  }

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[character]));
  }

  function interpolate(template, params = {}) {
    return String(template).replace(/\{\{(\w+)\}\}/g, (_, key) => escapeHtml(params[key] ?? ""));
  }

  function message(key, params = {}) {
    const locale = LOCALES[state.language] || LOCALES.zh;
    const fallback = LOCALES.en;
    const raw = locale[key] ?? fallback[key] ?? key;
    return typeof raw === "function" ? raw(params?.count ?? params.message ?? params.name ?? params) : interpolate(raw, params);
  }

  function normalizeLanguage(raw) {
    let value = "";
    if (typeof raw === "string") {
      value = raw;
    } else if (raw && typeof raw === "object") {
      const data = raw.data && typeof raw.data === "object" ? raw.data : {};
      value =
        raw.currentLanguage ||
        data.currentLanguage ||
        raw.lng ||
        data.lng ||
        raw.lang ||
        data.lang ||
        raw.language ||
        data.language ||
        raw.locale ||
        data.locale ||
        "";
    }
    const lang = String(value || "").trim().toLowerCase();
    if (lang.startsWith("zh")) return "zh";
    if (lang.startsWith("en")) return "en";
    return "";
  }

  function applyLanguage(lang) {
    const nextLanguage = normalizeLanguage(lang) || DEFAULT_LANGUAGE;
    const changed = state.language !== nextLanguage;
    state.language = nextLanguage;
    setCookie("MESSAGEQUEUE_LOCALE", state.language);
    setCookie("NEXT_LOCALE", state.language);
    document.documentElement.lang = state.language === "zh" ? "zh-CN" : "en";
    const title = message("title");
    document.title = title;
    const description = $("meta[name='description']");
    if (description) description.setAttribute("content", message("description"));
    if (changed && state.apiState === "degraded") {
      state.clusters = demoClustersFor(state.language);
    }
    return changed;
  }

  function createMessageId() {
    if (window.crypto?.randomUUID) return window.crypto.randomUUID();
    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  function parseApiError(body) {
    if (!body) {
      return { code: "", message: "" };
    }
    try {
      const parsed = JSON.parse(body);
      const error = parsed?.error;
      if (error && typeof error === "object") {
        return {
          code: String(error.code || "").trim(),
          message: String(error.message || "").trim()
        };
      }
    } catch {
      // Fall back to raw text below.
    }
    return { code: "", message: body.trim() };
  }

  function describeApiError(error, fallbackKey = "managementApiUnavailable") {
    const status = Number(error?.code || 0);
    const apiCode = String(error?.apiCode || "").trim().toLowerCase();
    const apiMessage = String(error?.apiMessage || error?.message || "").trim();
    if (status === 401 || apiCode === "unauthenticated") {
      return message("sessionRequiredCopy");
    }
    if (apiCode === "quota_exceeded") {
      return message("quotaExceededCopy", { message: apiMessage || message("quotaUnavailable") });
    }
    if (status === 403) {
      return message("permissionDeniedCopy");
    }
    if (status === 404 || apiCode === "not_found") {
      return message("resourceNotFoundCopy");
    }
    if (status === 400 || apiCode === "invalid_request") {
      return apiMessage || message("formError");
    }
    return apiMessage || message(fallbackKey);
  }

  function formatQuotaValue(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return "—";
    const rounded = Math.round(numeric * 1000) / 1000;
    return String(rounded).replace(/\.0+$/, "").replace(/(\.\d*[1-9])0+$/, "$1");
  }

  function formatQuotaItem(item) {
    if (!item) return "—";
    const available = formatQuotaValue(item.available);
    const limit = formatQuotaValue(item.limit);
    const unit = String(item.unit || "").trim();
    if (available === "—" && limit === "—") return "—";
    if (limit === "—" || limit === "") {
      return `${available}${unit ? ` ${unit}` : ""}`;
    }
    return `${available}/${limit}${unit ? ` ${unit}` : ""}`;
  }

  function requestSealosDesktopLanguage() {
    if (window.top === window) {
      return Promise.reject(new Error("not running in Sealos Desktop"));
    }

    const messageId = createMessageId();
    const payload = {
      messageId,
      apiName: SEALOS_DESKTOP_LANGUAGE_API,
      appKey: "",
      clientLocation: window.location.origin,
      success: false,
      data: {}
    };

    return new Promise((resolve, reject) => {
      const timer = window.setTimeout(() => {
        window.removeEventListener("message", handleMessage);
        reject(new Error("Sealos Desktop language request timed out"));
      }, SEALOS_DESKTOP_REQUEST_TIMEOUT_MS);

      function handleMessage(event) {
        if (event.source !== window.top) return;
        const data = event.data || {};
        if (data.messageId !== messageId) return;
        window.clearTimeout(timer);
        window.removeEventListener("message", handleMessage);
        if (data.success) {
          resolve(data.data);
        } else {
          reject(new Error(data.message || "Sealos Desktop language request failed"));
        }
      }

      window.addEventListener("message", handleMessage);
      window.top?.postMessage(payload, "*");
    });
  }

  async function syncSealosLanguage(eventData) {
    let nextLanguage = normalizeLanguage(eventData);
    if (!nextLanguage) {
      try {
        nextLanguage = normalizeLanguage(await requestSealosDesktopLanguage());
      } catch {
        return;
      }
    }
    if (nextLanguage && applyLanguage(nextLanguage)) {
      render();
    }
  }

  function setupSealosLanguageSync() {
    window.addEventListener("message", (event) => {
      if (window.top === window || event.source !== window.top) return;
      const data = event.data || {};
      if (data.apiName !== SEALOS_DESKTOP_EVENT_API || data.eventName !== SEALOS_DESKTOP_CHANGE_I18N_EVENT) return;
      syncSealosLanguage(data.data || data);
    });

    syncSealosLanguage();
  }

  function localizeStaticShell() {
    const staticMap = [
      ["brand-subtitle", "brandSubtitle"],
      ["nav-clusters-label", "navClusters"],
      ["nav-operations-label", "navOperations"],
      ["workspace-label", "workspace"],
      ["workspace-owner-title", "workspaceOwner"],
      ["workspace-owner-role", "ownerRole"],
      ["breadcrumb-workspace", "breadcrumbWorkspace"],
      ["breadcrumb-clusters", "breadcrumbClusters"],
      ["section-tag", "sectionTag"],
      ["page-title", "pageTitle"],
      ["page-description", "pageDescription"],
      ["back-button", "backToList"],
      ["create-button", "newCluster"],
      ["stat-label-total", "totalClusters"],
      ["stat-total-meta", "observedInWorkspace"],
      ["stat-label-ready", "ready"],
      ["stat-ready-meta", "observedAndServing"],
      ["stat-label-attention", "attention"],
      ["stat-attention-meta", "provisioningOrDegraded"],
      ["stat-label-engine", "engine"],
      ["stat-engine-meta", "strimziManaged"],
      ["cluster-list-title", "yourClusters"],
      ["search-input", "searchPlaceholder"],
      ["create-eyebrow", "newResource"],
      ["create-title", "createKafkaCluster"],
      ["create-description", "createDescription"],
      ["basic-section-title", "basicSectionTitle"],
      ["basic-section-copy", "basicSectionCopy"],
      ["field-name-label", "clusterName"],
      ["field-name-required", "required"],
      ["field-name-help", "lowercaseHint"],
      ["field-version-label", "kafkaVersionLabel"],
      ["field-version-recommended", "recommended"],
      ["profile-label", "profileLabel"],
      ["profile-help", "profileHelp"],
      ["profile-development-title", "profileDevelopmentTitle"],
      ["profile-development-copy", "profileDevelopmentCopy"],
      ["profile-development-meta", "profileDevelopmentMeta"],
      ["profile-standard-title", "profileStandardTitle"],
      ["profile-standard-copy", "profileStandardCopy"],
      ["profile-standard-meta", "profileStandardMeta"],
      ["profile-custom-title", "profileCustomTitle"],
      ["profile-custom-copy", "profileCustomCopy"],
      ["profile-custom-meta", "profileCustomMeta"],
      ["resource-section-title", "resourceSectionTitle"],
      ["resource-section-copy", "resourceSectionCopy"],
      ["field-brokers-label", "brokerCount"],
      ["field-brokers-help", "brokerCountHint"],
      ["field-cpu-label", "cpuPerBroker"],
      ["field-cpu-help", "cpuHint"],
      ["field-memory-label", "memoryPerBroker"],
      ["field-memory-help", "memoryHint"],
      ["field-storage-label", "storagePerBroker"],
      ["field-storage-help", "storageHint"],
      ["field-class-label", "storageClass"],
      ["field-policy-label", "deletionPolicyLabel"],
      ["field-class-placeholder", "storageClassPlaceholder"],
      ["impact-heading", "resourceFootprint"],
      ["impact-note", "creationAsync"],
      ["cancel-button", "cancel"],
      ["submit-create", "create"]
    ];

    for (const [id, key] of staticMap) {
      const node = document.getElementById(id);
      if (!node) continue;
      if (node.tagName === "INPUT") {
        if (id === "search-input") node.setAttribute("placeholder", message(key));
        continue;
      }
      node.textContent = message(key);
    }

    $("#refresh-button")?.setAttribute("aria-label", message("refreshData"));
    $("#refresh-button")?.setAttribute("title", message("refreshData"));
    $("#help-button")?.setAttribute("aria-label", message("openHelp"));
    $("#help-button")?.setAttribute("title", message("openHelp"));
    $("#search-input")?.setAttribute("placeholder", message("searchPlaceholder"));
    if ($("#create-button")) {
      $("#create-button").disabled = !writesEnabled();
      $("#create-button").setAttribute("title", writesEnabled() ? message("newCluster") : message("clusterCreationUnavailable"));
      setDisabledReason($("#create-button"), writesEnabled() ? "" : "api_unavailable");
    }
    $("#submit-create")?.setAttribute("value", "default");

    const kafkaVersion = $("#kafka-version");
    if (kafkaVersion?.options?.[0]) kafkaVersion.options[0].textContent = `3.9.0 (${message("recommended")})`;
    if (kafkaVersion?.options?.[1]) kafkaVersion.options[1].textContent = "4.0.0";
    const deletionPolicy = $("#deletion-policy");
    if (deletionPolicy?.options?.[0]) deletionPolicy.options[0].textContent = message("retainData");
    if (deletionPolicy?.options?.[1]) deletionPolicy.options[1].textContent = message("deleteWithCluster");
    $("#cluster-name")?.setAttribute("placeholder", "orders-dev");
    $("#storage-class")?.setAttribute("placeholder", message("storageClassPlaceholder"));
    $("#modal-close")?.setAttribute("aria-label", message("close"));
    $("#modal-close")?.setAttribute("title", message("close"));
    $("#form-error") && ($("#form-error").textContent = message("formError"));
    updateCreateSummary();

    const createButton = $("#create-button");
    if (createButton) createButton.textContent = message("newCluster");
  }

  function statusLabel(phase) {
    const value = String(phase || "unknown").toLowerCase();
    return (STATUS_MAP[state.language] || STATUS_MAP.zh)[value] || [(state.language === "zh" ? "未知" : "Unknown"), "degraded"];
  }

  function formatTime(value) {
    if (!value) return "—";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return new Intl.DateTimeFormat(state.language === "zh" ? "zh-CN" : "en-US", {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit"
    }).format(date);
  }

  function formatRelativeTime(value) {
    if (!value) return "—";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    const diffMinutes = Math.round((date.getTime() - Date.now()) / 60000);
    const absMinutes = Math.abs(diffMinutes);
    const locale = state.language === "zh" ? "zh-CN" : "en";
    const formatter = new Intl.RelativeTimeFormat(locale, { numeric: "auto" });
    if (absMinutes < 60) return formatter.format(diffMinutes, "minute");
    const diffHours = Math.round(diffMinutes / 60);
    if (Math.abs(diffHours) < 24) return formatter.format(diffHours, "hour");
    const diffDays = Math.round(diffMinutes / 1440);
    return formatter.format(diffDays, "day");
  }

  function formatEventTime(event) {
    if (!event) return "—";
    return formatRelativeTime(event.lastTimestamp || event.timestamp || event.time);
  }

  function formatBrokerCount(count) {
    const value = Number(count) || 0;
    if (state.language === "zh") return `${value} 个 broker`;
    return `${value} broker${value === 1 ? "" : "s"}`;
  }

  function formatBrokerStorage(size) {
    return state.language === "zh" ? `${size} Gi / broker` : `${size} Gi / broker`;
  }

  function formatConditionType(type) {
    const value = String(type || message("statusUnknown"));
    if (state.language !== "zh") return value;
    const map = {
      Ready: "就绪",
      ObservabilityReady: "可观测性就绪",
      Degraded: "降级",
      Progressing: "推进中",
      Suspended: "已挂起"
    };
    return map[value] || value;
  }

  function formatConditionStatus(status) {
    const value = String(status || message("statusUnknown"));
    if (state.language !== "zh") return value;
    const map = {
      True: "是",
      False: "否",
      Unknown: "未知"
    };
    return map[value] || value;
  }

  function localizeBackendText(text) {
    const value = String(text || "");
    const legacyReadyMessage = ["Strimzi reports the Kafka ", "cluster", " is ready"].join("");
    const normalized = value === legacyReadyMessage ? "Strimzi reports the Kafka instance is ready" : value;
    if (state.language !== "zh") return normalized;
    const map = {
      "Strimzi reports the Kafka instance is ready": "Strimzi 表示 Kafka 实例已就绪",
      "reconciliation is active": "调谐正在进行",
      "Waiting for the Kafka broker pod to become Ready.": "等待 Kafka broker Pod 就绪。",
      "Reconciling Strimzi resources.": "正在协调 Strimzi 资源。",
      "Kafka brokers are accepting connections.": "Kafka broker 正在接受连接。",
      "Broker metrics and logs are available.": "Broker 指标和日志已可用。",
      "Kafka user credentials reconciled.": "Kafka 用户凭据已完成协调。",
      "All 3 brokers reported Ready.": "3 个 broker 已全部就绪。",
      "MessageQueue accepted by the controller.": "MessageQueue 已被控制器接收。",
      "Kafka resource created through Strimzi.": "Kafka 资源正在通过 Strimzi 创建。"
    };
    return map[normalized] || normalized;
  }

  function parseSizeGi(value) {
    const match = String(value ?? "").match(/([0-9]+(?:\.[0-9]+)?)/);
    return match ? Number(match[1]) : 20;
  }

  function demoClustersFor(language) {
    const zh = language === "zh";
    const now = Date.now();
    const minutesAgo = (minutes) => new Date(now - minutes * 60 * 1000).toISOString();
    return [
      {
        metadata: {
          name: "orders-prod",
          namespace: "workspace-main",
          creationTimestamp: "2026-07-24T06:08:00Z"
        },
        spec: {
          engine: "kafka",
          kafka: { version: "3.9.0", replicas: 3 },
          storage: { size: "64Gi", className: "premium-rwo" },
          deletionPolicy: "Retain"
        },
        status: {
          phase: "Ready",
          ready: true,
          observedGeneration: 4,
          endpoint: "orders-prod-kafka-bootstrap.workspace-main.svc:9092",
          lastTransitionTime: "2026-07-24T06:14:18Z",
          conditions: [
            {
              type: "Ready",
              status: "True",
              reason: "KafkaReady",
              message: zh ? message("demoReadyCondition1") : message("demoReadyCondition1")
            },
            {
              type: "ObservabilityReady",
              status: "True",
              reason: "ScrapeConfigured",
              message: zh ? message("demoReadyCondition2") : message("demoReadyCondition2")
            }
          ],
          events: [
            { lastTimestamp: minutesAgo(12), message: message("demoReadyEvent1") },
            { lastTimestamp: minutesAgo(18), message: message("demoReadyEvent2") }
          ]
        }
      },
      {
        metadata: {
          name: "events-dev",
          namespace: "workspace-main",
          creationTimestamp: "2026-07-28T03:52:00Z"
        },
        spec: {
          engine: "kafka",
          kafka: { version: "3.9.0", replicas: 1 },
          storage: { size: "20Gi", className: "standard" },
          deletionPolicy: "Retain"
        },
        status: {
          phase: "Provisioning",
          ready: false,
          observedGeneration: 1,
          endpoint: "Pending",
          lastTransitionTime: "2026-07-28T03:52:00Z",
          conditions: [
            {
              type: "Ready",
              status: "False",
              reason: "WaitingForBrokers",
              message: zh ? message("demoProvisioningCondition1") : message("demoProvisioningCondition1")
            }
          ],
          events: [
            { lastTimestamp: minutesAgo(2), message: message("demoProvisioningEvent1") },
            { lastTimestamp: minutesAgo(1), message: message("demoProvisioningEvent2") }
          ]
        }
      }
    ];
  }

  function normalizedClusters() {
    return state.clusters.map((raw) => {
      const metadata = raw?.metadata || {};
      const spec = raw?.spec || {};
      const kafka = spec.kafka || {};
      const storage = spec.storage || {};
      const status = raw?.status || {};
      const conditions = Array.isArray(status.conditions) ? status.conditions : [];
      const phase = status.phase || (conditions.find((condition) => condition.type === "Ready" && condition.status === "True") ? "Ready" : "Provisioning");
      const firstEndpoint = status.endpoints?.[0];
      const endpoint = status.endpoint || status.bootstrapEndpoint || (typeof firstEndpoint === "string" ? firstEndpoint : firstEndpoint ? `${firstEndpoint.host}:${firstEndpoint.port}` : "Pending");
      return {
        raw,
        metadata,
        spec,
        kafka,
        storage,
        status,
        conditions,
        name: metadata.name || raw.name || "unnamed-instance",
        namespace: metadata.namespace || raw.namespace || "workspace",
        brokers: Number(kafka.brokers || kafka.replicas || spec.replicas || spec.brokers || status.topology?.brokers || 1),
        storageGi: parseSizeGi(kafka.storageGi || storage.size || spec.storageGi || 20),
        version: kafka.version || status.version || spec.version || "3.9.0",
        storageClass: kafka.storageClass || storage.storageClass || storage.className || storage.class || "default",
        deletionPolicy: spec.deletionPolicy || storage.deletionPolicy || "unspecified",
        phase,
        endpoint,
        lastTransitionTime: status.lastTransitionTime || status.conditions?.[0]?.lastTransitionTime || metadata.creationTimestamp || raw.creationTimestamp,
        events: Array.isArray(status.events) ? status.events : []
      };
    });
  }

  function selectedCluster() {
    if (state.route.view !== "detail") return null;
    const clusters = normalizedClusters();
    return clusters.find((cluster) => cluster.name === state.route.clusterName) || null;
  }

  function writesEnabled() {
    return state.apiState === "ready";
  }

  async function request(path, options = {}) {
    const response = await fetch(`${API_BASE}${path}`, {
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      credentials: "include",
      ...options
    });
    if (!response.ok) {
      const body = await response.text();
      const apiError = parseApiError(body);
      const error = new Error(apiError.message || body || `Request failed (${response.status})`);
      error.code = response.status;
      error.apiCode = apiError.code;
      error.apiMessage = apiError.message;
      throw error;
    }
    if (response.status === 204) return null;
    return response.json();
  }

  function setApiState(apiState, messageText = "") {
    if (state.apiState !== apiState || state.apiMessage !== messageText) {
      state.noticeDismissed = false;
    }
    state.apiState = apiState;
    state.apiMessage = messageText;
  }

  function renderNotice() {
    const region = $("#notice-region");
    if (!region) return;
    if (state.noticeDismissed) {
      region.innerHTML = "";
      return;
    }
    if (state.apiState === "degraded") {
      region.innerHTML = `<div class="notice" data-tone="warning"><span class="notice-icon" aria-hidden="true">!</span><div class="notice-copy"><strong>${escapeHtml(message("apiUnavailableTitle"))}</strong><p>${escapeHtml(message("apiUnavailableCopy", { message: state.apiMessage }))}</p></div><button class="icon-button notice-close" type="button" aria-label="${escapeHtml(message("close"))}" data-action="dismiss-notice">×</button></div>`;
    } else if (state.apiState === "forbidden") {
      const title = state.apiMessage === message("sessionRequiredCopy") ? message("sessionRequiredTitle") : message("permissionDeniedTitle");
      region.innerHTML = `<div class="notice" data-tone="error"><span class="notice-icon" aria-hidden="true">!</span><div class="notice-copy"><strong>${escapeHtml(title)}</strong><p>${escapeHtml(state.apiMessage || message("permissionDeniedCopy"))}</p></div></div>`;
    } else {
      region.innerHTML = "";
    }
  }

  function renderStats(clusters) {
    const ready = clusters.filter((cluster) => statusLabel(cluster.phase)[1] === "ready").length;
    const attention = clusters.length - ready;
    const set = (id, value) => {
      const node = $(id);
      if (node) node.textContent = value;
    };
    set("#stat-total", state.loading ? "—" : String(clusters.length));
    set("#stat-ready", state.loading ? "—" : String(ready));
    set("#stat-attention", state.loading ? "—" : String(attention));
    set("#stat-total-meta", state.apiState === "degraded" ? message("demoDataCopy") : message("observedInWorkspace"));
    set("#stat-ready-meta", message("stateReadyMeta"));
    set("#stat-attention-meta", message("stateAttentionMeta"));
    set("#stat-engine-meta", message("strimziManaged"));
    set("#nav-count", state.loading ? "—" : String(clusters.length));
    set("#workspace-name", clusters[0]?.namespace || message("workspacePending"));
    set("#workspace-name-state", state.loading ? message("workspacePending") : message("workspaceReady"));
  }

  function filteredClusters() {
    const query = state.search.trim().toLowerCase();
    return normalizedClusters().filter((cluster) => !query || `${cluster.name} ${cluster.namespace} ${cluster.version}`.toLowerCase().includes(query));
  }

  function renderRouteChrome() {
    const app = $("#app");
    const isDetail = state.route.view === "detail";
    const cluster = selectedCluster();
    const pageTitle = $("#page-title");
    const pageDescription = $("#page-description");
    const breadcrumbClusters = $("#breadcrumb-clusters");
    const createButton = $("#create-button");
    const backButton = $("#back-button");
    const statGrid = $("#stat-grid");
    const listPanel = $("#list-panel");
    const detailPanel = $("#detail-panel");
    const operationsHint = $("#operations-hint");

    if (app) app.dataset.view = isDetail ? "detail" : "list";
    if (pageTitle) {
      pageTitle.textContent = isDetail ? cluster?.name || message("detailPageTitle") : message("pageTitle");
    }
    if (pageDescription) {
      pageDescription.textContent = isDetail
        ? cluster
          ? `${cluster.namespace} · ${message("lastTransition")} ${formatTime(cluster.lastTransitionTime)}`
          : message("detailPageDescription")
        : message("pageDescription");
    }
    if (breadcrumbClusters) {
      breadcrumbClusters.textContent = isDetail
        ? `${message("breadcrumbClusters")} / ${cluster?.name || message("detailPageTitle")}`
        : message("breadcrumbClusters");
    }
    if (createButton) {
      const canWrite = writesEnabled();
      createButton.classList.toggle("is-hidden", isDetail);
      createButton.disabled = !canWrite || isDetail;
      createButton.textContent = message("newCluster");
      createButton.setAttribute(
        "title",
        !canWrite
          ? message("clusterCreationUnavailable")
          : isDetail
            ? message("detailPageTitle")
            : message("newCluster")
      );
    }
    if (backButton) {
      backButton.classList.toggle("is-hidden", !isDetail);
      backButton.setAttribute("aria-hidden", isDetail ? "false" : "true");
      backButton.setAttribute("aria-label", message("backToList"));
      backButton.setAttribute("title", message("backToList"));
    }
    statGrid?.classList.add("is-hidden");
    listPanel?.classList.toggle("is-hidden", isDetail);
    detailPanel?.classList.toggle("is-hidden", !isDetail);
    operationsHint?.classList.toggle("is-hidden", isDetail);
  }

  function renderClusterList() {
    const list = $("#cluster-list");
    if (!list) return;
    list.setAttribute("aria-busy", state.loading ? "true" : "false");
    if (state.loading) {
      list.innerHTML = `<div class="loading-state"><span>${escapeHtml(message("loadingDemoClusters"))}</span><span class="loading-bar" aria-hidden="true"></span></div>`;
      return;
    }

    const clusters = filteredClusters();
    const canWrite = writesEnabled();
    const subtitle = !canWrite ? message("clusterResourcesReadOnly", { count: clusters.length }) : message("clusterResourcesInWorkspace", { count: clusters.length });
    const subtitleNode = $("#list-subtitle");
    if (subtitleNode) subtitleNode.textContent = subtitle;

    if (!clusters.length) {
      const emptyCopy = canWrite ? message("createDevClusterHint") : message("createUnavailableHint");
      list.innerHTML = `<div class="empty-state"><strong>${escapeHtml(state.search ? message("noMatchingClusters") : message("noKafkaClustersYet"))}</strong><p>${escapeHtml(state.search ? message("tryDifferentName") : emptyCopy)}</p>${state.search || !canWrite ? "" : `<button class="button button-primary" type="button" data-action="open-create">${escapeHtml(message("createButtonShort"))}</button>`}</div>`;
      return;
    }

    const columns = [
      message("clusterNameColumn"),
      message("statusColumn"),
      message("versionColumn"),
      message("brokerColumn"),
      message("storageColumn"),
      message("namespaceColumn"),
      message("updatedColumn"),
      ""
    ];

    list.innerHTML = `<div class="cluster-table"><div class="cluster-table-head" aria-hidden="true">${columns.map((label, index) => `<span class="cluster-head-cell ${index === 0 ? "is-name" : ""}">${escapeHtml(label)}</span>`).join("")}</div><div class="cluster-table-body">${clusters
      .map((cluster) => {
        const [label, statusClass] = statusLabel(cluster.phase);
        const href = `#/clusters/${encodeURIComponent(cluster.name)}`;
        return `<a class="cluster-row" href="${href}" data-cluster-name="${escapeHtml(cluster.name)}" aria-label="${escapeHtml(message("openDetail"))} ${escapeHtml(cluster.name)}"><span class="cluster-name-cell"><span class="cluster-name">${escapeHtml(cluster.name)}</span></span><span class="cluster-status"><span class="status-badge status-${statusClass}">${escapeHtml(label)}</span></span><span class="cluster-version">${escapeHtml(cluster.version)}</span><span class="cluster-topology"><strong>${escapeHtml(formatBrokerCount(cluster.brokers))}</strong></span><span class="cluster-storage">${escapeHtml(formatBrokerStorage(cluster.storageGi))}</span><span class="cluster-namespace"><code>${escapeHtml(cluster.namespace)}</code></span><span class="cluster-updated">${escapeHtml(formatTime(cluster.lastTransitionTime))}</span><span class="cluster-action" aria-hidden="true">›</span></a>`;
      })
      .join("")}</div></div>`;
  }

  function overviewHtml(cluster) {
    const conditions = cluster.conditions.length ? cluster.conditions : [{ type: "Ready", status: "Unknown", reason: "NoCondition", message: message("noConditions") }];
    const events = cluster.events.length ? cluster.events : [{ time: "—", message: message("noRecentEvents") }];
    const deletionPolicy =
      cluster.deletionPolicy === "delete" || cluster.deletionPolicy === "Delete"
        ? message("deleteWithCluster")
        : cluster.deletionPolicy === "retain" || cluster.deletionPolicy === "Retain"
          ? message("retainData")
          : message("controllerDefault");

    return `<section class="detail-section"><div class="section-heading"><h3>${escapeHtml(message("observedState"))}</h3><span>${escapeHtml(message("generation"))} ${escapeHtml(cluster.status.observedGeneration || "—")}</span></div><dl class="info-grid"><div class="info-item"><dt>${escapeHtml(message("engine"))}</dt><dd>${escapeHtml(message("engineName"))}</dd></div><div class="info-item"><dt>${escapeHtml(message("kafkaVersion"))}</dt><dd>${escapeHtml(cluster.version)}</dd></div><div class="info-item"><dt>${escapeHtml(message("topology"))}</dt><dd>${escapeHtml(formatBrokerCount(cluster.brokers))}</dd></div><div class="info-item"><dt>${escapeHtml(message("storage"))}</dt><dd>${escapeHtml(formatBrokerStorage(cluster.storageGi))}</dd></div><div class="info-item"><dt>${escapeHtml(message("bootstrapEndpoint"))}</dt><dd><code>${escapeHtml(cluster.endpoint)}</code></dd></div><div class="info-item"><dt>${escapeHtml(message("deletionPolicy"))}</dt><dd>${escapeHtml(deletionPolicy)}</dd></div></dl></section><section class="detail-section"><div class="section-heading"><h3>${escapeHtml(message("conditions"))}</h3><span>${escapeHtml(message("controllerReported"))}</span></div><div class="condition-list">${conditions.map((condition) => `<div class="condition-row"><strong>${escapeHtml(formatConditionType(condition.type))} · ${escapeHtml(formatConditionStatus(condition.status))}</strong><span>${escapeHtml(localizeBackendText(condition.message || condition.reason || message("conditionPlaceholder")))}</span></div>`).join("")}</div></section><section class="detail-section"><div class="section-heading"><h3>${escapeHtml(message("recentEvents"))}</h3><span>${escapeHtml(message("latestFirst"))}</span></div><div class="event-list">${events.map((event) => `<div class="event-row"><time>${escapeHtml(formatEventTime(event))}</time><p>${escapeHtml(localizeBackendText(event.message || event.reason || message("controllerEvent")))}</p></div>`).join("")}</div></section>`;
  }

  function connectionsHtml(cluster) {
    const result = state.clientConfig;
    let configBody = `<div class="observability-box"><h3>${escapeHtml(message("clientConnection"))}</h3><p>${escapeHtml(message("clientConfigFetchNote", { name: cluster.name }))}</p><button class="button button-primary" type="button" data-action="load-client-config">${escapeHtml(message("loadClientConfig"))}</button></div>`;
    if (result?.name === cluster.name && result.loading) {
      configBody = `<div class="loading-state"><span>${escapeHtml(message("loadingClientConfig"))}</span><span class="loading-bar" aria-hidden="true"></span></div>`;
    } else if (result?.name === cluster.name && result.error) {
      configBody = `<div class="observability-box"><h3>${escapeHtml(message("clientConfigUnavailable"))}</h3><p>${escapeHtml(result.error)}</p><button class="button button-secondary" type="button" data-action="load-client-config">${escapeHtml(message("retryClientConfig"))}</button></div>`;
    } else if (result?.name === cluster.name && result.data) {
      const config = result.data;
      const servers = Array.isArray(config.bootstrapServers) ? config.bootstrapServers : [];
      const serverRows = servers.length ? servers : [cluster.endpoint];
      const degradedNotice = config.degraded ? `<div class="notice" data-tone="warning"><span class="notice-icon" aria-hidden="true">i</span><div class="notice-copy"><strong>${escapeHtml(message("clientConfigDegraded"))}</strong><p>${escapeHtml(config.message || "")}</p></div></div>` : "";
      configBody = `<div class="connection-block">${serverRows
        .map((server) => `<div class="copy-row"><code>${escapeHtml(server)}</code><button type="button" data-copy="${escapeHtml(server)}">${escapeHtml(message("copy"))}</button></div>`)
        .join("")}<dl class="info-grid"><div class="info-item"><dt>${escapeHtml(message("username"))}</dt><dd><code>${escapeHtml(config.username || `${cluster.name}-client`)}</code></dd></div><div class="info-item"><dt>${escapeHtml(message("secretReference"))}</dt><dd><code>${escapeHtml(config.secretRef || "Pending")}</code></dd></div><div class="info-item"><dt>${escapeHtml(message("transport"))}</dt><dd>${escapeHtml(config.transport || message("tls"))}</dd></div><div class="info-item"><dt>${escapeHtml(message("mechanism"))}</dt><dd>${escapeHtml(config.mechanism || message("scramSha512"))}</dd></div></dl>${degradedNotice}</div>`;
    }
    return `<section class="detail-section"><div class="section-heading"><h3>${escapeHtml(message("clientConnection"))}</h3><span>${escapeHtml(message("credentialsStayServerSide"))}</span></div>${configBody}<div class="notice" data-tone="warning"><span class="notice-icon" aria-hidden="true">i</span><div class="notice-copy"><strong>${escapeHtml(message("credentialsProtected"))}</strong><p>${escapeHtml(message("noSecretMaterial"))}</p></div></div></section><section class="detail-section"><div class="section-heading"><h3>${escapeHtml(message("authentication"))}</h3></div><dl class="info-grid"><div class="info-item"><dt>${escapeHtml(message("transport"))}</dt><dd>${escapeHtml(message("tls"))}</dd></div><div class="info-item"><dt>${escapeHtml(message("mechanism"))}</dt><dd>${escapeHtml(message("scramSha512"))}</dd></div><div class="info-item"><dt>${escapeHtml(message("kafkaUser"))}</dt><dd><code>${escapeHtml(cluster.name)}-client</code></dd></div><div class="info-item"><dt>${escapeHtml(message("access"))}</dt><dd>${escapeHtml(message("workspaceScoped"))}</dd></div></dl></section>`;
  }

  function settingsHtml(cluster) {
    const canWrite = writesEnabled();
    const deleteCopy =
      cluster.deletionPolicy === "delete" || cluster.deletionPolicy === "Delete"
        ? message("deleteDeleteImpact")
        : message("deleteRetainImpact");
    const error = state.deleteError?.name === cluster.name ? `<div class="form-error" role="alert">${escapeHtml(state.deleteError.message)}</div>` : "";
    return `<section class="detail-section"><div class="section-heading"><h3>${escapeHtml(message("dangerZone"))}</h3><span>${escapeHtml(message("deletionPolicy"))}</span></div><div class="danger-panel"><div><strong>${escapeHtml(message("deleteCluster"))}</strong><p>${escapeHtml(message("deleteClusterDescription"))} ${escapeHtml(deleteCopy)}</p>${canWrite ? "" : `<p>${escapeHtml(message("deleteUnavailable"))}</p>`}</div><button class="button button-danger" type="button" data-action="delete-cluster" ${canWrite && !state.deleteSubmitting ? "" : "disabled"}>${escapeHtml(state.deleteSubmitting ? message("deletingCluster") : message("deleteCluster"))}</button></div>${error}</section>`;
  }

  function logsHtml(cluster) {
    const result = state.observability.logs;
    if (result?.name === cluster.name && result.loading) {
      return `<section class="detail-section"><div class="loading-state"><span>${escapeHtml(message("loadingLogs"))}</span><span class="loading-bar" aria-hidden="true"></span></div></section>`;
    }
    if (result?.name === cluster.name && result.error) {
      return `<section class="detail-section"><div class="observability-box"><h3>${escapeHtml(message("logsUnavailable"))}</h3><p>${escapeHtml(result.error)} ${escapeHtml(message("logsOptional"))}</p><button class="button button-secondary" type="button" data-action="load-logs">${escapeHtml(message("retryLogs"))}</button></div></section>`;
    }
    if (result?.name === cluster.name && result.data) {
      return `<section class="detail-section"><div class="section-heading"><h3>${escapeHtml(message("brokerLogs"))}</h3><button class="button button-secondary" type="button" data-action="load-logs">${escapeHtml(message("refresh"))}</button></div><pre class="log-viewer" id="log-viewer">Loading…</pre></section>`;
    }
    return `<section class="detail-section"><div class="observability-box"><h3>${escapeHtml(message("liveLogs"))}</h3><p>${escapeHtml(message("logsFetchNote", { name: cluster.name }))}</p><button class="button button-primary" type="button" data-action="load-logs">${escapeHtml(message("loadLogs"))}</button></div></section>`;
  }

  function metricsHtml(cluster) {
    const result = state.observability.metrics;
    if (result?.name === cluster.name && result.loading) {
      return `<section class="detail-section"><div class="loading-state"><span>${escapeHtml(message("loadingMetrics"))}</span><span class="loading-bar" aria-hidden="true"></span></div></section>`;
    }
    if (result?.name === cluster.name && result.error) {
      return `<section class="detail-section"><div class="observability-box"><h3>${escapeHtml(message("metricsUnavailable"))}</h3><p>${escapeHtml(result.error)} ${escapeHtml(message("metricsOptional"))}</p><button class="button button-secondary" type="button" data-action="load-metrics">${escapeHtml(message("retryMetrics"))}</button></div></section>`;
    }
    const metrics = result?.data || null;
    if (!metrics) {
      return `<section class="detail-section"><div class="observability-box"><h3>${escapeHtml(message("brokerHealth"))}</h3><p>${escapeHtml(message("metricsFetchNote"))}</p><button class="button button-primary" type="button" data-action="load-metrics">${escapeHtml(message("loadMetrics"))}</button></div></section>`;
    }
    const degradedNotice = result.degraded ? `<div class="observability-box" data-tone="warning"><h3>${escapeHtml(message("metricsUnavailable"))}</h3><p>${escapeHtml(result.message || message("metricsProviderMissing"))}</p></div>` : "";
    return `<section class="detail-section"><div class="section-heading"><h3>${escapeHtml(message("brokerHealth"))}</h3><button class="button button-secondary" type="button" data-action="load-metrics">${escapeHtml(message("refresh"))}</button></div><div class="metric-grid">${[
      [message("metricsMessagesIn"), metrics.messagesIn ?? "—", message("perSecond")],
      [message("metricsMessagesOut"), metrics.messagesOut ?? "—", message("perSecond")],
      [message("metricsUnderReplicated"), metrics.underReplicated ?? "—", message("partitions")],
      [message("metricsConsumerLag"), metrics.consumerLag ?? "—", message("messages")]
    ]
      .map(
        ([name, value, unit]) => `<div class="metric-card"><span>${escapeHtml(name)}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(unit)}</small></div>`
      )
      .join("")}</div>${degradedNotice}</section>`;
  }

  function renderDetail() {
    const panel = $("#detail-content");
    if (!panel) return;
    const cluster = selectedCluster();
    if (!cluster) {
      panel.innerHTML = `<div class="empty-state empty-state-detail"><strong>${escapeHtml(message("detailPageTitle"))}</strong><p>${escapeHtml(message("detailPageDescription"))}</p><button class="button button-secondary" type="button" data-action="back-to-list">${escapeHtml(message("backToList"))}</button></div>`;
      return;
    }

    const [label, statusClass] = statusLabel(cluster.phase);
    const tabs = [
      ["overview", message("overview")],
      ["connections", message("connections")],
      ["logs", message("logs")],
      ["metrics", message("metrics")],
      ["settings", message("settings")]
    ];

    const body =
      state.tab === "overview"
        ? overviewHtml(cluster)
        : state.tab === "connections"
          ? connectionsHtml(cluster)
          : state.tab === "logs"
            ? logsHtml(cluster)
            : state.tab === "settings"
              ? settingsHtml(cluster)
              : metricsHtml(cluster);

    panel.innerHTML = `<div class="detail-header"><div class="detail-title"><h2 id="detail-title">${escapeHtml(cluster.name)}</h2><p><code>${escapeHtml(cluster.namespace)}</code> · ${escapeHtml(message("lastTransition"))} ${escapeHtml(formatTime(cluster.lastTransitionTime))}</p></div><div class="detail-actions"><span class="status-badge status-${statusClass}">${escapeHtml(label)}</span><button class="button button-secondary" type="button" data-action="refresh-detail">${escapeHtml(message("refresh"))}</button></div></div><div class="detail-tabs" role="tablist" aria-label="${escapeHtml(message("detailTabsLabel"))}">${tabs.map(([id, title]) => `<button class="tab-button ${state.tab === id ? "is-active" : ""}" type="button" role="tab" aria-selected="${state.tab === id}" data-tab="${id}">${escapeHtml(title)}</button>`).join("")}</div><div class="detail-body">${body}</div>`;

    if (state.tab === "logs" && state.observability.logs?.name === cluster.name && state.observability.logs.data) {
      const viewer = $("#log-viewer");
      if (viewer) viewer.textContent = state.observability.logs.data;
    }
  }

  function render() {
    localizeStaticShell();
    renderRouteChrome();
    const clusters = filteredClusters();
    renderNotice();
    if (state.route.view === "list") renderStats(clusters);
    renderClusterList();
    if (state.route.view === "detail") renderDetail();
    const operationsHint = $("#operations-hint");
    if (operationsHint) operationsHint.textContent = message("operationsComingSoon");
  }

  function renderWorkspaceQuotaNote() {
    const note = $("#quota-note");
    if (!note) return;
    const quotaState = state.workspaceQuota || { status: "idle", data: null, message: "" };
    let tone = "loading";
    let text = message("quotaLoading");

    if (quotaState.status === "ready" && quotaState.data) {
      const items = Array.isArray(quotaState.data.items) ? quotaState.data.items : [];
      const byType = Object.fromEntries(items.map((item) => [item.type, item]));
      text = message("quotaReady", {
        cpu: formatQuotaItem(byType.cpu),
        memory: formatQuotaItem(byType.memory),
        storage: formatQuotaItem(byType.storage)
      });
      tone = "ready";
      if (quotaState.data.degraded) {
        text = message("quotaDegraded", { message: quotaState.data.message || "" });
        tone = "degraded";
      }
    } else if (quotaState.status === "error") {
      text = quotaState.message || message("quotaUnavailable");
      tone = "warning";
    }

    note.textContent = text;
    note.dataset.qaState = tone;
  }

  async function loadClusters() {
    state.loading = true;
    setApiState("loading");
    render();
    try {
      const payload = await request(API_PREFIX);
      const items = Array.isArray(payload) ? payload : payload?.items || payload?.data || payload?.clusters || [];
      state.clusters = items;
      setApiState("ready");
    } catch (error) {
      if (error.code === 401 || error.code === 403) {
        state.clusters = [];
        setApiState("forbidden", describeApiError(error, "permissionDeniedCopy"));
      } else {
        state.clusters = demoClustersFor(state.language);
        setApiState("degraded", describeApiError(error, "managementApiUnavailable"));
      }
    } finally {
      state.loading = false;
      render();
      if (state.apiState === "ready") {
        void loadWorkspaceQuota();
      }
    }
  }

  async function loadWorkspaceQuota() {
    if (state.apiState !== "ready") {
      return;
    }
    state.workspaceQuota = { status: "loading", data: null, message: "" };
    renderWorkspaceQuotaNote();
    try {
      const payload = await request(`${API_PREFIX}/-/quota`);
      state.workspaceQuota = { status: "ready", data: payload, message: "" };
    } catch (error) {
      state.workspaceQuota = {
        status: "error",
        data: null,
        message: describeApiError(error, "quotaUnavailable")
      };
    }
    renderWorkspaceQuotaNote();
  }

  async function loadObservability(kind) {
    const cluster = selectedCluster();
    if (!cluster) return;
    state.observability[kind] = { name: cluster.name, loading: true };
    render();
    try {
      const query = kind === "logs" ? "component=broker&tailLines=200" : "key=throughput";
      const payload = await request(`${API_PREFIX}/${encodeURIComponent(cluster.name)}/${kind}?${query}`);
      if (kind === "logs") {
        const lines = Array.isArray(payload?.lines)
          ? payload.lines.map((line) => `${line.timestamp ? `[${line.timestamp}] ` : ""}${line.message}`).join("\n")
          : payload?.text || payload?.data || "";
        state.observability[kind] = { name: cluster.name, data: lines, degraded: payload?.degraded, message: payload?.message };
      } else {
        const values = Array.isArray(payload?.values) ? payload.values : [];
        const latest = values[values.length - 1]?.value;
        state.observability[kind] = {
          name: cluster.name,
          data: { messagesIn: latest ?? "—", messagesOut: "—", underReplicated: "—", consumerLag: "—" },
          degraded: payload?.degraded,
          message: payload?.message
        };
      }
    } catch (error) {
      state.observability[kind] = {
        name: cluster.name,
        error: describeApiError(error, "managementApiUnavailable")
      };
    }
    render();
  }

  async function loadClientConfig() {
    const cluster = selectedCluster();
    if (!cluster) return;
    state.clientConfig = { name: cluster.name, loading: true };
    render();
    try {
      const payload = await request(`${API_PREFIX}/${encodeURIComponent(cluster.name)}/client-config`);
      state.clientConfig = { name: cluster.name, data: payload };
    } catch (error) {
      state.clientConfig = {
        name: cluster.name,
        error: describeApiError(error, "managementApiUnavailable")
      };
    }
    render();
  }

  async function deleteCluster() {
    const cluster = selectedCluster();
    if (!cluster || !writesEnabled() || state.deleteSubmitting) return;
    if (!window.confirm(message("deleteConfirmPrompt", { name: cluster.name }))) return;
    state.deleteSubmitting = true;
    state.deleteError = null;
    renderDetail();
    try {
      await request(`${API_PREFIX}/${encodeURIComponent(cluster.name)}`, { method: "DELETE" });
      state.tab = "overview";
      state.clientConfig = {};
      state.observability = {};
      navigateToList();
      await loadClusters();
    } catch (error) {
      state.deleteError = {
        name: cluster.name,
        message:
          error.code === 401 || error.code === 403
            ? message("permissionDeniedCopy")
            : message("deleteFailed", { message: describeApiError(error, "managementApiUnavailable") })
      };
      renderDetail();
    } finally {
      state.deleteSubmitting = false;
      renderDetail();
    }
  }

  function selectedCreateProfile() {
    return document.querySelector("input[name='profile']:checked")?.value || "development";
  }

  function numberFieldValue(selector, fallback) {
    const parsed = Number($(selector)?.value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  }

  function readCreateSpec() {
    const profile = selectedCreateProfile();
    const lockedPreset = CREATE_PROFILES[profile];
    if (lockedPreset) {
      return { profile, ...lockedPreset };
    }
    return {
      profile: "custom",
      brokers: numberFieldValue("#broker-count", CREATE_PROFILES.development.brokers),
      cpu: $("#broker-cpu")?.value.trim() || CREATE_PROFILES.development.cpu,
      memory: $("#broker-memory")?.value.trim() || CREATE_PROFILES.development.memory,
      storageGi: numberFieldValue("#storage-size", CREATE_PROFILES.development.storageGi),
      storageClass: $("#storage-class")?.value.trim() || "",
      deletionPolicy: $("#deletion-policy")?.value || "Retain",
      titleKey: "profileCustomTitle"
    };
  }

  function writeCreateFields(spec) {
    if ($("#broker-count")) $("#broker-count").value = spec.brokers;
    if ($("#broker-cpu")) $("#broker-cpu").value = spec.cpu;
    if ($("#broker-memory")) $("#broker-memory").value = spec.memory;
    if ($("#storage-size")) $("#storage-size").value = spec.storageGi;
    if ($("#storage-class")) $("#storage-class").value = spec.storageClass || "";
    if ($("#deletion-policy")) $("#deletion-policy").value = spec.deletionPolicy;
  }

  function setCustomFieldsEnabled(enabled) {
    for (const id of CUSTOM_RESOURCE_FIELD_IDS) {
      const field = document.getElementById(id);
      if (!field) continue;
      field.disabled = !enabled;
      field.setAttribute("aria-disabled", String(!enabled));
    }
    const section = $("#custom-resource-fields");
    if (section) section.dataset.qaState = enabled ? "custom" : "preset";
  }

  function setDisabledReason(node, reason) {
    if (!node) return;
    if (reason) {
      node.dataset.qaDisabledReason = reason;
    } else {
      node.removeAttribute("data-qa-disabled-reason");
    }
  }

  function updateCreateSummary() {
    const summary = $("#impact-copy");
    if (!summary) return;
    const spec = readCreateSpec();
    const policy = spec.deletionPolicy === "Delete" ? message("deleteWithCluster") : message("retainData");
    summary.textContent = message("resourceFootprintCopy", {
      profile: message(spec.titleKey),
      brokers: spec.brokers,
      cpu: spec.cpu,
      memory: spec.memory,
      storageGi: spec.storageGi,
      policy
    });
    summary.dataset.qaState = spec.profile;
    renderWorkspaceQuotaNote();
  }

  function applyCreateProfile(profile) {
    state.createProfile = profile === "standard" || profile === "custom" ? profile : "development";
    if (state.createProfile !== "custom") {
      writeCreateFields(CREATE_PROFILES[state.createProfile]);
    }
    setCustomFieldsEnabled(state.createProfile === "custom");
    const submitButton = $("#submit-create");
    if (submitButton) {
      submitButton.dataset.qaState = state.createSubmitting ? "loading" : "ready";
      setDisabledReason(submitButton, writesEnabled() ? "" : "api_unavailable");
    }
    updateCreateSummary();
  }

  function resetCreateForm() {
    const form = $("#create-form");
    form?.reset();
    const development = $("#profile-development");
    if (development) development.checked = true;
    applyCreateProfile("development");
    const errorBox = $("#form-error");
    if (errorBox) {
      errorBox.hidden = true;
      errorBox.textContent = message("formError");
      errorBox.dataset.qaErrorCode = "invalid_request";
    }
    const submitButton = $("#submit-create");
    if (submitButton) {
      submitButton.disabled = false;
      submitButton.dataset.qaState = "ready";
      setDisabledReason(submitButton, writesEnabled() ? "" : "api_unavailable");
    }
  }

  function openCreateModal() {
    if (!writesEnabled()) return;
    resetCreateForm();
    $("#create-modal")?.showModal();
    $("#cluster-name")?.focus();
  }

  async function createCluster(event) {
    if (event.submitter?.value === "cancel") return;
    event.preventDefault();
    const form = event.currentTarget;
    const name = $("#cluster-name").value.trim();
    const spec = readCreateSpec();
    const errorBox = $("#form-error");
    if (!writesEnabled()) {
      errorBox.hidden = false;
      errorBox.textContent = message("formPermissionDenied");
      errorBox.dataset.qaErrorCode = "permission_denied";
      return;
    }
    if (!form.checkValidity()) {
      errorBox.hidden = false;
      errorBox.textContent = message("formError");
      errorBox.dataset.qaErrorCode = "invalid_request";
      form.reportValidity();
      return;
    }

    state.createSubmitting = true;
    $("#submit-create").disabled = true;
    $("#submit-create").dataset.qaState = "loading";
    errorBox.hidden = true;
    try {
      await request(`${API_PREFIX}`, {
        method: "POST",
        body: JSON.stringify({
          name,
          spec: {
            engine: "kafka",
            kafka: {
              version: $("#kafka-version").value,
              replicas: spec.brokers
            },
            resources: { cpu: spec.cpu, memory: spec.memory },
            storage: {
              size: `${spec.storageGi}Gi`,
              className: spec.storageClass || undefined
            },
            deletionPolicy: spec.deletionPolicy
          }
        })
      });
      $("#create-modal").close();
      state.tab = "overview";
      await loadClusters();
      navigateToCluster(name);
    } catch (error) {
      errorBox.hidden = false;
      const isPermissionDenied = error.code === 401 || (error.code === 403 && error.apiCode !== "quota_exceeded");
      errorBox.textContent = isPermissionDenied
        ? message("formPermissionDenied")
        : message("formCreateFailed", { message: describeApiError(error, "formError") });
      errorBox.dataset.qaErrorCode = isPermissionDenied
        ? "permission_denied"
        : error.apiCode === "quota_exceeded"
          ? "quota_exceeded"
          : "create_failed";
    } finally {
      state.createSubmitting = false;
      $("#submit-create").disabled = false;
      $("#submit-create").dataset.qaState = "ready";
    }
  }

  function bindEvents() {
    $("#refresh-button")?.addEventListener("click", loadClusters);
    $("#help-button")?.addEventListener("click", () => alert(message("noHelp")));
    $("#search-input")?.addEventListener("input", (event) => {
      state.search = event.currentTarget.value;
      renderClusterList();
      renderDetail();
    });
    $("#notice-region")?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-action='dismiss-notice']");
      if (!button) return;
      state.noticeDismissed = true;
      renderNotice();
    });
    $("#create-button")?.addEventListener("click", () => {
      openCreateModal();
    });
    $("#cluster-list")?.addEventListener("click", (event) => {
      const action = event.target.closest("[data-action]");
      if (action?.dataset.action === "open-create") {
        event.preventDefault();
        openCreateModal();
        return;
      }
      const row = event.target.closest("[data-cluster-name]");
      if (
        row &&
        event.button === 0 &&
        !event.metaKey &&
        !event.ctrlKey &&
        !event.shiftKey &&
        !event.altKey
      ) {
        event.preventDefault();
        navigateToCluster(row.dataset.clusterName);
      }
    });
    $("#detail-content")?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-action]");
      if (!button) return;
      if (button.dataset.action === "back-to-list") {
        navigateToList();
        return;
      }
      if (button.dataset.action === "refresh-detail") {
        loadClusters();
        return;
      }
      if (button.dataset.action === "dismiss-notice") {
        state.noticeDismissed = true;
        renderNotice();
        return;
      }
      if (button.dataset.action === "load-client-config") {
        loadClientConfig();
        return;
      }
      if (button.dataset.action === "load-logs") {
        loadObservability("logs");
        return;
      }
      if (button.dataset.action === "load-metrics") {
        loadObservability("metrics");
        return;
      }
      if (button.dataset.action === "delete-cluster") {
        deleteCluster();
        return;
      }
      const copyValue = button.getAttribute("data-copy");
      if (copyValue) navigator.clipboard?.writeText(copyValue);
    });
    $("#detail-content")?.addEventListener("click", (event) => {
      const tabButton = event.target.closest("[data-tab]");
      if (!tabButton) return;
      state.tab = tabButton.dataset.tab;
      renderDetail();
    });
    $("#create-form")?.addEventListener("submit", createCluster);
    $("#create-form")?.addEventListener("change", (event) => {
      if (event.target.matches("input[name='profile']")) {
        applyCreateProfile(event.target.value);
        return;
      }
      updateCreateSummary();
    });
    $("#create-form")?.addEventListener("input", updateCreateSummary);
    $("#create-modal")?.addEventListener("close", () => {
      resetCreateForm();
    });
    $("#create-modal")?.addEventListener("click", (event) => {
      if (event.target === $("#create-modal")) $("#create-modal").close();
    });
    $("#submit-create")?.addEventListener("click", () => {
      if (!writesEnabled()) return;
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && $("#create-modal")?.open) {
        $("#create-modal").close();
      }
    });
    window.addEventListener("hashchange", () => {
      const previous = { ...state.route };
      syncRouteFromLocation();
      if (state.route.view === "detail" && state.route.clusterName !== previous.clusterName) {
        state.tab = "overview";
        state.observability = {};
        state.clientConfig = {};
        state.deleteError = null;
      }
      window.scrollTo({ top: 0, behavior: "auto" });
      render();
    });
    $("#back-button")?.addEventListener("click", navigateToList);
  }

  function initFormDefaults() {
    localizeStaticShell();
    resetCreateForm();
    renderWorkspaceQuotaNote();
  }

  function boot() {
    applyLanguage(state.language);
    initFormDefaults();
    bindEvents();
    setupSealosLanguageSync();
    if (!location.hash || !location.hash.startsWith("#/clusters")) {
      history.replaceState(null, "", "#/clusters");
    }
    syncRouteFromLocation();
    render();
    loadClusters();
  }

  boot();
})();
