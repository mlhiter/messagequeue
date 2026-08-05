(() => {
  "use strict";

  const API_BASE = (window.MESSAGEQUEUE_API_BASE || "/api").replace(/\/$/, "");
  const API_PREFIX = "/v1/messagequeues";
  const DEFAULT_LANGUAGE = "zh";
  const SEALOS_DESKTOP_EVENT_API = "event-bus";
  const SEALOS_DESKTOP_LANGUAGE_API = "getLanguage";
  const SEALOS_DESKTOP_CHANGE_I18N_EVENT = "change_i18n";
  const SEALOS_DESKTOP_REQUEST_TIMEOUT_MS = 3000;
  const POLL_INTERVALS = {
    clusters: 15000,
    logs: 10000,
    monitoring: 30000
  };

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
      pageDescription: "先看列表，再进入详情页查看连接、日志和监控。",
      detailPageTitle: "实例详情",
      detailPageDescription: "这里单独展示一个实例的状态、连接和可观测信息。",
      backToList: "实例列表",
      createCluster: "创建实例",
      newCluster: "新建",
      totalClusters: "实例总数",
      observedInWorkspace: "在当前工作空间中观测到",
      ready: "运行中",
      observedAndServing: "运行中且可服务",
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
      desiredState: "期望状态",
      resourceFootprint: "资源规格",
      readyBrokers: "Broker 就绪",
      connectionState: "连接状态",
      connectionAvailable: "可连接",
      connectionPending: "等待端点",
      latestSignal: "最新信号",
      failureReason: "异常原因",
      noFailureReason: "暂无异常",
      instanceActions: "实例操作",
      moreActions: "更多操作",
      updateInstance: "更新",
      pauseInstance: "暂停",
      resumeInstance: "恢复",
      lifecycleUnavailable: "更新需要后端变更契约接入后可用",
      pausingInstance: "正在暂停…",
      resumingInstance: "正在恢复…",
      pauseFailed: (message) => `暂停失败：${message}`,
      resumeFailed: (message) => `恢复失败：${message}`,
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
      loadClientConfig: "加载客户端配置",
      retryClientConfig: "重试配置",
      loadingClientConfig: "正在加载客户端配置…",
      clientConfigUnavailable: "客户端配置不可用",
      clientConfigOptional: "连接配置暂不可用，实例管理仍可继续。",
      clientConfigFetchNote: (name) => `配置通过服务端获取，范围限定为 ${name}。`,
      bootstrapServers: "Bootstrap 服务",
      internalAddress: "内网地址",
      externalAddress: "外网地址",
      host: "Host",
      port: "Port",
      connectionString: "Bootstrap 地址",
      username: "用户名",
      password: "密码",
      securityProtocol: "安全协议",
      saslMechanism: "SASL 机制",
      clientSecret: "客户端 Secret",
      caSecret: "CA Secret",
      authentication: "认证",
      environmentVariables: "环境变量",
      sdkExample: "SDK 示例",
      loadCredentials: "显示密码",
      loadingCredentials: "正在读取凭据…",
      hidePassword: "隐藏密码",
      copyPassword: "复制密码",
      copyEnvironment: "复制环境变量",
      copyJavaProperties: "复制 Java 配置",
      credentialsUnavailable: "凭据暂不可用",
      copied: "已复制",
      copyFailed: "复制失败",
      credentialsRevealed: "凭据已显示",
      externalAccessDisabled: "外网访问未开启",
      externalAccessEnabling: "正在开启外网访问…",
      enableExternalAccess: "开启外网访问",
      disableExternalAccess: "关闭外网访问",
      disablingExternalAccess: "正在关闭…",
      externalAccessFailed: (message) => `外网访问操作失败：${message}`,
      disableExternalAccessConfirm: (name) => `确认关闭 ${name} 的外网访问？现有外网连接会中断。`,
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
      retryMetrics: "重试监控",
      brokerHealth: "Broker 健康",
      loadingMetrics: "正在加载监控数据…",
      monitoringUnavailable: "监控不可用",
      metricsOptional:
        "VictoriaMetrics 可能不可用，或者还没有为这个工作空间配置。",
      metricsProviderMissing:
        "监控提供器未配置，不会影响 Kafka 操作。",
      metricsMessagesIn: "消息进入",
      metricsMessagesOut: "消息流出",
      metricsUnderReplicated: "未同步副本",
      metricsConsumerLag: "消费者堆积",
      monitoring: "监控",
      monitoringOverview: "实例监控",
      resourceUsage: "资源变化",
      trafficHealth: "流量与健康",
      metricCpu: "CPU",
      metricMemory: "内存",
      metricStorage: "存储",
      metricThroughput: "吞吐",
      metricPartitionHealth: "分区健康",
      currentValue: "当前值",
      perSecond: "每秒",
      partitions: "分区",
      messages: "条消息",
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
      statusReady: "运行中",
      statusProvisioning: "准备中",
      statusUpdating: "更新中",
      statusDegraded: "降级",
      statusFailed: "失败",
      statusSuspended: "已挂起",
      statusDeleting: "删除中",
      statusLabel: "状态",
      details: "详情",
      loadingDemoClusters: "正在加载观测到的实例…",
      stateReadyMeta: "运行中且可服务",
      stateAttentionMeta: "正在创建或异常",
      demoReadyEvent1: "Kafka 用户凭据已完成协调。",
      demoReadyEvent2: "3 个 broker 已全部就绪。",
      demoProvisioningEvent1: "MessageQueue 已被控制器接收。",
      demoProvisioningEvent2: "Kafka 资源正在通过 Strimzi 创建。",
      demoReadyCondition1: "Kafka Broker 正在接受连接。",
      demoReadyCondition2: "Broker 监控和日志已可用。",
      demoProvisioningCondition1: "等待 Kafka broker Pod 就绪。",
      demoProvisioningCondition2: "正在协调 Strimzi 资源。",
      noRecentEvents: "暂无最近事件。",
      noConditions: "控制器尚未上报条件。",
      clusterScopedAccess: "工作空间范围访问",
      credentialSafety: "密码和私钥不会出现在浏览器日志里。",
      topicPlaceholder: "示例：orders-dev",
      topbarHelp: "帮助",
      managementApiUnavailable:
        "后端不可达。这里显示只读演示数据；请先接通 API 再创建实例。",
      clusterCreationUnavailable:
        "管理 API 不可用，暂时不能创建实例。",
      loadingState: "正在加载…",
      noHelp: "暂无帮助内容",
      operationsComingSoon: "运维功能正在完善中",
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
      detailPageDescription: "Inspect the selected instance's status, connections, logs, and monitoring here.",
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
      desiredState: "Desired state",
      resourceFootprint: "Resource footprint",
      readyBrokers: "Brokers ready",
      connectionState: "Connection state",
      connectionAvailable: "Connectable",
      connectionPending: "Waiting for endpoint",
      latestSignal: "Latest signal",
      failureReason: "Failure reason",
      noFailureReason: "No current failure",
      instanceActions: "Instance actions",
      moreActions: "More actions",
      updateInstance: "Update",
      pauseInstance: "Pause",
      resumeInstance: "Resume",
      lifecycleUnavailable: "Update requires the backend change contract to be connected",
      pausingInstance: "Pausing…",
      resumingInstance: "Resuming…",
      pauseFailed: (message) => `Pause failed: ${message}`,
      resumeFailed: (message) => `Resume failed: ${message}`,
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
      loadClientConfig: "Load client config",
      retryClientConfig: "Retry config",
      loadingClientConfig: "Loading client configuration…",
      clientConfigUnavailable: "Client configuration unavailable",
      clientConfigOptional: "Connection configuration is unavailable; instance management can continue.",
      clientConfigFetchNote: (name) => `Configuration is fetched through the server and scoped to ${name}.`,
      bootstrapServers: "Bootstrap servers",
      internalAddress: "Internal address",
      externalAddress: "External address",
      host: "Host",
      port: "Port",
      connectionString: "Bootstrap address",
      username: "Username",
      password: "Password",
      securityProtocol: "Security protocol",
      saslMechanism: "SASL mechanism",
      clientSecret: "Client Secret",
      caSecret: "CA Secret",
      authentication: "Authentication",
      environmentVariables: "Environment variables",
      sdkExample: "SDK example",
      loadCredentials: "Show password",
      loadingCredentials: "Loading credentials…",
      hidePassword: "Hide password",
      copyPassword: "Copy password",
      copyEnvironment: "Copy environment",
      copyJavaProperties: "Copy Java config",
      credentialsUnavailable: "Credentials unavailable",
      copied: "Copied",
      copyFailed: "Copy failed",
      credentialsRevealed: "Credentials shown",
      externalAccessDisabled: "External access is not enabled",
      externalAccessEnabling: "Enabling external access…",
      enableExternalAccess: "Enable external access",
      disableExternalAccess: "Disable external access",
      disablingExternalAccess: "Disabling…",
      externalAccessFailed: (message) => `External access operation failed: ${message}`,
      disableExternalAccessConfirm: (name) => `Disable external access for ${name}? Existing external connections will be interrupted.`,
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
      retryMetrics: "Retry monitoring",
      brokerHealth: "Broker health",
      loadingMetrics: "Loading monitoring data…",
      monitoringUnavailable: "Monitoring unavailable",
      metricsOptional: "VictoriaMetrics may be unavailable or not configured for this workspace.",
      metricsProviderMissing: "The monitoring provider is not configured; Kafka operations are unaffected.",
      metricsMessagesIn: "Messages in",
      metricsMessagesOut: "Messages out",
      metricsUnderReplicated: "Under-replicated",
      metricsConsumerLag: "Consumer lag",
      monitoring: "Monitoring",
      monitoringOverview: "Instance monitoring",
      resourceUsage: "Resource changes",
      trafficHealth: "Traffic and health",
      metricCpu: "CPU",
      metricMemory: "Memory",
      metricStorage: "Storage",
      metricThroughput: "Throughput",
      metricPartitionHealth: "Partition health",
      currentValue: "Current value",
      perSecond: "per second",
      partitions: "partitions",
      messages: "messages",
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
      details: "Details",
      loadingDemoClusters: "Loading observed instances…",
      stateReadyMeta: "Observed and serving",
      stateAttentionMeta: "Provisioning or degraded",
      demoReadyEvent1: "Kafka user credentials reconciled.",
      demoReadyEvent2: "All 3 brokers reported Ready.",
      demoProvisioningEvent1: "MessageQueue accepted by the controller.",
      demoProvisioningEvent2: "Kafka resource created through Strimzi.",
      demoReadyCondition1: "Kafka brokers are accepting connections.",
      demoReadyCondition2: "Broker monitoring and logs are available.",
      demoProvisioningCondition1: "Waiting for the Kafka broker pod to become Ready.",
      demoProvisioningCondition2: "Reconciling Strimzi resources.",
      noRecentEvents: "No recent events reported.",
      noConditions: "The controller has not reported a condition yet.",
      clusterScopedAccess: "Workspace scoped access",
      credentialSafety: "Passwords and private keys are never rendered in browser logs.",
      topicPlaceholder: "Example: orders-dev",
      topbarHelp: "Help",
      managementApiUnavailable:
        "The backend could not be reached. Demo data is read only; connect the API before creating instances.",
      clusterCreationUnavailable: "The management API is unavailable, so instances cannot be created yet.",
      loadingState: "Loading…",
      noHelp: "No help content yet",
      operationsComingSoon: "Operations are still being polished",
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
      ready: ["运行中", "ready"],
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
  const MONITORING_KEYS = ["cpu", "memory", "storage", "throughput", "consumer_lag", "partition_health"];

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
    openActionMenu: "",
    observability: {},
    clientConfig: {},
    clientCredentials: {},
    externalAccess: {},
    suspension: {},
    toast: null,
    createSubmitting: false,
    createProfile: "development",
    deleteSubmitting: false,
    deleteError: null,
    workspaceQuota: { status: "idle", data: null, message: "" },
    language: detectLanguage(),
    route: { view: "list", clusterName: "" }
  };

  const polling = {
    timers: { clusters: null, logs: null, monitoring: null },
    active: { clusters: false, logs: false, monitoring: false, clientConfig: false }
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
    syncAutoPolling();
  }

  function navigateToList() {
    state.openActionMenu = "";
    state.externalAccess = {};
    state.clientCredentials = {};
    commitRouteHash("#/clusters");
  }

  function navigateToCluster(name) {
    if (!name) return;
    state.tab = "overview";
    state.observability = {};
    state.externalAccess = {};
    state.clientCredentials = {};
    state.openActionMenu = "";
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

  function menuIcon(name) {
    const icons = {
      update: '<svg data-icon="inline-start" viewBox="0 0 24 24" aria-hidden="true"><path d="M21 12a9 9 0 1 1-2.64-6.36"></path><path d="M21 3v6h-6"></path></svg>',
      pause: '<svg data-icon="inline-start" viewBox="0 0 24 24" aria-hidden="true"><rect x="6" y="4" width="4" height="16" rx="1"></rect><rect x="14" y="4" width="4" height="16" rx="1"></rect></svg>',
      resume: '<svg data-icon="inline-start" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 4l14 8-14 8z"></path></svg>',
      delete: '<svg data-icon="inline-start" viewBox="0 0 24 24" aria-hidden="true"><path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M19 6l-1 14H6L5 6"></path><path d="M10 11v6"></path><path d="M14 11v6"></path></svg>',
      copy: '<svg data-icon="inline-start" viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="9" width="11" height="11" rx="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>',
      eye: '<svg data-icon="inline-start" viewBox="0 0 24 24" aria-hidden="true"><path d="M2 12s3.5-6 10-6 10 6 10 6-3.5 6-10 6-10-6-10-6z"></path><circle cx="12" cy="12" r="3"></circle></svg>',
      eyeOff: '<svg data-icon="inline-start" viewBox="0 0 24 24" aria-hidden="true"><path d="M3 3l18 18"></path><path d="M10.6 10.6A3 3 0 0 0 12 15a3 3 0 0 0 2.2-.97"></path><path d="M9.9 4.24A10.4 10.4 0 0 1 12 4c6.5 0 10 8 10 8a15.5 15.5 0 0 1-3.1 4.26"></path><path d="M6.6 6.6C3.7 8.55 2 12 2 12s3.5 8 10 8a10.6 10.6 0 0 0 4.2-.86"></path></svg>',
      key: '<svg data-icon="inline-start" viewBox="0 0 24 24" aria-hidden="true"><circle cx="7.5" cy="15.5" r="4.5"></circle><path d="M11 12l9-9"></path><path d="M15 6l3 3"></path></svg>',
      terminal: '<svg data-icon="inline-start" viewBox="0 0 24 24" aria-hidden="true"><path d="m4 7 5 5-5 5"></path><path d="M12 19h8"></path></svg>',
      code: '<svg data-icon="inline-start" viewBox="0 0 24 24" aria-hidden="true"><path d="m16 18 6-6-6-6"></path><path d="m8 6-6 6 6 6"></path></svg>'
    };
    return icons[name] || "";
  }

  function tabIcon(name) {
    const icons = {
      overview: '<svg class="tab-icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="3" width="7" height="7" rx="1.5"></rect><rect x="14" y="3" width="7" height="7" rx="1.5"></rect><rect x="3" y="14" width="7" height="7" rx="1.5"></rect><rect x="14" y="14" width="7" height="7" rx="1.5"></rect></svg>',
      connections: '<svg class="tab-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M10 13a5 5 0 0 0 7.07 0l2.12-2.12a5 5 0 0 0-7.07-7.07L11 4.93"></path><path d="M14 11a5 5 0 0 0-7.07 0L4.81 13.12a5 5 0 0 0 7.07 7.07L13 19.07"></path></svg>',
      logs: '<svg class="tab-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M14 2H7a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7z"></path><path d="M14 2v5h5"></path><path d="M9 13h6"></path><path d="M9 17h4"></path></svg>',
      monitoring: '<svg class="tab-icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="4" y="4" width="16" height="16" rx="4"></rect><path d="M8 14.5l2.7-3 2.8 2 3.5-5"></path><path d="M8 17h8"></path></svg>'
    };
    return icons[name] || "";
  }

  function metricIcon(key) {
    const icons = {
      cpu: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="7" y="7" width="10" height="10" rx="2"></rect><path d="M4 10h3"></path><path d="M4 14h3"></path><path d="M17 10h3"></path><path d="M17 14h3"></path><path d="M10 4v3"></path><path d="M14 4v3"></path><path d="M10 17v3"></path><path d="M14 17v3"></path></svg>',
      memory: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 8a4 4 0 0 1 4-4h4a4 4 0 0 1 4 4v8a4 4 0 0 1-4 4h-4a4 4 0 0 1-4-4z"></path><path d="M9 9h6"></path><path d="M9 13h6"></path><path d="M9 17h4"></path></svg>',
      storage: '<svg viewBox="0 0 24 24" aria-hidden="true"><ellipse cx="12" cy="6" rx="7" ry="3"></ellipse><path d="M5 6v12c0 1.7 3.1 3 7 3s7-1.3 7-3V6"></path><path d="M5 12c0 1.7 3.1 3 7 3s7-1.3 7-3"></path></svg>',
      throughput: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 17h10"></path><path d="m14 13 4 4-4 4"></path><path d="M20 7H10"></path><path d="m10 3-4 4 4 4"></path></svg>',
      consumer_lag: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h14"></path><path d="M5 18h9"></path><path d="M5 6h14"></path><path d="M17 15l3 3-3 3"></path></svg>',
      partition_health: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 6h16"></path><path d="M4 12h16"></path><path d="M4 18h16"></path><path d="M8 6v12"></path><path d="M16 6v12"></path></svg>'
    };
    return icons[key] || icons.throughput;
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

  let toastTimer = null;

  function renderToast() {
    const region = $("#toast-region");
    if (!region) return;
    if (!state.toast) {
      region.innerHTML = "";
      return;
    }
    region.innerHTML = `<div class="toast" data-tone="${escapeHtml(state.toast.tone || "success")}"><span class="toast-dot" aria-hidden="true"></span><span>${escapeHtml(state.toast.message)}</span></div>`;
  }

  function showToast(text, tone = "success") {
    state.toast = { message: text, tone };
    renderToast();
    if (toastTimer) window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(() => {
      state.toast = null;
      renderToast();
    }, 2200);
  }

  async function writeClipboard(value) {
    const text = String(value ?? "");
    if (!text) throw new Error("empty clipboard value");
    if (navigator.clipboard && window.isSecureContext !== false) {
      await navigator.clipboard.writeText(text);
      return;
    }
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "fixed";
    textarea.style.left = "-9999px";
    document.body.appendChild(textarea);
    textarea.select();
    const copied = document.execCommand("copy");
    textarea.remove();
    if (!copied) throw new Error("clipboard fallback failed");
  }

  async function copyText(value) {
    try {
      await writeClipboard(value);
      showToast(message("copied"));
      return true;
    } catch {
      showToast(message("copyFailed"), "error");
      return false;
    }
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

    $("#help-button")?.setAttribute("aria-label", message("openHelp"));
    $("#help-button")?.setAttribute("title", message("openHelp"));
    $("#search-input")?.setAttribute("placeholder", message("searchPlaceholder"));
    $("#detail-header-actions")?.setAttribute("aria-label", message("instanceActions"));
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

  function formatDeletionPolicy(cluster) {
    const value = String(cluster.deletionPolicy || "").toLowerCase();
    if (value === "delete") return message("deleteWithCluster");
    if (value === "retain") return message("retainData");
    return message("controllerDefault");
  }

  function readyBrokerLabel(cluster) {
    const desired = Number(cluster.brokers) || 0;
    const ready = Number(cluster.status.readyReplicas ?? cluster.raw?.status?.readyReplicas ?? 0);
    return desired > 0 ? `${ready}/${desired}` : "—";
  }

  function formatResourceFootprint(cluster) {
    const cpu = cluster.spec.resources?.cpu || cluster.kafka.cpu || "—";
    const memory = cluster.spec.resources?.memory || cluster.kafka.memory || "—";
    return `${formatBrokerCount(cluster.brokers)} · ${cpu} / ${memory} · ${formatBrokerStorage(cluster.storageGi)}`;
  }

  function latestSignal(cluster) {
    const latestEvent = cluster.events?.[0]?.message || cluster.events?.[0]?.reason;
    const latestCondition = cluster.conditions?.[0]?.message || cluster.conditions?.[0]?.reason;
    return localizeBackendText(latestEvent || latestCondition || message("noRecentEvents"));
  }

  function failureReason(cluster) {
    const failedCondition = cluster.conditions.find((condition) => {
      const status = String(condition.status || "").toLowerCase();
      const type = String(condition.type || "").toLowerCase();
      return status === "false" || type.includes("degraded") || type.includes("failed");
    });
    const phase = String(cluster.phase || "").toLowerCase();
    if (failedCondition) return localizeBackendText(failedCondition.message || failedCondition.reason || message("noFailureReason"));
    if (["degraded", "failed"].includes(phase)) return localizeBackendText(cluster.status.message || message("noFailureReason"));
    return message("noFailureReason");
  }

  function parseEndpoint(value, fallbackPort = "9092") {
    const raw = String(value || "").trim();
    if (!raw || raw === "Pending" || raw === "—") {
      return { host: "—", port: "—", connection: "—", available: false };
    }
    const first = raw.split(",")[0].trim();
    const withoutProtocol = first.replace(/^[a-z][a-z0-9+.-]*:\/\//i, "");
    const withoutPath = withoutProtocol.split("/")[0];
    const separator = withoutPath.lastIndexOf(":");
    const host = separator > -1 ? withoutPath.slice(0, separator) : withoutPath;
    const port = separator > -1 ? withoutPath.slice(separator + 1) : fallbackPort;
    return { host, port, connection: raw, available: Boolean(host && host !== "—") };
  }

  function endpointListFrom(value) {
    if (Array.isArray(value)) return value.filter(Boolean);
    if (typeof value === "string" && value.trim()) return [value.trim()];
    return [];
  }

  function connectionModel(cluster, config = {}) {
    const internalCandidates = [
      ...endpointListFrom(config.bootstrapServers),
      ...endpointListFrom(cluster.status.endpoints),
      cluster.endpoint
    ].filter(Boolean);
    const externalCandidates = [
      ...endpointListFrom(cluster.status.externalEndpoints),
      cluster.status.externalEndpoint,
      ...endpointListFrom(config.externalBootstrapServers),
      ...endpointListFrom(config.externalEndpoints),
      config.externalEndpoint,
      config.publicEndpoint
    ].filter(Boolean);
    return {
      internal: parseEndpoint(internalCandidates[0] || cluster.endpoint),
      external: externalCandidates.length ? parseEndpoint(externalCandidates[0]) : null
    };
  }

  function selectedBootstrapServers(model) {
    if (model.external?.available) return [model.external.connection];
    if (model.internal?.available) return [model.internal.connection];
    return [];
  }

  function credentialDataFor(cluster) {
    const result = state.clientCredentials;
    return result?.name === cluster.name && result.data ? result.data : null;
  }

  function mergedClientConfig(cluster, config = {}) {
    const credentials = credentialDataFor(cluster) || {};
    return {
      ...config,
      ...credentials,
      bootstrapServers: credentials.bootstrapServers || config.bootstrapServers,
      externalBootstrapServers: credentials.externalBootstrapServers || config.externalBootstrapServers,
      securityProtocol: credentials.securityProtocol || config.securityProtocol || "SASL_SSL",
      mechanism: credentials.mechanism || config.mechanism || "SCRAM-SHA-512",
      transport: credentials.transport || config.transport || "TLS",
      username: credentials.username || config.username || `${cluster.name}-client`,
      secretRef: credentials.secretRef || config.secretRef || cluster.status.clientSecretRef || `${cluster.name}-client`,
      caSecretRef: credentials.caSecretRef || config.caSecretRef || `${cluster.name}-cluster-ca-cert`
    };
  }

  function shellQuote(value) {
    return `'${String(value ?? "").replace(/'/g, "'\\''")}'`;
  }

  function javaQuote(value) {
    return String(value ?? "").replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  }

  function envTemplate(cluster, config, model, revealSecrets) {
    const servers = selectedBootstrapServers(model).join(",");
    const password = revealSecrets && config.password ? config.password : "${KAFKA_PASSWORD}";
    const ca = revealSecrets && config.caCertificate ? config.caCertificate : "${KAFKA_CA_CERT}";
    return [
      `KAFKA_BOOTSTRAP_SERVERS=${shellQuote(servers)}`,
      `KAFKA_SECURITY_PROTOCOL=${shellQuote(config.securityProtocol || "SASL_SSL")}`,
      `KAFKA_SASL_MECHANISM=${shellQuote(config.mechanism || "SCRAM-SHA-512")}`,
      `KAFKA_SASL_USERNAME=${shellQuote(config.username || `${cluster.name}-client`)}`,
      `KAFKA_SASL_PASSWORD=${shellQuote(password)}`,
      `KAFKA_CA_CERT=${shellQuote(ca)}`
    ].join("\n");
  }

  function javaPropertiesTemplate(cluster, config, model, revealSecrets) {
    const servers = selectedBootstrapServers(model).join(",");
    const password = revealSecrets && config.password ? config.password : "${KAFKA_PASSWORD}";
    return [
      `bootstrap.servers=${servers}`,
      `security.protocol=${config.securityProtocol || "SASL_SSL"}`,
      `sasl.mechanism=${config.mechanism || "SCRAM-SHA-512"}`,
      `sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${javaQuote(config.username || `${cluster.name}-client`)}" password="${javaQuote(password)}";`
    ].join("\n");
  }

  function copyIconButton(value, label, extra = "") {
    const disabled = !value || value === "—";
    return `<button class="copy-icon-button ${extra}" type="button" ${disabled ? "disabled" : `data-copy="${escapeHtml(value)}"`} aria-label="${escapeHtml(label)}" title="${escapeHtml(label)}">${menuIcon("copy")}</button>`;
  }

  function actionIconButton(action, label, icon, attrs = "") {
    return `<button class="copy-icon-button" type="button" data-action="${escapeHtml(action)}" ${attrs} aria-label="${escapeHtml(label)}" title="${escapeHtml(label)}">${menuIcon(icon)}</button>`;
  }

  function externalAccessDesired(cluster) {
    const spec = cluster?.spec || {};
    const kafka = spec.kafka || {};
    const candidate =
      kafka.listeners?.external ??
      spec.listeners?.external ??
      kafka.externalAccess ??
      spec.externalAccess;
    if (typeof candidate === "boolean") return candidate;
    if (candidate && typeof candidate === "object") return Boolean(candidate.enabled);
    return false;
  }

  function latestMetricValue(series) {
    const values = Array.isArray(series?.values) ? series.values : [];
    const value = values[values.length - 1]?.value;
    return Number.isFinite(Number(value)) ? Number(value) : null;
  }

  function formatMetricValue(value, unit = "") {
    if (value == null) return "—";
    const formatted = Math.abs(value) >= 100 ? Math.round(value).toLocaleString() : Number(value.toFixed(2)).toLocaleString();
    return unit ? `${formatted} ${unit}` : formatted;
  }

  function sparklineSvg(series) {
    const values = (Array.isArray(series?.values) ? series.values : [])
      .map((point) => Number(point.value))
      .filter((value) => Number.isFinite(value));
    if (values.length < 2) return `<div class="metric-sparkline is-empty" aria-hidden="true"></div>`;
    const width = 180;
    const height = 54;
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = max - min || 1;
    const coords = values
      .map((value, index) => {
        const x = values.length === 1 ? width : (index / (values.length - 1)) * width;
        const y = height - ((value - min) / span) * (height - 8) - 4;
        return [x, y];
      });
    const linePath = coords.map(([x, y], index) => `${index === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`).join(" ");
    const areaPath = `${linePath} L${width} ${height} L0 ${height} Z`;
    return `<svg class="metric-sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true"><path class="metric-area" d="${areaPath}"></path><path class="metric-line" d="${linePath}"></path></svg>`;
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
      "Broker metrics and logs are available.": "Broker 监控和日志已可用。",
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
      const phase = spec.suspend
        ? "Suspended"
        : status.phase || (conditions.find((condition) => condition.type === "Ready" && condition.status === "True") ? "Ready" : "Provisioning");
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

  function clusterByName(name) {
    if (!name) return null;
    return normalizedClusters().find((cluster) => cluster.name === name) || null;
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

  function actionMenuHtml(cluster, menuId) {
    const isOpen = state.openActionMenu === menuId;
    const suspended = String(cluster.phase || "").toLowerCase() === "suspended";
    const clusterName = escapeHtml(cluster.name);
    const toggleLabel = `${message("moreActions")} ${cluster.name}`;
    const deleteDisabled = !writesEnabled() || state.deleteSubmitting;
    const unavailableTitle = escapeHtml(message("lifecycleUnavailable"));
    const suspensionState = state.suspension?.name === cluster.name ? state.suspension : {};
    const suspensionBusy = Boolean(suspensionState.submitting || suspensionState.pending);
    const lifecycleIcon = suspended ? menuIcon("resume") : menuIcon("pause");
    const lifecycleLabel = suspensionBusy
      ? suspended
        ? message("resumingInstance")
        : message("pausingInstance")
      : suspended
        ? message("resumeInstance")
        : message("pauseInstance");
    const lifecycleDisabled = !writesEnabled() || suspensionBusy;
    return `<span class="action-menu" data-menu-open="${isOpen ? "true" : "false"}" data-menu-id="${escapeHtml(menuId)}"><button class="icon-button action-menu-trigger" type="button" data-action="toggle-row-actions" data-menu-id="${escapeHtml(menuId)}" data-cluster-name="${clusterName}" aria-haspopup="menu" aria-expanded="${isOpen ? "true" : "false"}" aria-label="${escapeHtml(toggleLabel)}" title="${escapeHtml(message("moreActions"))}"><span aria-hidden="true">⋯</span></button>${isOpen ? `<span class="action-menu-content" role="menu" aria-label="${escapeHtml(message("instanceActions"))}"><span class="action-menu-group" role="group"><button class="action-menu-item" type="button" role="menuitem" data-action="lifecycle-unavailable" title="${unavailableTitle}" disabled>${menuIcon("update")}<span>${escapeHtml(message("updateInstance"))}</span></button><button class="action-menu-item" type="button" role="menuitem" data-action="set-suspension" data-cluster-name="${clusterName}" data-suspended="${suspended ? "false" : "true"}" ${lifecycleDisabled ? "disabled" : ""}>${lifecycleIcon}<span>${escapeHtml(lifecycleLabel)}</span></button></span><span class="action-menu-separator" role="separator"></span><span class="action-menu-group" role="group"><button class="action-menu-item is-destructive" type="button" role="menuitem" data-action="delete-cluster" data-cluster-name="${clusterName}" ${deleteDisabled ? "disabled" : ""}>${menuIcon("delete")}<span>${escapeHtml(state.deleteSubmitting ? message("deletingCluster") : message("deleteCluster"))}</span></button></span></span>` : ""}</span>`;
  }

  function rowActionsHtml(cluster) {
    return `<span class="cluster-row-actions">${actionMenuHtml(cluster, `row:${cluster.name}`)}</span>`;
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
    const detailStatusSlot = $("#detail-status-slot");
    const detailHeaderActions = $("#detail-header-actions");
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
    if (detailStatusSlot) {
      const showStatus = Boolean(isDetail && cluster);
      detailStatusSlot.classList.toggle("is-hidden", !showStatus);
      detailStatusSlot.setAttribute("aria-hidden", showStatus ? "false" : "true");
      if (showStatus) {
        const [label, statusClass] = statusLabel(cluster.phase);
        detailStatusSlot.innerHTML = `<span class="status-badge status-${statusClass}">${escapeHtml(label)}</span>`;
      } else {
        detailStatusSlot.innerHTML = "";
      }
    }
    if (detailHeaderActions) {
      const showDetailActions = Boolean(isDetail && cluster);
      detailHeaderActions.classList.toggle("is-hidden", !showDetailActions);
      detailHeaderActions.setAttribute("aria-hidden", showDetailActions ? "false" : "true");
      detailHeaderActions.setAttribute("aria-label", message("instanceActions"));
      if (showDetailActions) {
        detailHeaderActions.innerHTML = detailActionsHtml(cluster);
      } else {
        detailHeaderActions.innerHTML = "";
      }
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

    const listDeleteError =
      state.deleteError && state.route.view === "list"
        ? `<div class="form-error list-delete-error" role="alert">${escapeHtml(state.deleteError.message)}</div>`
        : "";
    const columns = [
      message("clusterNameColumn"),
      message("statusColumn"),
      message("versionColumn"),
      message("brokerColumn"),
      message("storageColumn"),
      message("namespaceColumn"),
      message("updatedColumn"),
      message("instanceActions")
    ];

    list.innerHTML = `${listDeleteError}<div class="cluster-table"><div class="cluster-table-head" aria-hidden="true">${columns.map((label, index) => `<span class="cluster-head-cell ${index === 0 ? "is-name" : ""}">${escapeHtml(label)}</span>`).join("")}</div><div class="cluster-table-body">${clusters
      .map((cluster) => {
        const [label, statusClass] = statusLabel(cluster.phase);
        const href = `#/clusters/${encodeURIComponent(cluster.name)}`;
        return `<div class="cluster-row" role="link" tabindex="0" data-cluster-name="${escapeHtml(cluster.name)}" data-href="${href}" aria-label="${escapeHtml(message("openDetail"))} ${escapeHtml(cluster.name)}"><span class="cluster-name-cell"><span class="cluster-name">${escapeHtml(cluster.name)}</span></span><span class="cluster-status"><span class="status-badge status-${statusClass}">${escapeHtml(label)}</span></span><span class="cluster-version">${escapeHtml(cluster.version)}</span><span class="cluster-topology"><strong>${escapeHtml(formatBrokerCount(cluster.brokers))}</strong></span><span class="cluster-storage">${escapeHtml(formatBrokerStorage(cluster.storageGi))}</span><span class="cluster-namespace"><code>${escapeHtml(cluster.namespace)}</code></span><span class="cluster-updated">${escapeHtml(formatTime(cluster.lastTransitionTime))}</span>${rowActionsHtml(cluster)}</div>`;
      })
      .join("")}</div></div>`;
  }

  function overviewHtml(cluster) {
    const conditions = cluster.conditions.length ? cluster.conditions : [{ type: "Ready", status: "Unknown", reason: "NoCondition", message: message("noConditions") }];
    const events = cluster.events.length ? cluster.events : [{ time: "—", message: message("noRecentEvents") }];
    const [label, statusClass] = statusLabel(cluster.phase);
    const connection = parseEndpoint(cluster.endpoint);
    const summaryCards = [
      [message("observedState"), label, `${message("generation")} ${cluster.status.observedGeneration || "—"}`, `status-${statusClass}`],
      [message("readyBrokers"), readyBrokerLabel(cluster), message("controllerReported"), ""],
      [message("connectionState"), connection.available ? message("connectionAvailable") : message("connectionPending"), connection.connection, connection.available ? "status-ready" : "status-provisioning"],
      [message("resourceFootprint"), formatResourceFootprint(cluster), `${message("kafkaVersion")} ${cluster.version}`, ""],
      [message("deletionPolicy"), formatDeletionPolicy(cluster), message("storage"), ""],
      [message("failureReason"), failureReason(cluster), message("latestSignal"), failureReason(cluster) === message("noFailureReason") ? "" : "status-failed"]
    ];

    return `<section class="detail-section" data-testid="messagequeue.detail.overview"><div class="section-heading"><h3>${escapeHtml(message("observedState"))}</h3><span>${escapeHtml(message("latestSignal"))}: ${escapeHtml(latestSignal(cluster))}</span></div><div class="overview-grid">${summaryCards
      .map(([title, value, meta, tone]) => `<article class="overview-card ${tone ? `is-${tone}` : ""}"><span>${escapeHtml(title)}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(meta)}</small></article>`)
      .join("")}</div></section><section class="detail-section"><div class="section-heading"><h3>${escapeHtml(message("conditions"))}</h3><span>${escapeHtml(message("controllerReported"))}</span></div><div class="condition-list">${conditions.map((condition) => `<div class="condition-row"><strong>${escapeHtml(formatConditionType(condition.type))} · ${escapeHtml(formatConditionStatus(condition.status))}</strong><span>${escapeHtml(localizeBackendText(condition.message || condition.reason || message("conditionPlaceholder")))}</span></div>`).join("")}</div></section><section class="detail-section"><div class="section-heading"><h3>${escapeHtml(message("recentEvents"))}</h3><span>${escapeHtml(message("latestFirst"))}</span></div><div class="event-list">${events.map((event) => `<div class="event-row"><time>${escapeHtml(formatEventTime(event))}</time><p>${escapeHtml(localizeBackendText(event.message || event.reason || message("controllerEvent")))}</p></div>`).join("")}</div></section>`;
  }

  function connectionsHtml(cluster) {
    const result = state.clientConfig;
    const config = result?.name === cluster.name && result.data ? result.data : {};
    const credentialsResult = state.clientCredentials?.name === cluster.name ? state.clientCredentials : {};
    const mergedConfig = mergedClientConfig(cluster, config);
    const credentials = credentialDataFor(cluster);
    const model = connectionModel(cluster, mergedConfig);
    let degradedNotice = "";
    if (result?.name === cluster.name && result.loading) {
      degradedNotice = `<div class="loading-state"><span>${escapeHtml(message("loadingClientConfig"))}</span><span class="loading-bar" aria-hidden="true"></span></div>`;
    } else if (result?.name === cluster.name && result.error) {
      degradedNotice = `<div class="observability-box"><h3>${escapeHtml(message("clientConfigUnavailable"))}</h3><p>${escapeHtml(result.error)}</p><button class="button button-secondary" type="button" data-action="load-client-config">${escapeHtml(message("retryClientConfig"))}</button></div>`;
    } else if (result?.name === cluster.name && config.degraded) {
      degradedNotice = `<div class="observability-box" data-tone="warning" data-testid="messagequeue.detail.client-config-degraded"><h3>${escapeHtml(message("clientConfigUnavailable"))}</h3><p>${escapeHtml(localizeBackendText(config.message || message("clientConfigOptional")))}</p></div>`;
    }
    if (credentialsResult.error) {
      degradedNotice += `<div class="observability-box" data-tone="warning"><h3>${escapeHtml(message("credentialsUnavailable"))}</h3><p>${escapeHtml(credentialsResult.error)}</p></div>`;
    } else if (credentials?.degraded) {
      degradedNotice += `<div class="observability-box" data-tone="warning"><h3>${escapeHtml(message("credentialsUnavailable"))}</h3><p>${escapeHtml(localizeBackendText(credentials.message || message("clientConfigOptional")))}</p></div>`;
    }
    const endpointPanel = (kind, title, endpoint, action = "", qaState = "") => {
      const disabled = !endpoint?.available;
      const safe = endpoint || { host: "—", port: "—", connection: "—" };
      const stateName = qaState || (disabled ? "pending" : "ready");
      const testId = `messagequeue.detail.connection-${kind}`;
      const copyButton = (value, label) => copyIconButton(disabled ? "" : value, label);
      return `<article class="connection-endpoint ${disabled ? "is-disabled" : ""}" data-testid="${testId}" data-qa-state="${stateName}"><div class="connection-endpoint-header"><h4>${escapeHtml(title)}</h4>${action}</div><div class="connection-fields"><label><span>${escapeHtml(message("host"))}</span><code>${escapeHtml(safe.host)}</code>${copyButton(safe.host, `${message("copy")} ${message("host")}`)}</label><label><span>${escapeHtml(message("port"))}</span><code>${escapeHtml(safe.port)}</code>${copyButton(safe.port, `${message("copy")} ${message("port")}`)}</label><label class="is-wide"><span>${escapeHtml(message("connectionString"))}</span><code>${escapeHtml(safe.connection)}</code>${copyButton(safe.connection, `${message("copy")} ${message("connectionString")}`)}</label></div></article>`;
    };
    const externalAction = state.externalAccess?.name === cluster.name ? state.externalAccess : {};
    const observedExternal = Boolean(model.external?.available);
    const desiredExternal = externalAccessDesired(cluster);
    const externalBusy = Boolean(externalAction.submitting || externalAction.pending);
    const writesDisabled = !writesEnabled() || externalBusy;
    const externalError = externalAction.error
      ? `<p class="external-access-error" role="alert">${escapeHtml(externalAction.error)}</p>`
      : "";
    let externalPanel = "";
    if (observedExternal) {
      const disableLabel = externalBusy && externalAction.enabled === true
        ? message("externalAccessEnabling")
        : externalBusy && externalAction.enabled === false
          ? message("disablingExternalAccess")
          : message("disableExternalAccess");
      const disableButton = `<button class="button button-secondary external-access-action" type="button" data-action="set-external-access" data-enabled="false" ${writesDisabled ? "disabled" : ""}>${escapeHtml(disableLabel)}</button>`;
      externalPanel = endpointPanel("external", message("externalAddress"), model.external, disableButton, externalAction.error ? "error" : "on") + externalError;
    } else {
      const enabling = (externalBusy && externalAction.enabled === true) || desiredExternal;
      const stateLabel = enabling ? message("externalAccessEnabling") : message("externalAccessDisabled");
      externalPanel = `<article class="external-access-state" data-testid="messagequeue.detail.connection-external-disabled" data-qa-state="${externalAction.error ? "error" : enabling ? "enabling" : "off"}"><div><h4>${escapeHtml(message("externalAddress"))}</h4><p>${escapeHtml(stateLabel)}</p>${externalError}</div><button class="button button-primary" type="button" data-action="set-external-access" data-enabled="true" ${writesDisabled || enabling ? "disabled" : ""}>${escapeHtml(message("enableExternalAccess"))}</button></article>`;
    }
    const credentialsLoading = credentialsResult.loading;
    const passwordRevealed = Boolean(credentialsResult.revealed && credentials?.password);
    const passwordDisplay = passwordRevealed ? credentials.password : credentials?.password ? "••••••••••••" : "—";
    const passwordActions = credentialsLoading
      ? `<button class="copy-icon-button" type="button" disabled aria-label="${escapeHtml(message("loadingCredentials"))}" title="${escapeHtml(message("loadingCredentials"))}">${menuIcon("key")}</button>`
      : credentials?.password
        ? `${actionIconButton("toggle-password", passwordRevealed ? message("hidePassword") : message("loadCredentials"), passwordRevealed ? "eyeOff" : "eye")}${passwordRevealed ? actionIconButton("copy-password", message("copyPassword"), "copy") : ""}`
        : actionIconButton("load-client-credentials", message("loadCredentials"), "eye");
    const credentialRows = [
      [message("username"), mergedConfig.username || "—", copyIconButton(mergedConfig.username, `${message("copy")} ${message("username")}`)],
      [message("password"), passwordDisplay, passwordActions],
      [message("securityProtocol"), mergedConfig.securityProtocol || "SASL_SSL", copyIconButton(mergedConfig.securityProtocol || "SASL_SSL", `${message("copy")} ${message("securityProtocol")}`)],
      [message("saslMechanism"), mergedConfig.mechanism || "SCRAM-SHA-512", copyIconButton(mergedConfig.mechanism || "SCRAM-SHA-512", `${message("copy")} ${message("saslMechanism")}`)],
      [message("clientSecret"), mergedConfig.secretRef || "—", copyIconButton(mergedConfig.secretRef, `${message("copy")} ${message("clientSecret")}`)],
      [message("caSecret"), mergedConfig.caSecretRef || "—", copyIconButton(mergedConfig.caSecretRef, `${message("copy")} ${message("caSecret")}`)]
    ];
    const envBlock = envTemplate(cluster, mergedConfig, model, passwordRevealed);
    const javaBlock = javaPropertiesTemplate(cluster, mergedConfig, model, passwordRevealed);
    const snippet = (title, code, template, icon, label) => `<article class="config-snippet"><div class="config-snippet-head"><h4>${escapeHtml(title)}</h4>${actionIconButton("copy-client-template", label, icon, `data-template="${escapeHtml(template)}"`)}</div><pre><code>${escapeHtml(code)}</code></pre></article>`;
    return `<section class="detail-section" data-testid="messagequeue.detail.connections"><div class="section-heading"><h3>${escapeHtml(message("clientConnection"))}</h3></div><div class="connection-layout">${endpointPanel("internal", message("internalAddress"), model.internal)}${externalPanel}<article class="credential-panel" data-testid="messagequeue.detail.connection-auth"><div class="connection-endpoint-header"><h4>${escapeHtml(message("authentication"))}</h4></div><div class="credential-grid">${credentialRows.map(([label, value, action]) => `<label><span>${escapeHtml(label)}</span><code>${escapeHtml(value)}</code><span class="credential-actions">${action}</span></label>`).join("")}</div></article><div class="config-snippet-grid">${snippet(message("environmentVariables"), envBlock, "env", "terminal", message("copyEnvironment"))}${snippet(message("sdkExample"), javaBlock, "java", "code", message("copyJavaProperties"))}</div></div>${degradedNotice}</section>`;
  }

  function logsHtml(cluster) {
    const result = state.observability.logs;
    if (result?.name === cluster.name && result.loading) {
      return `<section class="detail-section"><div class="loading-state"><span>${escapeHtml(message("loadingLogs"))}</span><span class="loading-bar" aria-hidden="true"></span></div></section>`;
    }
    if (result?.name === cluster.name && result.error) {
      return `<section class="detail-section"><div class="observability-box"><h3>${escapeHtml(message("logsUnavailable"))}</h3><p>${escapeHtml(result.error)} ${escapeHtml(message("logsOptional"))}</p></div></section>`;
    }
    if (result?.name === cluster.name && Object.prototype.hasOwnProperty.call(result, "data")) {
      const degradedNotice = result.degraded ? `<div class="observability-box" data-tone="warning"><h3>${escapeHtml(message("logsUnavailable"))}</h3><p>${escapeHtml(result.message || message("logsOptional"))}</p></div>` : "";
      return `<section class="detail-section" data-testid="messagequeue.detail.logs"><div class="section-heading"><h3>${escapeHtml(message("brokerLogs"))}</h3></div>${degradedNotice}<pre class="log-viewer" id="log-viewer">${escapeHtml(message("loadingState"))}</pre></section>`;
    }
    return `<section class="detail-section"><div class="loading-state"><span>${escapeHtml(message("loadingLogs"))}</span><span class="loading-bar" aria-hidden="true"></span></div></section>`;
  }

  function monitoringHtml(cluster) {
    const result = state.observability.monitoring;
    if (result?.name === cluster.name && result.loading) {
      return `<section class="detail-section" data-testid="messagequeue.detail.monitoring" data-qa-state="loading"><div class="loading-state"><span>${escapeHtml(message("loadingMetrics"))}</span><span class="loading-bar" aria-hidden="true"></span></div></section>`;
    }
    if (result?.name === cluster.name && result.error) {
      return `<section class="detail-section" data-testid="messagequeue.detail.monitoring" data-qa-state="error"><div class="observability-box"><h3>${escapeHtml(message("monitoringUnavailable"))}</h3><p>${escapeHtml(result.error)} ${escapeHtml(message("metricsOptional"))}</p></div></section>`;
    }
    const metrics = result?.data || null;
    if (!metrics) {
      return `<section class="detail-section" data-testid="messagequeue.detail.monitoring" data-qa-state="loading"><div class="loading-state"><span>${escapeHtml(message("loadingMetrics"))}</span><span class="loading-bar" aria-hidden="true"></span></div></section>`;
    }
    const card = (key, title, unit) => {
      const series = metrics[key];
      const latest = latestMetricValue(series);
      return `<article class="metric-card" data-metric-key="${escapeHtml(key)}"><div class="metric-card-head"><span class="metric-icon">${metricIcon(key)}</span><div><span>${escapeHtml(title)}</span><small>${escapeHtml(series?.unit || unit || message("currentValue"))}</small></div></div><strong>${escapeHtml(formatMetricValue(latest, series?.unit || unit))}</strong>${sparklineSvg(series)}</article>`;
    };
    const degradedNotice = result.degraded ? `<div class="observability-box" data-tone="warning"><h3>${escapeHtml(message("monitoringUnavailable"))}</h3><p>${escapeHtml(result.message || message("metricsProviderMissing"))}</p></div>` : "";
    return `<section class="detail-section" data-testid="messagequeue.detail.monitoring" data-qa-state="${result.degraded ? "degraded" : "ready"}"><div class="section-heading"><h3>${escapeHtml(message("monitoringOverview"))}</h3></div><div class="metric-grid">${[
      card("cpu", message("metricCpu"), "cores"),
      card("memory", message("metricMemory"), "Mi"),
      card("storage", message("metricStorage"), "Gi"),
      card("throughput", message("metricThroughput"), message("perSecond")),
      card("consumer_lag", message("metricsConsumerLag"), message("messages")),
      card("partition_health", message("metricPartitionHealth"), message("partitions"))
    ].join("")}</div>${degradedNotice}</section>`;
  }

  function detailActionsHtml(cluster) {
    const suspended = String(cluster.phase || "").toLowerCase() === "suspended";
    const suspensionState = state.suspension?.name === cluster.name ? state.suspension : {};
    const suspensionBusy = Boolean(suspensionState.submitting || suspensionState.pending);
    const lifecycleLabel = suspensionBusy
      ? suspended
        ? message("resumingInstance")
        : message("pausingInstance")
      : suspended
        ? message("resumeInstance")
        : message("pauseInstance");
    const lifecycleIcon = suspended ? menuIcon("resume") : menuIcon("pause");
    const unavailableTitle = escapeHtml(message("lifecycleUnavailable"));
    const deleteDisabled = !writesEnabled() || state.deleteSubmitting;
    const lifecycleDisabled = !writesEnabled() || suspensionBusy;
    const updateLabel = message("updateInstance");
    const deleteLabel = state.deleteSubmitting ? message("deletingCluster") : message("deleteCluster");
    return `<button class="button button-secondary" type="button" aria-label="${escapeHtml(updateLabel)}" title="${unavailableTitle}" disabled>${menuIcon("update")}<span class="header-action-label">${escapeHtml(updateLabel)}</span></button><button class="button button-secondary" type="button" data-action="set-suspension" data-cluster-name="${escapeHtml(cluster.name)}" data-suspended="${suspended ? "false" : "true"}" aria-label="${escapeHtml(lifecycleLabel)}" title="${escapeHtml(lifecycleLabel)}" ${lifecycleDisabled ? "disabled" : ""}>${lifecycleIcon}<span class="header-action-label">${escapeHtml(lifecycleLabel)}</span></button><button class="button button-danger" type="button" data-action="delete-cluster" data-cluster-name="${escapeHtml(cluster.name)}" aria-label="${escapeHtml(deleteLabel)}" title="${escapeHtml(deleteLabel)}" ${deleteDisabled ? "disabled" : ""}>${menuIcon("delete")}<span class="header-action-label">${escapeHtml(deleteLabel)}</span></button>`;
  }

  function queueAutoLoadForActiveTab(cluster) {
    if (!cluster) return;
    if (state.tab === "connections" && state.apiState === "ready" && state.clientConfig?.name !== cluster.name) {
      window.setTimeout(() => loadClientConfig(), 0);
      return;
    }
    if (state.tab === "logs") {
      const result = state.observability.logs;
      if (result?.name !== cluster.name) window.setTimeout(() => loadObservability("logs"), 0);
      return;
    }
    if (state.tab === "monitoring") {
      const result = state.observability.monitoring;
      if (result?.name !== cluster.name) window.setTimeout(() => loadMonitoring(), 0);
    }
  }

  function renderDetail() {
    const panel = $("#detail-content");
    if (!panel) return;
    const cluster = selectedCluster();
    if (!cluster) {
      panel.innerHTML = `<div class="empty-state empty-state-detail"><strong>${escapeHtml(message("detailPageTitle"))}</strong><p>${escapeHtml(message("detailPageDescription"))}</p><button class="button button-secondary" type="button" data-action="back-to-list">${escapeHtml(message("backToList"))}</button></div>`;
      return;
    }

    const tabs = [
      ["overview", message("overview")],
      ["connections", message("connections")],
      ["logs", message("logs")],
      ["monitoring", message("monitoring")]
    ];
    if (!tabs.some(([id]) => id === state.tab)) state.tab = "overview";

    const body =
      state.tab === "overview"
        ? overviewHtml(cluster)
        : state.tab === "connections"
          ? connectionsHtml(cluster)
          : state.tab === "logs"
            ? logsHtml(cluster)
            : monitoringHtml(cluster);
    const deleteError = state.deleteError?.name === cluster.name ? `<div class="form-error detail-error" role="alert">${escapeHtml(state.deleteError.message)}</div>` : "";

    panel.innerHTML = `<div class="detail-layout"><div class="detail-tabs" role="tablist" aria-orientation="vertical" aria-label="${escapeHtml(message("detailTabsLabel"))}">${tabs.map(([id, title]) => `<button class="tab-button ${state.tab === id ? "is-active" : ""}" id="detail-tab-${escapeHtml(id)}" type="button" role="tab" aria-selected="${state.tab === id}" aria-controls="detail-tabpanel" data-tab="${id}">${tabIcon(id)}<span>${escapeHtml(title)}</span></button>`).join("")}</div><div class="detail-surface"><div class="detail-header"><div class="detail-title"><h2 id="detail-title">${escapeHtml(cluster.name)}</h2><p><code>${escapeHtml(cluster.namespace)}</code> · ${escapeHtml(message("lastTransition"))} ${escapeHtml(formatTime(cluster.lastTransitionTime))}</p></div></div>${deleteError}<div class="detail-body" id="detail-tabpanel" role="tabpanel" tabindex="0" aria-labelledby="detail-tab-${escapeHtml(state.tab)}">${body}</div></div></div>`;

    if (state.tab === "logs" && state.observability.logs?.name === cluster.name && Object.prototype.hasOwnProperty.call(state.observability.logs, "data")) {
      const viewer = $("#log-viewer");
      if (viewer) viewer.textContent = state.observability.logs.data || state.observability.logs.message || message("logsUnavailable");
    }
    queueAutoLoadForActiveTab(cluster);
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
    renderToast();
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

  function clearPollTimer(kind) {
    if (polling.timers[kind] != null) {
      window.clearTimeout(polling.timers[kind]);
      polling.timers[kind] = null;
    }
  }

  function shouldPoll(kind) {
    if (document.visibilityState === "hidden") return false;
    if (kind === "clusters") return true;
    if (state.route.view !== "detail" || !selectedCluster()) return false;
    return state.tab === kind;
  }

  async function pollPrimaryView() {
    await loadClusters({ background: true });
    if (state.route.view === "detail" && state.tab === "connections") {
      await loadClientConfig({ background: true });
    }
  }

  function schedulePoll(kind) {
    clearPollTimer(kind);
    if (!shouldPoll(kind)) return;
    polling.timers[kind] = window.setTimeout(async () => {
      polling.timers[kind] = null;
      if (!shouldPoll(kind)) return;
      if (kind === "clusters") {
        await pollPrimaryView();
      } else if (kind === "logs") {
        await loadObservability("logs", { background: true });
      } else {
        await loadMonitoring({ background: true });
      }
      schedulePoll(kind);
    }, POLL_INTERVALS[kind]);
  }

  function syncAutoPolling() {
    schedulePoll("clusters");
    schedulePoll("logs");
    schedulePoll("monitoring");
  }

  async function loadClusters(options) {
    if (polling.active.clusters) return;
    const background = Boolean(options && options.background === true);
    polling.active.clusters = true;
    if (!background) {
      state.loading = true;
      setApiState("loading");
      render();
    }
    try {
      const payload = await request(API_PREFIX);
      const items = Array.isArray(payload) ? payload : payload?.items || payload?.data || payload?.clusters || [];
      state.clusters = items;
      setApiState("ready");
      if (state.externalAccess?.pending) {
        const cluster = clusterByName(state.externalAccess.name);
        const observed = endpointListFrom(cluster?.status?.externalEndpoints).length > 0 || Boolean(cluster?.status?.externalEndpoint);
        if ((state.externalAccess.enabled && observed) || (!state.externalAccess.enabled && !observed && !externalAccessDesired(cluster))) {
          state.externalAccess = { name: state.externalAccess.name, enabled: state.externalAccess.enabled };
        }
      }
      if (state.suspension?.pending) {
        const cluster = clusterByName(state.suspension.name);
        const observed = Boolean(cluster?.spec?.suspend);
        if (observed === state.suspension.suspended) {
          state.suspension = { name: state.suspension.name, suspended: state.suspension.suspended };
        }
      }
    } catch (error) {
      if (error.code === 401 || error.code === 403) {
        if (!background) state.clusters = [];
        setApiState("forbidden", describeApiError(error, "permissionDeniedCopy"));
      } else {
        if (!background || !state.clusters.length) state.clusters = demoClustersFor(state.language);
        setApiState("degraded", describeApiError(error, "managementApiUnavailable"));
      }
    } finally {
      if (!background) state.loading = false;
      polling.active.clusters = false;
      render();
      if (!background && state.apiState === "ready") {
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

  async function loadObservability(kind, options) {
    const cluster = selectedCluster();
    if (!cluster || polling.active[kind]) return;
    const background = Boolean(options && options.background === true);
    const previous = state.observability[kind];
    polling.active[kind] = true;
    if (!background || previous?.name !== cluster.name || !Object.prototype.hasOwnProperty.call(previous, "data")) {
      state.observability[kind] = { name: cluster.name, loading: true };
      render();
    }
    try {
      const query = "component=broker&tailLines=200";
      const payload = await request(`${API_PREFIX}/${encodeURIComponent(cluster.name)}/${kind}?${query}`);
      const lines = Array.isArray(payload?.lines)
        ? payload.lines.map((line) => `${line.timestamp ? `[${line.timestamp}] ` : ""}${line.message}`).join("\n")
        : payload?.text || payload?.data || "";
      if (selectedCluster()?.name === cluster.name) {
        state.observability[kind] = { name: cluster.name, data: lines, degraded: payload?.degraded, message: payload?.message };
      }
    } catch (error) {
      if (selectedCluster()?.name === cluster.name && (!background || previous?.name !== cluster.name || !Object.prototype.hasOwnProperty.call(previous, "data"))) {
        state.observability[kind] = {
          name: cluster.name,
          error: describeApiError(error, "managementApiUnavailable")
        };
      }
    }
    polling.active[kind] = false;
    render();
  }

  async function loadMonitoring(options) {
    const cluster = selectedCluster();
    if (!cluster || polling.active.monitoring) return;
    const background = Boolean(options && options.background === true);
    const previous = state.observability.monitoring;
    polling.active.monitoring = true;
    if (!background || previous?.name !== cluster.name || !previous?.data) {
      state.observability.monitoring = { name: cluster.name, loading: true };
      render();
    }
    const results = await Promise.all(
      MONITORING_KEYS.map(async (key) => {
        try {
          const payload = await request(`${API_PREFIX}/${encodeURIComponent(cluster.name)}/metrics?key=${encodeURIComponent(key)}`);
          return { key, payload };
        } catch (error) {
          return { key, error };
        }
      })
    );
    const metrics = {};
    const errors = [];
    let degraded = false;
    for (const result of results) {
      if (result.payload) {
        metrics[result.key] = result.payload;
        degraded = degraded || Boolean(result.payload.degraded);
      } else if (result.error) {
        errors.push(describeApiError(result.error, "managementApiUnavailable"));
        degraded = true;
      }
    }
    if (selectedCluster()?.name === cluster.name) {
      if (!Object.keys(metrics).length) {
        if (!background || previous?.name !== cluster.name || !previous?.data) {
          state.observability.monitoring = {
            name: cluster.name,
            error: errors[0] || message("metricsProviderMissing")
          };
        }
      } else {
        const previousMetrics = previous?.data || {};
        state.observability.monitoring = {
          name: cluster.name,
          data: { ...previousMetrics, ...metrics },
          degraded,
          message: errors[0] || Object.values(metrics).find((metric) => metric?.message)?.message || ""
        };
      }
    }
    polling.active.monitoring = false;
    render();
  }

  async function loadClientConfig(options) {
    const cluster = selectedCluster();
    if (!cluster || polling.active.clientConfig) return;
    const background = Boolean(options && options.background === true);
    const previous = state.clientConfig;
    polling.active.clientConfig = true;
    if (!background || previous?.name !== cluster.name || !previous?.data) {
      state.clientConfig = { name: cluster.name, loading: true };
      render();
    }
    try {
      const payload = await request(`${API_PREFIX}/${encodeURIComponent(cluster.name)}/client-config`);
      if (selectedCluster()?.name === cluster.name) state.clientConfig = { name: cluster.name, data: payload };
    } catch (error) {
      if (selectedCluster()?.name === cluster.name && (!background || previous?.name !== cluster.name || !previous?.data)) {
        state.clientConfig = {
          name: cluster.name,
          error: describeApiError(error, "managementApiUnavailable")
        };
      }
    }
    polling.active.clientConfig = false;
    render();
  }

  async function loadClientCredentials(options = {}) {
    const cluster = selectedCluster();
    if (!cluster || state.clientCredentials?.loading) return null;
    const reveal = Boolean(options.reveal);
    const previous = state.clientCredentials;
    state.clientCredentials = { name: cluster.name, loading: true, revealed: Boolean(previous?.revealed || reveal) };
    renderDetail();
    try {
      const payload = await request(`${API_PREFIX}/${encodeURIComponent(cluster.name)}/client-credentials`);
      if (selectedCluster()?.name === cluster.name) {
        state.clientCredentials = { name: cluster.name, data: payload, revealed: Boolean(previous?.revealed || reveal) };
        if (reveal && payload?.password) showToast(message("credentialsRevealed"));
      }
      renderDetail();
      return payload;
    } catch (error) {
      const described = describeApiError(error, "managementApiUnavailable");
      if (selectedCluster()?.name === cluster.name) {
        state.clientCredentials = { name: cluster.name, error: described, revealed: false };
        renderDetail();
      }
      showToast(described, "error");
      return null;
    }
  }

  async function ensureClientCredentials(options = {}) {
    const cluster = selectedCluster();
    if (!cluster) return null;
    const existing = credentialDataFor(cluster);
    if (existing?.password) {
      if (options.reveal && !state.clientCredentials.revealed) {
        state.clientCredentials = { ...state.clientCredentials, revealed: true };
        renderDetail();
        showToast(message("credentialsRevealed"));
      }
      return existing;
    }
    return loadClientCredentials(options);
  }

  async function togglePasswordReveal() {
    const cluster = selectedCluster();
    if (!cluster) return;
    const existing = credentialDataFor(cluster);
    if (!existing?.password) {
      await loadClientCredentials({ reveal: true });
      return;
    }
    const revealed = !state.clientCredentials.revealed;
    state.clientCredentials = { ...state.clientCredentials, revealed };
    renderDetail();
    if (revealed) showToast(message("credentialsRevealed"));
  }

  async function copyClientTemplate(template) {
    const cluster = selectedCluster();
    if (!cluster) return;
    const revealSecrets = Boolean(state.clientCredentials?.revealed);
    const configResult = state.clientConfig?.name === cluster.name && state.clientConfig.data ? state.clientConfig.data : {};
    let config = mergedClientConfig(cluster, configResult);
    if (revealSecrets && !config.password) {
      const credentials = await ensureClientCredentials({ reveal: true });
      if (!credentials?.password) {
        showToast(message("credentialsUnavailable"), "error");
        return;
      }
      config = mergedClientConfig(cluster, configResult);
    }
    const model = connectionModel(cluster, config);
    const text = template === "java"
      ? javaPropertiesTemplate(cluster, config, model, revealSecrets)
      : envTemplate(cluster, config, model, revealSecrets);
    await copyText(text);
  }

  async function copyPassword() {
    const cluster = selectedCluster();
    if (!cluster) return;
    const credentials = credentialDataFor(cluster);
    if (!state.clientCredentials?.revealed || !credentials?.password) {
      showToast(message("credentialsUnavailable"), "error");
      return;
    }
    await copyText(credentials.password);
  }

  function replaceClusterView(payload) {
    const resource = payload?.data || payload;
    const name = resource?.metadata?.name || resource?.name;
    if (!name) return;
    const index = state.clusters.findIndex((item) => (item?.metadata?.name || item?.name) === name);
    if (index === -1) {
      state.clusters.push(resource);
    } else {
      state.clusters[index] = resource;
    }
  }

  async function setExternalAccess(enabled) {
    const cluster = selectedCluster();
    if (!cluster || !writesEnabled() || state.externalAccess?.submitting) return;
    if (!enabled && !window.confirm(message("disableExternalAccessConfirm", { name: cluster.name }))) return;
    state.externalAccess = { name: cluster.name, enabled, submitting: true, error: "" };
    renderDetail();
    try {
      const payload = await request(`${API_PREFIX}/${encodeURIComponent(cluster.name)}/external-access`, {
        method: "PUT",
        body: JSON.stringify({ enabled })
      });
      replaceClusterView(payload);
      state.externalAccess = { name: cluster.name, enabled, pending: true, error: "" };
      await loadClientConfig({ background: true });
    } catch (error) {
      state.externalAccess = {
        name: cluster.name,
        enabled,
        error: message("externalAccessFailed", { message: describeApiError(error, "managementApiUnavailable") })
      };
    }
    render();
  }

  async function setSuspension(name, suspended) {
    const cluster = name ? clusterByName(name) : selectedCluster();
    if (!cluster || !writesEnabled() || state.suspension?.submitting) return;
    state.suspension = { name: cluster.name, suspended, submitting: true, error: "" };
    state.openActionMenu = "";
    render();
    try {
      const payload = await request(`${API_PREFIX}/${encodeURIComponent(cluster.name)}/suspension`, {
        method: "PUT",
        body: JSON.stringify({ suspended })
      });
      replaceClusterView(payload);
      state.suspension = { name: cluster.name, suspended, pending: true, error: "" };
      await loadClusters({ background: true });
    } catch (error) {
      const described = describeApiError(error, "managementApiUnavailable");
      state.suspension = {
        name: cluster.name,
        suspended,
        error: suspended ? message("pauseFailed", { message: described }) : message("resumeFailed", { message: described })
      };
      showToast(state.suspension.error, "error");
    }
    render();
  }

  async function deleteCluster(name) {
    const cluster = name ? clusterByName(name) : selectedCluster();
    if (!cluster || !writesEnabled() || state.deleteSubmitting) return;
    if (!window.confirm(message("deleteConfirmPrompt", { name: cluster.name }))) return;
    state.deleteSubmitting = true;
    state.deleteError = null;
    state.openActionMenu = "";
    render();
    try {
      await request(`${API_PREFIX}/${encodeURIComponent(cluster.name)}`, { method: "DELETE" });
      state.tab = "overview";
      state.clientConfig = {};
      state.clientCredentials = {};
      state.observability = {};
      if (state.route.view === "detail" && state.route.clusterName === cluster.name) navigateToList();
      await loadClusters();
    } catch (error) {
      state.deleteError = {
        name: cluster.name,
        message:
          error.code === 401 || error.code === 403
            ? message("permissionDeniedCopy")
            : message("deleteFailed", { message: describeApiError(error, "managementApiUnavailable") })
      };
      render();
    } finally {
      state.deleteSubmitting = false;
      render();
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
    $("#help-button")?.addEventListener("click", () => alert(message("noHelp")));
    $("#search-input")?.addEventListener("input", (event) => {
      state.search = event.currentTarget.value;
      state.openActionMenu = "";
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
      if (action?.dataset.action === "toggle-row-actions") {
        event.preventDefault();
        event.stopPropagation();
        const menuId = action.dataset.menuId || "";
        state.openActionMenu = state.openActionMenu === menuId ? "" : menuId;
        renderClusterList();
        return;
      }
      if (action?.dataset.action === "delete-cluster") {
        event.preventDefault();
        event.stopPropagation();
        deleteCluster(action.dataset.clusterName);
        return;
      }
      if (action?.dataset.action === "set-suspension") {
        event.preventDefault();
        event.stopPropagation();
        setSuspension(action.dataset.clusterName, action.dataset.suspended === "true");
        return;
      }
      if (event.target.closest(".cluster-row-actions")) {
        event.preventDefault();
        event.stopPropagation();
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
        state.openActionMenu = "";
        navigateToCluster(row.dataset.clusterName);
      }
    });
    $("#detail-header-actions")?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-action]");
      if (!button) return;
      if (button.dataset.action === "delete-cluster") {
        deleteCluster(button.dataset.clusterName);
        return;
      }
      if (button.dataset.action === "set-suspension") {
        setSuspension(button.dataset.clusterName, button.dataset.suspended === "true");
      }
    });
    $("#cluster-list")?.addEventListener("keydown", (event) => {
      const row = event.target.closest(".cluster-row[data-cluster-name]");
      if (!row || event.target !== row || !["Enter", " "].includes(event.key)) return;
      event.preventDefault();
      state.openActionMenu = "";
      navigateToCluster(row.dataset.clusterName);
    });
    $("#detail-content")?.addEventListener("click", (event) => {
      const copyButton = event.target.closest("[data-copy]");
      if (copyButton) {
        const copyValue = copyButton.getAttribute("data-copy");
        if (copyValue) copyText(copyValue);
        return;
      }
      const button = event.target.closest("[data-action]");
      if (!button) return;
      if (button.dataset.action === "toggle-row-actions") {
        event.preventDefault();
        event.stopPropagation();
        const menuId = button.dataset.menuId || "";
        state.openActionMenu = state.openActionMenu === menuId ? "" : menuId;
        renderDetail();
        return;
      }
      if (button.dataset.action === "back-to-list") {
        navigateToList();
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
      if (button.dataset.action === "load-client-credentials") {
        loadClientCredentials({ reveal: true });
        return;
      }
      if (button.dataset.action === "toggle-password") {
        togglePasswordReveal();
        return;
      }
      if (button.dataset.action === "copy-password") {
        copyPassword();
        return;
      }
      if (button.dataset.action === "copy-client-template") {
        copyClientTemplate(button.dataset.template || "env");
        return;
      }
      if (button.dataset.action === "load-logs") {
        loadObservability("logs");
        return;
      }
      if (button.dataset.action === "load-metrics") {
        loadMonitoring();
        return;
      }
      if (button.dataset.action === "set-external-access") {
        setExternalAccess(button.dataset.enabled === "true");
        return;
      }
      if (button.dataset.action === "delete-cluster") {
        deleteCluster(button.dataset.clusterName);
        return;
      }
    });
    $("#detail-content")?.addEventListener("click", (event) => {
      const tabButton = event.target.closest("[data-tab]");
      if (!tabButton) return;
      state.tab = tabButton.dataset.tab;
      renderDetail();
      syncAutoPolling();
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
        return;
      }
      if (event.key === "Escape" && state.openActionMenu) {
        state.openActionMenu = "";
        renderRouteChrome();
        renderClusterList();
        renderDetail();
      }
    });
    document.addEventListener("click", (event) => {
      if (!state.openActionMenu || event.target.closest(".action-menu")) return;
      state.openActionMenu = "";
      renderRouteChrome();
      renderClusterList();
      renderDetail();
    });
    window.addEventListener("hashchange", () => {
      const previous = { ...state.route };
      syncRouteFromLocation();
      state.openActionMenu = "";
      if (state.route.view === "detail" && state.route.clusterName !== previous.clusterName) {
        state.tab = "overview";
        state.observability = {};
        state.clientConfig = {};
        state.clientCredentials = {};
        state.externalAccess = {};
        state.suspension = {};
        state.deleteError = null;
      }
      window.scrollTo({ top: 0, behavior: "auto" });
      render();
      syncAutoPolling();
    });
    document.addEventListener("visibilitychange", syncAutoPolling);
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
    void loadClusters().finally(syncAutoPolling);
  }

  boot();
})();
