# MessageQueue Frontend Semantic Test Contract

## 1. 模块信息

- 模块：MessageQueue frontend
- 页面：Kafka instance list, create dialog, and detail pages
- 适用版本：v0.1.9+
- 维护人：MessageQueue frontend owner

## 2. 页面入口

| 页面 | 路由 | 说明 |
| --- | --- | --- |
| 实例列表 | `#/clusters` | Header `新建` 按钮打开创建弹窗。 |
| 空列表 | `#/clusters` | Empty state create action打开同一个创建弹窗。 |
| 实例详情 | `#/clusters/{name}` | 展示概览、连接、日志、监控和页面级操作。 |

## 3. 语义标签清单

| 页面元素 | 代码位置 | data-testid | 类型 | 业务语义 | data-qa-* | 可操作 | 可断言 | 证据来源 | 关联风险 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 创建弹窗 | `frontend/index.html` | `messagequeue.create.modal` | panel | Kafka 实例创建入口 | `data-qa-module=messagequeue`, `data-qa-object=kafka-cluster` | 否 | 是 | requirement | 写资源入口 |
| 创建表单 | `frontend/index.html` | `messagequeue.create.form` | panel | 创建请求表单 | none | 否 | 是 | form | 表单合同漂移 |
| 实例名称 | `frontend/index.html` | `messagequeue.create.name-input` | field | MessageQueue 资源名称 | `data-qa-field=name` | 是 | 是 | API | Kubernetes DNS 名称 |
| Kafka 版本 | `frontend/index.html` | `messagequeue.create.version-select` | field | Kafka 版本 | `data-qa-field=kafka_version` | 是 | 是 | API | 不支持版本 |
| 规格选项 | `frontend/index.html` | `messagequeue.create.profile-option` | field | development / standard / custom 规格 | `data-qa-profile=development|standard|custom` | 是 | 是 | requirement | 规格选择错误 |
| Broker 数量 | `frontend/index.html` | `messagequeue.create.broker-input` | field | broker 副本数 | `data-qa-field=broker_count` | custom 时是 | 是 | API | 资源成本 |
| CPU | `frontend/index.html` | `messagequeue.create.cpu-input` | field | 每个 broker CPU | `data-qa-field=broker_cpu` | custom 时是 | 是 | API | 资源不足或超配 |
| 内存 | `frontend/index.html` | `messagequeue.create.memory-input` | field | 每个 broker 内存 | `data-qa-field=broker_memory` | custom 时是 | 是 | API | JVM 内存不足 |
| 存储 | `frontend/index.html` | `messagequeue.create.storage-input` | field | 每个 broker 存储 Gi | `data-qa-field=storage_gi` | custom 时是 | 是 | API | PVC 成本 |
| 存储类 | `frontend/index.html` | `messagequeue.create.storage-class-input` | field | StorageClass | `data-qa-field=storage_class` | custom 时是 | 是 | Kubernetes | 调度失败 |
| 删除策略 | `frontend/index.html` | `messagequeue.create.deletion-policy-select` | field | Retain / Delete | `data-qa-field=deletion_policy` | custom 时是 | 是 | safety | PVC 删除风险 |
| 资源摘要 | `frontend/index.html` | `messagequeue.create.summary` | state | 当前将提交的资源占用 | `data-qa-state=development|standard|custom` | 否 | 是 | state | 提交前误解资源 |
| 配额提示 | `frontend/index.html` | `messagequeue.create.quota-note` | state | 当前工作空间配额状态 | `data-qa-state=loading|ready|warning|degraded` | 否 | 是 | state | 配额信息不可见 |
| 错误提示 | `frontend/index.html` | `messagequeue.create.error` | error | 创建校验或 API 错误 | `data-qa-state=error`, `data-qa-error-code=*` | 否 | 是 | API | 失败不可见 |
| 提交按钮 | `frontend/index.html` | `messagequeue.create.submit-button` | action | 创建 Kafka 实例 | `data-qa-action=create`, `data-qa-state=ready|loading` | 是 | 是 | mutation | 写资源 |
| 详情概览 | `frontend/app.js` | `messagequeue.detail.overview` | panel | 实例健康摘要 | none | 否 | 是 | status | 误读可用性 |
| 连接信息 | `frontend/app.js` | `messagequeue.detail.connections` | panel | secret-free 内外网连接元数据 | none | 否 | 是 | API | 凭据泄露 |
| 内网连接 | `frontend/app.js` | `messagequeue.detail.connection-internal` | panel | 内网 host、port、连接串 | `data-qa-state=ready|pending` | ready 时复制字段 | 是 | API | 连接地址错误 |
| 外网未开启 | `frontend/app.js` | `messagequeue.detail.connection-external-disabled` | state | 外网访问关闭或无端点 | `data-qa-state=pending` | 否 | 是 | state | 误导公网可用 |
| 日志面板 | `frontend/app.js` | `messagequeue.detail.logs` | panel | 自动加载的 broker 日志 | none | 刷新 | 是 | API | 诊断入口不可用 |
| 监控面板 | `frontend/app.js` | `messagequeue.detail.monitoring` | panel | 自动加载的固定 key 监控 | `data-metric-key=*` | 刷新 | 是 | API | 暴露 raw query |
| 详情操作区 | `frontend/app.js` | `messagequeue.detail.header-actions` | action-group | 刷新、更新、暂停/恢复、删除 | none | 部分可操作 | 是 | mutation | 生命周期误操作 |

## 4. 状态枚举

| 元素 | data-qa-state 可选值 | 说明 |
| --- | --- | --- |
| `messagequeue.create.summary` | `development`, `standard`, `custom` | 当前规格来源。 |
| `messagequeue.create.quota-note` | `loading`, `ready`, `warning`, `degraded` | 当前工作空间配额摘要或降级状态。 |
| `messagequeue.create.submit-button` | `ready`, `loading` | 可提交或正在提交。 |
| `#custom-resource-fields` | `preset`, `custom` | 规格字段锁定或可编辑。 |
| `messagequeue.detail.connection-internal` | `ready`, `pending` | 内网连接串可用或等待端点。 |
| `messagequeue.detail.monitoring` | `loading`, `ready`, `degraded`, `error` | 监控自动加载并局部降级。 |

## 5. 禁用原因枚举

| disabled reason | 含义 | 自动化预期 |
| --- | --- | --- |
| `api_unavailable` | 管理 API 不可用 | 创建入口或提交动作不可用。 |

## 6. 错误码枚举

| error code | 含义 | 自动化预期 |
| --- | --- | --- |
| `invalid_request` | 本地或后端校验失败 | 表单应保留打开状态。 |
| `permission_denied` | 当前工作空间无写权限 | 表单应显示权限错误。 |
| `quota_exceeded` | 工作空间配额不足 | 表单应显示固定的人话配额提示。 |
| `create_failed` | 后端创建失败 | 表单应显示后端错误消息。 |

## 7. 资源绑定

| 页面元素 | 资源字段 | 说明 |
| --- | --- | --- |
| 创建弹窗 | `MessageQueue.metadata.name` | 来源为 `messagequeue.create.name-input`。 |

## 8. 覆盖说明

| 需求元素 | 覆盖状态 | 说明 |
| --- | --- | --- |
| development 默认规格 | covered | 默认 `500m / 1Gi / 10Gi / Retain`。 |
| standard 规格 | covered | `3 broker / 1 CPU / 2Gi / 20Gi / Retain`。 |
| custom 可编辑规格 | covered | broker、CPU、内存、存储、存储类、删除策略；后端按每 broker 8 CPU / 64Gi / 1024Gi 上限兜底。 |
| raw Strimzi YAML | skipped | 主流程不暴露 YAML 编辑器。 |
| Settings tab | skipped | 删除、更新、暂停/恢复入口在列表 more 和详情 header，不再通过 tab 控制。 |
| raw PromQL / LogsQL | skipped | 前端只能请求后端固定 key 和固定日志参数。 |
| 纯布局容器 | skipped | 不添加测试标签。 |

## 9. 变更规则

- 新增创建字段必须新增稳定 `data-testid`。
- 删除或重命名标签必须同步更新本文档。
- 自动化不得依赖 CSS class、DOM 层级或中文文案作为唯一定位方式。
- 不得把密码、kubeconfig、Secret data 写入任何语义标签。
- 连接串只能包含 secret-free 元数据，不得包含密码、私钥、kubeconfig、Secret data 或 raw query。
