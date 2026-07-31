# MessageQueue Create Semantic Test Contract

## 1. 模块信息

- 模块：MessageQueue frontend
- 页面：Kafka cluster create dialog
- 适用版本：v0.1.8+
- 维护人：MessageQueue frontend owner

## 2. 页面入口

| 页面 | 路由 | 说明 |
| --- | --- | --- |
| 集群列表 | `#/clusters` | Header `新建` 按钮打开创建弹窗。 |
| 空列表 | `#/clusters` | Empty state create action打开同一个创建弹窗。 |

## 3. 语义标签清单

| 页面元素 | 代码位置 | data-testid | 类型 | 业务语义 | data-qa-* | 可操作 | 可断言 | 证据来源 | 关联风险 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 创建弹窗 | `frontend/index.html` | `messagequeue.create.modal` | panel | Kafka 集群创建入口 | `data-qa-module=messagequeue`, `data-qa-object=kafka-cluster` | 否 | 是 | requirement | 写资源入口 |
| 创建表单 | `frontend/index.html` | `messagequeue.create.form` | panel | 创建请求表单 | none | 否 | 是 | form | 表单合同漂移 |
| 集群名称 | `frontend/index.html` | `messagequeue.create.name-input` | field | MessageQueue 资源名称 | `data-qa-field=name` | 是 | 是 | API | Kubernetes DNS 名称 |
| Kafka 版本 | `frontend/index.html` | `messagequeue.create.version-select` | field | Kafka 版本 | `data-qa-field=kafka_version` | 是 | 是 | API | 不支持版本 |
| 规格选项 | `frontend/index.html` | `messagequeue.create.profile-option` | field | development / standard / custom 规格 | `data-qa-profile=development|standard|custom` | 是 | 是 | requirement | 规格选择错误 |
| Broker 数量 | `frontend/index.html` | `messagequeue.create.broker-input` | field | broker 副本数 | `data-qa-field=broker_count` | custom 时是 | 是 | API | 资源成本 |
| CPU | `frontend/index.html` | `messagequeue.create.cpu-input` | field | 每个 broker CPU | `data-qa-field=broker_cpu` | custom 时是 | 是 | API | 资源不足或超配 |
| 内存 | `frontend/index.html` | `messagequeue.create.memory-input` | field | 每个 broker 内存 | `data-qa-field=broker_memory` | custom 时是 | 是 | API | JVM 内存不足 |
| 存储 | `frontend/index.html` | `messagequeue.create.storage-input` | field | 每个 broker 存储 Gi | `data-qa-field=storage_gi` | custom 时是 | 是 | API | PVC 成本 |
| 存储类 | `frontend/index.html` | `messagequeue.create.storage-class-input` | field | StorageClass | `data-qa-field=storage_class` | custom 时是 | 是 | Kubernetes | 调度失败 |
| 删除策略 | `frontend/index.html` | `messagequeue.create.deletion-policy-select` | field | Retain / Delete | `data-qa-field=deletion_policy` | custom 时是 | 是 | safety | PVC 删除风险 |
| 资源摘要 | `frontend/index.html` | `messagequeue.create.summary` | state | 当前将提交的资源占用 | `data-qa-state=development|standard|custom` | 否 | 是 | state | 提交前误解资源 |
| 错误提示 | `frontend/index.html` | `messagequeue.create.error` | error | 创建校验或 API 错误 | `data-qa-state=error`, `data-qa-error-code=*` | 否 | 是 | API | 失败不可见 |
| 提交按钮 | `frontend/index.html` | `messagequeue.create.submit-button` | action | 创建 Kafka 集群 | `data-qa-action=create`, `data-qa-state=ready|loading` | 是 | 是 | mutation | 写资源 |

## 4. 状态枚举

| 元素 | data-qa-state 可选值 | 说明 |
| --- | --- | --- |
| `messagequeue.create.summary` | `development`, `standard`, `custom` | 当前规格来源。 |
| `messagequeue.create.submit-button` | `ready`, `loading` | 可提交或正在提交。 |
| `#custom-resource-fields` | `preset`, `custom` | 规格字段锁定或可编辑。 |

## 5. 禁用原因枚举

| disabled reason | 含义 | 自动化预期 |
| --- | --- | --- |
| `api_unavailable` | 管理 API 不可用 | 创建入口或提交动作不可用。 |

## 6. 错误码枚举

| error code | 含义 | 自动化预期 |
| --- | --- | --- |
| `invalid_request` | 本地或后端校验失败 | 表单应保留打开状态。 |
| `permission_denied` | 当前工作空间无写权限 | 表单应显示权限错误。 |
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
| 纯布局容器 | skipped | 不添加测试标签。 |

## 9. 变更规则

- 新增创建字段必须新增稳定 `data-testid`。
- 删除或重命名标签必须同步更新本文档。
- 自动化不得依赖 CSS class、DOM 层级或中文文案作为唯一定位方式。
- 不得把密码、kubeconfig、Secret data 写入任何语义标签。
