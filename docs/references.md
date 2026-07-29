# References

These projects define upstream behavior or provide implementation evidence.
They are dependencies and references, not sources to copy wholesale.

## Kafka Runtime

- [Apache Kafka](https://kafka.apache.org/): broker protocol and operational
  semantics.
- [Strimzi Kafka Operator](https://github.com/strimzi/strimzi-kafka-operator):
  Kubernetes Kafka orchestration, users, upgrades, and exporters.
- [Strimzi documentation](https://strimzi.io/documentation/): supported API,
  Kafka versions, security, storage, and upgrade procedures.

## User-Facing Console

- [Kafbat UI](https://github.com/kafbat/kafka-ui): preferred actively maintained
  Kafka user console.
- [Provectus Kafka UI](https://github.com/provectus/kafka-ui): historical
  predecessor. Do not default to its unmaintained release line.

The Kafka console is a complete application with its own backend. It is not a
React component library and is not used to implement the management UI.

## Observability

- [VictoriaMetrics Operator](https://github.com/VictoriaMetrics/operator):
  shared scrape and alert resources.
- [VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics): metrics
  storage and query engine.
- [VictoriaLogs](https://docs.victoriametrics.com/victorialogs/): historical
  Kubernetes log storage and LogsQL behavior.

## Integration Boundary

- [Sealos](https://github.com/labring/sealos): reference for application
  registration, workspace authentication, quota, billing, and existing
  observability contracts.
- KubeBlocks is historical context only. No KubeBlocks runtime resource, API,
  library, image, or migration adapter belongs in this repository.

The current deployment pins Strimzi `0.46.0` and Kafka `3.9.0` by operator
contract. MessageQueue `v0.1.6` images were published to the test registry as
amd64 images:

- controller: `sha256:94e02ec6805c4dd36f3d7f834ae1bc45f2b7fd99df721f425fc694cb8bc03a9e`
- backend: `sha256:61f62b63ba9dde2a71cb970235481d46ae90d60022b7f66256cd07160437c6bc`
- frontend: `sha256:2b0cd7d2c30e48ca0fbf225944f752557ae0fa858080cf304633e2397d1ec32e`

Production values should set `global.imagePolicy.requireDigest=true` and use
these or a separately reviewed immutable digest set.
