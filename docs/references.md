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

Dependency versions and image digests must be pinned in deployment manifests
and recorded here when the first implementation slice is added.
