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
contract. MessageQueue `v0.1.9` images were published to the test registry as
amd64 images:

- controller: `sha256:5c47b5845b9f32112531a4dafc3887213a1c3eba1b61f9abd17a6c4e8e880b85`
- backend: `sha256:bedcef708a9e4f01586251e5536053a186fc6ec9d1af85f91c95af3f039cfe6f`
- frontend: `sha256:ea9e89ae512cbc4bd51b62cb037c20ea8de10eb7d123345713ef4efeb079c91e`

Production values should set `global.imagePolicy.requireDigest=true` and use
these or a separately reviewed immutable digest set.
