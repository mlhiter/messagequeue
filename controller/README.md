# Controller

This directory contains the Kubernetes controller and
`messagequeue.sealos.io/v1alpha1` API. The controller translates product-level
intent into Strimzi `Kafka`, KRaft `KafkaNodePool`, and SCRAM `KafkaUser`
resources and reports observed state through status conditions.

It does not generate Kafka StatefulSets, replace Strimzi, or reconcile
KubeBlocks resources.

The v1alpha1 runtime supports Kafka `3.9.0` and `4.0.0`, combined
broker/controller nodes, persistent JBOD storage, and an internal development
client. Credentials remain in the Strimzi-generated Secret; status exposes its
name only. Topic and User Operators receive explicit `200m/256Mi` requests and
`500m/512Mi` limits so Java startup is not broken by restrictive workspace
LimitRanges.

```bash
cd controller && go test ./...
cd controller && go build -o bin/messagequeue-controller ./cmd
docker build --platform linux/amd64 -f controller/Dockerfile -t messagequeue-controller:dev .
```

The installable CRD is at
`config/crd/bases/messagequeue.sealos.io_messagequeues.yaml`.
