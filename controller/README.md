# Controller

This directory will contain the Kubernetes controller and
`messagequeue.sealos.io` APIs. The controller translates product-level intent
into Strimzi resources and reports observed state through status conditions.

It does not generate Kafka StatefulSets, replace Strimzi, or reconcile
KubeBlocks resources.
