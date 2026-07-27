# Backend

This directory will contain the authenticated management API. It derives the
workspace namespace from server-side identity, performs Kubernetes permission
checks, and exposes bounded APIs for product resources, logs, and metrics.

It must not accept browser-controlled namespaces, raw PromQL, raw LogsQL, or
Secret payloads without an explicit authorized operation.
