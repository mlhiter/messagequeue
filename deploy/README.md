# Deployment

This directory will contain Helm charts, CRDs, release values, and installation
assets for the management UI, backend, controller, Strimzi integration, and
optional Kafbat console.

Production images target `linux/amd64`, use immutable versions and digests, and
must be installable without modifying the Sealos monorepo.
