# Frontend

This directory will contain the first-party MessageQueue management interface.
It owns cluster creation, lifecycle, status, connections, logs, monitoring, and
operation history. It must not embed Kafbat UI as the management surface.

The framework and package manifest will be introduced with the first vertical
slice so the choice is backed by an executable workflow and tests.
