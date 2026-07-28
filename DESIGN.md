---
name: MessageQueue
description: A restrained operational interface for managing message brokers.
---

<!--
SEED: re-run $impeccable document once there is UI code to capture actual
tokens and components.
-->

# Design System: MessageQueue

## Overview

**Creative North Star: "The Operator's Console"**

The primary surface is used by engineers scanning status, comparing resource
state, and making consequential changes during extended browser sessions. It
should be dense enough for repeated operations without feeling compressed or
decorative. Familiar controls, stable layout, and explicit status carry more
weight than visual novelty.

The reference register is Sealos for workspace context, Grafana for operational
scanning, and Kubernetes Dashboard for resource truth. The result must not look
like a marketing landing page, a raw YAML editor, the existing database
provider, or Kafbat UI.

The MessageQueue shell should follow the Sealos desktop convention: dark
sidebar, pale workspace canvas, low-radius cards, subtle borders, restrained
blue-violet emphasis, and dense control panels rather than decorative tiles.
The management UI defaults to Chinese and keeps English as an explicit toggle,
so every visible label, empty state, and form helper must be i18n-ready.

**Key Characteristics:**

- Restrained, information-first composition
- Stable navigation and scan-friendly tables
- Clear distinction between desired, observed, and failed state
- Short, state-driven motion with reduced-motion support

## Colors

Use a restrained strategy: neutral surfaces with one indigo-violet product
accent occupying no more than 10 percent of a screen. Actual OKLCH tokens will
be resolved from the first implemented management surface. Success, warning,
error, and informational colors must remain perceptually distinct and must
always be paired with text or icons.

**The Status-Only Color Rule.** Saturated color communicates action, selection,
or state. It is never background decoration.

## Typography

Use a single technical-humanist sans family for the interface and a compatible
monospace family for identifiers, endpoints, offsets, and logs. Font choices
and exact fixed-size roles will be resolved during the first UI implementation.
Do not use fluid type sizing in the application shell.

**The Operational Density Rule.** Headings establish local hierarchy without
competing with tables, logs, metrics, or controls.

## Elevation

The interface is flat by default. Depth comes from tonal surface changes,
dividers, and explicit overlay states. Shadows are reserved for menus, dialogs,
and transient overlays, never as decoration around every section.

**The Stable Surface Rule.** Hover and loading states must not resize or shift
surrounding content.

## Do's and Don'ts

### Do

- **Do** use familiar controls, stable dimensions, and explicit loading, empty,
  degraded, and permission-denied states.
- **Do** keep operational tables and log surfaces dense, readable, and
  keyboard-accessible.
- **Do** pair every state color with text or an icon.

### Don't

- **Don't** build a marketing landing page with oversized headlines,
  decorative gradients, or repeated feature-card grids.
- **Don't** expose a generic Kubernetes YAML editor as the primary workflow.
- **Don't** copy the existing database provider information architecture.
- **Don't** present Kafbat UI as the MessageQueue management interface.
- **Don't** use a dark, blue-heavy operations dashboard or color as decoration.
