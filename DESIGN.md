---
name: MessageQueue
description: A restrained operational interface for managing message brokers.
---

# Design System: MessageQueue

## Overview

**Creative North Star: "The Operator's Console"**

The primary surface is used by engineers scanning status, comparing resource
state, and making consequential changes during extended browser sessions. It
should be dense enough for repeated operations without feeling compressed or
decorative. Familiar controls, stable layout, and explicit status carry more
weight than visual novelty.

The reference register is DevBox for the list-first shell and header actions,
Sealos for workspace context, Grafana for operational scanning, and Kubernetes
Dashboard for resource truth. The result must not look like a marketing
landing page, a raw YAML editor, the existing database provider, or Kafbat UI.

The MessageQueue shell follows DevBox's list-first operational convention with
a light zinc workspace canvas, a compact top header, white operational
surfaces, hairline borders, black primary actions, semantic status colors, and
dense row cards rather than decorative tiles. The management UI follows the
Sealos Desktop language setting through the Desktop SDK protocol, so every
visible label, empty state, and form helper must remain i18n-ready.

**Key Characteristics:**

- Restrained, information-first composition
- Stable navigation and scan-friendly tables
- Dedicated list pages and detail pages instead of split-view inspection
- Clear distinction between desired, observed, and failed state
- Short, state-driven motion with reduced-motion support

## Colors

Use the Sealos/DevBox neutral palette: zinc canvas, white surfaces, zinc text
and borders, and a black/zinc primary action. Blue is reserved for informational
or provisioning state, not for decorative emphasis or button glow. Current
tokens live in `frontend/styles.css`; success, warning, error, and
informational colors must remain perceptually distinct and must always be
paired with text or icons.

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

The interface is flat by default. List surfaces follow DevBox exactly:
`0px 2px 8px -2px rgba(0, 0, 0, 0.08)`, `0.5px` borders, `8px` table-header
radius, and `12px` row/card radius. Buttons do not use decorative shadows.
Dialogs and popovers may use a restrained `shadow-lg`-class overlay treatment.

**The Stable Surface Rule.** Hover and loading states must not resize or shift
surrounding content.

## Interaction Model

The entry route is the cluster list. Its job is to orient, search, create, and
open a resource. The create action sits in the page header; public Desktop
installs keep it visibly disabled until workspace identity is connected.

Cluster rows navigate to a dedicated detail page. Detail pages own overview,
connection, logs, and metrics tabs, and always provide a clear return path to
the cluster list. Do not reintroduce a two-pane list-plus-detail layout.

Motion is intentionally quiet. List rows, tabs, buttons, and shell controls use
explicit color or opacity transitions only. Do not add hover lift,
`translateY`, active `scale`, animated shadow changes, bounce, or decorative
page-enter motion.

## Do's and Don'ts

### Do

- **Do** use familiar controls, stable dimensions, and explicit loading, empty,
  degraded, and permission-denied states.
- **Do** keep operational tables and log surfaces dense, readable, and
  keyboard-accessible.
- **Do** keep the list visible on the first screen; status summaries must not
  push the table below the fold.
- **Do** pair every state color with text or an icon.

### Don't

- **Don't** build a marketing landing page with oversized headlines,
  decorative gradients, or repeated feature-card grids.
- **Don't** expose a generic Kubernetes YAML editor as the primary workflow.
- **Don't** copy the existing database provider information architecture.
- **Don't** present Kafbat UI as the MessageQueue management interface.
- **Don't** put list and detail information into one split screen.
- **Don't** reintroduce a sidebar-first shell or a dark, blue-heavy operations
  dashboard.
