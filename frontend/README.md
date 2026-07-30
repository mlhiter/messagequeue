# Frontend

This directory contains the first-party MessageQueue management interface. It
owns cluster creation, lifecycle, status, connections, logs, and metrics. Kafbat
is an optional user-facing Kafka console and is not embedded here. The current
shell is list-first, with a compact header, search and create actions on the
top bar, dense table rows, and a separate detail page.

## Local development

The UI is intentionally dependency-light: it is a static HTML/CSS/JavaScript
application and uses the browser's `fetch` API. Node.js 18+ is only required for
the build and syntax check.

```bash
npm test
npm run build
npm run serve
```

Then open <http://localhost:4173>. The app requests the same-origin backend by
default. The container accepts `API_BASE_URL` (default `/api`) for the browser
API prefix and `BACKEND_URL` (default `http://messagequeue-backend:8080`) for
the nginx same-origin proxy. Build the image for production with:

```bash
docker build --platform linux/amd64 -t messagequeue-frontend:dev .
```

## Routes

The static UI uses hash routes so it can run behind the existing nginx static
entrypoint:

- `#/clusters`: the entry cluster list page with search and a header-level
  create action.
- `#/clusters/{name}`: a dedicated cluster detail page with Overview,
  Connections, Logs, Metrics, and Settings tabs.

Do not reintroduce a split view that renders the list and detail side by side.
Do not reintroduce a sidebar-first shell for this UI.

## API contract used by the UI

- `GET /api/v1/messagequeues`: list resources. The response is an
  `{ "items": [] }` envelope.
- `POST /api/v1/messagequeues`: create a Kafka resource from the form body. The
  browser sends `{ "name": "...", "spec": { "engine": "kafka", "kafka":
  { "version": "...", "replicas": 1 }, "resources": { "cpu": "1",
  "memory": "2Gi" }, "storage": { "size": "20Gi", "className": "..." },
  "deletionPolicy": "Retain" } }`; workspace identity is server-derived.
- `GET /api/v1/messagequeues/{name}` is reserved for detail refreshes.
- `GET /api/v1/messagequeues/{name}/client-config` loads safe connection
  metadata for the Connections tab. It may include a Secret name reference but
  never Secret `data`, passwords, private keys, or kubeconfigs.
- `DELETE /api/v1/messagequeues/{name}` is called from the Settings tab after
  explicit confirmation. The action is disabled unless write actions are
  enabled and the API is ready.
- `GET /api/v1/messagequeues/{name}/logs?component=broker&tailLines=200` and
  `GET /api/v1/messagequeues/{name}/metrics?key=throughput` are fixed, bounded
  observability queries. The UI never sends raw PromQL or LogsQL.

The list and detail views distinguish loading, empty, ready, provisioning,
degraded, failed, deleting, suspended, and permission-denied states. If the API
is unreachable, the UI renders clearly labelled read-only demo data so the
control surface remains inspectable; create, delete, and observability actions
still require a working backend. Public desktop installs keep write actions
disabled until the Sealos session/workspace adapter is connected. The browser
reads `window.MESSAGEQUEUE_CREATE_ENABLED` from `config.js`; when it is false or
the API is degraded/forbidden, create and delete controls remain disabled. A
metrics provider response with `degraded: true` is rendered as “Metrics
unavailable” rather than as zero-valued data.

The shell follows the Sealos Desktop language setting through the Desktop SDK
protocol. Standalone local development falls back to Chinese unless
`window.MESSAGEQUEUE_LOCALE`, `MESSAGEQUEUE_LOCALE`, `NEXT_LOCALE`, or the
browser language supplies a supported locale.
