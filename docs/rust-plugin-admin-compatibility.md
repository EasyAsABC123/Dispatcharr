# Plugin, Django admin and CLI compatibility audit

All application credit remains with the original Dispatcharr authors and contributors. This is an original-code inventory and proposed acceptance plan, not implemented plugin/admin/CLI parity. The Rust core application remains interpreter-free. No interpreter, executor or replacement admin was implemented by this audit.

## Actual plugin execution contract

`apps/plugins/loader.py:582` loads a Python directory containing `plugin.py` (preferred) or an `__init__.py` package. It imports an ordinary Python `Plugin` class and instantiates it with no arguments. Metadata attributes include name/version/description/author/help_url, fields and actions. A JSON `plugin.json` can describe metadata without importing an untrusted plugin. This is an in-process Python object contract, not a language-neutral C ABI, WebAssembly ABI, script subprocess protocol or REST-only extension API.

`run_action` at line848 calls `instance.run(action_id, params, context)` directly. Context contains merged persisted/default settings, the host Python logger, and an action-ID map (`_build_context:1014`). A dictionary return is passed through; another return is wrapped as `{status:ok,result:...}`. It closes old Django connections in finally, including failed actions. Stop supports `instance.stop(context)` or a declared stop action, receives disable/delete/reload reasons, and includes transaction/savepoint handling. There is no enforced per-action timeout or process memory isolation in this path.

Ordinary Python imports can directly access the original Django models/settings, Celery tasks/registries, Redis helpers, modules and global objects. `apps/plugins/apps.py` explicitly loads plugins in every cold web worker so their monkey patches apply; Celery has its own process-init discovery (`dispatcharr/celery.py`). A plugin can import native Python extension modules through normal import machinery, or start its own subprocess, but that does not define additional supported entrypoint languages. No automatic pip/requirements installation contract was found in these loader/install paths; do not invent one.

Loader aliases `_dispatcharr_plugin_...` and folder/package aliases, source reloading, same-timestamp edits, lazy submodule imports and `.reload_token` process coordination matter. Trust is persisted as enabled OR ever_enabled; a currently disabled but previously enabled plugin can still have trusted imported code. Thus disabled is not equivalent to never imported. Exceptions can leave a placeholder registry entry; reloading must retire stale modules/task ownership correctly.

## Persistence, routes and unchanged React

`apps/plugins/models.py` stores PluginConfig(key/name/version/description, enabled/ever_enabled, settings, historical owned_tasks, suspended_schedules, managed repo/slug/prerelease/deprecated flags, timestamps) and PluginRepo(URL/name/official/enabled, cached manifest/public key/signature status/fetch status and timestamps). Native empty-schema support for these tables is not an executable plugin runtime.

`apps/plugins/api_urls.py` mounts under `/api/plugins/`:

- `plugins/` GET list; `plugins/reload/` POST; `plugins/import/` POST ZIP.
- `plugins/{key}/settings/`, `run/`, `enabled/` POST; `delete/` DELETE; `logo/` GET.
- `repos/` GET/POST; `repos/{id}/` detail updates/delete; `repos/{id}/refresh/` POST.
- `repos/available/`, `repos/settings/`, `repos/preview/`, `repos/plugin-detail/`, `repos/install/` with their original methods.

The shared PluginAuthMixin uses `permissions_for_method`, so GET/HEAD are Standard while mutations/default OPTIONS require Admin; do not label every route admin-only. Plugin logo has no permission instances but still normal presented-authentication processing, explicit anonymous UI-network policy, IgnoreClientContentNegotiation, path containment and image/png FileResponse. It is not simply a public static file.

`frontend/src/api.js` plugin methods around2370–2588, `frontend/src/store/plugins.jsx`, and Plugins/PluginBrowse pages consume these routes and returned fields. The UI handles enabled/ever_enabled, fields/actions/defaults, help/author/logo URLs, managed installation/version/repo/filter/prerelease details and settings/action results. `LogFileView.jsx` recognizes original plugin logger prefixes. A native plugin listing backed only by DB rows cannot claim to preserve these flows or action behavior.

ZIP installation uses hidden staging under plugin storage, archive validation and mutation locking before replacement/discovery. Managed installation adds remote manifest/detail/download flows and detached GPG signature verification (`api_views.py:_verify_manifest_signature`), with tri-state verification behavior when a signature/key/tool cannot be used. Native signatures must preserve the exact original compact JSON plus trailing newline/order contract and original policy; replacing GPG with an unrelated checksum is not equivalent. Repository refresh runs at startup and on a default six-hour periodic task, configurable to0/off.

Disable/enable/delete/reload are not single-boolean writes: task names are attributed by source path and retained history; owned PeriodicTask rows are row-locked, suspended schedules recorded, only still-owned current schedules restored, change notifications emitted, stop hooks invoked in the original ordering, and task ownership/files retained appropriately after partial failures. Existing tests in `apps/plugins/tests/` cover these behaviors and staging/managed installs/reload/source-edit/event discovery/DB cleanup.

## Connect event hooks are a separate execution plane

`apps/connect/utils.py` first executes integration handlers and writes DeliveryLogs, then enumerates `iter_actions_for_event(event_name)`. It filters enabled plugin keys and calls each action with `{event:event_name,payload:payload}`; one failed handler is logged without preventing the others. Event action metadata and stable registry ordering matter. Realtime UpdateEventSink publishes WebSocket envelopes; it is not itself a compatible Connect/plugin executor. Native database audit events and realtime notices must not be described as migrated plugin callbacks.

## Concrete implementation choices and boundaries

The feasible native layer is the control plane: exact database metadata/settings/repo APIs, declarative manifest validation and field/action DTOs, ZIP staging/atomic replacement/file ownership, signatures/download policy, schedule ownership, and bounded action/job admission/result/cleanup transport. Those can be ported to Rust with actual original comparisons. They do not execute arbitrary legacy Python plugins.

An optional external Python/Django executor could preserve many legacy action/task plugins while keeping the Rust core image interpreter-free. It would be an explicit separately packaged service/tool, with a versioned bounded protocol, actual original model code/dependencies where necessary, isolated and declared DB/Redis/storage access, workload admission/timeouts/cancellation, error/result schemas, event ordering, task ownership, and resource accounting. Merely starting `python plugin.py` is not compatible with the original import context, ORM and Celery registries.

Such an executor cannot make a monkey patch in its own Python process alter the Rust web process. Plugins that patch original request handlers, singleton managers, model methods or middleware therefore require an explicit native extension port/interface or remain unsupported. RPC emulation cannot transparently reproduce arbitrary object references, ORM transactions, Python module identity, monkey patches, C-extension dependencies or shared process globals. Do not claim full legacy plugin compatibility or pure-Rust execution based on a successful external action example. Native Rust/Wasm plugin contracts, if later chosen, are new APIs, not1to1 legacy ABI replacements.

No external executor is enabled or implemented here. Keep unsupported execution explicit rather than silently declaring successful actions. The compatibility boundary must be visible in docs and acceptance ledgers; native core performance measurements must either exclude optional executor workloads explicitly or include their complete service resources when used.

## Django admin and operator CLI

Original `dispatcharr/urls.py` exposes `/admin` redirect and `/admin/` Django admin. The packaged nginx config redirects general admin URLs to `/login`, deliberately exempting the three-segment XC stream form when its username is literally admin. Direct backend/dedicated admin access still has real Django admin forms. Native React administration and matching REST APIs do not reproduce Django's admin HTML, session/CSRF forms, model permissions, group/user-permission editing, search/filter/change history or registered model actions.

Registrations include accounts User/Group, core UserAgent/StreamProfile/CoreSettings, M3U accounts/profiles/filters/server groups, streams/channels/groups, EPG sources/programmes, HDHR devices, VOD categories/movie/series/episode/provider relations. `apps/accounts/admin.py` wraps admin login with the same login throttle budget as JWT obtain. Static route fallback must not serve React200 for an unimplemented admin form and call it parity; admin-prefix changes must preserve the existing XC admin-username exception.

`manage.py` is Django's command dispatcher, including test settings selection. Existing deployment/operator expectations include migrate/collectstatic, createsuperuser, shell/dbshell/loaddata/dumpdata/check/showmigrations, plus custom commands:

| Command | Original behavior and source |
|---|---|
| `reset_network_access` | `core/management/commands/reset_network_access.py`: sets existing CoreSettings network-access row value to empty object through model save. |
| `reset_user_network [--user NAME]` | Removes only allowed_networks from selected/all users' custom properties; saves affected rows and reports count. |
| `kill_processes` | Enumerates host-visible processes and kills names/command lines containing ffmpeg or streamlink. Native owned-child cleanup is safer but is a distinct operator contract, not this command's literal breadth. |
| `dropdb` | Prompts for yes, closes DB connection, connects to maintenance postgres, drops/recreates configured database with configured TLS; instructs migrate afterward. This destructive command is not exercised or implemented here. |
| `proxy start|stop|restart --type hls|ts|all --channel ID --url URL` | `apps/proxy/management/commands/proxy.py`: accesses Django app-config proxy objects; output/errors and actual availability require original command probes. It is not automatically equivalent to native HTTP stop routes. |

README.md explicitly recommends `docker exec ... python manage.py createsuperuser` for first-admin setup on a public host. Existing native first-admin HTTP and empty-schema gates do not satisfy that CLI instruction. A native operator binary can port supported commands/flags/env/exit codes without a Python interpreter; it must have actual original tests. Arbitrary Django shell, migration Python, dumpdata natural-key/model behavior and custom plugin commands remain a different compatibility problem. An optional legacy maintenance container is an explicit separate tool, not a hidden interpreter in the core image. Running old migrate against a native-supported database also requires schema/version/backup coordination; empty-schema initialization and recorded migration rows are not an upgrade engine.

## Required acceptance gates

Capture original APIRequestFactory and real HTTP/browser contracts separately. Use owned disposable plugin folders/repos/keys/DB/Redis, original test fixtures and a local HTTP repo server; no public repo fetch is needed to prove installation behavior. Test role/header/HEAD/method/negotiation semantics, field/default/result body types, untrusted import without code execution, enable/ever-enabled trust, import/reload/stop/event exception cleanup, process restart/reload tokens, same-timestamp module edits and task discovery in web/Celery workers.

Prove transactional schedule ownership and concurrent enable/disable/reload/delete; preserve unrelated periodic tasks and successor installs. Exercise bounded ZIP bytes/file count/decompression/path handling, signature states/canonicalization and atomic upgrade rollback, file ownership and partial DB/filesystem failures. Any external executor needs physical child/task/DB ownership through cancellation and shutdown, bounded messages/results/pending jobs, no deadlock with ORM locks, and a multi-process resource matrix including its actual containers. Pure metadata/native extension ports need their own exact original or new-contract labels.

For CLI/admin, inventory actual deployed commands and capture help/flags/output/exit codes on disposable resources. First test readonly/admin auth/session/throttle/CSRF and safe network-reset commands; destructive database/process commands require separate explicit fixture and effect boundaries, never execution against the developer host. Record unsupported commands/admin pages honestly. Preserve full upstream attribution and license obligations for original Python/model/migration assets in any optional compatibility package.

## Executed original plugin oracle

[Source fixture2](../scripts/rust-plugin-contracts/verified-plugin-source-2.json) now supplies 31 original Factory/control responses and actual owned Python execution on a copied database. It verifies manifest-only discovery before trust, live import after enabling, ever-enabled disabled imports, merged context/defaults and unknown settings, dictionary/scalar/undeclared/failed actions, actual ORM mutation and connection cleanup, and reload/disable/delete stop reasons. The owned directory/config rows and internal Docker project are removed; the default database is unchanged. The first failed helper-mount setup is retained. [Full source evidence and reproduction](../scripts/rust-plugin-contracts/README.md) explicitly exclude native executor/API acceptance, remote/managed installation, schedules, concurrency, network HEAD and performance. This execution evidence supersedes the audit's no-execution status only for the named original probe; no interpreter or replacement admin has been implemented.
