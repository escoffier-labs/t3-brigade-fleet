# Attached app operations

Before using tool names below, read [tool compatibility](tool-compatibility.md).
T3 v0.1.3 consolidates several tools. Use the attached catalog and the matching
argument schema. Earlier names in this reference apply only to older catalogs.

## Projects, configuration, and environment

Start with `t3_project_list` and `t3_project_read` to identify the registered
project and its workspace/scripts. These are environment-level reads. Most
thread, queue, question, and scheduler operations accept explicit scope. See
[supervision.md](supervision.md) for cross-project reads and `projectId`. Read
the live schema for pagination. Project/preview pages can shift with the
current snapshot.

On builds that consolidate thread configuration, omit `modelSelection` to
read or supply it to set the provider, model, and options using the live catalog.
Supply `threadId` to select the target when the server accepts it. Otherwise
the calling thread is the target. Permission modes stay unchanged. On older
builds, use `t3_thread_configuration` for scoped reads and check the write
scope in the live configuration tool schema.
`t3_project_update` changes project settings and preserves omitted fields.
On T3 0.1.6, project updates can advertise `pinned` and `archived`. Use
`archived: true` to hide a finished project from pickers while preserving its
threads and files, and `archived: false` to restore it. `pinned` controls its
position in project pickers. Check the selected server's live schema first.
Older servers can drop these fields. Verify the returned `archivedAt` or
`pinnedAt` state. Do not substitute `t3_project_delete` for archival.

Project create/update/delete/clone and environment writes are operator actions.
`t3_environment_read` reports server identity/preferences, while
`t3_environment_preferences_update` writes environment-wide settings and
requires the operator to meet its live capability gate. Omitted
fields remain unchanged, and empty `customInstructions` clears them. Check the live instruction limit.
Avoid durable defaults or custom-instruction changes unless requested.

Do not create projects as a routine dispatch repair. `t3_project_clone` only
clones, then registration is separate. `t3_project_create` with no workspace
root makes a Git repository and first commit. Each creation is a new request,
and an existing registered workspace is rejected. `t3_project_delete` does not
delete the repository directory, and nonempty projects require force. These
operations need their own task scope and authorization.

## Preview before browser fallback

For an attached app preview, call `preview_status`, then `preview_open` to
initialize or reuse the thread-bound inline preview. Use an alternative browser
only when T3 tools are absent after bounded discovery, the user requests another
browser, or `preview_open` explicitly reports unsupported or unavailable.
An actionable first-call error is not evidence of unavailability: correct the
reported arguments, tab selection, or initialization issue and retry the T3
call. If the corrected call still fails without establishing unavailability,
report the concrete blocker. See [orchestration.md](orchestration.md) for bounded
tool discovery. `t3_preview_list` identifies this thread's tabs. Pin `tabId`
when multiple tabs could be confused. `preview_open` can create a separate tab
with `reuseExistingTab: false`.

Current-build caveats [unverified]: prefer `open: true`. Earlier builds had
unresponsive background tabs with `open: false`, and loopback URLs were
rewritten to environment-port targets. Only when the T3 server binds a
non-loopback address, bind the dev server to an address that server can reach,
within authorized network exposure.
Verify state with `preview_evaluate` after `preview_navigate` or `preview_click`.
These behaviors have not been retested on 0.0.46 or 0.1.x.

Inspect `preview_snapshot` before interacting. Its semantic elements,
diagnostics, action history, and screenshot guide locator-based `preview_click`
and `preview_type`. Prefer Playwright locators over coordinates. Wait for
specific locator/text/URL evidence through `preview_wait_for`, and use
`preview_press` / `preview_scroll` as needed. Reinspect after state changes.
`preview_navigate` can target a URL or an environment port, using the observed
dev server rather than a guessed port. Use `preview_hover`, `preview_drag`,
`preview_select`, `preview_upload`, and `preview_dialog` for their respective
interactions. Check live schemas for arguments.

Snapshot text can be truncated and reports omissions. `preview_evaluate` can
read more within its live result limit, but JavaScript may mutate page state.
Apply the task's authorization to navigation, clicks, uploads, evaluation, and
form submission. Do not infer that a form is authorized from a visible button.

Nightly 20261110.4465 supports top-level `await` in `preview_evaluate`
([upstream change](https://github.com/pingdotgg/t3code/pull/31925)).
On older builds, wrap asynchronous evaluation in an async function and invoke
it. Evaluation can still change page state. Await support grants no additional
authorization.

For a screenshot artifact use `preview_snapshot` with `save: true` and retain
`screenshotPath`. Embed it as `![description](<returned-screenshot-path>)` in a
reply when useful. An image-only tool result is not a saved artifact.
`includeImage: false` can return only the saved path and URL. Use
`preview_resize` for responsive layouts, `preview_set_appearance` for color
scheme, and `preview_recording_start` / `preview_recording_stop` for requested
demos. Recording stop transfers a file once: report its returned path after
transfer succeeds. Close the thread-owned tab with `t3_preview_close` when appropriate.

## HTML surfaces

`html_preview` and `html_render` render agent HTML in a thread. Use the live
schema for their inputs and outputs. They require a T3 thread.

On nightly 20261108.4367 and newer, both tools accept an absolute `.html`
file path in `path` as an alternative to inline `html`. Pass exactly one.
Use inline `html` in supervised threads. When the active permissions allow
local file input, pass the path to `html_preview`, inspect the result, and
pass the same path to `html_render`. Keep the file unchanged between
preview and render. Older servers may also require inline HTML.
See [the upstream change](https://github.com/pingdotgg/t3code/pull/32833).

## Attachments: upload, then send

Prepare with `t3_attachment_prepare_upload` using exact name, MIME type, and
byte size. It returns `attachmentId`, expiry, and `relativeUrl`. POST exact
bytes to that returned relative URL on the attached MCP server's HTTP origin.
Use the exposed supported transport, not an invented private endpoint.
Then send metadata with the returned ID through `t3_thread_send_attachments`,
or pending uploads through an authorized `t3_thread_launch`.

Preparation/upload alone does not send a message. Send acceptance proves
neither provider consumption nor completed work. The adapter decides provider
attachment support. Use current MIME rules and provider constraints without
inventing a general file-size limit. A recipient cannot have broader permission
modes than the caller.

Attachment send has no retry key and every call creates a message. After an
uncertain send, inspect message/run state before retrying. Failed delivery can
retain claimed files when dispatch is uncertain. `t3_attachment_discard`
removes pending uploads only, never already-delivered attachments.

## Device surfaces

Use `device_list` before `device_open` when the device ID is unknown. Opening
can boot a simulator/emulator and returns a pinned `agent-device` CLI invocation
for subsequent control. Use that returned invocation rather than guessed
flags. `device_screenshot` captures an open device. `device_close` removes it
from the panel, and `shutdown: true` also powers it off. Keep boot, control,
and shutdown within the requested scope.
