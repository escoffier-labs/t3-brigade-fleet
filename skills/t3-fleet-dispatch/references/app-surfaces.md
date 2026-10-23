# Attached app operations

## Projects, configuration, and environment

Start with `t3_project_list` and `t3_project_read` to identify the registered
project and its workspace/scripts. These are environment-level reads. Most
thread, queue, question, and scheduler operations accept explicit scope. See
[supervision.md](supervision.md) for cross-project reads and `projectId`. Read
the live schema for pagination. Project/preview pages can shift with the
current snapshot.

`t3_thread_configuration` reads a scoped thread's selection/modes.
`t3_thread_configure` changes only the calling thread's provider/model/options,
using the live catalog. It does not change permission modes or other threads.
`t3_project_update` changes project settings and preserves omitted fields.
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
