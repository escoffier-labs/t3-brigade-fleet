# Tool names across T3 builds

Discover the attached tool catalog before calling a tool. T3's
[consolidation change](https://github.com/pingdotgg/t3code/pull/25423), included
in v0.1.3, replaces the earlier tools below with action tools. Older running
servers can still expose the earlier names. Select the form actually advertised
by that server. A missing earlier name alone does not mean the feature is absent.

| Earlier tool | Consolidated call |
| --- | --- |
| `t3_queue_list`, `t3_queue_read` | `t3_queue` with `action: "list"` or `"read"` |
| `t3_queue_edit`, `t3_queue_cancel`, `t3_queue_reorder`, `t3_queue_promote_to_steer` | `t3_queue` with `action: "edit"`, `"cancel"`, `"reorder"`, or `"promote_to_steer"` |
| `t3_pending_request_list`, `t3_pending_request_read`, `t3_pending_request_respond` | `t3_pending_request` with `action: "list"`, `"read"`, or `"respond"` |
| `t3_thread_fork`, `t3_thread_merge_back`, `t3_thread_transfers` | `t3_thread_transfer` with `action: "fork"`, `"merge_back"`, or `"status"` |
| `t3_thread_configuration` | `t3_thread_configure` without `modelSelection` |
| `update_scheduled_task` | `schedule_task` with `scheduledTaskId` and only the fields to change |
| `preview_click`, `preview_type`, `preview_hover`, `preview_select`, `preview_drag`, `preview_upload`, `preview_press`, `preview_scroll` | `preview_act` with the corresponding `action`: `"click"`, `"type"`, `"hover"`, `"select"`, `"drag"`, `"upload"`, `"press"`, or `"scroll"` |
| PR link/unlink actions on `t3_thread_update` | `link_pull_request` or `unlink_pull_request` |

The other references use the earlier names for compatibility. Apply this table
when the server advertises the consolidated form, and inspect its argument schema
before constructing its arguments.

Keep `threadId` for the thread scope. For context merge-back, the source is
`threadId`, replacing the earlier `sourceThreadId`. Keep `targetThreadId` for the
related destination in the same project. Context status takes a thread ID, not
a transfer ID. Queue operations keep `queuedRunId`, `beforeRunId`, and
`targetRunId` as appropriate. Question reads/responses keep `requestId` and
`answers`. They still cannot approve a permission request.

Supply `modelSelection` to set the provider, model, or options. Omit it to
read the selection and modes. Use an explicit `threadId` when the live
schema supports it. An omitted thread ID selects the caller. Permission modes
stay unchanged.

For schedule updates, omit fields to preserve them. `enabled: false` pauses the
named task. Creation still needs `prompt` and a structured `schedule` object.
Never omit `scheduledTaskId` when the intent is to update an existing task.

For preview actions, retain the observed `tabId` and snapshot locator. Press uses
`key` and optional `modifiers` on the focused element. Upload uses absolute
`paths`, with an empty array cancelling the picker. Prefer snapshot locators to
coordinates and check the live schema for each action's required fields.

## Access by action

| Tool | Read operations | Operations that need write authority |
| --- | --- | --- |
| `t3_thread_configure` | Omit `modelSelection` | Supply `modelSelection` |
| `t3_thread_transfer` | `status` | `fork`, `merge_back` |
| `t3_queue` | `list`, `read` | `edit`, `cancel`, `reorder`, `promote_to_steer` |
| `t3_pending_request` | `list`, `read` | `respond` |
| `preview_act` | None. Read with the available snapshot or status tool. | All actions require an authorized live thread and tab. |

The matrix describes individual operations. A server can withhold a mixed-action
tool from a read-only client. Check the advertised catalog and the caller's
existing access ceiling before using it.
