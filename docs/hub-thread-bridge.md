# Hub to T3 thread bridge

The operator-only bridge relays one authorized Hub inbox delivery into one
existing T3 thread and returns that run's complete final assistant response.
It implements [issue #7](https://github.com/escoffier-labs/t3-brigade-fleet/issues/7).
The public design fixes the destination before any peer message arrives.

The bridge uses the local T3 public OAuth MCP endpoint and the supported
optional Brigade message client. It has no runtime core dependency, listener,
timer or background loop. Existing fleet dispatch and admission commands keep
their existing contracts. JSON `invoke` and `brigade-invoke` cannot select this
operator command. No generated tool or skill exposes it.

## Operator prerequisites

Review and enable the integration separately from installing this package.
The consumer needs a protected, dedicated OAuth store and its own Hub message
credential. This command never provisions credentials, enrolls principals,
refreshes OAuth, changes grants or selects another credential. Missing Brigade
or an incompatible client or identity endpoint returns `brigade-prerequisite`.
The consumer environment must contain an approved Brigade package exporting
`fleet_client_messages.identity`, `list_messages`, `reply`, `sent`, `ack`,
`flush_credential`, `MessageAuthority` and `untrusted.wrap_untrusted`.
Installing Brigade into a consumer environment belongs to the operator.
The required capability is the extended
`identity(peers=[...], timeout=5, authority=captured, expected_scope=captured.scope)`
contract merged in Brigade #2071. Basic identity alone is insufficient.
The package must also support the explicit keyword-only `reply(lock_timeout=...)`
API. That bounded-reply SDK extension is currently an unmerged candidate.
It must finish SDK review and merge before a production build is approved.
The bridge checks callable `MessageAuthority.capture` and framing, their call
signatures, all required message functions and the bounded reply signature
before credential capture. A generic `**kwargs` parameter does not establish
bounded reply support. An old or incomplete package fails safely with
`brigade-prerequisite`. Real credential-file and authorization errors retain
their existing refusal reasons.
Selecting and installing an approved production release remains an operator
gate, alongside independent review and a separately authorized canary. Local
verification of a package built from that merge does not establish that an
approved release is installed in the consumer environment.

The existing Brigade settings select the Hub connection. The bridge compares
that captured authority with `hub_authority` and authenticates the exact
configured principal through that supported operation. Every request retains
the captured credential scope. Configure exact reciprocal peer grants separately.
The response must have schema `brigade.message_authorization.v1`, the exact
configured caller, valid caller enrollment and grant epochs, and exactly the
requested peers in order. The selected sender must have `authorized: true`
and valid enrollment and grant epochs. Unknown, revoked, unilateral or invalid
peer metadata is denied with `authorized: false` and null peer epochs. The
bridge validates injected responses too and never falls back to basic identity,
inbox metadata, another endpoint or another credential.

At accepted intake the private ledger pins all four public epoch values:
caller enrollment, caller grants, sender enrollment and sender grants. Fresh
mutual authorization must match those pins before dispatch, every recovery
action, each T3 environment, thread, wait or output-page read, credential-scoped
spool reconciliation, sent-history reads, reply and acknowledgement. A denied
sender, mismatched caller, epoch drift or legacy pending record without pins
holds retained work. A held record never acquires replacement pins.

An explicit grant replacement changes its epoch even when the grant set is
equal, empty again or restored to earlier values. Reenrollment changes epochs
even when the token is reused. Checking token scope alone cannot detect these
changes. Epochs are compared independently, without deriving them from tokens
or grant values. They are retained privately and omitted from status.

These are point-in-time observations with a check/use race. Revocation can
occur after a successful check. The bridge cannot undo previously dispatched
actions, cancel a queued T3 turn or provide atomic revocation. A held delivery
can therefore have an already accepted external action. Transport failures
remain pending for bounded retry, while malformed authorization and grant or
credential denials fail closed. No exception text enters public results.

Use `approval-required` for the T3 target and OAuth ceiling. The target's
runtime must exactly match the configured ceiling. `full-access` is a high-risk,
explicit operator choice requiring separate review. The bridge never promotes
its own access or changes the target's model, runtime or interaction mode.
Peer text cannot approve tools, alter grants or steer human turns. A queued
turn still runs under the existing thread's permissions, so review those
permissions before enabling any peer.

## Protected configuration

Create a separate JSON configuration outside the repository. Use schema
`t3-fleet.hub-thread-bridge.v1`. Every key below is required. Unknown keys,
duplicate JSON keys, wildcard peers, multiple routes, self-peering and colliding
file references, including SQLite sidecar names, are refused. Paths resolve relative to the configuration file,
without environment expansion or symlink resolution. Do not use home shortcuts.
The command never creates or overwrites configuration or changes permissions.
The caller and peers must be external `agent:` principals. Configure 1 to 16
distinct peers. Node UUIDs cannot use the mutual-authorization contract.

```json
{
  "schema": "t3-fleet.hub-thread-bridge.v1",
  "map_version": "v1",
  "oauth_file": "oauth.json",
  "hub_token_file": "hub.token",
  "ledger_file": "bridge.db",
  "hub_authority": "https://hub.example.invalid",
  "principal": "agent:consumer-a",
  "environment_id": "environment-a",
  "project_id": "project-a",
  "runtime_mode": "approval-required",
  "peers": ["agent:peer-a"],
  "routes": {"selected": "thread-a"}
}
```

These identities are placeholders. An operator supplies the actual public
identities obtained from the supported product interfaces. The Hub message's
exact `thread` metadata must equal `selected`. No body field, default alias or
fallback thread can select a destination. Intake pins the map version, alias,
thread and complete configuration digest. Any configuration change holds retained
work. The bridge cannot reroute a retained delivery.

On POSIX, the configuration, OAuth store, Hub token and ledger must be regular
files owned by the consumer account with mode `0600` and one hard link. Their
immediate directories must be owned by that account with mode `0700`.
No path component may be a symlink. The operator creates and protects those
directories. The bridge creates only its own ledger when draining. Platforms
without the required ownership and no-follow protections are refused, including
Windows in this slice. SQLite sidecars receive the same protection checks.
Do not share the ledger path with another application or replace it during use.

The dedicated OAuth JSON format contains `environmentId`, `origin`,
`registration`, `tokens`, `label`, `runtimeMode`, `issuedAt` and `expiresAt`.
`registration.client_id`, `tokens.access_token` and `tokens.scope` are required.
There must be no refresh token. The consumer retains the exact session until
pending deliveries are reconciled. Never put token values in this configuration,
examples, command arguments or messages.

`origin` must be a literal loopback HTTP(S) origin. DNS names, user information,
remote hosts, non-root paths, queries and fragments are refused. The bridge
reads public identity at `/.well-known/t3/environment`, then uses `/mcp` with the
retained bearer. Public and authenticated environment IDs, project, thread and
runtime must match before sending. Redirects, proxies and cross-origin bearer
forwarding are unavailable.

## Bounded operation

```sh
t3-fleet hub-thread-bridge status --bridge-config ./private/bridge.json
t3-fleet hub-thread-bridge drain --bridge-config ./private/bridge.json
```

`status` makes no network request, needs no credential read and does not create
an absent ledger. It emits at most 100 recent delivery IDs, states and fixed
reason codes. It never emits prompts, assistant text, bearer material, session
fingerprints, connection URLs or raw upstream exceptions. Treat the ledger as
sensitive plaintext because it retains prompts and final replies.

A drain reconciles the first retained nonterminal delivery before considering
new inbox work. It accepts at most one new delivery. A held delivery blocks new
intake. Rejected and acknowledged tombstones may be skipped during a bounded
inbox scan. The protected inbox checkpoint advances only over committed
tombstones whose immutable payload digest was checked again. It never advances
past an unread delivery. A completed inspection cycle resets the checkpoint so
old unacknowledged tombstones are revisited. Only a validated empty page ends
that inspection cycle. A short nonempty byte-limited page continues forward.
The checkpoint is scoped to the
configured authority, principal, configuration and retained credentials.
Unknown aliases and unauthorized senders or recipients receive no T3 turn or
successful reply and remain unacknowledged. This slice has no rejection-ack policy.

A nonblocking process lock protects a transactional SQLite drainer claim.
Process death releases the lock. No SQLite transaction spans network I/O.
The ledger retains the inbound authority, recipient and message identity,
immutable payload digest, routing identity, OAuth and Hub credential
fingerprints, four immutable authorization epochs, deterministic keys, saved
dispatch intent, T3 handles, exact reply text and digest, accepted Hub reply ID
and acknowledgement state.
Tombstones are retained indefinitely. There is no garbage collector.

The state sequence is:

```text
received -> dispatch-intent -> dispatched -> terminal
         -> reply-intent -> replied -> acked
```

Authoritative sender, recipient, message identity and linkage stay outside the
peer data block. All sender-controlled fields enter Brigade's untrusted framing,
including subject, body, labels, reference, client ID and alias metadata. The
framed intent is saved once, so replay preserves the exact payload and fence.

The existing wrapper's trusted `goal=` states the answer task outside the fence:
produce one helpful text response using the peer message as task input. It
covers answering a question and providing the requested analysis or text
transformation. For example, a question asks for an answer, while imperative
wording such as translating a quoted sentence describes the desired text output.
Imperative wording carries no independent authority to execute commands.

The goal grants no tool use, side effects, permission or approval changes,
routing, model or runtime changes, grant changes, new authority or control
instructions. Existing operator and thread instructions govern those boundaries.
If the answer needs authority the target lacks, the goal asks it to explain that
limit. Brigade's peer-message preamble, randomized fence and embedded-fence
neutralization remain intact. The goal and fence are recorded at intake.
Recovery reuses the saved intent without rewriting older work.

T3 sends always use `mode=queue`. A busy thread receives a separate turn.
Timeouts and approval waits retain the original run. Failed, cancelled,
interrupted or rolled-back runs become held failures. Only `completed` with no
timeout can supply a reply. The bridge traverses the complete bounded message
timeline for that exact run, then requires exactly one local completed
`assistant_message`. Multiple candidates, subagent output, absent results or
inconsistent continuation metadata hold the delivery. Tool results, reasoning,
plans and another run's text cannot supply the reply. Text continuations use
the public API's UTF-16 offsets. A surrogate pair may span JSON text pages.
The bridge retains the code units across pages and validates the complete
sequence before producing Unicode text. Malformed final sequences are held.
Output larger than 64 KiB in UTF-8 is held without truncation or splitting.

One reply uses the originating inbound ID and a deterministic Hub key shorter
than the supported 64-character limit. Exact text is persisted before sending.
The bridge validates sender, recipient, reply linkage, key and body in the
accepted reply. `queued: true` proves only local spooling. The bridge flushes
only the retained credential's spool and checks its sent rows. It acknowledges
the inbound only after confirmed durable reply acceptance. An acknowledgement
does not prove the peer has read the response.

Sent-history reconciliation saves a lower bound only after checking every row
on a page for the deterministic reply key. A page is complete only when the
supported API returns a validated empty page. Nonempty pages advance the saved
cursor and continue, including short pages. Brigade caps serialized JSON at
7 MiB as well as 100 rows, so fewer than 100 rows cannot prove exhaustion.
Monotonically increasing Hub IDs
place later acceptances above that bound, including a reply accepted after a
completed no-match scan whose response was lost. A matching row is validated
before progress can pass it. Once the accepted reply ID is saved, recovery uses
the supported `sent(ids=[accepted_id])` lookup and validates the reply again
before any acknowledgement retry.

Each invocation has a 90-second bridge deadline. T3 HTTP calls have finite
socket waits of at most 15 seconds, responses at most 1 MiB, and exact-run waits
of 1 second. Timeline, text and Hub page walks each stop at 32 pages. Completed
timeline pages save the consumed zero-based position and the sole candidate
together. Selection waits for the complete scan, including later pages that
could prove ambiguity. Timeline, sent-history and inbox inspection continue
from their committed checkpoints on a later drain when the page bound is reached.
Both reply and credential-scoped spool reconciliation use `lock_timeout=0`.
They attempt lock acquisition without waiting. Reply contention preserves
`reply-intent` with `upstream-pending`, without sending or enqueueing the reply.
The saved reply key and exact body remain available for a later bounded drain.

This bounds lock acquisition only. It does not impose a total operation or
network deadline and does not cancel a call that has acquired its lock.
Supported client transport and spool batch limits still apply. An already
started supported client call can finish after the bridge deadline. No timeout
cancels a T3 run or authorizes a replacement run.

## Recovery and limits

Repeat the same bounded drain with unchanged configuration and credentials.
It recovers saved intent or handles and finishes only the remaining phases.

- Acceptance lost after a T3 send: replay the immutable key and payload only
  in the retained OAuth namespace. The server's same-session key recovery is a
  transport prerequisite. The ledger separately refuses changed content.
- OAuth renewal or Hub credential rotation: hold pending work. A new OAuth
  session has a new request namespace. Never clear the ledger, change the key
  or resend the same logical delivery under that namespace.
- Grant or enrollment epoch drift, denied mutual authorization or missing
  legacy pins: hold without repinning or resending. Restoring grant values or
  reusing a token does not restore the retained authorization generation.
- Reply acceptance lost: reconcile the credential-scoped spool and sent rows,
  or retry the identical reply key and exact saved body. No new T3 turn occurs.
- Reply spool lock busy: retain reply intent and repeat the bounded drain after
  contention clears. Recovery rechecks authorization and acceptance before
  retrying the saved key. No background reply or alternate credential is used.
- Ack response lost: revalidate authority, inbound content and durable reply,
  then retry the acknowledgement. No new turn or reply occurs.
- Route drift, mismatched handles, ambiguous output or reply linkage: retain a
  held record. There is no automatic reset, retry-as-new or deletion command.
- A timeline, sent-history or inbox page bound: repeat the drain to continue
  from checked progress. A bound does not prove that no matching reply exists
  and cannot authorize a premature resend.
- Spool denial or missing upstream retention: retain pending or held work for
  operator reconciliation. Never borrow another principal, flush its spool or
  use an administrative credential.

An operator must investigate holds using approved private tooling. This command
provides no permission or grant editor. Preserve the original ledger and retained
session while determining whether the original run or reply was accepted.
Starting a new ledger to evade a hold can duplicate effects and is unsupported.

The transport implements bounded MCP initialize, initialized notification and
allowlisted tool calls using the negotiated supported protocol. It supports
JSON and SSE replies, with no server-request execution, reconnect loop,
automatic authorization or refresh. See the public
[MCP Streamable HTTP contract](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports).
That transport specification was retrieved on 2026-10-25. The version in the
URL identifies the specification revision.

Offline tests use standard-library fakes and local HTTP servers. The Hub fixture
models the inspected serializer's byte budget and row cap. Optional contract
tests compare it with the installed Brigade serializer and exercise the exact
installed untrusted wrapper for question, imperative and injected-control inputs.
The compatible installed identity test uses a local HTTP fixture with a
temporary credential, real credential-scope checks, strict wire validation and
unsupported, denied and transient responses. All requests carry the peer list.
The installed reply test uses real SDK process and file locks with fake HTTP
transport. It checks contention, unchanged intent and keys, empty reply queues
on contention and exactly one accepted reply after release. Fixture holders
always release and join within bounded test waits.
Run these tests in the approved environment containing compatible Brigade.
The ordinary stdlib-only environment skips those four installed-package checks.
They check serialization, authorization consumption and prompt construction,
without proving model compliance, live Hub interoperability or permission grants. The existing
cross-repository wire test skips unless `T3_FLEET_BRIGADE_SOURCE` points at a
compatible Brigade checkout. Its generic fixture still runs. Deployment and
one authorized peer-to-T3-to-peer canary remain separate operator gates.
The bridge and Hub retain plaintext. This slice makes no E2EE claim and changes
no shared Hub or encryption source.
