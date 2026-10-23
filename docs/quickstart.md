# Quick start

Validate an offline controller first. A real dispatch also needs a
reachable T3 Code target and configured Brigade model admission.

## Roles

`controller-a` runs the local client. `target-a` and `target-b` are reviewed,
allowlisted targets. Replace these generic roles only in operator config.

## 1. Install

```bash
git clone https://github.com/escoffier-labs/t3-brigade-fleet
cd t3-brigade-fleet
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
t3-fleet --help
```

Python 3.11 or newer is required. No third-party Python runtime dependencies
are installed. Real dispatch requires a separately configured Brigade CLI.

## 2. Validate the offline config

```bash
cp examples/local.example.toml .fleet.toml
chmod 600 .fleet.toml
export T3_FLEET_CONFIG="$PWD/.fleet.toml"
t3-fleet doctor
t3-fleet capacity
```

The config schema is `t3-fleet.config.v1`. The local example selects one local
host and a deliberately unavailable placeholder agent. It performs no network
discovery. Doctor returns `state: "blocked_offline"` with a blocked host entry.
Capacity reports the same state and unknown active count. These diagnostics
exit `0` when the check completes. Read the state before deciding a host is
ready. Invalid configuration exits `2`. No model dispatch occurs.

## 3. Configure real targets and admission

Complete [deployment prerequisites](deployment.md#before-a-real-dispatch) on
the controller and each target. Copy `examples/fleet.example.toml` into a
protected operator config and replace its executable paths, expected hostnames,
and exact project/workspace mappings. In target-authoritative mode, keep
`global_active_limit` equal to the sum of enabled host limits.

Configure the external Brigade CLI, node enrollment, and a logical seat from
the operator's admitted roster. Policy enrollment off still requires model
admission and an explicit seat. With policy enrollment required, configure each
canonical repository identity first and prefer Hub routing with `--host auto`
and no seat override. Neither doctor nor a T3 project default grants admission.

Run scoped checks before dispatch:

```bash
t3-fleet doctor --host target-a --repository example-repo
t3-fleet capacity --host target-a
```

Check the selected host's eligibility and both host and repository capacity.
An aggregate block may belong to another host.

## 4. Submit authorized work

This example uses policy enrollment off and `approval-required`. Replace the
placeholder seat with a currently admitted one and resolve the revision to a
real immutable 40-character lowercase commit before running it.

```bash
umask 077
cat > .prompt.txt <<'PROMPT'
Summarize the public commands in README.md.
PROMPT

t3-fleet submit \
  --host target-a \
  --repository example-repo \
  --seat example-seat \
  --source-revision 0123456789abcdef0123456789abcdef01234567 \
  --title "readme summary" \
  --prompt-file .prompt.txt \
  --runtime-mode approval-required
```

Obtain operator approval for this work and scope first. Save the exact returned
qualified `request_id`. In target-authoritative mode, omit a caller-chosen ID.
A parent delegation instead uses `--delegation-id` with a title and protected
prompt. Its receipt supplies placement, seat, repository, and revision.

## 5. Inspect attention and completion

```bash
t3-fleet wait --request-id <qualified-request-id> --timeout 600 --include-output
t3-fleet status --request-id <qualified-request-id>
```

Wait returns on terminal state, attention, or timeout. A timeout does not stop
work. A `needs_attention` result carries `pending_request`. Use its unchanged
`id` and `respond_with` for the operator's response. Do not invent IDs or
answers. For an authorized approval:

```bash
t3-fleet approve \
  --request-id <qualified-request-id> \
  --approval-id <pending-request-id> \
  --decision accept
```

`answer` is JSON-stdin-only. Place the operator's reviewed response in a
protected JSON file with `request_id`, `input_id`, and `answers`, then use
`t3-fleet answer < "$ANSWER_JSON"`. Followup refuses an unanswered attention
request. Enrolled continuations revalidate policy and reservation first.
Claim completion only after fresh status for the same request says `succeeded`.

## 6. Interrupt, follow up, or recover

Use the operator's exact authorization for each action:

```bash
t3-fleet interrupt --request-id <qualified-request-id>
t3-fleet followup \
  --request-id <qualified-request-id> \
  --prompt-file .followup.txt \
  --runtime-mode approval-required
t3-fleet recover --request-id <qualified-request-id>
```

These are separate actions, not a sequence to run blindly. Recovery reconciles
an unknown request and never resubmits it. A lapsed terminal reservation needs
new admission before continuation. See [recovery](recovery.md) for queue and
identity boundaries and [security](security-model.md) for approval scope.
