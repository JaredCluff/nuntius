# HANDOFF — legatus-nuntius (NATS messaging substrate agent)

**Written:** 2026-06-06, just before an operator-ordered Mac Studio restart.
**Owner:** `legatus-nuntius` (this agent). Recover from this file, then DELETE it once recovered.
**Committed in:** `~/gitrepos/nuntius` (my nuntius repo). My runtime cwd is `/Users/jared.cluff/code/legatus-labs` (NOT a git repo), which is why the handoff lives here instead.
**My identity:** `NUNTIUS_INSTANCE_ID=legatus-nuntius`; config `coordination/mcp-config/legatus-nuntius.json` (nuntius MCP → `nats://127.0.0.1:4222`; subs `republic.>,claude.legatus-nuntius.in.>`; + sequential-thinking + exa).
**Domain:** the NATS messaging substrate — nuntius relay (`~/gitrepos/nuntius`, this repo), NATS infra, the NGS cloud-NATS cutover, and the wake-system roster derivation.

## Current state (as of restart)
- **Inbox fully drained.** `~/.nats-data/force-feed-state/legatus-nuntius/` was EMPTY (0 unacked). Latest inbound was `republic-routing#1868` (moderator: TASK 1 GO + leaf-cutover HOLD), already handled last cycle.
- **No WIP of mine is uncommitted or unpushed** (other than this handoff file).

## What I completed this cycle (verified)
- **TASK 1 — roster fallback** in `moderator/bin/republic-instances.sh:125`: `.mcpServers.nuntius.env.NUNTIUS_INSTANCE_ID // .mcpServers.nats.env.NATS_INSTANCE_ID // empty`. **LIVE on `main@d3d82ac`** via merged **PR #7** (legatus-ai/moderator). Branch `fix/roster-fallback-nats-block` (commit `2f02c96`) is pushed; `diff main..branch` is empty.
- **Closed duplicate PR #8** (legatus-ai/moderator) — opened 12:55 after #7 merged 00:02, nothing to merge.
- **Reported status** to `claude.moderator.in.status`.

## Parked / blocked (NOT on me)
- **NGS leaf-node cutover (Option B)** — on **HOLD per moderator directive**; must NOT execute until the operator picks **tier-vs-leaf**. Runbook (ready-to-execute spec): `legatus-labs/coordination/NGS_LEAF_CUTOVER_PLAN.md`. Residual operator decisions to capture before executing: bridged-subject set; reuse `:4222` vs dedicated leaf port; HA `leaf:1→2`; dedicated `.creds`. Helper: `legatus-labs/coordination/cutover-swap.py` (block-replacement mode would delete the nuntius MCP block — the #7 roster fallback exists so the wake roster survives that path).
- **NATS/JetStream CLI+MCP DDD/hex-via-Vulcan rewrite** (operator msgs 1215/1231) — repos `nats-cli-rust` + `nats-mcp-rust` under `legatus-labs/`. **Undispatched to me** + tied to the gated NGS decision + needs cloud creds. Do NOT start unilaterally; await moderator dispatch.

## Pre-existing domain branches to be aware of (NOT my session WIP — verify before acting)
- `legatus-labs/legatus-consul-nuntius/` on branch **`feat/ngs-creds-auth`** (clean tree) — NGS creds/auth work, relevant to the gated cutover's dedicated-`.creds` decision. Confirm ownership/state before touching.
- This repo (`~/gitrepos/nuntius/`) was on branch `docs/animus-claude-opencode-section` (commit `709acf4`, a merged-PR-#3 docs commit, looks stale-local) — not touched this session except this handoff commit.

## Recovery steps (do in order after restart)
1. Verify bus: `mcp__nuntius__js_stream_info{name:"republic-routing"}` + `mcp__nuntius__agent_discover` (expect ~12 agents, me registered). If MCP not wired this session, fall back to CLI `/Users/jared.cluff/.local/bin/nats` on `nats://127.0.0.1:4222`.
2. Drain inbox: check `~/.nats-data/force-feed-state/legatus-nuntius/*.json` (empty = clean). For each pending msg: act, then ack via `nats pub republic.routing-ack.legatus-nuntius.<msg_id> ""`. Also `nats stream get republic-routing -S "claude.legatus-nuntius.in.task"` for the latest dispatch.
3. **Next step:** check for the operator's **tier-vs-leaf decision** or a **rewrite dispatch**. If leaf cutover is greenlit → execute per `coordination/NGS_LEAF_CUTOVER_PLAN.md` (staged, careful — it reconfigures the bus; rollback = drop the uplink). Otherwise: honest standby (queue empty, one item operator-gated). Do NOT manufacture work.
4. Report to `claude.moderator.in.status` (the moderator reads this subject). gh API writes (PR close/comment/review) work headless; only `git push` to `legatus-labs/*` 403s — `legatus-ai/*` pushes fine.
5. **DELETE this file** (`git rm HANDOFF.md` in `~/gitrepos/nuntius`, or `rm`) once recovered.
