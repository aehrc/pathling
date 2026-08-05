# Escalating to OpenSpec

Reached only when the Step 4 design gate fires. Ordinary features never touch OpenSpec.

A framework change is the case where a written design earns its ceremony.

The `spec-driven` schema's artifacts are dependency-gated, not linear:

```
proposal → { design, specs } → tasks
```

`design` and `specs` each unblock once `proposal` exists; `tasks` needs both.

**Before approval — draft the design artifacts only:**

```bash
openspec new change "<kebab-name>"
openspec status --change "<kebab-name>"
openspec instructions proposal --change "<kebab-name>"
```

1. Write `proposal.md` — what the change is, why a within-framework solution does not work, and the
   alternatives rejected.
2. Write `design.md` — the blast radius, which layers change, what existing behaviour is affected.
   It unblocks as soon as the proposal exists.
3. **Stop. Write no implementation code and no `tasks.md`.** Present the proposal and design.

The `openspec-continue-change` skill creates exactly one artifact per invocation and stops, which
suits this: use it to produce the proposal, then the design, then hold.

**After approval — hand implementation over:**

4. Create `specs` and then `tasks` (`openspec-continue-change` again, once per artifact).
5. Implementation runs through `openspec-apply-change`, which works from `tasks.md`. It is a
   **driver**, not a helper: once it takes over, it owns the implementation loop. Do not also run
   Steps 5–7 of the `implement-pathling` skill against the same work — that is two drivers on one
   change.
6. When implementation is complete, resume `implement-pathling` at **Step 8** (exclusion sweep), and
   carry on through commit, PR, and review as normal.
7. Archive the change once the work lands (`openspec-archive-change`).

Under `--unattended`, abort after step 3. The gate never self-approves.

> Worked example: issue #2389 (`$index`) threads a new variable through expression-parameter
> evaluation. It touches evaluation context, so it gates. Issues like #2380 (string functions) and
> #2385 (existence functions) are registry-slot work and do not.
