# Escalating to OpenSpec

Reached only when the Step 4 design gate fires. Ordinary features never touch OpenSpec.

A framework change is the case where a written design earns its ceremony.

The `spec-driven` schema's artifacts are dependency-gated, not linear:

```
proposal → { design, specs } → tasks
```

`design` and `specs` each unblock once `proposal` exists; `tasks` needs both.

**Before approval — draft design and specs, but no tasks or code:**

```bash
openspec new change "<kebab-name>"
openspec status --change "<kebab-name>"
openspec instructions proposal --change "<kebab-name>"
```

1. Write `proposal.md` — what the change is, why a within-framework solution does not work, and the
   alternatives rejected.
2. Write `design.md` and `specs/**/*.md` — both unblock as soon as the proposal exists, and neither
   depends on the other. `design.md` covers the blast radius, which layers change, what existing
   behaviour is affected; `specs` are the delta requirements for each affected capability.
3. **Stop. Write no `tasks.md` and no implementation code.** Present the proposal, design, and specs
   together.

The `openspec-continue-change` skill creates exactly one artifact per invocation and stops, picking
whichever artifact is first `ready`. For this schema that does not reliably mean `design` — `specs`
and `design` both unblock as soon as `proposal` exists, so a given invocation may produce either one
first. Call it repeatedly after the proposal until **both** `design.md` and `specs/**/*.md` exist,
then hold for approval regardless of which one it produced first.

**After approval — hand implementation over:**

4. Create `tasks` (`openspec-continue-change` once more — `design` and `specs` are already done, so
   `tasks` is the only artifact left ready).
5. Implementation runs through `openspec-apply-change`, which works from `tasks.md`. It is a
   **driver**, not a helper: once it takes over, it owns the implementation loop. Do not also run
   Steps 5–7 of the `implement-pathling` skill against the same work — that is two drivers on one
   change.
6. When implementation is complete, resume `implement-pathling` at **Step 8** (exclusion sweep), and
   carry on through commit, review, and PR as normal.
7. Archive the change once the work lands (`openspec-archive-change`).

Under `--unattended`, abort after step 3. The gate never self-approves.

> Worked example: issue #2389 (`$index`) threads a new variable through expression-parameter
> evaluation. It touches evaluation context, so it gates. Issues like #2380 (string functions) and
> #2385 (existence functions) are registry-slot work and do not.
