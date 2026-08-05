---
name: implement-pathling
description: >
  End-to-end implementation of a FHIRPath feature in Pathling, from a GitHub issue to a pull request
  ready for final review. Invoke as /implement-pathling <issue-number>.
disable-model-invocation: true
---

# Implement a Pathling FHIRPath feature

Drives a FHIRPath issue from `gh issue view` to an open PR. It produces working code, not plans.

Repository: `aehrc/pathling`, default branch `main`.

```
/implement-pathling <issue-number> [--worktree] [--unattended]
```

- `--worktree` — work in an isolated worktree at `.worktrees/<issue-number>`. Use when several
  issues are in flight at once.
- `--unattended` — no user is available. Every gate becomes an abort, except the test-matrix review
  (see table below), which proceeds and reports instead. **Required** when this skill runs inside a
  dispatched subagent, which cannot ask anything. Thread it through to every skill this one delegates
  to (`fhirpath-spec` and transitively `cache-github-repo`; and `fhirpath-test-designer`, whose
  matrix-review gate this governs) — they cannot tell on their own that no one is available to
  answer a question.

This skill **stops at the PR**. It does not merge, and does not wait for CI.

---

## Step 0 — Resolve mode

Print the resolved mode before doing anything, so a misfire is visible now rather than at Step 10.

Detect whether this session is already in a linked worktree — a dispatcher may have placed it in
one, in which case do not create another:

```bash
[ "$(git rev-parse --git-dir)" != "$(git rev-parse --git-common-dir)" ] && echo "already in worktree"
```

Gate behaviour by mode:

| Gate | Interactive | `--unattended` |
|---|---|---|
| Spec ambiguity (Step 3) | Present findings, wait | **Abort** with the ambiguity report |
| Design (Step 4) | Draft an OpenSpec change, wait | **Abort** with the drafted change in place |
| Test matrix review (Step 6) | Present matrix, wait for review | Proceed with the matrix as designed; list any case flagged uncertain in the return value |
| Review triage (Step 11) | Ask about findings needing judgment | Leave unapplied, list them in the return value |

"Abort" means: stop, leave the branch and commits in place, and return a report naming the gate and
the decision needed. Do not guess past a gate.

Print, for example:
`Mode: worktree=.worktrees/2385, unattended=false — will stop after the PR is opened.`

## Step 1 — Read the issue

```bash
gh issue view <N> --repo aehrc/pathling --comments
```

Pathling's FHIRPath issues are terse — typically a spec link and a list of functions. Expect to do
the specification and design work yourself; the issue is scope, not a design.

Note whether the issue covers **multiple functions**. If so, decide whether they land as one commit
or several on the same branch. Split when functions differ in complexity or touch different areas;
keep together when they are variations on one mechanism. All commits go into a single PR.

## Step 2 — Create the branch

Pathling's convention (CONTRIBUTING.md) is `issue/<number>` — no slug.

```bash
git fetch origin
git switch -c issue/<N> origin/main
```

Branch off `origin/main` rather than a local `main`: it guarantees a fresh base and works inside a
linked worktree.

**If the branch already exists, stop and report it.** The repo carries a dozen stale local
branches; silently reusing one builds on the wrong base. Report what exists and let the user decide
whether to resume, rename, or delete it.

With `--worktree`, create the worktree and branch together:

```bash
git worktree add .worktrees/<N> -b issue/<N> origin/main
cd .worktrees/<N>
```

## Step 3 — Research the specification

Use the `fhirpath-spec` skill. Gather the signature, input and output types, collection behaviour,
empty-propagation rules, error conditions, spec examples, and any FHIR-specific binding.

Cross-check behaviour against what the corpus expects: the cases under
`fhirpath/src/test/resources/fhirpath-js/cases/` encode fhirpath.js behaviour, and
`fhirpath-ptl/cases/` encode Pathling's own.

### Spec-ambiguity gate

Stop and present when:

- the spec is silent on a case the implementation must handle;
- the corpus contradicts the spec text;
- reference implementations diverge from each other.

Present the spec quote, the conflicting behaviours, and a recommendation. Do not resolve an
ambiguity silently — a silent resolution becomes an undocumented divergence.

## Step 4 — Locate the pattern, classify the change

Find the closest existing function of the same shape and mirror its structure. Most FHIRPath
functions in Pathling are one method on a provider class:

```
fhirpath/src/main/java/au/csiro/pathling/fhirpath/function/provider/
```

A method annotated `@FhirPathFunction` is registered automatically through
`MethodDefinedFunction.mapOf(...)` in `StaticFunctionRegistry`. A **new** provider class needs one
additional line there. Substantial logic belongs in a `*Logic` helper class, as `ConversionFunctions`
delegates to `ConversionLogic`.

### Design gate — decided by paths, not by judgment

**No gate.** Proceed autonomously when the change is confined to:

- a new `@FhirPathFunction` method on an existing provider class
- a new provider class plus its one registry line
- a `*Logic` helper for that function
- a `*DslTest` class or method
- the YAML exclusion configs

**Gate.** Stop when the change touches any of:

- `fhirpath/.../parser/` — grammar or visitor
- `fhirpath/.../operator/`
- `fhirpath/.../evaluation/` — evaluation context or resolvers
- `Collection` subclasses, or `fhirpath/.../column/` representations
- the type system — `TypeSpecifier`, `FhirPathType`, type resolution
- `fhirpath/.../definition/`
- the structure of `StaticFunctionRegistry` or `MethodDefinedFunction` themselves, as opposed to
  adding an entry

Everything a later feature inherits belongs behind the gate.

### What the gate does: escalate to OpenSpec

A framework change is the case where a written design earns its ceremony. Ordinary features never
touch OpenSpec.

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
   Steps 5–7 of this skill against the same work — that is two drivers on one change.
6. When implementation is complete, resume this skill at **Step 8** (exclusion sweep), and carry on
   through commit, PR, and review as normal.
7. Archive the change once the work lands (`openspec-archive-change`).

Under `--unattended`, abort after step 3. The gate never self-approves.

> Worked example: issue #2389 (`$index`) threads a new variable through expression-parameter
> evaluation. It touches evaluation context, so it gates. Issues like #2380 (string functions) and
> #2385 (existence functions) are registry-slot work and do not.

## Step 5 — Implement

Write the implementation and its tests together.

Javadoc on a new function follows the existing providers: a description, `@param`, `@return`, and an
`@see` link to the governing spec section. Add `@SqlOnFhirConformance(Profile.…)` where the function
maps to a SQL-on-FHIR profile feature — check sibling functions rather than guessing.

## Step 6 — Design and write the tests

Use the `fhirpath-test-designer` skill, passing `--unattended` through if this run has it. It owns
the dimension matrix and the DSL surface, including the constraints that bite: descriptions are
mandatory on every assertion, and there is **one subject per `@FhirPathTest` method**.

Test classes live in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/dsl/`, named by capability
(`StringFunctionsDslTest`), never by issue number. Prefer adding a method to the existing class for
that capability over creating a new one.

## Step 7 — Run the tests

Format first — the build runs `spotless:check` before compiling, so an unformatted file fails as a
build error rather than a style warning:

```bash
mvn spotless:apply -pl fhirpath
```

Then widen the net in stages:

```bash
mvn test -pl fhirpath -Dtest='StringFunctionsDslTest#testUpper'   # the new tests
mvn test -pl fhirpath -Dtest=StringFunctionsDslTest               # the capability
mvn test -pl fhirpath -Dtest=YamlReferenceImplTest                # fhirpath.js corpus
mvn test -pl fhirpath -Dtest=YamlFhirPathTest                     # Pathling corpus
mvn test -pl fhirpath                                             # the module
```

Two things to recognise in the output:

- **`Excluded test passed when expected outcome was error`** — a feature you implemented made an
  excluded case pass. Expected, and it is the signal for Step 8.
- **Errors like `cannot access java.util.List`** — the upstream modules are stale, not a real
  compile error. Rebuild them: `mvn -o test-compile -pl fhirpath -am`.

If a test failure is ambiguous, check the spec before assuming the test is wrong. Existing tests are
correct unless the spec clearly contradicts them.

## Step 8 — Sweep the exclusion baseline

Use the `pathling-yaml-exclusions` skill. Search by function name **and** by issue number, then
remove, narrow, or reclassify what is in scope.

Most Pathling FHIRPath issues own no exclusion at all. **An empty search result is a finding to
report, not a step to skip** — say that nothing matched, rather than implying the baseline was
already clean.

## Step 9 — Commit

Follow CONTRIBUTING.md: a type prefix, a short summary of the objective, and a body explaining why
rather than restating the diff. The `Co-Authored-By` trailer must name the model actually running
this skill (e.g. `Claude Sonnet 5`, `Claude Opus 5`) — never hardcode a specific tier.

```bash
git add <specific files>
git commit -m "$(cat <<'EOF'
feat: Support <capability> (#<N>)

<why this was needed and what it enables, a few sentences>

Co-Authored-By: <this model's own name> <noreply@anthropic.com>
EOF
)"
```

## Step 10 — Push and open the PR

```bash
git push -u origin issue/<N>
gh pr create --repo aehrc/pathling --title "<short title>" --body "$(cat <<'EOF'
## Summary

- <what was implemented>
- <key design decisions>

Closes #<N>

## Verification

- <test classes added or extended, and the result>
- <exclusion changes, or "no exclusions matched this feature">

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Keep the body short. Report what was verified and how, not a narrative of the work.

## Step 11 — Review and triage

Dispatch a reviewer in a fresh context using the `pathling-fhirpath-review` rubric, giving it the
range `$(git merge-base origin/main HEAD)..HEAD`. A reviewer that has not seen the reasoning behind
the change grades the result on its own terms.

Triage what comes back:

- **Apply** — Critical and Important findings that are clear-cut: wrong results, unhandled input
  shapes, missing tests for behaviour the change claims to support, missing annotations or
  registration. Default to fixing rather than debating.
- **Escalate** — anything that would change public API, alter spec semantics, expand scope beyond
  the issue, or extend the framework. These re-enter the Step 4 gate. Under `--unattended`, leave
  them unapplied and list them.
- **Decline** — Minor findings that conflict with established patterns elsewhere, or that do not
  survive a second read of the cited code. Note them briefly and move on. The reviewer is not
  infallible; push back with reasoning.

After applying fixes, re-run Step 7, then commit and push:

```bash
git commit -m "$(cat <<'EOF'
fix: Address review findings for #<N>

<what changed and why>

Co-Authored-By: <this model's own name> <noreply@anthropic.com>
EOF
)"
```

## Step 12 — Report and stop

Return:

- the PR number and URL
- the tests added and their result
- exclusion changes, or an explicit note that none matched
- any gate that fired and what it needs, including test-matrix cases the test designer flagged as
  uncertain when run `--unattended`
- review findings left unapplied, with the reason

Then stop. Merging is the user's decision.

---

## Reminders

- **Write code, not plans.** If the issue already describes the work, implement it.
- **Bound the exploration.** Read the closest existing implementation and its tests, then start.
- **The spec decides.** Not intuition, and not the current implementation's behaviour.
- **Existing tests are correct** unless the spec clearly contradicts them.
- **Report evidence.** Name the command and its result, rather than asserting that things pass.
