---
name: cache-github-repo
description: >
  Clone and locally cache a pinned version of a GitHub repository for fast, deterministic access by
  other skills — for example a reference implementation consulted for spec cross-checking. Manages a
  tracked per-project config file mapping `org/repo` to a pinned version, and a version-keyed local
  cache shared across projects. Invoke directly to set or change a pin
  (`configure org/repo [--version ref]`), or let another skill delegate to it to make sure a pinned
  repo is available (`ensure org/repo [--unattended]`). Trigger on phrases like "cache this repo",
  "pin a reference implementation", "configure the version of X to use", or when a skill needs a
  stable local copy of an external GitHub repository.
argument-hint: <ensure|configure> <org/repo> [--version <ref>] [--unattended]
allowed-tools: Bash
---

# Cache a GitHub repo locally, pinned to a version

Keeps other skills from depending on ad hoc, machine-specific setup (a hand-made symlink, a path
that only exists on one developer's machine) to reach reference material that lives in a GitHub
repository. Owns two things:

- a tracked, per-project config file recording which pinned version of which `org/repo` this project
  uses
- a local, version-keyed cache of that repo, shared across every project, worktree, and clone on this
  machine

Scope is deliberately narrow: `github.com` repositories identified by `org/repo`, nothing else.

## Config file

`.claude/repo-cache.yaml` at the project root (created on first `configure`). One line per pinned
repo, key is the GitHub `org/repo` slug:

```yaml
HL7/fhirpath.js: 3.16.4
```

The key doubles as the clone identifier (`https://github.com/<org>/<repo>.git`) — nothing else needs
recording. **Commit this file.** It is what makes the pin reproducible across every contributor,
clone, and CI run, rather than a personal, silent choice that could differ machine to machine.

## Cache location

```
~/.cache/claude-skills/github-repo-cache/<org>/<repo>/<version>/
```

Shared across every project on this machine and keyed by version, so two projects pinning different
versions of the same repo never conflict, and a version bump doesn't disturb whatever was cached for
the old one.

## Mode: `ensure <org/repo> [--unattended]`

The path other skills delegate to. **Never asks a question when `--unattended` is passed** — a
dispatched subagent or an unattended pipeline has no one to answer it.

1. Resolve the project root (`git rev-parse --show-toplevel`, or `$PWD` if not inside a git repo) and
   read `.claude/repo-cache.yaml` there. Look up `<org/repo>`.
2. **Entry missing:**
   - `--unattended` literally passed as an argument to this invocation → fail immediately. Report:
     `"<org/repo> has no configured version — run '/cache-github-repo configure <org/repo>' first"` and
     stop. Do not guess a version, do not attempt to ask.
   - Otherwise → run the `configure` steps below for this repo, then continue. This includes when the
     current session has some general "auto mode"/"operate autonomously" disposition — that governs
     tool-permission friction, not whether a human can answer a question, and does not satisfy the
     `--unattended` condition above. Only the literal flag does.
3. **Entry present** → let `VERSION` be the recorded value. Compute
   `CACHE_DIR=~/.cache/claude-skills/github-repo-cache/<org>/<repo>/<version>`.
   - `$CACHE_DIR/.git` exists → already cached. Report `$CACHE_DIR` and stop.
   - Missing → clone it (see "Cloning a pinned ref" below), then report `$CACHE_DIR`.

## Mode: `configure <org/repo> [--version <ref>]`

The explicit, user-invoked path — and also what `ensure` falls into automatically when attended and
the entry is missing.

1. If `--version <ref>` was given, use it as `VERSION` and skip to step 3.
2. Otherwise, find the candidates:
   ```bash
   git ls-remote --tags --sort=-v:refname "https://github.com/<org>/<repo>.git" | head -5
   ```
   Present the most recent tag as "latest stable" and `main` as the alternative. You MUST NOT pick one
   and write the config yourself, even under an "auto mode"/"proceed autonomously" disposition — wait
   for the user's choice (e.g. via `AskUserQuestion`) first. If the network call fails, say so and ask
   the user to supply a ref directly instead of guessing.

   Whichever is chosen, **resolve it to a concrete, immutable reference before recording it**:
   - A tag → record the tag name; it is already immutable.
   - `main` (or any branch) → resolve it to its current commit SHA
     (`git ls-remote https://github.com/<org>/<repo>.git main`) and record **the SHA**, not the
     branch name. Recording a branch name would silently re-resolve to a different commit on every
     future run — the entire point of pinning is that a resolved answer stays the resolved answer.
3. Write `<org/repo>: <VERSION>` into `.claude/repo-cache.yaml` at the project root, creating the
   file if it doesn't exist and updating the entry in place if the key is already present.
4. Clone it immediately (see below) so the project is usable right away rather than deferring the
   fetch to the next `ensure` call.
5. Report the recorded version and the local cache path.

## Cloning a pinned ref

```bash
CACHE_DIR=~/.cache/claude-skills/github-repo-cache/<org>/<repo>/<version>
if [ ! -d "$CACHE_DIR/.git" ]; then
  TMP=$(mktemp -d)
  if ! git clone --quiet --depth 1 --branch "<version>" \
        "https://github.com/<org>/<repo>.git" "$TMP" 2>/dev/null; then
    # <version> isn't a branch or tag name (e.g. a SHA resolved from `main`) — fetch it directly.
    git init --quiet "$TMP"
    git -C "$TMP" remote add origin "https://github.com/<org>/<repo>.git"
    git -C "$TMP" fetch --quiet --depth 1 origin "<version>"
    git -C "$TMP" checkout --quiet FETCH_HEAD
  fi
  mkdir -p "$(dirname "$CACHE_DIR")"
  if [ ! -d "$CACHE_DIR/.git" ]; then
    mv "$TMP" "$CACHE_DIR"
  else
    rm -rf "$TMP"   # another caller already populated it first — discard the redundant clone
  fi
fi
```

Each attempt clones into its own `mktemp -d`, so two concurrent `ensure` calls never race over a
shared temp path. The final move only proceeds if nobody has already populated `$CACHE_DIR`, so this
isn't a fully atomic claim against another writer finishing in between — but the failure mode is at
worst an occasional wasted clone, never a corrupted destination.

If the clone fails outright (network unreachable, ref doesn't exist upstream), report the error
clearly and stop. Do not leave a partial `$CACHE_DIR` behind.

## What this skill does not do

- Does not track "latest" live. Once a version is recorded, it stays pinned until someone runs
  `configure` again — nothing re-resolves it automatically or on a schedule.
- Does not assume a specific consumer. Any skill needing a stable local copy of a GitHub repo can
  delegate to `ensure org/repo`, not just one reference implementation.
- Does not clone anything not hosted on `github.com` — the key format and clone-URL derivation both
  assume GitHub specifically.
