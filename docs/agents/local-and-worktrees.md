# Local files and worktrees

The rules for files that are never committed, and for working in git worktrees. Other documents
and skills point here rather than restating them.

## Where untracked files go

Three directories at the root of a checkout hold files outside the normal source tree:

| Directory  | Committed | Shared between checkouts | For                                                                 |
| ---------- | --------- | ------------------------ | ------------------------------------------------------------------- |
| `.local`   | No        | Yes                      | Anything kept out of GitHub that every checkout should reach        |
| `.private` | No        | No                       | State that belongs to one checkout                                  |
| `.scratch` | Yes       | Through git              | Issue files of work without a specification, see `issue-tracker.md` |

### `.local`

`.local` is for anything that must not go to GitHub but should be reachable at the same path from
every checkout of the repository. In a linked worktree it is a symbolic link to the main
checkout's `.local`, so a file written there from one worktree can be read from any other. Two
conventions apply inside it:

- **`work/<branch-name>/`** holds per-branch scratch work: working notes, throwaway scripts,
  captured output, and anything else that belongs to the branch but not to the repository. Keying
  it by branch keeps one issue's working files apart from another's, now that all checkouts share
  the directory.
- **`handoffs/`** holds handoff documents, for contributors whose own agent instructions send them
  there.

Anything else in `.local` is up to the contributor.

### `.private`

`.private` is for state that belongs to one checkout and would break if another checkout used it.
It is never linked. `scripts/mvn-isolated.sh` keeps its Maven repository, Ivy cache and Spark
configuration in `.private/mvn-isolated/`. Removing a worktree removes its `.private` with it.

## Worktrees

- **Worktrees live under `.claude/worktrees/`**, for example `.claude/worktrees/issue/<N>`,
  matching the root that the `EnterWorktree` tool uses and that cannot be configured. The
  directory is gitignored. The `implement-pathling` skill's `--worktree` flag uses this path.
- **`.local` is linked automatically** by the `post-checkout` hook in `.githooks`, which runs when
  `git worktree add` (or `EnterWorktree`) checks out a new worktree. It needs
  `git config core.hooksPath .githooks`, and git takes the hook from the checkout that runs
  `git worktree add`, so that checkout must be on a branch that has it. Without the hook, create the link from the new worktree:

    ```bash
    main=$(git worktree list --porcelain | sed -n '1s/^worktree //p')
    mkdir -p "$main/.local" && ln -s "$main/.local" .local
    ```

    A worktree on a branch older than the hook has a `.gitignore` that matches only a `.local`
    directory. The hook also adds `/.local` to the repository's `info/exclude`, so the link is
    ignored there too; without the hook it shows as untracked. Do not commit it.

- **Run Maven through `scripts/mvn-isolated.sh`** in a linked worktree, in place of `mvn` and with
  the same arguments. It gives the checkout its own Maven repository and Ivy cache, so an
  `install` or a Python or R test run does not affect other checkouts. Maven still falls back to
  the shared repository for any Pathling module the checkout has not installed itself, so build
  with `-am` (or install the upstream modules first) to avoid testing against another checkout's
  build. Build `library-runtime` with `install` before running the Python or R tests through it.
  In the main checkout, use plain `mvn` unless asked otherwise. CONTRIBUTING.md describes the
  script in full.
- **Submodules are not populated** by `git worktree add`, so a fresh worktree needs
  `git submodule update --init` before the SQL-on-FHIR compliance tests can run.
