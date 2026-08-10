# Commit and pull request templates

Read at Step 9 (the feature commit), Step 10 (the review fix), and Step 11 (push and PR) of
`implement-pathling`.

## Commit

CONTRIBUTING.md governs the shape: a type prefix, a short summary of the objective, and a body
explaining why the change was needed rather than restating the diff.

The `Co-Authored-By` trailer must name the model actually running this skill (for example
`Claude Sonnet 5`, `Claude Opus 5`) — never hardcode a specific tier.

```bash
git add <specific files>
git commit -m "$(cat <<'EOF'
feat: Support <capability> (#<N>)

<why this was needed and what it enables, a few sentences>

Co-Authored-By: <this model's own name> <noreply@anthropic.com>
EOF
)"
```

## Folding in a review fix

Review runs before the first push, so the history is still private and worth keeping clean. Fold
the fix into the commit it belongs to:

```bash
git add <specific files>
git commit --amend --no-edit          # when it belongs to the most recent commit
```

When the fix spans several commits, or the original commit message no longer describes what the
commit does, a separate commit is the better answer — rewriting further back buys little:

```bash
git commit -m "$(cat <<'EOF'
fix: Address review findings for #<N>

<what changed and why>

Co-Authored-By: <this model's own name> <noreply@anthropic.com>
EOF
)"
```

## Push and open the pull request

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

When Step 0 resolved `<base>` to something other than `origin/main` (a `--base` override), add
`--base <branch>` to the `gh pr create` command above, where `<branch>` is `<base>` with any
`origin/` prefix stripped (for example `fix/local_claude`, not `origin/fix/local_claude`). Without
it, `gh pr create` defaults to the repository's default branch (`main`) regardless of what ref the
branch was actually built from, and the opened PR's diff would include every commit unique to the
override base — not just this issue's work. Retarget with `gh pr edit --base main` once that
override base has merged into `main`, at which point the diff collapses to just the feature commits.
