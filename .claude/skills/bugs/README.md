# bugs

Claude Code skills for the life of a Pathling bug: writing the report, triaging one from outside
the team, checking a report, and deciding whether a fix lets the issue close.

## Skills

| Skill                 | Use it when                                          | Result                                                                                                      |
| --------------------- | ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `/bugs:standard`      | Writing, rewriting or judging a bug report           | The standard a report must meet, with its draft check, template, examples and labels                        |
| `/bugs:draft`         | You hit a failure in a session and want it filed     | A report drafted from the session, gaps asked about, filed after you approve                                |
| `/bugs:triage`        | An external user filed an incomplete report          | A reconstructed report, a reply, or both, drafted in `.local/triage/<N>/`                                   |
| `/bugs:review-report` | A report is drafted and you want its writing checked | `PASS` or `FAIL` against 14 checks (text only, nothing is run)                                              |
| `/bugs:verify-report` | You want a report's claims checked                   | Each claim confirmed, contradicted or unverifiable, from runs, the spec and a duplicate search              |
| `/bugs:verify-fix`    | An issue looks fixed but is still open               | `CLOSE`, `PARTIAL`, `NOT_FIXED` or `INCOMPLETE`, from the root cause, the fix diff and adversarial variants |

They follow one another: `draft` or `triage` builds a report to `standard`; `review-report` and
`verify-report` check it; `verify-fix` checks the fix. Each can also be run on its own.

The skills draft and report. Filing, commenting on and closing an issue stay with you.

## How it loads

This folder is a plugin inside the project's skills directory: Claude Code finds
`.claude-plugin/plugin.json` here and loads every skill under `skills/` with the `bugs:` prefix.
A checkout of the repository is all it needs; there is nothing to install or enable.

## Layout

```
.claude-plugin/plugin.json   the plugin's name and version
skills/<name>/SKILL.md       one skill each
references/                  shared by the skills, linked as ../../references/<file>
  template.md                the structure of a report
  examples.md                a finished report and a failing one, side by side
  pathling-build.md          installing a release, building a commit, and running Python against it
```

## Editing

- Put anything two skills need in `references/`, and link to it; keep each fact in one place.
- A skill refers to another by its full name, for example `bugs:verify-fix`.
- Check the plugin after a change with `claude plugin validate .claude/skills/bugs`.
