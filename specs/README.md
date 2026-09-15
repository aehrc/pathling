# Specifications

One directory per specification, `NNN-short-name`, numbered in the order they
were written. Numbers are never reused: a completed specification keeps its
number after it moves to `archive/`.

A directory sitting directly under `specs/` is outstanding work — not built yet,
or being built now. `archive/` is the record of what has been implemented.

Each directory holds:

| File | Contents |
| --- | --- |
| `spec.md` | What is being built and why. Requirements and success criteria, no implementation detail. |
| `plan.md` | Technical context, module structure, phases. |
| `research.md` | Each resolved technical question: decision, rationale, rejected alternatives. |
| `data-model.md` | Entities and their rules, where the feature involves data. |
| `contracts/` | Interface contracts, where the feature exposes an interface. |
| `quickstart.md` | End-to-end scenarios that validate the feature. |
| `tasks.md` | Dependency-ordered tasks with traceability back to the requirements. |
| `checklists/` | Quality checks applied to the specification itself, with findings recorded. |
| `evidence/` | The measurements the decisions rest on, and scripts that reproduce them. |

`evidence/` matters as much as the rest. Several requirements exist because a
measurement ruled out the obvious alternative, and a decision whose evidence has
been lost cannot be revisited — only re-litigated. Reproduction scripts are kept
runnable and free of machine-specific paths.

This supersedes `openspec/` for work of this kind. OpenSpec remains for changes
that extend the FHIRPath framework itself, where its proposal format and tooling
are in use.
