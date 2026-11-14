# Issue tracker: Local Markdown

Issues for this repo live as markdown files. A feature that has a specification keeps its tickets beside it, in `specs/NNN-short-name/issues/`, where they are tracked and reviewed with the spec. Anything else lives in `.scratch/`.

## Conventions

- One feature per directory: `specs/NNN-short-name/` when the feature has a specification (see `specs/README.md`), otherwise `.scratch/<feature-slug>/`
- Implementation issues are one file per ticket at `<feature-dir>/issues/<NN>-<slug>.md`, numbered from `01` — never a single combined tickets file
- Where a feature is delivered in stages, `issues/README.md` groups its tickets by stage, and each stage is a GitHub sub-issue of the feature's issue that links its tickets
- Triage state is recorded as a `Status:` line near the top of each issue file (see `triage-labels.md` for the role strings)
- Comments and conversation history append to the bottom of the file under a `## Comments` heading

## When a skill says "publish to the issue tracker"

Create a new file under `specs/NNN-short-name/issues/` if the feature has a specification, otherwise under `.scratch/<feature-slug>/` (creating the directory if needed). A specification itself goes in `specs/`.

## When a skill says "fetch the relevant ticket"

Read the file at the referenced path. The user will normally pass the path or the issue number directly.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a file with one **child** file per ticket.

- **Map**: `.scratch/<effort>/map.md` — the Notes / Decisions-so-far / Fog body.
- **Child ticket**: `.scratch/<effort>/issues/NN-<slug>.md`, numbered from `01`, with the question in the body. A `Type:` line records the ticket type (`research`/`prototype`/`grilling`/`task`); a `Status:` line records `claimed`/`resolved`.
- **Blocking**: a `Blocked by: NN, NN` line near the top. A ticket is unblocked when every file it lists is `resolved`.
- **Frontier**: scan `.scratch/<effort>/issues/` for files that are open, unblocked, and unclaimed; first by number wins.
- **Claim**: set `Status: claimed` and save before any work.
- **Resolve**: append the answer under an `## Answer` heading, set `Status: resolved`, then append a context pointer (gist + link) to the map's Decisions-so-far in `map.md`.
