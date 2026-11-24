---
name: draft
description: >
    Draft a bug report from the current session: collects the failure, the commands and the versions
    from what was run, reproduces it, and asks the user only about what is still missing. Use when a
    developer or agent hits a failure and wants it filed, or asks to report a bug or write one up.
---

# Skill: Draft a bug report from the session

## 1. Purpose and posture

Someone in this session (a developer, or an agent working for one) hit a failure and wants it
filed. The session already holds most of the report: the call that failed, the error, the
checkout. You **harvest** that evidence first, reproduce the failure, and **grill** the user only
on the gaps the session and the environment cannot fill. The finished report meets the standard
in the `bugs:standard` skill.

The session's memory of what happened is a lead, and the reproduction is the evidence. A fact
enters the report as observed only once it has been run in step 3.

## 2. Method

### Step 1: Harvest

Map the session onto the sections of the template,
[../../references/template.md](../../references/template.md):

- **From the session:** the failing call or command, the exact error and trace, the inputs used,
  what the user expected, how the failure came up.
- **From the environment:** the version under test
  (`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`), the commit
  (`git rev-parse --short HEAD`, and the branch), and the Python, Java and Spark versions that ran.

Mark each section **harvested** (with its source in the session), **ambiguous** or **missing**.

**Done when** every template section has a status.

### Step 2: Duplicates and known fixes

Search open and closed issues and PRs for the error text and the call name
(`gh issue list --state all --search "<text>"`, `gh pr list --state all --search "<text>"`).

- The same failure, still unfixed: tell the user and stop; offer a comment on the existing issue
  with any new evidence.
- The same failure, with a fix already merged: offer a comment on the existing issue in place of a
  new report.
- A related but distinct failure: continue, and link it under Related.

**Done when** both terms have been searched and every match is classified.

### Step 3: Reproduce

Find the smallest public-API call that fails with the harvested error, on the version in use: a
release in a throwaway environment, or a development branch from the repository's own environment.
Read [../../references/pathling-build.md](../../references/pathling-build.md) before the first
run.

- Keep the full output: the trace and the versions that ran. A trace shows a later reproduction
  hits the same failure, which matters most when the exception is generic.

Run another version (the latest release, `main`) only when the user asks in step 5. The report names
only versions that ran.

**Done when** the minimal snippet fails with the harvested error, and its output is saved.

### Step 4: Ground the expected behaviour

Read the specification as a fully compliant engine would execute it, then adapt for Pathling's
limits. Behaviour the failure prevents you from running is labelled _not verified end to end_.

**Done when** the expected behaviour cites a rule with its link and gives an input that should
match, or is marked as a question for step 5.

### Step 5: Grill the gaps

Ask the user about each section still ambiguous or missing, and about nothing the session already
answers. Typical gaps:

- **How it was found**, unless the session shows it. Ask; it is the one line that is never
  inferred.
- **Expected behaviour** when the specification allows more than one reading.
- **Other versions to run**, such as the latest release, to show whether released users are
  affected.
- **Scope, workaround or suspected cause** the user wants included, each labelled as a hypothesis
  unless it is run.

Use `AskUserQuestion`: up to four independent questions per round, each with a recommended answer
drawn from the session. Ask dependent questions in a later round. Run anything an answer adds
(another version, a scope case) before it enters the report.

When no human is in the session (an agent filing on its own), infer each gap as
`bugs:intake` step 3 does, label each inference in the report, and list the open questions in
the message that delivers the draft.

**Done when** every template section is filled or reads "Not investigated", and every question
asked has an answer or is listed as open.

### Step 6: Draft and check

Draft with the template. Check it against the Draft check in `bugs:standard` and fix what fails.
For a second opinion that is not anchored on this session, run `bugs:review-report` on the draft
in a subagent.

**Done when** every Draft check answer is yes.

### Step 7: Show, then file

Show the draft and the suggested labels (per `bugs:standard`) to the user. File it
(`gh issue create`, or `gh issue edit <N> --body-file <file>` for a rewrite) once they approve,
since an issue is public.

**Done when** the user has approved and the issue is filed, or has declined and the draft is saved
in `.private/` with its path given.
