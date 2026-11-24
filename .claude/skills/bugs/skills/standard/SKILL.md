---
name: standard
description: The standard for a good GitHub bug report, with its draft check, template, examples and labels. Use when writing, rewriting or judging a bug report.
---

# File a bug report

## What a good report looks like

A good bug report lets a stranger see the same failure in minutes and decide what to do about it,
without asking you anything.

**It is about one failure.** The title names the call and the error. Related symptoms go in
separate reports or under Related.

**It is reproducible as written.** The snippet runs unchanged and is as short as it can be while
still failing. The versions named are the versions that were run, with exact numbers.

**It places the failure in Pathling.** When the call passes through another component, such as a
terminology server, Spark or a proxy, the report names that component and its version, and says
whether the failure persists without it or with another one. If you did not try, say "Not
investigated". A failure that turns out to belong to the dependency is fixed elsewhere, so this is
the first thing a triager needs to know.

**For the server, it gives the deployment and the request.** It says how the server was started
(the image and `docker run` command, or the configuration that matters), what sits between the
client and the server (a proxy and the forwarded headers it sets), and the exact request as a
runnable `curl` command. Server behaviour often depends on these more than on the versions.

**It quotes the error, not a description of it.** The message is in a code block, so the report can
be found by searching for it. The full stack trace is in a collapsed block, with home directories
shortened to `...`.

**Every claim is observed or labelled as inferred.** Observed means you saw it. Inferred means
derived from the spec or from reading code, and is said to be. Expected behaviour cites the rule,
links its section, and gives an input that should match.

**It stays within the evidence.** It makes no claim about scope, history or other tools ("affects
all searches", "worked before", "works in HAPI") that you did not run. A suspected cause, scope,
workaround or fix is welcome only as a labelled hypothesis, never as fact.

**It is self-contained and finished.** It reads without the thread that prompted it. It has no
drafting notes, TODOs or unfilled placeholders. A section you did not investigate says "Not
investigated".

**It is safe to publish.** It has no secrets, names, emails or home paths, and internals appear
only in the stack trace and the labelled hypothesis.

### Title

Name the call and the failure: `search_to_column rejects Observation?value-quantity with
InvalidSearchParameterException (SampledData)`. A reader who sees only the title knows what to try.

### Draft check

Each answer must be yes.

- Can someone run the snippet without editing it?
- Is the exact error quoted, with the full trace in a collapsed block?
- Is each claim observed, or labelled as inferred (with the spec rule linked for expected behaviour)?
- Does the report name only versions that were run, and no vague ones ("latest")?
- If the call passes through another component (terminology server, Spark, proxy), are the
  component and its version named, and does the report say whether the failure persists without
  it, or "Not investigated"?
- For a server failure, are the way it was started, anything in front of it, and the exact request
  as a runnable command all given?
- Does it cover one failure, with no unrun claims about scope, history or other tools?
- Is every placeholder filled or its line removed, and are the drafting notes in the message to
  the user, not the body?
- Is the "How it was found" line true, as the user confirmed it or as you did it?
- Is it free of secrets, names, emails and home paths?
- Are issues and PRs named with their titles at first mention?

For a finished report and a failing one side by side, read
[../../references/examples.md](../../references/examples.md). The structure is in
[../../references/template.md](../../references/template.md).

## Building one

- From a failure in the current session: the `bugs:draft` skill.
- From an incomplete issue filed by an external user: the `bugs:intake` skill.

## Labels

Until the label scheme proposed in #2961 (Define a label scheme: one kind per issue, optional
modifiers) is adopted in `CONTRIBUTING.md`, apply this much of it:

- Always `bug` as the one kind.
- A component label (for example `fhirpath`, `server`, `library-api`) only when the failing call
  makes it obvious; otherwise propose one in the message to the user.
- `security`, `breaking change` and the line labels (`PoF_line`, `Bunsen_line`) only when the
  user says so, because each carries meaning beyond the defect.

Once the scheme is adopted, follow it as written there.
