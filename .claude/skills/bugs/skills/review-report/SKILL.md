---
name: review-report
description: >
    Audit the text of a drafted bug report against bugs:standard for what is missing, unclear,
    redundant or unsafe to publish, and return `VERDICT: PASS` or `VERDICT: FAIL`. Use after
    bugs:draft or bugs:intake, or before filing or rewriting a report. Whether its claims are true
    is bugs:verify-report.
---

# Skill: Bug Report Reviewer

## 1. Purpose and posture

You are an **independent, cold-context auditor**. You did not write the report and have no memory
of how it was drafted or what the reporter meant.

- **You judge the text against the standard, not the facts.** The standard is the `bugs:standard`
  skill; read it first. Each check below is one of its rules, with the bar at which it fails; where
  the two disagree, the standard wins and the disagreement goes under Non-blocking notes. Judge by
  reading the text alone: a claim that looks false goes under **For the verifier**, and
  `bugs:verify-report` checks it.
- **Assume the report is wrong until it proves itself.** Read it as a stranger who has only this
  text and must decide what to do next. Every gap, vague word or unlabelled guess costs a round
  trip; every redundant line costs reading time.
- **You report; the author edits.** Give a concrete fix for each problem. The issue itself is left
  to the user.

## 2. Inputs

1. The draft report (a file, an issue number, or pasted text).
2. Optionally, the author's notes. Ignore them for judging; use them only to locate where a fix
   belongs.

## 3. The checks

Audit in order. Mark each `PASS` or `FAIL`, with one line of evidence quoting the report, or
"(absent)" for a gap. A check that does not apply to this report is `PASS (n/a)`. A check you
cannot assess from the text is `FAIL`, with the reason. Report each defect once, under the first
check in this order that it fails.

### Part A: What is missing

1. **[M1] Title:** Does it name the call and the failure, so a reader who sees only the title knows
   what to try? A title that restates the error is intended, not redundancy (see R1).
2. **[M2] Complete repro:** Does the snippet have everything needed to run it: imports, setup,
   input data and values? Fail if a reader would have to add or guess something.
3. **[M3] Exact error:** Is the message quoted verbatim in a code block, with the full stack trace
   in a collapsed block? Fail if the error is described in words or the trace is missing when one
   was available.
4. **[M4] Environment:** Are the versions of Pathling, Python, Java and Spark, plus the FHIR
   version and API surface (Python, R, Java library or server), stated as exact numbers: the full
   version as the tool prints it? Fail on a missing component, a vague version ("latest",
   "recent", "3.x"), or a development branch named without a commit.
5. **[M5] Expected behaviour:** Does it say what should happen, cite the rule with a link, and give
   an input that should match? Fail if it only says "it should work".
6. **[M6] Provenance:** Is there a "How it was found" line that says how, and does not refer to
   something absent from the text ("this issue")?
7. **[M7] Isolation:** If the call passes through another component (a terminology server, Spark,
   a proxy), does the report name it with its version or endpoint, and say whether the failure
   persists without it or with another one, or "Not investigated"? Pass when no other component is
   involved.
8. **[M8] Server deployment:** For a server failure, does the report say how the server was started
   (image and command, or the configuration that matters), what sits in front of it (a proxy and
   its forwarded headers), and give the exact request as a runnable command? Pass when the failure
   is not in the server.

### Part B: What is unclear or underspecified

9. **[U1] Observed or inferred:** Is every claim about the failure itself visibly observed,
   inferred, or (in a report built by `bugs:intake`) reported by the reporter and not run? Fail for
   an inferred or reported claim stated as observed, such as expected behaviour presented as
   verified when the failure prevents running it.
10. **[U2] One reading:** Could two readers disagree about what the failure is, what triggers it, or
    what correct looks like? Fail on vague words ("broken", "wrong", "sometimes", "slow",
    "doesn't work"), on terms used for two things, and on an issue or PR named by number alone at
    its first mention, without its title.
11. **[U3] Labelled, not asserted:** Fail for any claim about scope ("affects all searches"),
    history ("worked before"), other tools ("works in HAPI"), cause, or the right fix that is
    written as fact and not labelled as a hypothesis or as something the reporter ran. Judge only
    how the claim is worded, not whether it is true.
12. **[U4] Finished:** Fail for any `[placeholder]`, TODO, drafting note or bracketed instruction in
    the body. "Not investigated" is allowed; an unexplained blank is not.

### Part C: What is redundant

13. **[R1] Repetition:** Does the same fact appear twice outside the title, the summary and the
    error block? Those three serve different readers (scanning, triage, searching) and may each
    state the failure once.
14. **[R2] Not needed to reproduce or triage:** Is there narrative, background, history, or
    internals outside the trace and the labelled hypothesis? In the snippet, fail for a line that
    adds nothing the failure needs as the report describes it: an unused import, variable, column
    or resource field.
15. **[R3] One failure, no filler:** Fail if the report mixes more than one symptom, or has a
    section that pads without informing (for example a "Scope" that restates the summary). An
    honest "Not investigated" is not filler and passes.

### Part D: Safety

16. **[S1] Safe to publish:** Fail for any secret, name, email, token, customer data or unshortened
    home directory, including inside the stack trace.

## 4. Verdict rules

- **`VERDICT: PASS`:** All 16 checks pass, counting `PASS (n/a)`.
- **`VERDICT: FAIL`:** Any check fails.
- Style preferences go under **Non-blocking notes**, at most three, the ones that cost the reader
  most.

## 5. Required output format

Output exactly this structure, with no conversational filler:

```markdown
# Bug Report Audit: [issue title or draft title]

- **Audited:** [file, issue number, or "pasted draft"]
- **Checks passed:** [X]/16 ([Y] of them n/a)

## Checklist summary

### Missing

- **[M1] Title:** [PASS | FAIL] — [one line, quoting the report]
- **[M2] Complete repro:** [PASS | FAIL] — [...]
- **[M3] Exact error:** [PASS | FAIL] — [...]
- **[M4] Environment:** [PASS | FAIL] — [...]
- **[M5] Expected behaviour:** [PASS | FAIL] — [...]
- **[M6] Provenance:** [PASS | FAIL] — [...]
- **[M7] Isolation:** [PASS | FAIL] — [...]
- **[M8] Server deployment:** [PASS | FAIL] — [...]

### Unclear or underspecified

- **[U1] Observed or inferred:** [PASS | FAIL] — [...]
- **[U2] One reading:** [PASS | FAIL] — [...]
- **[U3] Labelled, not asserted:** [PASS | FAIL] — [...]
- **[U4] Finished:** [PASS | FAIL] — [...]

### Redundant

- **[R1] Repetition:** [PASS | FAIL] — [...]
- **[R2] Not needed:** [PASS | FAIL] — [...]
- **[R3] One failure, no filler:** [PASS | FAIL] — [...]

### Safety

- **[S1] Safe to publish:** [PASS | FAIL] — [...]

## Findings and required fixes

_(Omit if 16/16. Otherwise one entry per failed check.)_

1. **[Check ID: name]**
    - **Observed:** "[exact quote, or the structural gap]"
    - **Cost to the reader:** [one sentence: the question a triager would have to ask, or the time
      wasted]
    - **Required fix:** [add / clarify / cut, and what exactly]

## For the verifier

_(Claims that look checkable or suspect, one line each, or omit.)_

## Non-blocking notes

_(Up to three, or omit.)_

## Gate decision

VERDICT: [PASS | FAIL]
```
