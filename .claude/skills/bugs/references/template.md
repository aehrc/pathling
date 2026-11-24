# Bug report template

```markdown
## Summary

One or two sentences: what fails, on what.

## Environment

Versions actually run (Pathling, Python, Java, Spark), FHIR version, API surface used.

Any other component the call reached (terminology server, proxy) with its version or endpoint, and
whether the failure persists without it, or "Not investigated".

For a server failure: how it was started (image and `docker run` command, or the configuration that
matters) and what sits in front of it (a proxy and the forwarded headers it sets).

## How it was found

One true line (for example "Reproduced while reviewing #123 (Title of the issue)").

## Steps to reproduce

A runnable snippet, as short as it gets. For the server, the request as a `curl` command.

## Actual behaviour (observed)

The exact error message in a code block.

<details><summary>Stack trace</summary>

Full trace, with home directories shortened to `...`.

</details>

## Expected behaviour (from the specification, not verified end to end)

What should happen, with the spec rule linked. Include the input that should match.

## Optional

Each item is a labelled hypothesis or an observation you made. Anything you did not investigate
reads "Not investigated".

- Suspected cause (hypothesis)
- Scope (only what you ran)
- Workaround (only one you tried)
- Suggested acceptance tests

Related: #issue (title)
```
