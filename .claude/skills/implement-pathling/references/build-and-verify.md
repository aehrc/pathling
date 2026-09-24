# Building and verifying a FHIRPath change

Read at Step 7 of `implement-pathling`, and again after applying review fixes in Step 10.

## Format before compiling

The build runs `spotless:check` ahead of compilation, so an unformatted file fails as a build
error rather than a style warning:

```bash
mvn spotless:apply -pl fhirpath
```

## The command ladder

Widen the net in stages, so a failure is attributed to the narrowest scope that reproduces it
rather than to the whole module:

```bash
mvn test -pl fhirpath -Dtest='StringFunctionsDslTest#testUpper'   # the new tests
mvn test -pl fhirpath -Dtest=StringFunctionsDslTest               # the capability
mvn test -pl fhirpath -Dtest=YamlReferenceImplTest                # fhirpath.js corpus
mvn test -pl fhirpath -Dtest=YamlFhirPathTest                     # Pathling corpus
mvn test -pl fhirpath                                             # the module
```

## Three gotchas that bite here

All are recorded in `.claude/CLAUDE.md`; they matter at this step specifically.

- **Stale upstream modules produce nonsense errors.** `cannot access java.util.List` and similar
  from `-pl fhirpath` mean `utilities`, `encoders`, or `terminology` are stale, not that the code
  is broken. Rebuild them with `-am`. If `-am` alone does not clear it — `Unresolved compilation
  problem: log cannot be resolved` at runtime, for instance — add `clean`.

- **The last rung needs the `sql-on-fhir` submodule.** `mvn test -pl fhirpath` runs
  `FhirViewShareableComplianceTest`, which reads the corpus from the `sql-on-fhir/` submodule, so it
  needs `git submodule update --init` first. This bites under `--worktree` in particular:
  `git worktree add` does not populate submodules, so a fresh worktree always starts without it.

- **The exclusion baseline polices itself.** Excluded conformance cases are still executed and
  asserted to fail in the recorded way. `Excluded test passed when expected outcome was error` is
  therefore the expected, correct signal that a feature just implemented has made an exclusion
  obsolete. Carry it into Step 8 rather than working around it.
