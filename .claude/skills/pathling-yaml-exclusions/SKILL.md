---
name: pathling-yaml-exclusions
description: >
  Manage the YAML conformance-test exclusion baselines at
  fhirpath/src/test/resources/fhirpath-js/config.yaml and fhirpath-ptl/config.yaml. Use this skill
  when a newly implemented FHIRPath feature makes previously-excluded conformance cases pass, when
  the build fails with "Excluded test passed when expected outcome was ...", when auditing the
  baseline for stale or mislabelled entries, or when deciding how to record a case that Pathling
  cannot yet handle. Trigger on phrases like "exclusion", "excluded test", "config.yaml",
  "known failures", "conformance baseline", "YamlReferenceImplTest", "YamlFhirPathTest", or any
  request to remove, narrow, or reclassify a test exclusion.
---

# Pathling YAML test exclusions

Pathling runs two YAML conformance suites, each with its own exclusion baseline:

| Test class | Config | Corpus |
|---|---|---|
| `YamlReferenceImplTest` | `fhirpath-js/config.yaml` | The fhirpath.js reference test corpus |
| `YamlFhirPathTest` | `fhirpath-ptl/config.yaml` | Pathling's own cases |

An exclusion is not a mute button. It is an **assertion about how a case currently fails**.

## The baseline polices itself

Excluded cases are still executed. The runner asserts the case produces the recorded `outcome`,
and only then reports it as skipped. If an excluded case starts passing, the build fails with:

```
Excluded test passed when expected outcome was error
```

That is the machine-checkable done-signal for feature work: implement a feature, and every
exclusion it obsoletes turns the build red until it is cleaned up. You cannot silently leave a
stale exclusion behind.

| `outcome` | Meaning | Build fails when |
|---|---|---|
| `error` (default) | The case throws an exception | It passes, or fails an assertion instead of throwing |
| `failure` | The case runs but produces the wrong result | It passes, or throws instead of failing |
| `pass` | The case passes, but is excluded for another reason | It fails or throws |
| *explicitly null* (`outcome:`) | The case is **not run at all** | Never — unverified |

Prefer `error` or `failure`. An explicitly-null `outcome` opts the case out of verification
entirely, which is how baselines rot. Use it only when running the case is itself the problem
(a hang, an OOM), and say so in the `comment`.

`UnsupportedFhirPathFeatureError` is handled separately — such cases are skipped regardless of
`outcome`.

## Rule anatomy

```yaml
excludeSet:
  - title: Global exclusions        # required
    comment: |                      # optional
      Why this block exists.
    exclude:
      - title: Unsupported resources # required
        type: feature                # convention only — see taxonomy below
        id: "#2418"                  # issue this is tracked under
        outcome: failure             # defaults to "error"
        comment: |                   # why, and what would resolve it
          ...
        expression:                  # matcher — see below
          - "^StructureDefinition"
```

### Matchers

A rule may carry several matchers; they are **OR**-ed. A case is excluded by the **first** rule
whose matchers match it.

| Matcher | Semantics |
|---|---|
| `any: [...]` | **Substring** match against the case's expression **or** its description |
| `expression: [...]` | **Regex**, unanchored (find, not full match), against the expression only |
| `function: [...]` | Matches by function name |
| `spel: [...]` | SpEL predicate over the case |

Prefer `any` with a complete expression string for surgical exclusions — it is the easiest to
verify and the hardest to over-match. Reach for `expression` regexes only when a genuine family
of cases shares a shape, and remember they are unanchored: `"toQuantity"` also matches
`convertsToQuantity`.

### Fields that do not work

Three fields are parsed but never applied. Do not rely on them, and do not add new uses:

- **`glob` on an exclude block** — documented as scoping a block to certain test files, but never
  read. **Every block applies to every case file.** A rule you believe is scoped to one corpus
  file will silently mask matching cases everywhere.
- **`desc` on a rule** — declared as a matcher but never converted to a predicate. Use `any`,
  which already matches descriptions.
- **`disabled: true`** — works, and suppresses the rule entirely (the case runs unexcluded).

Separately, the system property `au.csiro.pathling.test.yaml.exclusionsOnly` is read and logged
but never applied — it does not filter anything. Ignore it.

## Type taxonomy

`type` is a free-form string with no validation. Current usage in `fhirpath-js/config.yaml`:

| `type` | Count | Meaning |
|---|---|---|
| `feature` | 23 | Capability not yet implemented |
| `new-feature` | 9 | Synonym for `feature` — **being retired** |
| `bug` | 5 | Pathling defect; the case should pass |
| `wontfix` | 23 | Pathling deliberately diverges, or the case itself is invalid |

**Use `feature`, not `new-feature`.** When you touch a block containing `new-feature`, migrate it.

`wontfix` deserves scrutiny. It currently absorbs two different things: genuinely invalid test
cases, and unimplemented capabilities mislabelled as invalid. For example a `wontfix` block titled
*"Parse error — not valid FHIRPath syntax"* contains `('a'|'b'|'c').join(',')`,
`(1|2|3).sum()` and `(1|2|3).aggregate($this+$total, 0)` — all valid FHIRPath, and all really
`feature` gaps. If a `wontfix` entry you encounter is actually a capability gap, reclassify it and
give it an issue id.

## The sweep

Run this whenever a feature lands, and whenever the build reports an excluded test passing.

### 1. Find the rules in scope

Search by every handle the feature has — function name, operator, expression fragment, and the
issue number:

```bash
cd fhirpath/src/test/resources
rg -n 'toQuantity' fhirpath-js/config.yaml fhirpath-ptl/config.yaml
rg -n '#2391' fhirpath-js/config.yaml fhirpath-ptl/config.yaml
```

**An empty result is not evidence the feature is fully working.** Most open FHIRPath issues own no
exclusion at all — the corpus may simply not cover the feature. When nothing matches, say so
explicitly in your report rather than concluding there was nothing to do.

### 2. Decide, per rule

| Decision | When |
|---|---|
| **REMOVE** | Every case the rule matches now passes |
| **NARROW** | The matcher is over-broad — some cases pass now, others still legitimately fail. Rewrite it, preferring `any` with exact expressions, so it catches only the residual failures |
| **RECLASSIFY** | The residual failure is real but recorded under the wrong `type`, or the `outcome` changed (a case that used to throw now returns a wrong result → `error` becomes `failure`) |
| **KEEP** | The rule describes a real gap unrelated to this change |

### 3. Verify

```bash
mvn spotless:apply -pl fhirpath
mvn test -pl fhirpath -Dtest=YamlReferenceImplTest
mvn test -pl fhirpath -Dtest=YamlFhirPathTest
```

Both must be 0 failures, 0 errors. Two failure modes to read correctly:

- *"Excluded test passed when expected outcome was error"* → the rule is now obsolete for that
  case. REMOVE or NARROW it.
- A plain failure on a case you just un-excluded → the feature does not actually cover it. Restore
  the exclusion (NARROW it) and say which cases remain, or fix the implementation.

To check whether a specific rule is still needed without editing the file, disable it by id:

```bash
mvn test -pl fhirpath -Dtest=YamlReferenceImplTest \
  -Dau.csiro.pathling.test.yaml.disabledExclusions='#2391,#2418'
```

Cases the rule was masking now run for real. This is the cheapest way to answer "is this
exclusion still earning its place?"

### 4. Report

State per rule what changed and why: rule title, decision, and the evidence. Report the suite
output, not a claim that it is green. If the sweep found nothing in scope, report that too.

## Hygiene for any rule you add or modify

- `feature` / `bug` → carries `id: "#NNNN"` pointing at an **open** Pathling issue. Verify with
  `gh issue view NNNN --json state,title`. Several ids in the baseline point at closed issues
  (#2163, #2398, #2383) — fix those when you touch their block.
- `wontfix` → needs a `comment` justifying the divergence or explaining why the case is invalid.
  No id required, but confirm it is genuinely not a capability gap.
- Every rule needs a `title`, and a `comment` whenever the title alone does not explain the
  decision.
- The matcher must match at least one real case. A rule matching nothing is dead weight; the
  runner will not tell you, so check with `rg` against the case files under
  `fhirpath-js/cases/` or `fhirpath-ptl/cases/`.
- No two rules should match the same case — the first wins, and the second becomes invisible.
- Set `outcome` deliberately. Do not leave it null to make a stubborn case go away.

## Do not

- Add an exclusion to turn a red build green when the cause is a regression in your own change.
  An exclusion records a *pre-existing* gap.
- Widen an existing matcher to swallow a new failure. Add a separate, narrower rule instead, so
  the two gaps stay independently trackable.
- Rely on `glob`, `desc`, or `exclusionsOnly` (see above).
