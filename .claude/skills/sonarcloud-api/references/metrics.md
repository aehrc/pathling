# SonarCloud Metrics Reference

Use these metric keys with the Measures API endpoints.

## Table of contents

- [Size metrics](#size-metrics)
- [Complexity metrics](#complexity-metrics)
- [Issue metrics](#issue-metrics)
- [Coverage metrics](#coverage-metrics)
- [Duplication metrics](#duplication-metrics)
- [Maintainability metrics](#maintainability-metrics)
- [Reliability metrics](#reliability-metrics)
- [Security metrics](#security-metrics)
- [New code metrics](#new-code-metrics)

---

## Size metrics

| Key                     | Description                                   |
| ----------------------- | --------------------------------------------- |
| `ncloc`                 | Lines of code (excluding comments and blanks) |
| `lines`                 | Total lines                                   |
| `statements`            | Number of statements                          |
| `functions`             | Number of functions                           |
| `classes`               | Number of classes                             |
| `files`                 | Number of files                               |
| `directories`           | Number of directories                         |
| `comment_lines`         | Number of comment lines                       |
| `comment_lines_density` | Comments percentage                           |

---

## Complexity metrics

| Key                    | Description                     |
| ---------------------- | ------------------------------- |
| `complexity`           | Cyclomatic complexity           |
| `cognitive_complexity` | Cognitive complexity            |
| `file_complexity`      | Average complexity per file     |
| `function_complexity`  | Average complexity per function |
| `class_complexity`     | Average complexity per class    |

---

## Issue metrics

| Key                     | Description           |
| ----------------------- | --------------------- |
| `violations`            | Total issues          |
| `blocker_violations`    | Blocker issues        |
| `critical_violations`   | Critical issues       |
| `major_violations`      | Major issues          |
| `minor_violations`      | Minor issues          |
| `info_violations`       | Info issues           |
| `open_issues`           | Open issues           |
| `confirmed_issues`      | Confirmed issues      |
| `reopened_issues`       | Reopened issues       |
| `false_positive_issues` | False positive issues |
| `wont_fix_issues`       | Won't fix issues      |

---

## Coverage metrics

| Key                    | Description                 |
| ---------------------- | --------------------------- |
| `coverage`             | Overall coverage percentage |
| `line_coverage`        | Line coverage percentage    |
| `branch_coverage`      | Branch coverage percentage  |
| `lines_to_cover`       | Lines to cover              |
| `uncovered_lines`      | Uncovered lines             |
| `conditions_to_cover`  | Conditions to cover         |
| `uncovered_conditions` | Uncovered conditions        |
| `tests`                | Number of unit tests        |
| `test_success_density` | Test success percentage     |
| `test_failures`        | Number of test failures     |
| `test_errors`          | Number of test errors       |
| `skipped_tests`        | Number of skipped tests     |
| `test_execution_time`  | Test execution time (ms)    |

---

## Duplication metrics

| Key                        | Description                       |
| -------------------------- | --------------------------------- |
| `duplicated_lines`         | Number of duplicated lines        |
| `duplicated_lines_density` | Duplication percentage            |
| `duplicated_blocks`        | Number of duplicated blocks       |
| `duplicated_files`         | Number of files with duplications |

---

## Maintainability metrics

| Key                                        | Description                  |
| ------------------------------------------ | ---------------------------- |
| `code_smells`                              | Total code smells            |
| `sqale_index`                              | Technical debt (minutes)     |
| `sqale_debt_ratio`                         | Technical debt ratio         |
| `sqale_rating`                             | Maintainability rating (A-E) |
| `effort_to_reach_maintainability_rating_a` | Effort to reach A rating     |

Ratings: A (0-5%), B (5-10%), C (10-20%), D (20-50%), E (>50%)

---

## Reliability metrics

| Key                              | Description                  |
| -------------------------------- | ---------------------------- |
| `bugs`                           | Total bugs                   |
| `reliability_rating`             | Reliability rating (A-E)     |
| `reliability_remediation_effort` | Effort to fix bugs (minutes) |

Ratings: A (0 bugs), B (>=1 minor), C (>=1 major), D (>=1 critical), E (>=1 blocker)

---

## Security metrics

| Key                           | Description                  |
| ----------------------------- | ---------------------------- |
| `vulnerabilities`             | Total vulnerabilities        |
| `security_rating`             | Security rating (A-E)        |
| `security_remediation_effort` | Effort to fix (minutes)      |
| `security_hotspots`           | Security hotspots count      |
| `security_hotspots_reviewed`  | Reviewed hotspots            |
| `security_hotspots_to_review` | Hotspots to review           |
| `security_review_rating`      | Security review rating (A-E) |

---

## New code metrics

Metrics for new code (since new code period):

| Key                            | Description                     |
| ------------------------------ | ------------------------------- |
| `new_violations`               | New issues                      |
| `new_bugs`                     | New bugs                        |
| `new_vulnerabilities`          | New vulnerabilities             |
| `new_code_smells`              | New code smells                 |
| `new_security_hotspots`        | New security hotspots           |
| `new_coverage`                 | Coverage on new code            |
| `new_line_coverage`            | Line coverage on new code       |
| `new_branch_coverage`          | Branch coverage on new code     |
| `new_lines_to_cover`           | New lines to cover              |
| `new_uncovered_lines`          | New uncovered lines             |
| `new_duplicated_lines`         | New duplicated lines            |
| `new_duplicated_lines_density` | New duplication percentage      |
| `new_lines`                    | New lines of code               |
| `new_technical_debt`           | New technical debt              |
| `new_sqale_debt_ratio`         | New technical debt ratio        |
| `new_maintainability_rating`   | New code maintainability rating |
| `new_reliability_rating`       | New code reliability rating     |
| `new_security_rating`          | New code security rating        |

---

## Common metric combinations

### Quality overview

```
metricKeys=bugs,vulnerabilities,code_smells,coverage,duplicated_lines_density
```

### Security focus

```
metricKeys=vulnerabilities,security_hotspots,security_rating,security_review_rating
```

### New code quality

```
metricKeys=new_bugs,new_vulnerabilities,new_code_smells,new_coverage,new_duplicated_lines_density
```

### Size and complexity

```
metricKeys=ncloc,files,functions,complexity,cognitive_complexity
```

### Full quality gate metrics

```
metricKeys=new_reliability_rating,new_security_rating,new_maintainability_rating,new_coverage,new_duplicated_lines_density
```
