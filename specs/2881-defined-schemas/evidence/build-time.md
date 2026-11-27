# The regular build time of `fhirpath` before and after Phase 6

Taken for T068 of [tasks.md](../tasks.md) (SC-005): the time the regular test build of `fhirpath`
takes before and after slice 06 changes the YAML loads and adds its tests.

## Method

Each checkout was built with `scripts/mvn-isolated.sh -q install -pl fhirpath -am -DskipTests`, and
then `scripts/mvn-isolated.sh test -pl fhirpath` was timed by its wall clock, with the default fork
count of one per processor. The two runs were made one straight after the other, the after run
first, on the same machine: an 11-processor arm64 Mac, Java 21.0.3, Spark 4.1.0.

- **Before**: `9ea9952489`, the base of slice 06.
- **After**: `dbea50839b`, slice 06 with T046, T047 and T054.

## Results

| Run    | Tests | Skipped | Wall clock | Maven total |
| ------ | ----: | ------: | ---------: | ----------: |
| Before | 8,519 |     870 |      190 s |    3:08 min |
| After  | 8,592 |     870 |      184 s |    3:01 min |

The after run was 6 seconds faster, which is inside the noise of this machine (see the caveat), so
Phase 6 adds no measurable time to the build. Its 73 new tests are the 46 of `AbsentElementTest`
(T042) and the 27 of the three new test classes.

The time of the classes that slice 06 adds or changes, from the Surefire reports, in seconds. A
class's time includes starting its Spring context and Spark session in its fork, which is most of the
time of the small classes.

| Class                                |     Tests | Before | After |
| ------------------------------------ | --------: | -----: | ----: |
| `YamlReferenceImplTest`              |     1,824 |   73.2 |  44.0 |
| `YamlFhirPathTest`                   |     1,268 |   40.3 |  25.0 |
| `AbsentElementTest`                  | 119 / 165 |   31.7 |  29.4 |
| `YamlTestRunnerTest`                 |         4 |   37.8 |  43.5 |
| `ArbitraryObjectResolverFactoryTest` |        14 |      - |  38.9 |
| `FhirResolverFactoryTest`            |         7 |      - |  42.9 |
| `YamlTestArgumentProviderTest`       |         6 |      - |  47.2 |

The derivation of each YAML suite's schemas (T047) is not timed on its own. It runs inside the two
YAML classes, once per suite and resource type, and both classes took less time after than before,
so it costs less than the variation between runs. The three new classes run in forks of their own
beside the long classes, which set the build time (`AnnotatedTypesParityTest` took 97 s after and
113 s before), so they do not lengthen it.

## Caveat: the machine was not quiet

Other work, including another slice's builds, ran on the machine during both runs. The 1-minute load
average was 10.8 at the start of the after run and 139 at its end, and 91.5 at the start of the
before run and 193 at its end; the test forks themselves account for part of it. An earlier before
run, taken while a heavier build competed, took 255 s with a load average reaching about 85. The
figures above compare two runs under similar conditions, but their absolute values, and any
difference of a few seconds, are not reliable.
