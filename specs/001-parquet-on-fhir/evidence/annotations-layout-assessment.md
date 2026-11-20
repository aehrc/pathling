# Annotation layout options: constraints, mitigations and ranking

An assessment of the storage options for annotations, for the owner, before the
evidence pack goes to the specification owner (ticket 46, item 5). The options
are:

- **D4:** where annotations are stored;
- **D5:** flat or grouped annotation fields;
- **D6:** the quantity canonical annotation.

It uses only evidence already on file. **[M]** marks a measured claim, with its
file. **[I]** marks one inferred from documentation, code or reasoning.

The rankings in section 4 are the expert's assessment for the owner. Whether
they go to the specification owner is the owner's call, because the pack is
meant to carry no steer.

Constraint names are those of the decision structure:

- **C3:** annotations are lazy; an absent one stays absent.
- **C4:** metadata that is present but unused costs nothing. This is a target
  for the final solution.
- **C5:** System values carry annotations only.
- **C6:** the exact value is preserved.
- **C7:** client needs.
- **C9:** one stored format.
- **C10a:** annotations never take part in equality.
- **C10b:** primitive ids and extensions follow fhirpath.js.
- **H1:** not slower on the SQL on FHIR benchmarks.
- **P0:** rely on the SQL optimiser, with no FHIRPath-level analysis.

The mitigations named are those already decided:

- **CF:** compute-first.
- **ER:** the existence rewrites.
- **PZ:** pairing shape (c), `arrays_zip(x, _x.__a, _x.__b)`, with a
  `coalesce(x, array())` guard on each input.
- **PK:** comparisons and combining on a projected key.
- **D3b:** Pathling's own pruning rule, held in reserve under K6's conditions.

## 1. Where each mitigation works

| Mitigation | Spark versions | Evidence |
|---|---|---|
| CF | OSS 4.3.0, and DBR 19 (Spark 4.2 with backports), Photon on and off, name and address views only. Nothing on OSS 4.0–4.2, or on DBR 17.3 with Photon on (Photon off not run) | [M] [rewrites-layer1](annotations-design-rewrites-layer1.md), [project-before-generate-databricks](annotations-design-project-before-generate-databricks.md) |
| CF + ER | OSS 4.3.0: all 26 views prune. OSS 4.0.2: 4 prune, 22 read whole | [M] [rewrites-layer1](annotations-design-rewrites-layer1.md) |
| PZ | OSS 4.0.2 and 4.3.0, one array level down. Not below a repeating parent | [M] [pairing-shapes](annotations-design-pairing-shapes.md) |
| PK | Every version. It is about correctness, not reads | [I] |
| D3b | OSS 4.0.2 and 4.3.0: 26 of 26 views. DBR 17.3 with Photon off. DBR 17.3 with Photon on misses the pairing shapes (cases 3c–3f) [M]; DBR 19 untested for the rule and for the two-level case. On 4.0.2 the rule bails on the new `exists`/`all` forms (6 of 10 extra views) | [M] [lambda-pruning-rule](annotations-design-lambda-pruning-rule.md), [lambda-pruning-databricks](annotations-design-lambda-pruning-databricks.md), [rewrites-layer1](annotations-design-rewrites-layer1.md) |
| Building Pathling on Spark 4.3 | Waits for a Delta release for 4.3, likely December 2026. Also needs Netty and Scala pins and one source change | [M] [spark-4-3-compatibility](spark-4-3-compatibility.md); date [I] |

## 2. Constraints and impacts, with mitigations

### D4: where annotations are stored

Columns:

- **`_x`:** inside the primitive's `_x` group. This is what Pathling built.
- **`__x`:** a sibling annotation group.
- **Parent:** separate sibling fields at the parent's level, as the
  specification says today.
- **Zipped:** every primitive is one structure holding value, id, extension and
  annotations.

"OK" means the option does not limit Pathling on that constraint.

| Constraint | `_x` | `__x` | Parent | Zipped |
|---|---|---|---|---|
| C3 | OK [M, built] | OK: Pathling tolerates absent columns, and a fallback read is about 11 lines [M, [quantity-root](annotations-design-quantity-root.md) B] | OK, as `__x` [M] | OK [I] |
| C4, value-only reads | Prunes at every depth on 4.0.2 and 4.3.0 [M, [zipped-layout](annotations-design-zipped-layout.md)] | As `_x`, since the value stays plain [I] | As `_x` [I] | **Repeating primitive below a repeating parent reads the whole element on every Spark version:** `name[].given[]` 79x, `dosage[].timing.event[]` 29x; with dates only and no extensions, 2x and 3.5x [M]. Singular and one-level reads are equal [M] |
| C4, annotation reads one level down | PZ prunes on 4.0.2 and 4.3.0 [M] | PZ applies [I] | PZ applies [I] | No pairing needed; prunes as one-level struct fields [I] |
| C4, annotation reads below a repeating parent | **Reads the whole `_event`, extensions included: 321,789 bytes, about 14x `__x`'s** on 4.3.0. On 4.0.2 it reads all of `dosage` (322,054) [M, section 5] | 4.3.0: no extensions; reads the whole `__event` group, so `start` and `end` even when only `start` is used (22,452 for the pair, 20,965 for start only). 4.0.2: all of `dosage`, the same as `_x` [M, section 5] | 4.3.0: exactly the annotation arrays used (22,398 for the pair, 19,456 for start only). 4.0.2: all of `dosage` [M, section 5] | Reads the whole structure: 39.6 MB against `_x`'s 38.3 MB [M] |
| C4, foreign annotations | Read along with `_x` whenever `_x` is read whole [I] | Read along with `__x` whenever `__x` is read whole, which happens inside a lambda on 4.3.0 [M for the known fields, section 5; I for foreign ones] | Never read: each has its own column [I] | Read along with the element whenever it is read whole [I] |
| C5, C6, C9 | OK [I] | OK [I] | OK [I] | OK. Conversion to System drops the id and extension fields [I] |
| C7 (output type independent of what is stored) | OK [M, built] | OK [I] | OK [I] | Output must project `.value` [I] |
| C10a in Pathling | Met by PK [I] | Met by PK [I] | Met by PK [I] | Met by PK [I]. The hazard for other SQL consumers is a format matter: see section 4 |
| C10b | OK, by pairing with `_x` [I] | OK [I] | OK [I] | OK, and the most natural fit [I] |
| H1 | No benchmark view reads an annotated repeating primitive below a repeating element [I, from view names] | As `_x` [I] | As `_x` [I] | **At risk:** views that read `name.given` (such as PatientDemographics) read the whole element [I, from the 79x figure] |
| P0 | OK | OK | OK | OK |
| Pathling rework | None | Writer and annotation readers move. The quantity canonical is paired at traversal from the parent, as `x` with `_x` is today [I]. Tickets 11–14 and 41 are reworked | As `__x`, with more names | Writer, a mapping from storage to the engine's element, the JSON projection (ticket 35) and the legacy export [I] |

Mitigations for the D4 impacts:

- **`_x` below a repeating parent (about 14–16x):**
  - D3b only. It is verified on OSS 4.0.2 and 4.3.0, and on DBR 17.3 with Photon
    off, for cases 3c–3f, which pair `event` with the whole `_event` [M]. On
    pairing shape (c) over the two-level probe's data it was not run [I].
  - DBR 17.3 with Photon on misses the pairing shapes (cases 3c–3f) [M]; DBR 19
    untested for the rule and for the two-level case.
  - CF, ER and PZ do not reach it on any version, 4.3.0 included [M].
- **`__x` and parent-level:** on Spark 4.3.0 they avoid it with no rule, under
  pairing shape (c) [M, section 5]. On 4.0.2 every layout reads the whole
  repeating parent [M], so only D3b can help there. D3b with shape (c) on the
  sibling and parent-level layouts was not run [I].
- **Zipped value reads (79x and 29x):**
  - CF, ER and PZ do not apply, because the read is a nested lambda that stock
    Spark does not prune, 4.3.0 included [M].
  - Whether D3b covers it is untested. This is a gap, and a probe is needed to
    find out.
  - DuckDB prunes neither layout below a list, so DuckDB shows no difference
    [M].

### D5: flat or grouped annotation fields

| | Flat | Grouped |
|---|---|---|
| Row-group skipping | Same as grouped [M, [pushdown](annotations-design-pushdown.md) arm 6] | Same as flat [M] |
| C3, C4, P0, H1 | OK [I] | OK. One more struct level to prune through, which Spark 4.3 handles in `transform` and `filter` [I] |
| Pathling rework | Dates are already flat. The quantity's built struct is flattened, as design doc §4.7 proposes [I] | The quantity is already grouped. The dates move to `__bounds` [I] |
| What each guarantees | Fields are independent in the schema | The fields appear in the schema together, but not filled together in a row [I] |

No D5 impact needs a mitigation.

### D6: the quantity canonical annotation

Every option is held to the decided SHALL: one stored representation per
magnitude.

Columns:

- **Flat code + exact:** separate `__canonical_code` and
  `__canonical_value_exact` (FlexiDecimal) fields.
- **Struct:** today's built `__canonical_exact` structure.
- **Fixed scale:** the specification's `DECIMAL(38,6)` value.
- **Ordered bytes:** an order-preserving byte encoding.

| | Flat code + exact | Struct (built) | Fixed scale | Ordered bytes |
|---|---|---|---|---|
| C6, exact value | OK, up to 38 digits [M, ticket 12] | OK [M] | **Fails:** `1 ng`, `250 pg` and `4 ng` all store as `0.000000` and compare equal (#1). A comparison cannot rely on it alone [I] | OK, by construction [I] |
| Uniqueness (SHALL) | **Today: unique within a unit and across scale conversions, but not across offset units.** `37 Cel` and `98.6 [degF]` store at scale 20, `310.15 K` at scale 2, so SQL sees 2 distinct values for one magnitude. The planned strip makes all 13 groups unique [M, section 5] | As flat | Unique by type [I] | Unique by construction [I] |
| Row-group skipping | **None:** values are unordered across scales [M, [quantity-root](annotations-design-quantity-root.md) A] | None [M] | Skips: 5,039 rows scanned for 4,999 matches [M] | In principle yes. Spark's pushdown on binary columns and Delta's skipping on binary statistics are unverified, and Delta is believed not to skip [I] |
| C4, quantity output | Reads no unused annotation leaf [M, quantity-root A] | As flat [I] | As flat [I] | As flat [I] |
| H1 | Annotated comparison takes 51 ms against 61 ms computed [M, ticket 12] | As flat | Usable only as a pre-filter beside the exact value [I] | Needs a codec in the comparator [I] |
| P0, C10a | OK | OK | OK | OK |
| Pathling rework | Small: rename and flatten [I] | None | Add the field back, reversing decision 137 [I] | A codec user-defined function in the writer, a decode for arithmetic, and a second comparator path [I] |

Mitigations for the D6 impacts:

- **Skipping with an exact value:** keep a fixed-scale field beside the exact
  one. Each half is measured; the pair is not. It works on any Spark version
  [I].
- **The fixed-scale correctness loss:** mitigated only by keeping the exact
  value too.

## 3. Unmitigated impacts

| Impact | Size | Where it applies |
|---|---|---|
| Zipped: value-only read of a repeating primitive below a repeating parent | 79x (`name.given`) and 29x (`dosage.timing.event`) with heavy extensions; 2x and 3.5x with dates only [M] | Every Spark version. D3b untested. Mainstream views |
| `_x`: annotation read of a repeating primitive below a repeating parent | About 14x on 4.3.0 against `__x` and parent-level [M] | Spark ≤ 4.3 without D3b, and DBR 17.3 with Photon on [M]; DBR 19 untested for the rule and for the two-level case. On 4.3.0 it is specific to `_x`; on 4.0.2 every layout reads the whole parent. Rare now [I]; becomes common if hash-style annotations on repeating strings arrive |
| FlexiDecimal options (flat and struct): no row-group skipping on quantity comparisons | Every row group scanned [M] | Every engine. Only a fixed-scale field beside, or ordered bytes, would change it |
| Fixed scale alone: loss of small magnitudes | Wrong equality below about 10^-6 in canonical units [I, #1] | Format |
| On CF + ER, old runtimes still read whole: OSS 4.0–4.2, DBR 17.3 (measured with Photon on only), possibly DBR 18 | Up to whole-element reads (DBR `name.family` 7.3x) [M] | Not a layout matter. D3b is the bridge |

Gaps 2 and 4 were probed on 2026-11-05 (section 5). The other two are
unverified, and the pack marks them so:

1. D3b on zipped nested-lambda value reads.
3. Ordered-byte pushdown in Spark and Delta.

## 4. Rankings

### D4

**Format only.**

| Rank | Option | Why |
|---|---|---|
| 1 | `_x` | Plain values, so SQL equality is safe (C10a). One alignment rule, shared with FHIR JSON's ids and extensions. The quantity canonical stays inside its element, as in the legacy layout. Practically tied with `__x` |
| 2 | `__x` | `_x` stays exactly as FHIR defines it. The cost is a second positional array, and a quantity canonical beside its element rather than inside it |
| 3 | Parent | The specification today. One positional array per annotation, the most names to discover, and the most schema change per added annotation |
| 4 | Zipped | Native SQL equality, `DISTINCT` and `array_contains` compare metadata (C10a, measured). It departs from FHIR JSON and the legacy layout. It wins only on schema stability and alignment |

**Pathling-inclusive.**

| Rank | Option | Why |
|---|---|---|
| 1 | `_x` | Built, no rework, no risk to H1, no wait. The one cost is the about-14x read below a repeating parent, which only D3b covers (measured on cases 3c–3f, not on the probe's shape (c) [I]). It is now measured as specific to `_x` on 4.3.0 |
| 2 | `__x` | Avoids that read on 4.3.0 with no rule [M], but reworks tickets 11–14 and 41. Parent-level prunes more finely still, reading only the annotations used [M] |
| 3 | Parent | As `__x`, with heavier schema handling |
| 4 | Zipped | The 79x value read on every Spark version puts H1 at risk, and it needs the most rework |

### D5

Flat is best in both rankings. Skipping is equal, flat matches the
specification's `start` and `end` and the legacy quantity fields, and it adds
no nesting. Its only rework is flattening the quantity.

### D6

**Format only.**

| Rank | Option | Why |
|---|---|---|
| 1 | Flat code + exact | Exact, unique once stripped, and readable without a codec |
| 2 | Ordered bytes | Exact, unique and ordered, but opaque to every consumer (K9) |
| 3 | Struct | As flat, with nesting |
| 4 | Fixed scale alone | Readable and skippable, but loses magnitudes (C6) |

**Pathling-inclusive.** Best is flat code + exact, with a fixed-scale value
beside it as optional. The rework is small and the two together give exactness
and skipping.

### Where the rankings disagree

- **D4, `_x` against `__x`:** near-tied on format. `_x` leads once Pathling's
  rework and timing count. The probe of 2026-11-05 narrows the margin: on Spark
  4.3 the about-14x read is now measured as an `_x` cost that the other two
  placements avoid with no rule. The ranking stays as it is, because the shape
  is rare today and no benchmark view uses it. It would flip toward `__x`, or
  parent-level, if annotations on repeating primitives inside repeating
  elements became common, such as hashes on `name.given`.
- **D6, struct:** on format it ranks third. Pathling-inclusive it is the
  zero-rework option, so it would rank first if timing were all that counted.
- **D5:** no disagreement.

## 5. Probes run on 2026-11-05

### Uniqueness of the stored quantity canonical value

The probe uses Pathling's own canonicaliser, `QuantityCanonicalKind.canonicalOf`
then `toExactRow`, from the built `fhir-schema`. It covers 13 groups of equal
quantities, today and with the planned strip: the canonical value's trailing
zeros are stripped before encoding. The rows are written to Parquet and counted
with `count(DISTINCT ...)` in Spark 4.0.2 SQL.

Script: [scripts/annotations-design/canonical-uniqueness/](scripts/annotations-design/canonical-uniqueness/).
Results: `results-4.0.2.txt` beside it.

| Group | Today, distinct stored values | With strip |
|---|---|---|
| `0.195`, `0.1950`, `0.19500 g` | 1: (195, 3) | 1 |
| `37.0`, `37`, `37.00 Cel` | 1: (31015000000000000000000, 20, K) | 1: (31015, 2, K) |
| `37 Cel`, `310.15 K`, `310.150 K` | **2** | 1 |
| `0 Cel`, `273.15 K` | **2** | 1 |
| `98.6 [degF]`, `37 Cel`, `310.15 K` | **2** | 1 |
| Eight other groups: `m`/`cm`/`mm`, `kg`/`g`, `100 g`/`0.1 kg`, `mg`/`g`/`ug`, `[in_i]`/`cm`/`m`, `h`/`min`/`s`, `L`/`mL`/`m3`, `mmol/L`/`mol/L` | 1 each | 1 each |

**This corrects the earlier claim** of (195, 3) against (1950, 4). That pair is
what `FlexiDecimal.toValue` gives when called directly, but the stored
annotation goes through UCUM canonicalisation, which already normalises
multiplicative conversions. The real break is offset units. Celsius and
Fahrenheit come out at scale 20, while the same temperature written in kelvin
comes out at scale 2. So `GROUP BY` or a join on the stored value splits one
temperature in two. The planned strip fixes every case tried.

### Annotation reads of a repeating primitive below a repeating parent, by placement

The data is `MedicationRequest`-like: `dosage[].timing.event[]`, plus a top-level
`timing` for the direct case. There are 4,000 rows with extension-heavy
metadata (513,612 of 541,373 file bytes are extension leaves) and the null and
length edge cases of the pairing probe. The same logical data is written in
three placements:

- **X:** `_event[] {id, extension, __start, __end}`;
- **S:** `_event[] {id, extension}` and `__event[] {start, end}`;
- **P:** `_event[] {id, extension}`, `__event_start[]` and `__event_end[]`.

Pairing uses shape (c) with `coalesce` guards. No Pathling rule is installed.

Script: [scripts/annotations-design/two-level-annotations/](scripts/annotations-design/two-level-annotations/).
Results: `results-4.0.2.txt` and `results-4.3.0.txt` beside it.

Bytes read:

| Query | Spark | X (`_x`) | S (`__x`) | P (parent) |
|---|---|---|---|---|
| Direct: value + start + end | 4.0.2 and 4.3.0 | 20,775 | 20,775 | 20,773 |
| Direct: start only | 4.0.2 and 4.3.0 | 18,809 | 18,809 | 18,808 |
| In dosage lambda: value + start + end | 4.3.0 | **321,789**, extensions read | 22,452 | 22,398 |
| In dosage lambda: start only | 4.3.0 | **320,302**, extensions read | 20,965 (`start` and `end`) | 19,456 (`start` only) |
| In dosage lambda: value only (control) | 4.3.0 | 19,488 | 19,488 | 19,488 |
| Every in-lambda query | 4.0.2 | about 322,000: all of `dosage` | the same | the same |

Results are equal to layout X's in every query, on both versions.

Reading:

- On Spark 4.3.0, sibling `__x` and parent-level avoid the about-14x whole-`_x`
  read with no rule.
  - Sibling `__x` still reads its whole annotation group inside a lambda.
  - Parent-level reads exactly the annotation arrays used.
- On Spark 4.0.2 no placement helps: Spark reads the whole repeating parent, so
  only D3b can prune there. D3b was not run on this probe [I].
- The probe ran on OSS 4.0.2 and 4.3.0 only, not on Databricks.

## 6. Corrections

**2026-11-05, Photon and Databricks runtimes.** Earlier text said D3b misses the
pairing shapes "with Photon on" without naming the runtime. That was measured on
DBR 17.3 LTS only ([lambda-pruning-databricks](annotations-design-lambda-pruning-databricks.md)).
On DBR 19 the rule was not run; only compute-first was, on the name and address
views, with Photon on and off
([project-before-generate-databricks](annotations-design-project-before-generate-databricks.md)).
The two-level probe of section 5 ran on OSS 4.0.2 and 4.3.0 only. The text now
reads "DBR 17.3 with Photon on [M]; DBR 19 untested for the rule and for the
two-level case".

Also, "only D3b covers the `_x` read below a repeating parent" was marked [M]
as a whole. D3b pruning that read is measured on cases 3c–3f, which pair
`event` with the whole `_event` through `zip_with` or `arrays_zip`, on OSS 4.0.2
and 4.3.0 and DBR 17.3 with Photon off. D3b was not run on the two-level probe,
whose pairing is shape (c), so that part is [I].

The same check found that "compute-first prunes nothing on DBR 17.3" was
measured with Photon on only; Photon off was not run. Sections 1 and 3 now say
so.
