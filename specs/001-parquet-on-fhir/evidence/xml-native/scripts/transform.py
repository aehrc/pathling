"""A prototype of FHIR XML ingest by schema inference and generated SQL, with no FHIR parser.

Usage: python -I transform.py

Part 1 infers which paths are present, rewrites the inferred schema against a stand-in for the
FHIR definitions, parses again with from_xml and reshapes with generated SQL. Part 2 reads with
the inferred schema itself and fixes cardinality and types in the SQL instead. Part 3 covers
bundles, decimal text, parse modes and whitespace. Needs PySpark 4.0.
"""

import json

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType

NS = 'xmlns="http://hl7.org/fhir"'
H = 'xmlns="http://www.w3.org/1999/xhtml"'
DOCS = [
    f"""<Patient {NS}><id value="p1"/>
  <meta><lastUpdated value="2020-01-01T10:00:00.120+10:00"/></meta>
  <text><status value="generated"/><div {H}><p>Hello <b>bold</b> &amp; world</p></div></text>
  <extension url="http://ex/race"><valueCoding><system value="urn:r"/><code value="2106-3"/></valueCoding></extension>
  <identifier><system value="urn:mrn"/><value value="  12 34  "/></identifier>
  <active value="true"/>
  <name><family value="Smith"/><given value="John"/><given id="g2" value="Q"><extension url="http://ex/q"><valueString value="x"/></extension></given></name>
  <gender value="male"/>
  <birthDate value="1970-01-01"><extension url="http://ex/t"><valueDateTime value="1970-01-01T05:00:00Z"/></extension></birthDate>
  <multipleBirthInteger value="2"/>
  <contained><Patient><id value="c1"/></Patient></contained>
</Patient>""",
    f'<Patient {NS}><id value="p2"/><name><family value="Doe"/></name><deceasedBoolean value="false"/>'
    '<photo><data value="aGVsbG8g\nd29ybGQ="/></photo></Patient>',
    f'<Observation {NS}><id value="o2"/><contained><Patient><id value="cp"/></Patient></contained>'
    '<status value="final"/></Observation>',
    f'<Patient {NS}><id value="pd"/><text><status value="generated"/><div {H}>a &lt;b&gt; not a tag <i>tag</i>'
    ' &amp; <!-- c --> x</div></text><name><family value="  Sp  ace "/></name></Patient>',
]

# A stand-in for the definitions: each type's elements in definition order, as
# (element, type, repeating). A type whose name begins with a lower-case letter is a primitive.
DEFS = {
    "Patient": [
        ("id", "id", 0),
        ("meta", "Meta", 0),
        ("text", "Narrative", 0),
        ("extension", "Extension", 1),
        ("identifier", "Identifier", 1),
        ("active", "boolean", 0),
        ("name", "HumanName", 1),
        ("gender", "code", 0),
        ("birthDate", "date", 0),
        ("deceasedBoolean", "boolean", 0),
        ("multipleBirthInteger", "integer", 0),
        ("photo", "Attachment", 1),
    ],
    "Meta": [("extension", "Extension", 1), ("lastUpdated", "instant", 0)],
    "Narrative": [
        ("extension", "Extension", 1),
        ("status", "code", 0),
        ("div", "xhtml", 0),
    ],
    "Extension": [
        ("extension", "Extension", 1),
        ("url", "uri", 0),
        ("valueCoding", "Coding", 0),
        ("valueDateTime", "dateTime", 0),
        ("valueString", "string", 0),
    ],
    "Identifier": [
        ("extension", "Extension", 1),
        ("system", "uri", 0),
        ("value", "string", 0),
    ],
    "HumanName": [
        ("extension", "Extension", 1),
        ("family", "string", 0),
        ("given", "string", 1),
    ],
    "Attachment": [("extension", "Extension", 1), ("data", "base64Binary", 0)],
    "Coding": [
        ("extension", "Extension", 1),
        ("system", "uri", 0),
        ("code", "code", 0),
    ],
}
STORE = {"boolean": "BOOLEAN", "integer": "INT", "base64Binary": "BINARY"}
ROOT = r"'^\\s*(?:<\\?xml[^>]*\\?>\\s*)?(?:<!--.*?-->\\s*)*<([A-Za-z]+)[\\s>]'"
SPAN = r"'(?s)(<div\\s[^>]*xmlns=\"http://www\\.w3\\.org/1999/xhtml\".*?</div>)\\s*</text>'"


def is_primitive(fhir_type: str) -> bool:
    """Returns whether a stand-in type is a primitive."""
    return fhir_type[0].islower()


def element_of(data_type):
    """Returns the element type of an array, or the type itself."""
    return data_type.elementType if isinstance(data_type, ArrayType) else data_type


def fix(inferred: StructType, fhir_type: str, path: str, dropped: list) -> str:
    """Rewrites an inferred structure against a type, returning DDL for the parse schema."""
    children = {name: (child, rep) for name, child, rep in DEFS[fhir_type]}
    out = []
    for f in inferred.fields:
        if f.name == "_id" or (f.name == "_url" and fhir_type == "Extension"):
            out.append(f"{f.name}: STRING")
            continue
        if f.name.startswith("_"):
            continue
        if f.name not in children:
            dropped.append(f"{path}.{f.name}")
            continue
        child, rep = children[f.name]
        inner = element_of(f.dataType)
        if child == "xhtml":
            ddl = "STRING"
        elif is_primitive(child):
            parts = ["_value: STRING"]
            if isinstance(inner, StructType):
                if "_id" in inner.fieldNames():
                    parts.append("_id: STRING")
                if "extension" in inner.fieldNames():
                    ext = element_of(inner["extension"].dataType)
                    parts.append(
                        f"extension: ARRAY<{fix(ext, 'Extension', f'{path}.{f.name}.extension', dropped)}>"
                    )
            ddl = f"STRUCT<{', '.join(parts)}>"
        else:
            ddl = fix(inner, child, f"{path}.{f.name}", dropped)
        out.append(f"`{f.name}`: " + (f"ARRAY<{ddl}>" if rep else ddl))
    return f"STRUCT<{', '.join(out)}>"


def value(e: str, fhir_type: str) -> str:
    """Converts a primitive's value attribute to its stored type."""
    v = f"{e}._value"
    if fhir_type == "base64Binary":
        return f"try_to_binary({v}, 'base64')"
    return f"CAST({v} AS {STORE[fhir_type]})" if fhir_type in STORE else v


def metadata(e: str, st):
    """Returns the metadata group of a primitive and the condition for its presence, or None."""
    names = st.fieldNames() if isinstance(st, StructType) else []
    present = [n for n in ["_id", "extension"] if n in names]
    if not present:
        return None
    parts = []
    if "_id" in names:
        parts.append(f"'id', {e}._id")
    if "extension" in names:
        ext = st["extension"].dataType.elementType
        parts.append(
            f"'extension', transform({e}.extension, x -> {gen('x', ext, 'Extension')})"
        )
    condition = " OR ".join(f"{e}.{n} IS NOT NULL" for n in present)
    return f"CASE WHEN {condition} THEN named_struct({', '.join(parts)}) END", condition


def gen(e: str, st: StructType, fhir_type: str) -> str:
    """Generates the expression reshaping a parsed structure to the layout, in definition order."""
    parts = []
    if "_id" in st.fieldNames():
        parts.append(f"'id', {e}._id")
    for name, child, rep in DEFS[fhir_type]:
        if name == "url" and fhir_type == "Extension":
            parts.append(f"'url', {e}._url")
            continue
        if name not in st.fieldNames():
            continue
        inner = element_of(st[name].dataType)
        if child == "xhtml":
            # The parsed div only says whether the resource has a narrative of its own. Its value is
            # taken from the raw document, because the parse has decoded its text.
            parts.append(f"'{name}', CASE WHEN {e}.{name} IS NOT NULL THEN __div END")
        elif is_primitive(child) and rep:
            parts.append(f"'{name}', transform({e}.{name}, x -> {value('x', child)})")
            meta = metadata("x", inner)
            if meta:
                parts.append(
                    f"'_{name}', CASE WHEN exists({e}.{name}, x -> {meta[1]}) THEN transform({e}.{name}, x -> {meta[0]}) END"
                )
        elif is_primitive(child):
            parts.append(f"'{name}', {value(f'{e}.{name}', child)}")
            meta = metadata(f"{e}.{name}", inner)
            if meta:
                parts.append(f"'_{name}', {meta[0]}")
        elif rep:
            var = f"x{len(e)}"
            parts.append(
                f"'{name}', transform({e}.{name}, {var} -> {gen(var, inner, child)})"
            )
        else:
            parts.append(f"'{name}', {gen(f'{e}.{name}', inner, child)}")
    # A structure absent from a row stays absent, rather than becoming an empty object.
    return f"CASE WHEN {e} IS NOT NULL THEN named_struct({', '.join(parts)}) END"


def part1(spark: SparkSession) -> None:
    print(
        "== Part 1: infer presence, correct the schema, parse with from_xml, reshape with generated SQL"
    )
    spark.createDataFrame([(x,) for x in DOCS], "value STRING").createOrReplaceTempView(
        "raw"
    )
    spark.sql(
        f"CREATE OR REPLACE TEMPORARY VIEW docs AS SELECT value FROM raw WHERE regexp_extract(value, {ROOT}, 1) = 'Patient'"
    )
    inferred = (
        spark.read.option("rowTag", "Patient")
        .option("inferSchema", "false")
        .xml(spark.table("docs").rdd.map(lambda r: r.value))
        .schema
    )
    dropped = []
    ddl = fix(inferred, "Patient", "Patient", dropped)
    print(f"   paths the definitions do not describe, to be reported: {dropped}")
    parsed = spark.createDataFrame([], f"r {ddl}").schema["r"].dataType
    sql = f"""SELECT to_json(named_struct('resourceType', 'Patient', 'r', {gen("r", parsed, "Patient")})) AS j
      FROM (SELECT from_xml(value, '{ddl}', map('ignoreSurroundingSpaces', 'false', 'mode', 'FAILFAST')) AS r,
                   nullif(regexp_extract(value, {SPAN}, 1), '') AS __div
            FROM docs)"""
    print(f"   generated SQL: {len(sql)} characters")
    for row in spark.sql(sql).collect():
        doc = json.loads(row.j)
        doc.update(doc.pop("r"))
        print(f"   {json.dumps(doc)}")


def part2(spark: SparkSession) -> None:
    print(
        "== Part 2: read with the inferred all-string schema and fix cardinality and types in SQL"
    )
    docs = [
        f'<Patient {NS}><id value="p1"/><name><family value="A"/><given value="X"/></name><active value="true"/>'
        '<multipleBirthInteger value="2"/></Patient>',
        f'<Patient {NS}><id value="p2"/><name><family value="B"/></name><active value="yes"/></Patient>',
    ]
    df = spark.read.options(rowTag="Patient", inferSchema="false").xml(
        spark.sparkContext.parallelize(docs)
    )
    print(f"   inferred: {df.schema.simpleString()}")
    df.createOrReplaceTempView("p")

    def as_array(e, t):
        return (
            e
            if isinstance(t, ArrayType)
            else f"CASE WHEN {e} IS NULL THEN NULL ELSE array({e}) END"
        )

    name_t = df.schema["name"].dataType
    given_t = element_of(name_t)["given"].dataType
    lenient = spark.sql("SELECT CAST('yes' AS BOOLEAN) AS b").first().b
    print(f"   Spark's own cast of 'yes' to BOOLEAN: {lenient}")
    out = spark.sql(
        f"""SELECT id._value AS id,
          transform({as_array("name", name_t)}, n -> named_struct('family', n.family._value,
              'given', transform({as_array("n.given", given_t)}, g -> g._value))) AS name,
          CASE active._value WHEN 'true' THEN true WHEN 'false' THEN false END AS active,
          CASE WHEN active._value NOT IN ('true', 'false') THEN 'Patient.active: ' || active._value END AS finding,
          CAST(multipleBirthInteger._value AS INT) AS multipleBirthInteger
        FROM p"""
    )
    print(f"   result: {out.schema.simpleString()}")
    for r in out.collect():
        print(f"   {r}")


def part3(spark: SparkSession) -> None:
    print(
        "== Part 3a: a bundle, its entries exploded and a URN reference resolved, in SQL"
    )
    bundle = (
        f'<Bundle {NS}><type value="collection"/>'
        '<entry><fullUrl value="urn:uuid:aaa"/><resource><Patient><id value="bp1"/></Patient></resource></entry>'
        '<entry><resource><Observation><id value="bo1"/><status value="final"/><subject><reference value="urn:uuid:aaa"/>'
        '</subject><valueQuantity><value value="1e2"/></valueQuantity></Observation></resource></entry></Bundle>'
    )
    spark.createDataFrame([(bundle,)], "value STRING").createOrReplaceTempView(
        "bundles"
    )
    spark.sql(
        """CREATE OR REPLACE TEMPORARY VIEW bx AS SELECT from_xml(value, 'STRUCT<entry: ARRAY<STRUCT<
          fullUrl: STRUCT<_value: STRING>, resource: STRUCT<Patient: STRUCT<id: STRUCT<_value: STRING>>,
          Observation: STRUCT<id: STRUCT<_value: STRING>, subject: STRUCT<reference: STRUCT<_value: STRING>>,
          valueQuantity: STRUCT<value: STRUCT<_value: STRING>>>>>>>').entry AS entry FROM bundles"""
    )
    spark.sql(
        """WITH u AS (SELECT entry, map_from_entries(filter(transform(entry,
              x -> named_struct('k', x.fullUrl._value, 'v', concat('Patient/', x.resource.Patient.id._value))),
              p -> p.k IS NOT NULL AND p.v IS NOT NULL)) AS m FROM bx)
        SELECT o.id._value AS id, coalesce(m[o.subject.reference._value], o.subject.reference._value) AS subject,
               o.valueQuantity.value._value AS decimal_source,
               CAST(CAST(o.valueQuantity.value._value AS DOUBLE) AS STRING) AS decimal_via_double
        FROM u LATERAL VIEW explode(transform(entry, x -> x.resource.Observation)) t AS o WHERE o IS NOT NULL"""
    ).show(truncate=False)

    print(
        "== Part 3b: decimal text through a double, as the JSON route stores it (decision 68)"
    )
    spark.sql(
        "SELECT x AS source, CAST(CAST(x AS DOUBLE) AS STRING) AS via_double FROM VALUES "
        "('1.50'), ('0.0001'), ('12345678901234567890.5'), ('3'), ('1e2') AS v(x)"
    ).show(truncate=False)

    print("== Part 3c: an invalid boolean under each parse mode")
    bad = [f'<Patient {NS}><id value="b1"/><active value="yes"/></Patient>']
    spark.createDataFrame([(x,) for x in bad], "value STRING").createOrReplaceTempView(
        "bad"
    )
    schema = "STRUCT<id: STRUCT<_value: STRING>, active: STRUCT<_value: BOOLEAN>>"
    try:
        spark.sql(
            f"SELECT from_xml(value, '{schema}', map('mode', 'FAILFAST')) FROM bad"
        ).collect()
        print("   FAILFAST: no error")
    except Exception as e:  # noqa: BLE001 - the error class is what is being reported.
        print(f"   FAILFAST: fails the job ({type(e).__name__})")
    r = (
        spark.sql(
            f"SELECT from_xml(value, '{schema}', map('mode', 'PERMISSIVE')) AS r FROM bad"
        )
        .first()
        .r
    )
    print(f"   PERMISSIVE: {r}")

    print("== Part 3d: surrounding whitespace in an attribute value")
    ws = [f'<Patient {NS}><name><family value="  Sp  ace "/></name></Patient>']
    spark.createDataFrame([(x,) for x in ws], "value STRING").createOrReplaceTempView(
        "ws"
    )
    for opt in ["true", "false"]:
        v = (
            spark.sql(
                f"SELECT from_xml(value, 'STRUCT<name: ARRAY<STRUCT<family: STRUCT<_value: STRING>>>>', "
                f"map('ignoreSurroundingSpaces', '{opt}')).name[0].family._value AS v FROM ws"
            )
            .first()
            .v
        )
        print(f"   ignoreSurroundingSpaces={opt}: {v!r}")

    print("== Part 3e: a singular element given twice")
    dup = [f'<Patient {NS}><gender value="male"/><gender value="female"/></Patient>']
    spark.createDataFrame([(x,) for x in dup], "value STRING").createOrReplaceTempView(
        "dup"
    )
    v = (
        spark.sql(
            "SELECT from_xml(value, 'STRUCT<gender: STRUCT<_value: STRING>>').gender._value AS v FROM dup"
        )
        .first()
        .v
    )
    print(f"   gender: {v!r}")


def main() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")
    part1(spark)
    part2(spark)
    part3(spark)


if __name__ == "__main__":
    main()
