"""What Spark makes of a narrative, and two ways of recovering it from the raw document.

Usage: python -I narrative.py

Needs PySpark 4.0.
"""

from pyspark.sql import SparkSession

H = 'xmlns="http://www.w3.org/1999/xhtml"'
STATUS = '<status value="generated"/>'

# Finds the resource's own narrative: the first XHTML div that is closed immediately before a
# closing text element. Used only where the parser reports a div of the resource's own.
SPAN = r"'(?s)(<div\\s[^>]*xmlns=\"http://www\\.w3\\.org/1999/xhtml\".*?</div>)\\s*</text>'"

PARSED = [
    (
        "plain text",
        f"<Patient><text>{STATUS}<div {H}><p>Jane Smith, female</p></div></text></Patient>",
    ),
    (
        "escaped text, a comment and an attribute",
        f'<Patient><text>{STATUS}<div lang="en" {H}><p>Jane &lt;Smith&gt; &amp; co</p><!-- c --></div>'
        "</text></Patient>",
    ),
]

EDGES = {
    "nested divs": f"<Patient><text>{STATUS}<div {H}><div><p>a</p></div><div>b</div></div></text></Patient>",
    "whitespace before </text>": f"<Patient><text>{STATUS}<div {H}>a</div>\n   </text></Patient>",
    "comment before </text>": f"<Patient><text>{STATUS}<div {H}>a</div><!-- x --></text></Patient>",
    "attributes on the div": f'<Patient><text>{STATUS}<div lang="en" {H}>a</div></text></Patient>',
    "prefixed XHTML": f'<Patient><text>{STATUS}<h:div xmlns:h="http://www.w3.org/1999/xhtml">a</h:div>'
    "</text></Patient>",
    "only a contained resource has one": f"<Observation><contained><Patient><text>{STATUS}<div {H}>contained"
    "</div></text></Patient></contained></Observation>",
    "CDATA holding </div></text>": f"<Patient><text>{STATUS}<div {H}><pre><![CDATA[</div></text>]]></pre></div>"
    "</text></Patient>",
}

REWRITE_SOURCE = (
    f'<Patient xmlns="http://hl7.org/fhir"><text>{STATUS}<div {H}>\n'
    '  <p class="x">Jane &lt;Smith&gt; &amp; co "q"</p><!-- c -->\n\t<td><![CDATA[1 < 2]]></td>\n'
    '</div></text><gender value="female"/></Patient>'
)


def main() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")

    print("== 1. The div read as STRING, by from_xml and by the xs_any wildcard column")
    spark.createDataFrame(PARSED, "case STRING, value STRING").createOrReplaceTempView(
        "parsed"
    )
    for r in spark.sql(
        """SELECT case,
          from_xml(value, 'STRUCT<text: STRUCT<div: STRING>>').text.div AS as_string,
          from_xml(value, 'STRUCT<text: STRUCT<status: STRUCT<_value: STRING>, xs_any: STRING>>').text.xs_any
            AS wildcard
        FROM parsed"""
    ).collect():
        print(
            f"   {r.case}\n      as STRING: {r.as_string!r}\n      xs_any:    {r.wildcard!r}"
        )

    print("== 2. Extracting the narrative from the raw document")
    spark.createDataFrame(
        list(EDGES.items()), "case STRING, value STRING"
    ).createOrReplaceTempView("edges")
    for r in spark.sql(
        f"""SELECT case,
          from_xml(value, 'STRUCT<text: STRUCT<div: STRING>>').text.div IS NOT NULL AS has_own_div,
          nullif(regexp_extract(value, {SPAN}, 1), '') AS extracted
        FROM edges"""
    ).collect():
        print(f"   {r.case:36} own div={r.has_own_div!s:5} -> {r.extracted}")

    print("== 3. Rewriting the narrative as a value attribute before parsing")
    spark.createDataFrame([(REWRITE_SOURCE,)], "value STRING").createOrReplaceTempView(
        "source"
    )
    # Newlines, carriage returns and tabs are escaped too, because XML normalises them to spaces
    # inside an attribute value.
    spark.sql(
        f"""CREATE OR REPLACE TEMPORARY VIEW rewritten AS
        WITH s AS (SELECT value, regexp_extract(value, {SPAN}, 1) AS span FROM source)
        SELECT span, CASE WHEN span = '' THEN value ELSE replace(value, span, concat('<div value="',
          replace(replace(replace(replace(replace(replace(replace(span,
            '&', '&amp;'), '<', '&lt;'), '>', '&gt;'), '"', '&quot;'),
            '\\n', '&#10;'), '\\r', '&#13;'), '\\t', '&#9;'), '"/>')) END AS value
        FROM s"""
    )
    r = spark.sql(
        """SELECT span, from_xml(value,
          'STRUCT<text: STRUCT<div: STRUCT<_value: STRING>>, gender: STRUCT<_value: STRING>>') AS p
        FROM rewritten"""
    ).first()
    print(f"   div read back: {r.p.text.div._value!r}")
    print(
        f"   identical to the source span: {r.p.text.div._value == r.span}; gender: {r.p.gender._value}"
    )
    rdd = spark.table("rewritten").select("value").rdd.map(lambda x: x.value)
    inferred = (
        spark.read.option("rowTag", "Patient")
        .option("inferSchema", "false")
        .xml(rdd)
        .schema
    )
    print(f"   inferred: {inferred.simpleString()}")


if __name__ == "__main__":
    main()
