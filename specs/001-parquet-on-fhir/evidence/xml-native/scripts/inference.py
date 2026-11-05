"""What Spark infers from FHIR XML, with and without type inference, including mixed content.

Usage: python -I inference.py

Needs PySpark 4.0.
"""

from pyspark.sql import SparkSession

NS = 'xmlns="http://hl7.org/fhir"'
PATIENT = f"""<Patient {NS}>
   <text>
      <status value="generated"/>
      <div xmlns="http://www.w3.org/1999/xhtml">
         <p>Jane Smith, female, born 1980-03-15</p>
      </div>
   </text>
   <name>
      <family value="Smith"/>
      <given value="Jane"/>
   </name>
   <gender value="female"/>
   <birthDate value="1980-03-15"/>
</Patient>"""

CASES = [
    ("a Patient with one name", [PATIENT]),
    (
        "one name in every row",
        [f'<Patient {NS}><name><family value="A"/></name></Patient>'] * 2,
    ),
    (
        "one row with two names",
        [
            f'<Patient {NS}><name><family value="A"/></name></Patient>',
            f'<Patient {NS}><name><family value="B"/></name><name><family value="C"/></name></Patient>',
        ],
    ),
    (
        "typed values",
        [
            f'<Patient {NS}><gender value="male"/><birthDate value="1970-01-01"/>'
            f'<multipleBirthInteger value="2"/><active value="true"/></Patient>'
        ],
    ),
    (
        "full and partial dates in one column",
        [
            f'<Patient {NS}><birthDate value="1970-01-01"/></Patient>',
            f'<Patient {NS}><birthDate value="1970"/></Patient>',
        ],
    ),
    (
        "a string that looks like a date",
        [f'<Patient {NS}><identifier><value value="2106-3"/></identifier></Patient>'],
    ),
    (
        "a primitive carrying only an extension",
        [
            f'<Patient {NS}><birthDate><extension url="u"><valueString value="x"/></extension></birthDate></Patient>'
        ],
    ),
    (
        "a contained resource",
        [
            f'<Patient {NS}><contained><Observation><id value="o"/></Observation></contained></Patient>'
        ],
    ),
]

MIXED = {
    "text, element, text": "<R><div> ASS <p> dsds</p> dddd</div></R>",
    "two elements of one name": "<R><div>A <p>one</p> B <p>two</p> C</div></R>",
    "element before text": "<R><div><b>bold</b> x <p>para</p></div></R>",
}


def main() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")
    sc = spark.sparkContext
    for title, docs in CASES:
        print(f"== {title}")
        for opts in [{}, {"inferSchema": "false"}]:
            schema = (
                spark.read.options(rowTag="Patient", **opts)
                .xml(sc.parallelize(docs))
                .schema
            )
            print(f"   {str(opts) if opts else 'default':24} {schema.simpleString()}")

    print("== samplingRatio=0.1 over 200 rows with an id and one row with a gender")
    docs = [f'<Patient {NS}><id value="{i}"/></Patient>' for i in range(200)]
    docs.append(f'<Patient {NS}><gender value="male"/></Patient>')
    schema = (
        spark.read.options(rowTag="Patient", inferSchema="false", samplingRatio="0.1")
        .xml(sc.parallelize(docs))
        .schema
    )
    print(f"   {schema.simpleString()}")

    print("== mixed content: text interleaved with child elements")
    for title, xml in MIXED.items():
        rdd = sc.parallelize([xml])
        df = spark.read.option("rowTag", "R").xml(rdd)
        as_string = (
            spark.read.option("rowTag", "R").schema("div STRING").xml(rdd).first().div
        )
        print(f"   {title}: {xml}")
        print(
            f"      inferred:      {df.schema['div'].dataType.simpleString()} -> {df.first().div}"
        )
        print(f"      as STRING:     {as_string!r}")


if __name__ == "__main__":
    main()
