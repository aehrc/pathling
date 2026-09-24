"""combine / union across two collections of the same FHIR type whose fitted
SQL schemas differ.

  Patient.name         -> array<struct<id, family>>
  Patient.contact.name -> array<struct<id, given, period<start>>>

combine = concat(l, r); union = array_union(l, r). Both need a common element
type. Cases: one side void (absent), and both sides present but differently
shaped.
"""
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("combine").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.sql("""
  select array(named_struct('id','1','family','Smith')) as a,
         array(named_struct('id','2','given',array('Jo'),
                            'period', named_struct('start','2020'))) as b
""")
print("a:", df.schema["a"].dataType.simpleString())
print("b:", df.schema["b"].dataType.simpleString())


def case(label, expr):
    try:
        d = df.selectExpr(f"{expr} as x")
        print(f"OK   {label}\n       -> {d.schema['x'].dataType.simpleString()}"
              f"\n       -> {d.collect()[0][0]}")
    except Exception as e:
        print(f"FAIL {label}\n       -> {str(e).split(chr(10))[0][:130]}")


print("\n=== 1. one side absent (void) ===")
case("concat(array<void>, a)", "concat(cast(null as array<void>), a)")
case("array_union(array<void>, a)", "array_union(cast(null as array<void>), a)")

print("\n=== 2. both present, different fitted shapes ===")
case("concat(a, b)", "concat(a, b)")
case("array_union(a, b)", "array_union(a, b)")

print("\n=== 3. does a positional struct cast silently misalign? ===")
case("cast a as b's type",
     "cast(a as array<struct<id:string,given:array<string>,"
     "period:struct<start:string>>>)")

print("\n=== 4. fix: by-name projection to the merged type, then concat ===")
MERGED_A = ("transform(a, x -> named_struct("
            "'id', x.id, 'family', x.family,"
            " 'given', cast(null as array<string>),"
            " 'period', cast(null as struct<start:string>)))")
MERGED_B = ("transform(b, x -> named_struct("
            "'id', x.id, 'family', cast(null as string),"
            " 'given', x.given, 'period', x.period))")
case("concat(merged a, merged b)", f"concat({MERGED_A}, {MERGED_B})")
case("array_union(merged a, merged b)", f"array_union({MERGED_A}, {MERGED_B})")

print("\n=== 5. traversal into the merged result ===")
case("merged.family", f"transform(concat({MERGED_A}, {MERGED_B}), x -> x.family)")

print("\n=== 6. array_union with a void side then traversal ===")
case("union(void, a).family",
     "transform(array_union(cast(null as array<void>), a), x -> x.family)")

spark.stop()
