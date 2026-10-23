"""Ticket 32: the stored schema and rows of every route at size s.

Every XML route is compared with XML lines exactly, and every JSON bundle route with JSON lines exactly. XML lines
are compared with JSON lines with every decimal cast to a double first, because XML stores a decimal's text and
JSON the text of a double (owner, 2026-10-22). Schemas must be equal in every comparison.
Usage: gate.py <jsonDir> <pathling-compare dir> <jsonBundleDir>
"""
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType
from pathling import PathlingContext
from pathling.fhir import MimeType
pc = PathlingContext.create(enable_terminology=False); spark = pc.spark; spark.sparkContext.setLogLevel("ERROR")
D, H, J = sys.argv[1:4]
XML = MimeType.FHIR_XML
jvm = spark._jvm
defs = jvm.au.csiro.pathling.definition.fhir.FhirDefinitionContext.of(pc._jpc.getFhirEncoders().getContext())

def by_value(col, data_type, node):
    """A column with every decimal beneath it cast to a double."""
    if isinstance(data_type, ArrayType):
        return F.transform(col, lambda v: by_value(v, data_type.elementType, node))
    fields = []
    for f in data_type.fields:
        entry = node.entry(f.name)
        c = col[f.name]
        if entry.isPresent() and entry.get().isElement():
            e = entry.get(); prim = e.primitiveType()
            inner = f.dataType.elementType if isinstance(f.dataType, ArrayType) else f.dataType
            if prim.isPresent() and prim.get().toCode() == "decimal":
                c = F.transform(c, lambda v: v.cast("double")) if isinstance(f.dataType, ArrayType) else c.cast("double")
            elif not prim.isPresent() and isinstance(inner, StructType):
                c = by_value(c, f.dataType, node.elementStructure(e).get())
        fields.append(c.alias(f.name))
    return F.when(col.isNotNull(), F.struct(*fields))

def decimals_by_value(df, t):
    node = jvm.au.csiro.pathling.schema.DefinitionCanonicalStructure.forResource(defs, t)
    s = by_value(F.struct(*[F.col(c) for c in df.columns]), df.schema, node).alias("s")
    return df.select(s).select("s.*")

def compare(label, df, ref):
    same = df.schema == ref.schema
    extra = df.exceptAll(ref).count() if same else -1
    missing = ref.exceptAll(df).count() if same else -1
    ok = same and extra == 0 and missing == 0
    print(label, "schema", "equal" if same else "DIFFERENT", "extra", extra, "missing", missing, "OK" if ok else "FAIL", flush=True)
    return ok

bundles = spark.read.text(f"{H}/data/xml-bundles/s", wholetext=True).cache()
txn = spark.read.text(f"{H}/data/xml-bundles-txn/s", wholetext=True).cache()
jcol = spark.read.text(f"{J}/collection.ndjson").cache()
jtxn = spark.read.text(f"{J}/transaction.ndjson").cache()
bad = 0
for t in ["Patient", "Condition", "Encounter", "Observation"]:
    json_lines = pc.encode(spark.read.text(f"{D}/{t}.ndjson"), t).cache()
    xml_lines = pc.encode(spark.read.text(f"{H}/data/xml/s/{t}.ndxml"), t, input_type=XML).cache()
    print(t, "rows", json_lines.count())
    bad += not compare(f"{t} xml-lines/json-lines, decimals by value", decimals_by_value(xml_lines, t), decimals_by_value(json_lines, t))
    bad += not compare(f"{t} xml-bundles/xml-lines", pc.encode_bundle(bundles, t, input_type=XML), xml_lines)
    bad += not compare(f"{t} xml-bundles-txn/xml-lines", pc.encode_bundle(txn, t, input_type=XML), xml_lines)
    bad += not compare(f"{t} json-bundles/json-lines", pc.encode_bundle(jcol, t), json_lines)
    bad += not compare(f"{t} json-bundles-txn/json-lines", pc.encode_bundle(jtxn, t), json_lines)
print("ALL EQUAL" if bad == 0 else f"{bad} DIFFERENT")
