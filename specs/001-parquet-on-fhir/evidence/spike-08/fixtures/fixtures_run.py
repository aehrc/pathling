"""Runs the ingress-strictness fixtures against A0: HEAD's JSON route, the from_xml XML route, and, as a reference,
HEAD's HAPI XML route. Expected failures are the point: A0 applies no per-value validation.

Each fixture is encoded alone, then decoded, and the outcome is compared with what decision 133 expects of it:
  store      conformant (or breaking only an element-level constraint, or a text-only type passed through):
             stored, and decoded back equal to the input (JSON numbers compared by their text, XML canonically)
  invalid    fail by default, naming the resource, path and value (or, under drop, removed with a finding)
  contained  dropped with an informational finding, never failing
  left-out   a document of another type, not selected
  reject     a document that is not one resource (malformed, duplicate key, top-level array, trailing text)
  unsettled  decision 133 does not say
A "finding" here is what the route reports today: StrictnessCheck's column-level findings (and, for from_xml,
the paths its prune dropped). Neither is per value, so neither meets decision 133's _findings column.

Then every fixture is encoded again in one batch with the other fixtures of its type, and a stored row that
differs from the row stored alone is flagged: principle 6 (the same input gives the same outcome).

Usage: fixtures_run.py <harnessScriptsDir> <ingressStrictnessDir> <spike08FixturesDir>
"""
import json, os, re, sys
import xml.etree.ElementTree as ET
from pyspark.sql import DataFrame, functions as F
from pathling import PathlingContext

scripts, strictness, here = sys.argv[1:]
sys.path.insert(0, scripts)
from fromxml_route import XmlRoute

XML = "application/fhir+xml"
pc = PathlingContext.create(enable_terminology=False)
spark = pc.spark
spark.sparkContext.setLogLevel("OFF")
route = XmlRoute(pc)
jvm = spark._jvm


def load(path):
    return [tuple(line.rstrip("\n").split("\t", 2)) for line in open(path) if line.strip()]


json_fixtures = (load(f"{strictness}/fixtures.tsv") + load(f"{strictness}/fixtures-lexical.tsv")
                 + load(f"{here}/fixtures-json-extra.tsv"))
xml_fixtures = load(f"{here}/fixtures-xml.tsv")

EXPECT = {
    **{k: "store" for k in ["c0-conformant", "c0-decimal-text", "c0-prim-ext-only", "c0-prim-ext-array",
                            "c0-prim-ext-array-only", "c0-value-integer", "c0-marital", "c0-family",
                            "c0-identifier", "c0-quantity-integral",
                            # Element-level constraints are not checked; text-only types pass through.
                            "c6-bad-code-required", "c6-ext-value-and-nested", "c6-bad-id", "c6-bad-uri-ws",
                            "l-code-whitespace", "l-code-required-case", "l-uuid-no-prefix",
                            "l-string-whitespace-only",
                            "x-prefixed", "x-value-surrounding-space", "x-narrative-markup", "c2-fhir-comments@xml"]},
    **{k: "invalid" for k in ["c2-null", "c2-empty-obj", "c2-empty-arr", "c2-empty-string", "c2-fhir-comments",
                              "c3-unknown-root", "c3-unknown-nested", "c3-r5-field",
                              "c4-string-for-integer", "c4-string-for-decimal", "c4-number-for-string",
                              "c4-string-for-boolean", "c4-object-for-string",
                              "c5-scalar-for-array", "c5-array-for-scalar", "c5-nested-array",
                              "c5-array-for-string-primitive",
                              "c6-bad-date", "c6-int-overflow",
                              "l-datetime-no-tz", "l-datetime-no-seconds", "l-date-no-padding", "l-date-with-time",
                              "l-instant-date-only", "l-instant-no-tz", "l-time-bad", "l-positiveint-zero",
                              "l-decimal-string-bad", "l-base64-bad",
                              "x-text-content", "x-attr-on-resource"]},
    "c7-contained": "contained",
    "c7-unknown-resourcetype": "left-out", "c7-wrong-resourcetype": "left-out",
    **{k: "reject" for k in ["c1-malformed", "c1-duplicate-key", "c1-top-level-array", "c1-trailing-text",
                             "c1-duplicate-attribute", "c1-two-roots", "x-no-namespace"]},
    "c6-choice-two-types": "unsettled", "c7-no-resourcetype": "unsettled",
}


def expected(fid):
    base = fid[2:] if fid.startswith("x-") else fid
    if fid == "x-c2-fhir-comments":
        return EXPECT["c2-fhir-comments@xml"]
    return EXPECT.get(fid, EXPECT.get(base, "?"))


def target_type(doc, fid):
    m = re.search(r'"resourceType"\s*:\s*"([A-Za-z]+)"', doc) or re.search(r"^<(?:\w+:)?([A-Z][A-Za-z]+)", doc)
    t = m.group(1) if m else "Patient"
    # A document of an unknown or another type is offered to the Patient read, as a mixed file would be.
    return "Patient" if t in ("Florb",) or fid.endswith("wrong-resourcetype") else t


def error_text(e):
    text = str(e)
    causes = sorted(set(re.findall(r"\[([A-Z_]+(?:\.[A-Z_]+)*)\]", text)))
    # HAPI's own message, where HAPI failed inside a task.
    hapi = re.search(r"(HAPI-\d+: [^\n]{0,140})", text)
    if hapi:
        return ("HAPI " + hapi.group(1)).strip()
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    first = next((l for l in lines if re.match(r"^(:\s*)?[a-z][\w.]*(Exception|Error)\b", l)), lines[0] if lines else "")
    return (" ".join(causes) + " " + first[:150]).strip()


def canonical(doc):
    """XML canonical form, prefixes rewritten so that a prefixed and a default namespace compare equal."""
    return ET.canonicalize(xml_data=doc, rewrite_prefixes=True)


def lexical(text):
    """Parses JSON keeping every number's text, so 1.50 and 1.5 differ."""
    return json.loads(text, parse_float=lambda s: ("num", s), parse_int=lambda s: ("num", s))


def json_diff(a, b, path="", out=None):
    out = [] if out is None else out
    if isinstance(a, dict) and isinstance(b, dict):
        for k in list(a) + [k for k in b if k not in a]:
            json_diff(a.get(k, "<absent>"), b.get(k, "<absent>"), f"{path}.{k}", out)
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b)):
            json_diff(x, y, f"{path}[{i}]", out)
    elif a != b:
        def show(v):
            return v[1] if isinstance(v, tuple) else json.dumps(v)
        out.append(f"{path[1:]}: {show(a)[:40]} -> {show(b)[:40]}")
    return out


def column_findings(t, observed_schema):
    return [str(f) for f in route.transformer.findings(t, observed_schema)]


def json_observed(df):
    strings = getattr(df._jdf, "as")(jvm.org.apache.spark.sql.Encoders.STRING())
    return spark._jsparkSession.read().option("mode", "FAILFAST").json(strings).schema()


def stored_rows(encoded):
    return [json.dumps(r.asDict(recursive=True), default=str, sort_keys=True) for r in encoded.collect()]


def run(fid, doc, mode):
    """mode: json (HEAD), fromxml, hapi. Returns a dict describing the outcome."""
    t = target_type(doc, fid)
    df = spark.createDataFrame([(doc,)], "value string")
    out = {"type": t}
    try:
        if mode == "json":
            encoded = pc.encode(df, t)
            out["findings"] = column_findings(t, json_observed(df))
        elif mode == "fromxml":
            encoded = route.fit_lines(df, t)
            out["findings"] = sorted(set(route.undescribed)) + column_findings(t, route.shaped._jdf.schema())
        else:
            encoded = pc.encode(df, t, input_type=XML)
            out["findings"] = None
        rows = stored_rows(encoded)
        out["rows"] = rows
        if not rows:
            out["outcome"] = "0 rows"
            return out
        if mode == "json":
            decoded = [r[0] for r in DataFrame(pc._jpc.decode(encoded._jdf, t, "application/fhir+json"), spark).collect()]
            try:
                source = lexical(doc)
            except ValueError:
                source = None
            diffs = (["the input is not one JSON object, so it cannot be compared"] if source is None or len(rows) != 1
                     else [d for x in decoded for d in json_diff(source, lexical(x))])
        else:
            decoded = [r[0] for r in (route.decode(encoded, t) if mode == "fromxml" else
                                      DataFrame(pc._jpc.decode(encoded._jdf, t, XML), spark)).collect()]
            diffs = []
            try:
                a = canonical(doc)
            except ET.ParseError:
                a = None
                diffs.append("the input is not well-formed XML, so it cannot be compared")
            for x in decoded if a is not None else []:
                b = canonical(x)
                if a != b:
                    n = next((i for i in range(min(len(a), len(b))) if a[i] != b[i]), min(len(a), len(b)))
                    diffs.append(f"@{n}: {a[max(0, n - 25):n + 35]!r} -> {b[max(0, n - 25):n + 35]!r}")
        out["decoded"] = decoded
        out["diffs"] = diffs
        out["outcome"] = (f"{len(rows)} rows; " if len(rows) != 1 else "stored; ") + ("round-trips equal" if not diffs else "differs: " + "; ".join(diffs[:4]))
    except Exception as e:
        out["outcome"] = "ERROR " + error_text(e)
    return out


def verdict(exp, o):
    err = o["outcome"].startswith("ERROR")
    finding = bool(o.get("findings"))
    if exp == "store":
        return "yes" if o["outcome"] == "stored; round-trips equal" else "no"
    if exp in ("invalid", "reject"):
        if err:
            return "yes (fails, whole job)"
        return "no (column-level finding only)" if finding else "no (silent)"
    if exp == "contained":
        if err:
            return "no (fails)"
        return "partial (dropped, column-level finding)" if finding else "no (dropped silently)"
    if exp == "left-out":
        return "yes" if o["outcome"] == "0 rows" else "no"
    return "-"


out_txt = open(f"{here}/fixtures_run.txt", "w")


def emit(line):
    out_txt.write(line + "\n")
    out_txt.flush()


emit(f"spark {spark.version}")
for jar in sorted(__import__("glob").glob(spark.conf.get("spark.jars.ivy", "") + "/jars/au.csiro.pathling_library-runtime-*.jar")):
    emit(f"pathling jar {os.path.basename(jar)} {__import__('hashlib').sha256(open(jar, 'rb').read()).hexdigest()}")
report = []
for routes, fixtures in ((("json",), json_fixtures), (("fromxml", "hapi"), xml_fixtures)):
    for fid, cat, doc in fixtures:
        exp = expected(fid)
        for mode in routes:
            o = run(fid, doc, mode)
            o.update({"id": fid, "category": cat, "route": mode, "expected": exp, "meets": verdict(exp, o)})
            report.append(o)
            emit(f"{mode:8} {fid:34} {exp:9} {o['meets']:38} {o['outcome'][:240]}")
            if o.get("findings"):
                emit(f"{'':8} {'':34} findings: {'; '.join(o['findings'])[:300]}")

# Principle 6: each fixture in one batch with the other fixtures of its type, where it was stored alone.
emit("\n== batch: a stored row that differs from the row stored alone (principle 6)")
for routes, fixtures in ((("json",), json_fixtures), (("fromxml",), xml_fixtures)):
    for mode in routes:
        alone = {o["id"]: o for o in report if o["route"] == mode and o.get("rows") and len(o["rows"]) == 1}
        by_type = {}
        for fid, cat, doc in fixtures:
            if fid in alone:
                by_type.setdefault(alone[fid]["type"], []).append((fid, doc))
        for t, items in sorted(by_type.items()):
            df = spark.createDataFrame([(d,) for _, d in items], "value string")
            try:
                encoded = pc.encode(df, t) if mode == "json" else route.fit_lines(df, t)
                batch = {json.loads(r)["id"]: r for r in stored_rows(encoded)}
            except Exception as e:
                emit(f"{mode:8} {t:12} batch of {len(items)} ERROR {error_text(e)}")
                continue
            changed = []
            for fid, _ in items:
                a = json.loads(alone[fid]["rows"][0])
                b = json.loads(batch.get(a.get("id"), "{}"))
                d = json_diff(a, b)
                # A field absent alone and null in the batch is the schema's union, not a change of value.
                d = [x for x in d if not re.search(r": \"<absent>\" -> null$", x)]
                if d:
                    changed.append((fid, d))
            emit(f"{mode:8} {t:12} batch of {len(items):2}: {len(changed)} stored differently")
            for fid, d in changed:
                emit(f"{'':8} {'':12}   {fid:34} {'; '.join(d)[:250]}")

out_txt.close()
