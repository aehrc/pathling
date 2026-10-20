"""Spike 08: times the fit pass, encode and decode separately, for one source, one route and one extensions setting.

Off-contract; not a SQL on FHIR result. Settings are the harness's frozen ones: one warmup plus five
iterations in a warm JVM, the raw input cached untimed, max_nesting_level left at its default of 3, and the
driver memory set by the caller (16g at size l).

Phases (defined once, in evidence/spike-08/README.md, and applied identically to every route):
  fit     From the call that turns the cached raw text into an encoded dataset until it returns. Spark infers
          the schema eagerly inside that call: one full scan of the input, plus building the transform on
          the driver from the inferred schema. It ends with the fitted schema (FR-011) on the driver and a
          lazy dataset whose schema is fixed. HEAD: pc.encode / pc.encode_bundle. from_xml: XmlRoute.fit_*.
  encode  The noop write of that dataset: every row and every column materialised, with nothing pruned,
          and no disk I/O. A count would let Spark prune columns, so it is not used.
  decode  The encoded dataset is written to Parquet once, untimed, and read back. Decode is the noop write of
          the decoded text over that read. A control, the noop write of the Parquet read alone, gives the
          scan's own cost. HEAD: pc._jpc.decode. from_xml: XmlRoute.decode.
Every phase runs in a job group of its own, and the number of Spark jobs in each is recorded, so that work
landing in an unexpected phase is visible.

Fidelity, untimed, as the existing scripts check it: the decoded documents of the first 500 input rows against
the input, JSON as parsed objects (numbers compared by value), XML exactly (narrative line feeds as &#xa;) and
after canonicalisation. For bundles the reference is the resource's line in <xmlDir>/<Type>.ndxml.

Usage: phases.py <json|xml-lines|xml-bundles|xml-bundles-txn> <pathling|fromxml|aplus|avariant|b|orig> <jsonDir> <xmlDir>
                 <bundleDir> <outJson> <Type> [<Type> ...]
Env: ENABLE_EXTENSIONS=1 to encode extensions; PATHLING_LABEL names the build. Phase 2 (2026-10-19) added the
candidate routes aplus, avariant and b (scripts/spike_routes.py, with the jar of Spike08*.java on --jars), whose
policy SPIKE_POLICY sets (fail by default), and the lexical JSON fidelity count. The pathling and fromxml
routes, and everything this script recorded for them, are unchanged.
"""
import json, os, re, shutil, statistics, sys, tempfile, time
import xml.etree.ElementTree as ET
from pyspark.sql import DataFrame
from pathling import PathlingContext

source, route_name, json_dir, xml_dir, bundle_dir, out_file, *resources = sys.argv[1:]
assert source in ("json", "xml-lines", "xml-bundles", "xml-bundles-txn")
assert route_name in ("pathling", "fromxml", "aplus", "avariant", "b", "orig")
assert not (source == "json" and route_name == "fromxml")
assert not (source != "json" and route_name in ("avariant", "orig"))
WARMUP, N, FIDELITY_SAMPLE = 1, 5, 500
XML = "application/fhir+xml"
JSON = "application/fhir+json"
ID = re.compile(r'<id value="([^"]+)"/>')

EXT = os.environ.get("ENABLE_EXTENSIONS") == "1"
pc = PathlingContext.create(enable_terminology=False, enable_extensions=EXT)
spark = pc.spark
sc = spark.sparkContext
sc.setLogLevel("ERROR")

if route_name == "fromxml":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from fromxml_route import XmlRoute
    route = XmlRoute(pc)
elif route_name != "pathling":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from spike_routes import ROUTES
    route = ROUTES[route_name](pc)


def fit(raw, res):
    if route_name not in ("pathling", "fromxml"):
        return route.fit(source, raw, res)
    if route_name == "fromxml":
        return route.fit_lines(raw, res) if source == "xml-lines" else route.fit_bundles(raw, res)
    if source == "json":
        return pc.encode(raw, res)
    if source == "xml-lines":
        return pc.encode(raw, res, input_type=XML)
    return pc.encode_bundle(raw, res, input_type=XML)


def decode(stored, res):
    if route_name not in ("pathling", "fromxml"):
        return route.decode(source, stored, res)
    if route_name == "fromxml":
        return route.decode(stored, res)
    return DataFrame(pc._jpc.decode(stored._jdf, res, JSON if source == "json" else XML), spark)


def noop(df):
    df.write.format("noop").mode("overwrite").save()


def phase(group, fn):
    """Runs one phase in a job group of its own; returns (ms, result, jobs)."""
    sc.setJobGroup(group, group)
    t = time.perf_counter()
    r = fn()
    ms = (time.perf_counter() - t) * 1000
    jobs = len(sc.statusTracker().getJobIdsForGroup(group))
    sc.setJobGroup("untimed", "untimed")
    return ms, r, jobs


def stats(xs):
    return {"mean": statistics.mean(xs), "min": min(xs), "max": max(xs), "samples": xs}


def one_line(doc):
    return doc.replace("\r", "&#xd;").replace("\n", "&#xa;")


# Untimed: the input in memory, so reading files is not measured. Bundles are shared by every type.
bundles = None
if source in ("xml-bundles", "xml-bundles-txn"):
    bundles = spark.read.text(bundle_dir, wholetext=True).cache()
    n_bundles = bundles.count()

tmp = tempfile.mkdtemp(prefix="phases-")
results = {}
for res in resources:
    if source == "json":
        raw = spark.read.text(f"{json_dir}/{res}.ndjson").cache()
        n_in = raw.count()
    elif source == "xml-lines":
        raw = spark.read.text(f"{xml_dir}/{res}.ndxml").cache()
        n_in = raw.count()
    else:
        raw = bundles
        n_in = spark.read.text(f"{xml_dir}/{res}.ndxml").count()

    fits, encodes, fit_jobs, encode_jobs, fit_plans = [], [], [], [], []
    encoded = None
    for i in range(WARMUP + N):
        f_ms, encoded, f_jobs = phase(f"fit-{res}-{i}", lambda: fit(raw, res))
        f_plan = route.plan_ms if route_name != "pathling" else None
        e_ms, _, e_jobs = phase(f"encode-{res}-{i}", lambda: noop(encoded))
        if i >= WARMUP:
            fits.append(f_ms); encodes.append(e_ms); fit_jobs.append(f_jobs); encode_jobs.append(e_jobs); fit_plans.append(f_plan)

    # Untimed: the encoded dataset of the last iteration, materialised as Parquet and read back.
    path = f"{tmp}/{res}"
    encoded.write.mode("overwrite").parquet(path)
    stored = spark.read.parquet(path)
    n_enc = stored.count()

    decodes, scans, decode_jobs, decode_plans = [], [], [], []
    for i in range(WARMUP + N):
        d_ms, _, d_jobs = phase(f"decode-{res}-{i}", lambda: noop(decode(stored, res)))
        d_plan = route.plan_ms if route_name != "pathling" else None
        s_ms, _, _ = phase(f"scan-{res}-{i}", lambda: noop(stored))
        if i >= WARMUP:
            decodes.append(d_ms); scans.append(s_ms); decode_jobs.append(d_jobs); decode_plans.append(d_plan)

    r = {"rowsIn": n_in, "rowsEncoded": n_enc, "storedLeafFields": len(re.findall(r'"type":"[a-z]', stored.schema.json())),
         "fitMs": stats(fits), "encodeMs": stats(encodes), "decodeMs": stats(decodes), "parquetScanControlMs": stats(scans),
         "jobs": {"fit": fit_jobs, "encode": encode_jobs, "decode": decode_jobs}}
    if route_name != "pathling":
        # The part of fit and decode spent building the plan in Python over py4j (see fromxml_route.py).
        r["fitPlanPy4jMs"] = stats(fit_plans)
        r["decodePlanPy4jMs"] = stats(decode_plans)
    if bundles is not None:
        r["bundles"] = n_bundles
    if route_name == "fromxml":
        r["undescribed"] = sorted(set(route.undescribed))
    if route_name not in ("pathling", "fromxml"):
        r["policy"] = route.policy

    # Untimed fidelity, as the existing scripts check it.
    if source == "json":
        orig = {d["id"]: d for d in (json.loads(x.value) for x in raw.limit(FIDELITY_SAMPLE).collect())}
        back = [json.loads(x[0]) for x in decode(stored.filter(stored.id.isin(list(orig))), res).collect()]
        r.update({"fidelitySample": len(back), "fidelityExactEqual": sum(1 for d in back if orig.get(d.get("id")) == d)})
        detail = f"exact={r['fidelityExactEqual']}/{len(back)}"
        if route_name not in ("pathling", "fromxml"):
            # Phase 2: numbers compared by their text as well, which the check above (by value) cannot see.
            lexical = lambda text: json.loads(text, parse_float=lambda x: ("n", x), parse_int=lambda x: ("n", x))
            lex_orig = {json.loads(x.value)["id"]: lexical(x.value) for x in raw.limit(FIDELITY_SAMPLE).collect()}
            lex_back = [lexical(x[0]) for x in decode(stored.filter(stored.id.isin(list(lex_orig))), res).collect()]
            r["fidelityLexicalEqual"] = sum(1 for d in lex_back if lex_orig.get(d.get("id")) == d)
            detail += f" lexical={r['fidelityLexicalEqual']}/{len(lex_back)}"
    else:
        reference = spark.read.text(f"{xml_dir}/{res}.ndxml")
        orig = {ID.search(x.value).group(1): x.value for x in reference.limit(FIDELITY_SAMPLE).collect()}
        back = [x[0] for x in decode(stored.filter(stored.id.isin(list(orig))), res).collect()]
        exact = sum(1 for d in back if orig.get(ID.search(d).group(1)) == one_line(d))
        canon = sum(1 for d in back if ET.canonicalize(xml_data=orig[ID.search(d).group(1)]) == ET.canonicalize(xml_data=d))
        r.update({"fidelitySample": len(back), "fidelityExactEqual": exact, "fidelityCanonicalEqual": canon})
        detail = f"exact={exact}/{len(back)} canonical={canon}/{len(back)}"
    r["rowsDecoded"] = decode(stored, res).count()
    print(res, n_in, n_enc, r["rowsDecoded"], detail,
          "fit", round(statistics.mean(fits)), "encode", round(statistics.mean(encodes)),
          "decode", round(statistics.mean(decodes)), "scan", round(statistics.mean(scans)),
          "jobs", fit_jobs[0], encode_jobs[0], decode_jobs[0], flush=True)
    results[res] = r
    shutil.rmtree(path, ignore_errors=True)
    if bundles is None:
        raw.unpersist()

shutil.rmtree(tmp, ignore_errors=True)
json.dump({"pathling": os.environ.get("PATHLING_LABEL"), "enableExtensions": EXT, "source": source, "route": route_name,
           "warmup": WARMUP, "iterations": N, "results": results}, open(out_file, "w"), indent=2)
