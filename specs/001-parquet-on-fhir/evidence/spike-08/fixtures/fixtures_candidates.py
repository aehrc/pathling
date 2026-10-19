"""Runs the ingress-strictness fixtures against spike 08's candidates (phase 2): A+ (JSON and XML), A-variant
(JSON) and B (JSON and XML). Each fixture is encoded alone under both of decision 133's policies:
  fail   the default: an invalid value must fail the job, naming the resource, the path and the value;
  drop   with the _findings column: an invalid value is left out and recorded per value, the rest is stored.
The expectations are fixtures_run.py's (EXPECT), unchanged. A verdict is "yes" where:
  store      neither policy reports anything but contained, and the drop run decodes back equal to the input
             (JSON numbers compared by their text, XML canonically, prefixes rewritten);
  invalid    fail raises, and drop records a finding that is not informational;
  contained  fail does not raise, and drop records a contained finding;
  left-out   neither policy stores a row or raises;
  reject     fail raises, and drop stores no row.
Then each route encodes all fixtures of a type in one batch under drop, and a stored row that differs from
the row stored alone is flagged (principle 6). Output: fixtures_candidates.txt.
Usage: fixtures_candidates.py <harnessScriptsDir> <ingressStrictnessDir> <spike08FixturesDir>
"""
import json, os, re, sys
import xml.etree.ElementTree as ET
from pathling import PathlingContext

scripts, strictness, here = sys.argv[1:]
sys.path.insert(0, scripts)
sys.path.insert(0, here)
from spike_routes import ROUTES

# The expectations and helpers of the A0 runner, so both tables rest on the same reading of decision 133.
import importlib.util
spec = importlib.util.spec_from_file_location("a0", os.path.join(here, "fixtures_run.py"))
src = open(os.path.join(here, "fixtures_run.py")).read()
EXPECT_SRC = src[src.index("EXPECT = {"):src.index("def expected(fid)")]
ns = {}
exec(EXPECT_SRC, ns)
EXPECT = ns["EXPECT"]

pc = PathlingContext.create(enable_terminology=False)
spark = pc.spark
spark.sparkContext.setLogLevel("OFF")


def expected(fid):
    base = fid[2:] if fid.startswith("x-") else fid
    if fid == "x-c2-fhir-comments":
        return EXPECT["c2-fhir-comments@xml"]
    return EXPECT.get(fid, EXPECT.get(base, "?"))


def load(path):
    return [tuple(line.rstrip("\n").split("\t", 2)) for line in open(path) if line.strip()]


json_fixtures = (load(f"{strictness}/fixtures.tsv") + load(f"{strictness}/fixtures-lexical.tsv")
                 + load(f"{here}/fixtures-json-extra.tsv"))
xml_fixtures = load(f"{here}/fixtures-xml.tsv")


def target_type(doc, fid):
    m = re.search(r'"resourceType"\s*:\s*"([A-Za-z]+)"', doc) or re.search(r"^<(?:\w+:)?([A-Z][A-Za-z]+)", doc)
    t = m.group(1) if m else "Patient"
    return "Patient" if t in ("Florb",) or fid.endswith("wrong-resourcetype") else t


def error_text(e):
    text = str(e)
    m = re.search(r"(Invalid (?:content|[A-Za-z]+ document)[^\n]{0,220})", text)
    if m:
        return m.group(1)
    causes = sorted(set(re.findall(r"\[([A-Z_]+(?:\.[A-Z_]+)*)\]", text)))
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    first = next((l for l in lines if re.match(r"^(:\s*)?[a-z][\w.]*(Exception|Error)\b", l)), lines[0] if lines else "")
    return (" ".join(causes) + " " + first[:180]).strip()


def lexical(text):
    return json.loads(text, parse_float=lambda s: ("n", s), parse_int=lambda s: ("n", s))


def canonical(doc):
    return ET.canonicalize(xml_data=doc, rewrite_prefixes=True)


routes = {}


def route(name, policy):
    os.environ["SPIKE_POLICY"] = policy
    os.environ["SPIKE_FINDINGS"] = "1" if policy == "drop" else "0"
    r = ROUTES[name](pc)
    return r


def run(name, source, fid, doc, policy):
    t = target_type(doc, fid)
    r = route(name, policy)
    df = spark.createDataFrame([(doc,)], "value string")
    out = {"type": t}
    try:
        encoded = r.fit(source, df, t)
        rows = encoded.collect()
        out["rows"] = len(rows)
        out["findings"] = [] if "_findings" not in encoded.columns else \
            [f"{f['path']} {f['kind']}: {f['value']}" for row in rows for f in (row["_findings"] or [])]
        out["stored"] = [json.dumps({k: v for k, v in row.asDict(recursive=True).items() if k != "_findings"},
                                    default=str, sort_keys=True) for row in rows]
        if rows and policy == "drop":
            stored = encoded.drop("_findings")
            decoded = [x[0] for x in r.decode(source, stored, t).collect()]
            if source == "json":
                try:
                    same = lexical(doc) == lexical(decoded[0])
                except ValueError:
                    same = False
            else:
                try:
                    same = canonical(doc) == canonical(decoded[0])
                except ET.ParseError:
                    same = False
            out["roundtrip"] = same
        out["outcome"] = "ok"
    except Exception as e:
        out["outcome"] = "ERROR " + error_text(e)
    return out


def verdict(exp, fail, drop):
    raised = fail["outcome"].startswith("ERROR")
    dropped_error = drop["outcome"].startswith("ERROR")
    kinds = [f.split(" ")[1].rstrip(":") for f in drop.get("findings", [])]
    errors = [k for k in kinds if k != "contained"]
    if exp == "store":
        return "yes" if not raised and not dropped_error and not errors and drop.get("roundtrip") else "no"
    if exp == "invalid":
        if raised and errors:
            return "yes"
        if raised:
            return "partial (fails; drop records nothing)" if not dropped_error else "partial (fails; drop fails too)"
        return "no (findings only)" if errors else "no (silent)"
    if exp == "contained":
        return "yes" if not raised and "contained" in kinds else "no"
    if exp == "left-out":
        return "yes" if not raised and fail.get("rows") == 0 and drop.get("rows") == 0 else "no"
    if exp == "reject":
        return "yes" if raised and drop.get("rows", 0) == 0 else "no"
    return "-"


def norm(x):
    """A field absent and a field null are the same: the batch's schema is the union of its rows."""
    if isinstance(x, dict):
        return {k: norm(v) for k, v in x.items() if v is not None}
    if isinstance(x, list):
        return [norm(v) for v in x]
    return x


def deep(a, b, path, out):
    a, b = norm(a), norm(b)
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            deep(a.get(k), b.get(k), f"{path}.{k}" if path else k, out)
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b)):
            deep(x, y, f"{path}[{i}]", out)
    elif a != b:
        out.append(f"{path}: {json.dumps(a)[:40]} -> {json.dumps(b)[:40]}")


lines = []


def emit(line):
    lines.append(line)
    print(line, flush=True)


emit(f"spark {spark.version}")
plan = [("aplus", "json", json_fixtures), ("avariant", "json", json_fixtures), ("b", "json", json_fixtures),
        ("aplus", "xml-lines", xml_fixtures), ("b", "xml-lines", xml_fixtures)]
alone = {}
for name, source, fixtures in plan:
    for fid, cat, doc in fixtures:
        exp = expected(fid)
        fail = run(name, source, fid, doc, "fail")
        drop = run(name, source, fid, doc, "drop")
        v = verdict(exp, fail, drop)
        alone[(name, source, fid)] = (drop, doc)
        label = f"{name}/{'json' if source == 'json' else 'xml'}"
        emit(f"{label:13} {fid:34} {exp:9} {v:36} fail: {fail['outcome'][:110]}")
        emit(f"{'':13} {'':34} {'':9} {'':36} drop: {drop['outcome'][:20]} rows={drop.get('rows')} "
             f"roundtrip={drop.get('roundtrip')} findings: {'; '.join(drop.get('findings', []))[:200]}")

emit("\n== batch under drop: a stored row that differs from the row stored alone (principle 6)")
for name, source, fixtures in plan:
    by_type = {}
    for fid, cat, doc in fixtures:
        drop, _ = alone[(name, source, fid)]
        if drop.get("rows") == 1:
            by_type.setdefault(drop["type"], []).append((fid, doc, drop["stored"][0]))
    r = route(name, "drop")
    for t, items in sorted(by_type.items()):
        df = spark.createDataFrame([(d,) for _, d, _ in items], "value string")
        try:
            encoded = r.fit(source, df, t).drop("_findings")
            batch = {}
            for row in encoded.collect():
                d = row.asDict(recursive=True)
                batch[d.get("id")] = json.dumps(d, default=str, sort_keys=True)
        except Exception as e:
            emit(f"{name}/{source:9} {t:12} batch of {len(items)} ERROR {error_text(e)}")
            continue
        changed = []
        for fid, _, stored in items:
            a = json.loads(stored)
            b = json.loads(batch.get(a.get("id"), "{}"))
            diffs = []
            deep(a, b, "", diffs)
            if diffs:
                changed.append((fid, diffs))
        emit(f"{name}/{source:9} {t:12} batch of {len(items):2}: {len(changed)} stored differently")
        for fid, d in changed:
            emit(f"{'':22} {fid:34} {'; '.join(d)[:200]}")

open(os.path.join(here, "fixtures_candidates.txt"), "w").write("\n".join(lines) + "\n")
