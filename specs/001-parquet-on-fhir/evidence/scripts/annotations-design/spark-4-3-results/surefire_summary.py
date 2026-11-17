"""Summarises Surefire XML reports per module, and groups failures by root-cause message.

Usage: surefire_summary.py <reports-root> [--failures baseline-root]
The root holds one directory per module. With --failures, a failing test that also fails in the
baseline root is marked "also fails on baseline".
"""
import collections
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

MODULES = ["utilities", "fhir-schema", "encoders", "io", "terminology", "fhirpath", "library-api"]


def scan(root):
    out = {}
    for m in MODULES:
        run = fail = err = skip = 0
        bad = {}
        for f in glob.glob(os.path.join(root, m, "TEST-*.xml")):
            for tc in ET.parse(f).getroot().iter("testcase"):
                run += 1
                name = tc.get("classname") + "." + tc.get("name")
                node = None
                for tag in ("failure", "error"):
                    node = tc.find(tag)
                    if node is not None:
                        if tag == "failure":
                            fail += 1
                        else:
                            err += 1
                        bad[name] = (node.get("type") or "") + ": " + (node.get("message") or "")[:200]
                        break
                else:
                    if tc.find("skipped") is not None:
                        skip += 1
        out[m] = (run, fail, err, skip, bad)
    return out


def main():
    root = sys.argv[1]
    base = scan(sys.argv[sys.argv.index("--failures") + 1]) if "--failures" in sys.argv else None
    res = scan(root)
    print("module\trun\tfailures\terrors\tskipped")
    for m, (run, fail, err, skip, _) in res.items():
        print("%s\t%d\t%d\t%d\t%d" % (m, run, fail, err, skip))
    for m, (_, _, _, _, bad) in res.items():
        if not bad:
            continue
        groups = collections.defaultdict(list)
        for name, msg in bad.items():
            known = base is not None and name in base[m][4]
            msg = re.sub(r"\d+", "N", msg)[:160]
            groups[(msg, known)].append(name)
        print("\n== %s: %d failing tests" % (m, len(bad)))
        for (msg, known), names in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            print("%5d %s [%s]" % (len(names), msg, "also fails on baseline" if known else "new"))
            for n in names[:3]:
                print("        e.g. " + n)


main()
