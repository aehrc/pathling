# Ticket 32: XML and bundles without HAPI

Measurements and checks behind ticket 32 (`../../issues/32-xml-and-bundles-without-hapi.md`), made 2026-10-21 on
`issue/2831-xml-bundles` on an Apple M3 Pro (11 cores, 36 GB), Java 21, PySpark 4.0.4 for the scripts. The
benchmark data is `clinical-wide` from `sql-on-fhir.js` (`794e8a7f`); the XML data and the phase harness are the
pathling-compare harness's (`.local/work/issue/2367/pathling-compare/` in the main checkout, not tracked).

| File | What it shows |
|---|---|
| `outputs/inference-cost.txt`, `scripts/infer_cost.py`, `scripts/infer_cost2.py` | **Superseded (owner, 2026-10-22).** Why numbers were typed by marks: Spark's XML inference with `inferSchema=true` costs 12.9 s against 2.7 s with `false` on the Observation scan at `l`. The marks are gone: XML stores a decimal's text, and types integers from the definitions |
| `scripts/gate.py`, `outputs/gate-s.txt` | At size `s`, after the owner's decisions of 2026-10-22: XML collection and transaction bundles equal XML lines, and JSON collection and transaction bundles equal JSON lines, under `exceptAll` both ways; XML lines equal JSON lines with every decimal cast to a double, since XML keeps a decimal's text. Schemas are equal throughout, and narratives are compared byte for byte |
| `scripts/cmp.py`, `outputs/cmp-8b1ee26.txt` | SC-011: the harness label `phases-2831xml-8b1ee26` against the bar `phases-pof-switch-26d7332`, ratio of pooled minimums, extensions off and on pooled. Measured before the owner's decisions of 2026-10-22, which removed the marks' pass from the fit |
| `scripts/make_json_bundles.py`, `scripts/json_bundle_phases.py`, `outputs/jb-*.json` | JSON bundles, which the harness lacks: one per patient made from the corpus, fit, encode and decode timed as the harness times its sources, on this branch (`this`) and on the bar's jar (`bar`), in one session, extensions off. The `this` runs were on a build before the last change to `io`'s XHTML writing, which the JSON route does not use |

Reproduction: `gate.py <jsonDir> <pathling-compare> <jsonBundleDir>`, after `make_json_bundles.py <jsonDir> <out>`;
`json_bundle_phases.py <bundles.ndjson> <out.json> <Type...>`; `cmp.py <barLabel> <label>` over the harness's
`results/`. Each runs under a venv with the build's `lib/python` installed and a private Ivy home.
