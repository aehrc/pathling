# phases-fromxml-514f8f8: the from_xml XML route on the A0 jar (spike 08)

The same jar as `phases-10.0.0-SNAPSHOT-514f8f8` (`3eb0a768fa6c…`, unmodified), with the XML route of
`scripts/fromxml_route.py` (SHA-256 `eda25324c20f…`) in place of HAPI's. The route reaches the definitions and
`ResourceTransformer.toLayout` over py4j. The XML sources only, with no JSON; extensions off and on.

Run on 2026-10-19 from 15:14 to 16:00, the third label of the batch (`run-phases.sh <label> venv-514f8f8
fromxml`). `fitPlanPy4jMs` and `decodePlanPy4jMs` are the part of each fit and decode spent building the plan
in Python, which is a prototype artefact. Other processes: as for `phases-pof-switch-26d7332`.
