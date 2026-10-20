"""Groups a JFR recording's CPU samples by the stage of B's encode they belong to.

Each sample is given to the first rule matched walking from the innermost frame outwards, so a JDK method such
as String.charAt counts towards the stage that called it.
"""
import collections
import json
import subprocess
import sys

RULES = [
    ("checks (regex, calendar, base64, duplicate keys)",
     ("au.csiro.pathling.io.Spike08Checks", "java.util.regex.", "java.time.", "DupDetector")),
    ("Jackson parse", ("com.fasterxml.jackson.",)),
    ("B's own row building (Spike08Json, findings, maps, lists)",
     ("au.csiro.pathling.io.Spike08Json", "au.csiro.pathling.io.Spike08Findings",
      "au.csiro.pathling.io.Spike08Layout", "au.csiro.pathling.io.Spike08B")),
    ("Encoders.row: Row to internal row", ("serializefromobject",)),
    ("input: internal row to String", ("deserializetoobject", "UTF8String.toString")),
]

jfr, path = sys.argv[1], sys.argv[2]
raw = subprocess.run([jfr, "print", "--json", "--stack-depth", "128", "--events", "jdk.ExecutionSample", path],
                     check=True, capture_output=True, text=True).stdout
events = json.loads(raw)["recording"]["events"]
counts, other = collections.Counter(), collections.Counter()
for e in events:
    frames = [f"{f['method']['type']['name'].replace('/', '.')}.{f['method']['name']}"
              for f in e["values"]["stackTrace"]["frames"]]
    stage = next((name for frame in frames for name, keys in RULES if any(k in frame for k in keys)), None)
    if stage is None:
        stage = "other"
        other[frames[0] if frames else "?"] += 1
    counts[stage] += 1
total = sum(counts.values())
print(f"{total} samples")
for name, n in counts.most_common():
    print(f"{n / total:6.1%}  {n:5}  {name}")
print("\nTop 'other' leaf frames:")
for f, n in other.most_common(8):
    print(f"  {n:5}  {f}")
