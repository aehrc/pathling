"""Writes provenance for one run. Usage: write_meta.py <out> <label> <venv> <data> <repo> [<pathling-src>]."""
import datetime, hashlib, json, platform, subprocess, sys

import pyspark

out, label, venv, data, repo, *rest = sys.argv[1:]
src = rest[0] if rest else ""


def sh(*a, cwd=None):
    r = subprocess.run(a, capture_output=True, text=True, cwd=cwd)
    return (r.stdout + r.stderr).strip()


meta = {
    "label": label, "venv": venv, "data": data, "python": platform.python_version(),
    "pyspark": pyspark.__version__, "java": sh("java", "-version").splitlines()[0],
    "timestamp": datetime.datetime.now().isoformat(),
    "sqlOnFhirJsSha": sh("git", "rev-parse", "HEAD", cwd=repo),
    "machine": sh("sysctl", "-n", "machdep.cpu.brand_string") + ", " + sh("sysctl", "-n", "hw.ncpu") + " cpus",
}
if src:
    meta["pathlingSha"] = sh("git", "rev-parse", "HEAD", cwd=src)
    meta["pathlingDirty"] = bool(sh("git", "status", "--porcelain", cwd=src))
    meta["pathlingSubject"] = sh("git", "log", "-1", "--format=%s", cwd=src)
    # The runtime jar of whatever version the checkout builds, not its sources or javadoc jar.
    import glob
    jar = [j for j in glob.glob(f"{src}/library-runtime/target/library-runtime-*.jar")
           if not j.endswith(("-sources.jar", "-javadoc.jar"))][0]
    meta["builtJar"] = jar
    meta["builtJarSha256"] = hashlib.sha256(open(jar, "rb").read()).hexdigest()
json.dump(meta, open(out, "w"), indent=2)
