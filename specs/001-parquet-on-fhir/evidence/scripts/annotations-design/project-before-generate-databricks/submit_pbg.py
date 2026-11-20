"""Submits the compute-first run: one single-node r5d.large job cluster, DBR 17.3 LTS, dedicated
access, Photon ON, with the Pathling jars (LPR_ENGINE=1). Usage: submit_pbg.py <name> [photon|nophoton] [spark_version key, default 17.3.x-scala2.13; DBR 19 is 19.x-scala2.13].
Prints the job and run ids. The cluster ends with the run; delete the job afterwards.
"""
import json
import os
import subprocess
import sys

name = sys.argv[1]
engine = sys.argv[2] if len(sys.argv) > 2 else "photon"
VERSION = sys.argv[3] if len(sys.argv) > 3 else "17.3.x-scala2.13"
# DATABRICKS_USER is your user name in the workspace, which owns the uploaded files and the cluster.
USER = os.environ["DATABRICKS_USER"]
WS = f"/Workspace/Users/{USER}/lpr"
base, out = f"dbfs:/lpr/data-{name}", f"dbfs:/lpr/out-{name}"
cluster = {
    "spark_version": VERSION, "node_type_id": "r5d.large", "num_workers": 0,
    "data_security_mode": "SINGLE_USER", "single_user_name": USER,
    "runtime_engine": "PHOTON" if engine == "photon" else "STANDARD",
    "spark_conf": {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]",
                   "spark.databricks.io.cache.enabled": "false"},
    "custom_tags": {"ResourceClass": "SingleNode"},
    "spark_env_vars": {"JNAME": "zulu21-ca-amd64", "LPR_ENGINE": "1", "LPR_USER": USER},
    "cluster_log_conf": {"dbfs": {"destination": "dbfs:/lpr/logs"}},
    "init_scripts": [{"workspace": {"destination": f"/Users/{USER}/lpr/init.sh"}}],
    "aws_attributes": {"availability": "ON_DEMAND"},
}
tasks = [{"task_key": "py", "job_cluster_key": "c",
          "spark_python_task": {"python_file": f"{WS}/run_pbg.py", "source": "WORKSPACE",
                                "parameters": [base, out]}}]
body = {"name": f"pbg-{name}", "timeout_seconds": 3600, "max_concurrent_runs": 1,
        "job_clusters": [{"job_cluster_key": "c", "new_cluster": cluster}], "tasks": tasks}


def cli(*args):
    r = subprocess.run(["databricks", *args, "-p", "pathling", "-o", "json"], capture_output=True, text=True)
    if r.returncode:
        sys.exit(r.stderr)
    return json.loads(r.stdout)


job_id = cli("jobs", "create", "--json", json.dumps(body))["job_id"]
run = cli("jobs", "run-now", str(job_id), "--no-wait")
print(json.dumps({"job_id": job_id, "run_id": run["run_id"]}))
