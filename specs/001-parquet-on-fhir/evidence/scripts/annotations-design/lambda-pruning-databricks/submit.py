"""Submits a one-time run on single-node r5d.large job clusters, DBR 17.3 LTS, dedicated access.

Usage: submit.py <name> <photon|nophoton> <smoke|full|engine> <rows>
 - smoke: the Python task with a few cases on Parquet.
 - full: the Python task (py4j install, the whole matrix) and then a Scala notebook task (Scala
   install), on one cluster.
 - engine: the Python task with the 18 engine views, on a cluster that also has the Pathling jars.
Prints the job and run ids. Clusters end with the run; timeout_seconds caps the cost.
"""
import json
import os
import subprocess
import sys

DBFS = "dbfs:/lpr"
# DATABRICKS_USER is your user name in the workspace, which owns the uploaded files and the cluster.
USER = os.environ["DATABRICKS_USER"]
WS = f"/Workspace/Users/{USER}/lpr"
name, engine, mode, rows = sys.argv[1:5]
base, out = f"{DBFS}/data-{name}", f"{DBFS}/out-{name}"
cluster = {
    "spark_version": "17.3.x-scala2.13",
    "node_type_id": "r5d.large",
    "num_workers": 0,
    "data_security_mode": "SINGLE_USER",
    "single_user_name": USER,
    "runtime_engine": "PHOTON" if engine == "photon" else "STANDARD",
    "spark_conf": {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*]",
        # The disk cache would serve repeat reads from local SSD and hide the bytes read.
        "spark.databricks.io.cache.enabled": "false",
    },
    "custom_tags": {"ResourceClass": "SingleNode"},
    # The jars come through an init script (see init.sh), so that nothing is installed as a library.
    "spark_env_vars": {"JNAME": "zulu21-ca-amd64", "LPR_ENGINE": "1" if mode == "engine" else "0",
                       "LPR_USER": USER},
    "cluster_log_conf": {"dbfs": {"destination": "dbfs:/lpr/logs"}},
    "init_scripts": [{"workspace": {"destination": f"/Users/{USER}/lpr/init.sh"}}],
    "aws_attributes": {"availability": "ON_DEMAND"},
}
tasks = [{"task_key": "py", "job_cluster_key": "c",
          "spark_python_task": {"python_file": f"{WS}/run_probe.py", "source": "WORKSPACE",
                                "parameters": [mode, base, out, rows]}}]
if mode == "full":
    tasks.append({"task_key": "scala", "job_cluster_key": "c", "depends_on": [{"task_key": "py"}],
                  "notebook_task": {"notebook_path": f"{WS}/scala_check", "source": "WORKSPACE",
                                    "base_parameters": {"base": base, "out": f"{out}-scala"}}})
body = {"name": f"lpr-{name}", "timeout_seconds": 7200, "max_concurrent_runs": 1,
        "job_clusters": [{"job_cluster_key": "c", "new_cluster": cluster}], "tasks": tasks}


def cli(*args):
    r = subprocess.run(["databricks", *args, "-p", "pathling", "-o", "json"], capture_output=True,
                       text=True)
    if r.returncode:
        sys.exit(r.stderr)
    return json.loads(r.stdout)


# A job is created because `jobs submit` cannot share one cluster between tasks. The job is
# deleted once the run is done (`databricks jobs delete <job_id>`).
job_id = cli("jobs", "create", "--json", json.dumps(body))["job_id"]
run = cli("jobs", "run-now", str(job_id), "--no-wait")
print(json.dumps({"job_id": job_id, "run_id": run["run_id"]}))
