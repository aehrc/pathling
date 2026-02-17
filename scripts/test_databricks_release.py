#!/usr/bin/env python3
"""
Automates testing of a candidate Pathling release on Databricks.

Configures a nominated Databricks cluster with snapshot Maven artifacts,
a dev PyPI package, and an R package from a GitHub Actions build, then
runs test notebooks and reports pass/fail.

Author: John Grimes
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# The Maven group and artifact for the Pathling library runtime.
PATHLING_MAVEN_GROUP = "au.csiro.pathling"
PATHLING_MAVEN_ARTIFACT = "library-runtime"
PATHLING_PYPI_PACKAGE = "pathling"

# The Maven Central snapshots repository URL.
MAVEN_SNAPSHOTS_REPO = "https://central.sonatype.com/repository/maven-snapshots/"

# The default Databricks runtime version.
DEFAULT_DATABRICKS_RUNTIME = "17.3.x-scala2.13"

# The default timeout in minutes for cluster startup and notebook runs.
DEFAULT_TIMEOUT_MINUTES = 30

# The default GitHub repository for downloading R package artifacts.
DEFAULT_GITHUB_REPO = "aehrc/pathling"

# The DBFS directory and fixed filename for the uploaded R package.
DBFS_R_PACKAGE_DIR = "dbfs:/FileStore/R"
R_PACKAGE_UPLOAD_NAME = "pathling_latest_tar.gz"

# The environment variable required for Java 21 on Databricks.
JAVA_ENV_VAR = {"JNAME": "zulu21-ca-amd64"}

# The interval in seconds between polling attempts.
POLL_INTERVAL_SECONDS = 30


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Test a candidate Pathling release on Databricks.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s \\
    --maven-version 9.4.0-SNAPSHOT \\
    --pypi-version 9.4.0.dev0 \\
    --github-run-id 12345678 \\
    --cluster-id 0123-456789-abcde123 \\
    --notebooks /Users/user@example.com/test-python \\
                /Users/user@example.com/test-r
        """,
    )
    parser.add_argument(
        "--maven-version",
        required=True,
        help="SNAPSHOT version of the Maven artifacts (e.g., 9.4.0-SNAPSHOT).",
    )
    parser.add_argument(
        "--pypi-version",
        required=True,
        help="Dev release version of the Python package on PyPI (e.g., 9.4.0.dev0).",
    )
    parser.add_argument(
        "--github-run-id",
        required=True,
        help="GitHub Actions run ID with the R package artifact attached.",
    )
    parser.add_argument(
        "--cluster-id",
        required=True,
        help="Databricks cluster ID to configure and test on.",
    )
    parser.add_argument(
        "--notebooks",
        required=True,
        nargs="+",
        help="Databricks notebook paths to execute as tests.",
    )
    parser.add_argument(
        "--databricks-runtime",
        default=DEFAULT_DATABRICKS_RUNTIME,
        help=f"Databricks runtime version (default: {DEFAULT_DATABRICKS_RUNTIME}).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_MINUTES,
        help=f"Timeout in minutes for cluster startup and each notebook run "
        f"(default: {DEFAULT_TIMEOUT_MINUTES}).",
    )
    parser.add_argument(
        "--github-repo",
        default=DEFAULT_GITHUB_REPO,
        help=f"GitHub repository for R package download "
        f"(default: {DEFAULT_GITHUB_REPO}).",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile to use (e.g., 'pathling'). "
        "If not set, uses the DATABRICKS_CONFIG_PROFILE environment variable "
        "or the Databricks CLI default.",
    )
    return parser.parse_args(argv)


def run_command(
    args: list[str],
    *,
    capture_json: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess | dict:
    """Run a subprocess command, optionally parsing JSON output."""
    result = subprocess.run(args, capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        error_detail = stderr or stdout
        raise RuntimeError(
            f"Command failed (exit {result.returncode}): {' '.join(args)}\n"
            f"{error_detail}"
        )
    if capture_json:
        return json.loads(result.stdout)
    return result


def info(message: str) -> None:
    """Print an informational message to stdout."""
    print(f">> {message}", flush=True)


def error(message: str) -> None:
    """Print an error message to stderr."""
    print(f"!! {message}", file=sys.stderr, flush=True)


def get_cluster_info(cluster_id: str) -> dict:
    """Retrieve the current cluster configuration."""
    return run_command(
        ["databricks", "clusters", "get", cluster_id, "--output", "json"],
        capture_json=True,
    )


def terminate_cluster(cluster_id: str, timeout_minutes: int) -> None:
    """Terminate the cluster and wait for it to reach TERMINATED state."""
    cluster = get_cluster_info(cluster_id)
    state = cluster.get("state", "UNKNOWN")

    if state == "TERMINATED":
        info("Cluster is already terminated.")
        return

    info(f"Terminating cluster (current state: {state})...")
    run_command(
        [
            "databricks",
            "clusters",
            "delete",
            cluster_id,
            "--timeout",
            f"{timeout_minutes}m0s",
        ]
    )
    info("Cluster terminated.")


def build_cluster_edit_payload(
    cluster_info: dict,
    runtime_version: str,
) -> dict:
    """
    Build the JSON payload for editing cluster configuration.

    Preserves existing cluster settings while updating the runtime version
    and ensuring the Java 21 environment variable is set.
    """
    # Start with key fields from the existing config that must be preserved.
    payload = {
        "cluster_id": cluster_info["cluster_id"],
        "cluster_name": cluster_info["cluster_name"],
        "spark_version": runtime_version,
        "node_type_id": cluster_info.get("node_type_id", ""),
    }

    # Preserve worker configuration.
    if "num_workers" in cluster_info:
        payload["num_workers"] = cluster_info["num_workers"]
    if "autoscale" in cluster_info:
        payload["autoscale"] = cluster_info["autoscale"]

    # Preserve optional fields if present.
    for field in [
        "driver_node_type_id",
        "spark_conf",
        "custom_tags",
        "cluster_log_conf",
        "init_scripts",
        "aws_attributes",
        "azure_attributes",
        "gcp_attributes",
        "policy_id",
        "enable_elastic_disk",
        "data_security_mode",
        "runtime_engine",
        "single_user_name",
    ]:
        if field in cluster_info:
            payload[field] = cluster_info[field]

    # Merge the Java 21 environment variable into existing env vars.
    existing_env = cluster_info.get("spark_env_vars", {})
    payload["spark_env_vars"] = {**existing_env, **JAVA_ENV_VAR}

    return payload


def cluster_needs_reconfiguration(
    cluster_info: dict,
    runtime_version: str,
) -> bool:
    """Check whether the cluster needs to be reconfigured."""
    current_runtime = cluster_info.get("spark_version", "")
    current_env = cluster_info.get("spark_env_vars", {})
    current_jname = current_env.get("JNAME", "")

    if current_runtime != runtime_version:
        info(f"Runtime mismatch: current={current_runtime}, required={runtime_version}")
        return True
    if current_jname != JAVA_ENV_VAR["JNAME"]:
        info(
            f"JNAME mismatch: current={current_jname!r}, "
            f"required={JAVA_ENV_VAR['JNAME']!r}"
        )
        return True
    return False


def configure_cluster(
    cluster_id: str,
    runtime_version: str,
) -> None:
    """Edit the cluster configuration to set runtime and environment."""
    cluster_info = get_cluster_info(cluster_id)

    payload = build_cluster_edit_payload(cluster_info, runtime_version)

    info(
        f"Configuring cluster: runtime={runtime_version}, "
        f"JNAME={JAVA_ENV_VAR['JNAME']}..."
    )
    run_command(
        [
            "databricks",
            "clusters",
            "edit",
            "--json",
            json.dumps(payload),
            "--no-wait",
        ]
    )
    info("Cluster configuration updated.")


def get_library_status(cluster_id: str) -> list[dict]:
    """Get the current library statuses for the cluster."""
    result = run_command(
        [
            "databricks",
            "libraries",
            "cluster-status",
            cluster_id,
            "--output",
            "json",
        ],
        capture_json=True,
    )
    # The CLI returns a list directly, while the REST API wraps it in
    # {"library_statuses": [...]}.
    if isinstance(result, list):
        return result
    return result.get("library_statuses", [])


def is_pathling_library(library: dict) -> bool:
    """Check whether a library definition is a Pathling library."""
    maven = library.get("maven", {})
    if maven:
        coords = maven.get("coordinates", "")
        if coords.startswith(f"{PATHLING_MAVEN_GROUP}:{PATHLING_MAVEN_ARTIFACT}:"):
            return True

    pypi = library.get("pypi", {})
    if pypi:
        package = pypi.get("package", "")
        # Match "pathling", "pathling==1.2.3", "pathling>=1.0", etc.
        if package == PATHLING_PYPI_PACKAGE or package.startswith(
            f"{PATHLING_PYPI_PACKAGE}=="
        ):
            return True

    return False


def build_install_payload(
    cluster_id: str,
    maven_version: str,
    pypi_version: str,
) -> dict:
    """Build the JSON payload for installing Pathling libraries."""
    return {
        "cluster_id": cluster_id,
        "libraries": [
            {
                "maven": {
                    "coordinates": (
                        f"{PATHLING_MAVEN_GROUP}:"
                        f"{PATHLING_MAVEN_ARTIFACT}:"
                        f"{maven_version}"
                    ),
                    "repo": MAVEN_SNAPSHOTS_REPO,
                },
            },
            {
                "pypi": {
                    "package": f"{PATHLING_PYPI_PACKAGE}=={pypi_version}",
                },
            },
        ],
    }


def uninstall_pathling_libraries(cluster_id: str) -> None:
    """Uninstall any existing Pathling libraries from the cluster."""
    statuses = get_library_status(cluster_id)
    pathling_libs = [
        s["library"] for s in statuses if is_pathling_library(s["library"])
    ]

    if not pathling_libs:
        info("No existing Pathling libraries to uninstall.")
        return

    info(f"Uninstalling {len(pathling_libs)} existing Pathling library/libraries...")
    payload = {"cluster_id": cluster_id, "libraries": pathling_libs}
    run_command(["databricks", "libraries", "uninstall", "--json", json.dumps(payload)])
    info("Uninstall requested (will take effect on next restart).")


def install_pathling_libraries(
    cluster_id: str,
    maven_version: str,
    pypi_version: str,
) -> None:
    """Install the specified Pathling Maven and PyPI libraries."""
    payload = build_install_payload(cluster_id, maven_version, pypi_version)

    info(
        f"Installing libraries: Maven {PATHLING_MAVEN_GROUP}:"
        f"{PATHLING_MAVEN_ARTIFACT}:{maven_version}, "
        f"PyPI {PATHLING_PYPI_PACKAGE}=={pypi_version}..."
    )
    run_command(["databricks", "libraries", "install", "--json", json.dumps(payload)])
    info("Library installation requested.")


def start_cluster(cluster_id: str, timeout_minutes: int) -> None:
    """Start the cluster and wait for it to reach RUNNING state."""
    cluster = get_cluster_info(cluster_id)
    state = cluster.get("state", "UNKNOWN")

    if state == "RUNNING":
        info("Cluster is already running.")
        return

    info(f"Starting cluster (current state: {state})...")
    run_command(
        [
            "databricks",
            "clusters",
            "start",
            cluster_id,
            "--timeout",
            f"{timeout_minutes}m0s",
        ]
    )
    info("Cluster is now running.")


def restart_cluster(cluster_id: str, timeout_minutes: int) -> None:
    """Restart a running cluster and wait for it to reach RUNNING state."""
    info("Restarting cluster to pick up library changes...")
    run_command(
        [
            "databricks",
            "clusters",
            "restart",
            cluster_id,
            "--timeout",
            f"{timeout_minutes}m0s",
        ]
    )
    info("Cluster restarted and running.")


def wait_for_libraries_installed(
    cluster_id: str,
    timeout_minutes: int,
) -> None:
    """Poll library status until all libraries are installed or failed."""
    deadline = time.time() + timeout_minutes * 60
    info("Waiting for libraries to be installed...")

    while time.time() < deadline:
        statuses = get_library_status(cluster_id)
        if not statuses:
            info("All libraries installed successfully.")
            return

        pending = []
        failed = []
        for s in statuses:
            status = s.get("status", "UNKNOWN")
            lib_desc = _describe_library(s.get("library", {}))
            if status in ("PENDING", "RESOLVING", "INSTALLING"):
                pending.append(lib_desc)
            elif status == "FAILED":
                failed.append((lib_desc, s.get("messages", ["Unknown error"])))

        if failed:
            for lib_desc, messages in failed:
                error(f"Library installation failed: {lib_desc}")
                for msg in messages:
                    error(f"  {msg}")
            raise RuntimeError("One or more libraries failed to install.")

        if not pending:
            info("All libraries installed successfully.")
            return

        info(f"  {len(pending)} library/libraries still installing...")
        time.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f"Library installation timed out after {timeout_minutes} minutes."
    )


def _describe_library(library: dict) -> str:
    """Return a human-readable description of a library."""
    if "maven" in library:
        return f"Maven: {library['maven'].get('coordinates', '?')}"
    if "pypi" in library:
        return f"PyPI: {library['pypi'].get('package', '?')}"
    if "jar" in library:
        return f"JAR: {library['jar']}"
    return str(library)


def download_r_package(
    github_run_id: str,
    github_repo: str,
) -> Path:
    """
    Download the R package artifact from a GitHub Actions run.

    Returns the path to the extracted R package tarball.
    """
    info(
        f"Downloading R package from GitHub Actions run {github_run_id} "
        f"({github_repo})..."
    )

    tmpdir = Path(tempfile.mkdtemp(prefix="pathling-r-"))
    download_dir = tmpdir / "r-package"

    run_command(
        [
            "gh",
            "run",
            "download",
            github_run_id,
            "--repo",
            github_repo,
            "--name",
            "r-package",
            "--dir",
            str(download_dir),
        ]
    )

    # Find the R package tarball in the downloaded artifact.
    tarballs = list(download_dir.glob("pathling_*.tar.gz"))
    if not tarballs:
        raise RuntimeError(
            f"No R package tarball (pathling_*.tar.gz) found in artifact "
            f"from run {github_run_id}."
        )

    r_package_path = tarballs[0]
    info(f"Downloaded R package: {r_package_path.name}")
    return r_package_path


def upload_r_package_to_dbfs(local_path: Path) -> str:
    """Upload the R package tarball to DBFS with a fixed name."""
    dbfs_path = f"{DBFS_R_PACKAGE_DIR}/{R_PACKAGE_UPLOAD_NAME}"

    info(f"Uploading R package to {dbfs_path}...")
    # Ensure the target directory exists.
    run_command(["databricks", "fs", "mkdir", DBFS_R_PACKAGE_DIR], check=False)
    run_command(["databricks", "fs", "cp", str(local_path), dbfs_path, "--overwrite"])
    info("R package uploaded to DBFS.")
    return dbfs_path


def build_notebook_run_payload(
    cluster_id: str,
    notebook_path: str,
    timeout_seconds: int,
) -> dict:
    """Build the JSON payload for submitting a notebook run via the v2.1 API."""
    return {
        "run_name": f"pathling-release-test: {notebook_path}",
        "tasks": [
            {
                "task_key": "notebook_test",
                "existing_cluster_id": cluster_id,
                "notebook_task": {
                    "notebook_path": notebook_path,
                },
            },
        ],
        "timeout_seconds": timeout_seconds,
    }


def submit_notebook(
    cluster_id: str,
    notebook_path: str,
    timeout_seconds: int,
) -> dict:
    """
    Submit a notebook run without waiting for completion.

    Returns a dict with notebook_path and run_id.
    """
    payload = build_notebook_run_payload(cluster_id, notebook_path, timeout_seconds)

    info(f"Submitting notebook run: {notebook_path}...")
    submit_result = run_command(
        [
            "databricks",
            "jobs",
            "submit",
            "--json",
            json.dumps(payload),
            "--no-wait",
            "--output",
            "json",
        ],
        capture_json=True,
    )

    run_id = str(submit_result["run_id"])
    info(f"  Run ID: {run_id}")
    return {"notebook_path": notebook_path, "run_id": run_id}


def poll_notebook_runs(
    pending_runs: list[dict],
    timeout_minutes: int,
) -> list[dict]:
    """
    Poll multiple notebook runs until all complete or time out.

    Each entry in pending_runs must have notebook_path and run_id.
    Returns a list of result dicts.
    """
    timeout_seconds = timeout_minutes * 60
    deadline = time.time() + timeout_seconds
    results = []
    active = list(pending_runs)

    while active and time.time() < deadline:
        still_running = []
        for run in active:
            run_id = run["run_id"]
            notebook_path = run["notebook_path"]

            run_info = run_command(
                ["databricks", "jobs", "get-run", run_id, "--output", "json"],
                capture_json=True,
            )

            state = run_info.get("state", {})
            lifecycle = state.get("life_cycle_state", "UNKNOWN")

            if lifecycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                result_state = state.get("result_state", "UNKNOWN")
                state_message = state.get("state_message", "")
                run_url = run_info.get("run_page_url", "")
                success = result_state == "SUCCESS"

                results.append(
                    {
                        "notebook_path": notebook_path,
                        "success": success,
                        "run_id": run_id,
                        "run_url": run_url,
                        "result_state": result_state,
                        "state_message": state_message,
                    }
                )
                status = "passed" if success else "FAILED"
                info(f"  Notebook {status}: {notebook_path}")
            else:
                info(f"  Notebook {notebook_path}: {lifecycle}...")
                still_running.append(run)

        active = still_running
        if active:
            time.sleep(POLL_INTERVAL_SECONDS)

    # Cancel any runs that timed out.
    for run in active:
        run_id = run["run_id"]
        notebook_path = run["notebook_path"]
        info(f"  Notebook run timed out: {notebook_path}, cancelling...")
        run_command(
            ["databricks", "jobs", "cancel-run", run_id],
            check=False,
        )
        results.append(
            {
                "notebook_path": notebook_path,
                "success": False,
                "run_id": run_id,
                "run_url": "",
                "result_state": "TIMEDOUT",
                "state_message": f"Run timed out after {timeout_minutes} minutes.",
            }
        )

    return results


def print_summary(results: list[dict]) -> None:
    """Print a summary of notebook run results."""
    print("\n" + "=" * 60)
    print("NOTEBOOK TEST RESULTS")
    print("=" * 60)

    all_passed = True
    for r in results:
        status = "PASS" if r["success"] else "FAIL"
        if not r["success"]:
            all_passed = False
        print(f"  [{status}] {r['notebook_path']}")
        if r.get("run_url"):
            print(f"         {r['run_url']}")
        if not r["success"] and r.get("state_message"):
            print(f"         Error: {r['state_message']}")

    print("=" * 60)
    if all_passed:
        print("All notebooks passed.")
    else:
        print("One or more notebooks FAILED.")
    print("=" * 60 + "\n")


def main(argv: list[str] | None = None) -> int:
    """Run the Databricks release test workflow."""
    args = parse_args(argv)

    # Set the Databricks CLI profile via environment variable so that all
    # subprocess calls to the databricks CLI inherit it.
    if args.profile:
        os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile

    info("Pathling Databricks release test")
    info(f"  Maven version:   {args.maven_version}")
    info(f"  PyPI version:    {args.pypi_version}")
    info(f"  GitHub run ID:   {args.github_run_id}")
    info(f"  Cluster ID:      {args.cluster_id}")
    info(f"  Runtime:         {args.databricks_runtime}")
    info(f"  Notebooks:       {', '.join(args.notebooks)}")
    info(f"  Timeout:         {args.timeout} minutes")
    print()

    try:
        # Check whether the cluster configuration needs updating.
        cluster_info = get_cluster_info(args.cluster_id)
        needs_reconfig = cluster_needs_reconfiguration(
            cluster_info, args.databricks_runtime
        )

        if needs_reconfig:
            # Step 1: Terminate the cluster for a clean configuration.
            info("Step 1: Terminating cluster...")
            terminate_cluster(args.cluster_id, args.timeout)
            print()

            # Step 2: Configure cluster runtime and environment.
            info("Step 2: Configuring cluster...")
            configure_cluster(args.cluster_id, args.databricks_runtime)
            print()
        else:
            info("Cluster configuration is already correct, skipping reconfiguration.")
            print()

        # Step 3: Uninstall existing Pathling libraries.
        info("Step 3: Removing existing Pathling libraries...")
        uninstall_pathling_libraries(args.cluster_id)
        print()

        # Step 4: Install new Pathling libraries.
        info("Step 4: Installing new Pathling libraries...")
        install_pathling_libraries(
            args.cluster_id, args.maven_version, args.pypi_version
        )
        print()

        # Step 5: Download R package from GitHub Actions.
        info("Step 5: Downloading R package...")
        r_package_path = download_r_package(args.github_run_id, args.github_repo)
        print()

        # Step 6: Upload R package to DBFS.
        info("Step 6: Uploading R package to DBFS...")
        upload_r_package_to_dbfs(r_package_path)
        print()

        # Step 7: Start or restart the cluster.
        info("Step 7: Starting cluster...")
        if needs_reconfig or cluster_info.get("state") != "RUNNING":
            start_cluster(args.cluster_id, args.timeout)
        else:
            restart_cluster(args.cluster_id, args.timeout)
        print()

        # Step 8: Wait for libraries to be installed.
        info("Step 8: Waiting for library installation...")
        wait_for_libraries_installed(args.cluster_id, args.timeout)
        print()

        # Step 9: Run test notebooks in parallel.
        info("Step 9: Running test notebooks...")
        timeout_seconds = args.timeout * 60
        pending_runs = []
        for notebook in args.notebooks:
            run = submit_notebook(args.cluster_id, notebook, timeout_seconds)
            pending_runs.append(run)
        print()

        info("Waiting for notebook runs to complete...")
        results = poll_notebook_runs(pending_runs, args.timeout)
        print()

        # Step 10: Print summary.
        print_summary(results)

        all_passed = all(r["success"] for r in results)
        return 0 if all_passed else 1

    except RuntimeError as e:
        error(str(e))
        return 1
    except KeyboardInterrupt:
        error("Interrupted by user.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
