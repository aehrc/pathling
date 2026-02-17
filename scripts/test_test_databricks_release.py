"""
Tests for the Databricks release testing script.

Author: John Grimes
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from test_databricks_release import (
    build_cluster_edit_payload,
    build_install_payload,
    build_notebook_run_payload,
    cluster_needs_reconfiguration,
    is_pathling_library,
    main,
    parse_args,
)


class TestParseArgs:
    """Tests for argument parsing."""

    def test_all_required_args(self):
        """All required arguments are correctly parsed."""
        args = parse_args(
            [
                "--maven-version",
                "9.4.0-SNAPSHOT",
                "--pypi-version",
                "9.4.0.dev0",
                "--github-run-id",
                "12345678",
                "--cluster-id",
                "0123-456789-abc",
                "--notebooks",
                "/path/to/notebook1",
                "/path/to/notebook2",
            ]
        )
        assert args.maven_version == "9.4.0-SNAPSHOT"
        assert args.pypi_version == "9.4.0.dev0"
        assert args.github_run_id == "12345678"
        assert args.cluster_id == "0123-456789-abc"
        assert args.notebooks == ["/path/to/notebook1", "/path/to/notebook2"]

    def test_default_values(self):
        """Optional arguments use sensible defaults."""
        args = parse_args(
            [
                "--maven-version",
                "9.4.0-SNAPSHOT",
                "--pypi-version",
                "9.4.0.dev0",
                "--github-run-id",
                "123",
                "--cluster-id",
                "abc",
                "--notebooks",
                "/nb",
            ]
        )
        assert args.databricks_runtime == "17.3.x-scala2.13"
        assert args.timeout == 30
        assert args.github_repo == "aehrc/pathling"

    def test_custom_optional_args(self):
        """Custom values for optional arguments are correctly parsed."""
        args = parse_args(
            [
                "--maven-version",
                "9.4.0-SNAPSHOT",
                "--pypi-version",
                "9.4.0.dev0",
                "--github-run-id",
                "123",
                "--cluster-id",
                "abc",
                "--notebooks",
                "/nb",
                "--databricks-runtime",
                "16.4.x-scala2.12",
                "--timeout",
                "60",
                "--github-repo",
                "other/repo",
            ]
        )
        assert args.databricks_runtime == "16.4.x-scala2.12"
        assert args.timeout == 60
        assert args.github_repo == "other/repo"

    def test_missing_required_arg_exits(self):
        """Missing a required argument causes a SystemExit."""
        with pytest.raises(SystemExit):
            parse_args(["--maven-version", "9.4.0-SNAPSHOT"])

    def test_multiple_notebooks(self):
        """Multiple notebook paths are collected into a list."""
        args = parse_args(
            [
                "--maven-version",
                "v",
                "--pypi-version",
                "v",
                "--github-run-id",
                "1",
                "--cluster-id",
                "c",
                "--notebooks",
                "/nb1",
                "/nb2",
                "/nb3",
            ]
        )
        assert args.notebooks == ["/nb1", "/nb2", "/nb3"]


class TestIsPathlingLibrary:
    """Tests for identifying Pathling libraries in cluster library lists."""

    def test_maven_pathling_library(self):
        """A Maven library with Pathling coordinates is identified."""
        lib = {
            "maven": {
                "coordinates": "au.csiro.pathling:library-runtime:9.3.0",
            }
        }
        assert is_pathling_library(lib) is True

    def test_maven_snapshot_library(self):
        """A Maven SNAPSHOT library with Pathling coordinates is identified."""
        lib = {
            "maven": {
                "coordinates": "au.csiro.pathling:library-runtime:9.4.0-SNAPSHOT",
                "repo": "https://central.sonatype.com/repository/maven-snapshots/",
            }
        }
        assert is_pathling_library(lib) is True

    def test_pypi_pathling_library(self):
        """A PyPI Pathling package is identified."""
        lib = {"pypi": {"package": "pathling==9.3.0"}}
        assert is_pathling_library(lib) is True

    def test_pypi_pathling_no_version(self):
        """A PyPI Pathling package without a version pin is identified."""
        lib = {"pypi": {"package": "pathling"}}
        assert is_pathling_library(lib) is True

    def test_unrelated_maven_library(self):
        """An unrelated Maven library is not identified as Pathling."""
        lib = {
            "maven": {
                "coordinates": "org.apache.spark:spark-core_2.12:3.5.0",
            }
        }
        assert is_pathling_library(lib) is False

    def test_unrelated_pypi_library(self):
        """An unrelated PyPI package is not identified as Pathling."""
        lib = {"pypi": {"package": "pandas==2.0.0"}}
        assert is_pathling_library(lib) is False

    def test_jar_library(self):
        """A JAR library is not identified as Pathling."""
        lib = {"jar": "dbfs:/path/to/some.jar"}
        assert is_pathling_library(lib) is False

    def test_empty_library(self):
        """An empty library definition is not identified as Pathling."""
        assert is_pathling_library({}) is False


class TestBuildClusterEditPayload:
    """Tests for building the cluster edit API payload."""

    def test_basic_payload(self):
        """A basic cluster config produces the expected payload."""
        cluster_info = {
            "cluster_id": "test-123",
            "cluster_name": "test-cluster",
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
        }
        payload = build_cluster_edit_payload(cluster_info, "17.3.x-scala2.12")

        assert payload["cluster_id"] == "test-123"
        assert payload["cluster_name"] == "test-cluster"
        assert payload["spark_version"] == "17.3.x-scala2.12"
        assert payload["node_type_id"] == "i3.xlarge"
        assert payload["num_workers"] == 2
        assert payload["spark_env_vars"]["JNAME"] == "zulu21-ca-amd64"

    def test_preserves_existing_env_vars(self):
        """Existing environment variables are preserved alongside JNAME."""
        cluster_info = {
            "cluster_id": "test-123",
            "cluster_name": "test-cluster",
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 1,
            "spark_env_vars": {"MY_VAR": "my_value", "OTHER": "other"},
        }
        payload = build_cluster_edit_payload(cluster_info, "17.3.x-scala2.12")

        assert payload["spark_env_vars"]["MY_VAR"] == "my_value"
        assert payload["spark_env_vars"]["OTHER"] == "other"
        assert payload["spark_env_vars"]["JNAME"] == "zulu21-ca-amd64"

    def test_jname_overrides_existing(self):
        """If JNAME is already set, it is overridden with the correct value."""
        cluster_info = {
            "cluster_id": "test-123",
            "cluster_name": "test-cluster",
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 1,
            "spark_env_vars": {"JNAME": "old-value"},
        }
        payload = build_cluster_edit_payload(cluster_info, "17.3.x-scala2.12")

        assert payload["spark_env_vars"]["JNAME"] == "zulu21-ca-amd64"

    def test_preserves_autoscale(self):
        """Autoscale configuration is preserved."""
        cluster_info = {
            "cluster_id": "test-123",
            "cluster_name": "test-cluster",
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "autoscale": {"min_workers": 1, "max_workers": 4},
        }
        payload = build_cluster_edit_payload(cluster_info, "17.3.x-scala2.12")

        assert payload["autoscale"] == {"min_workers": 1, "max_workers": 4}
        assert "num_workers" not in payload

    def test_preserves_optional_fields(self):
        """Optional fields like spark_conf and custom_tags are preserved."""
        cluster_info = {
            "cluster_id": "test-123",
            "cluster_name": "test-cluster",
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 1,
            "spark_conf": {"spark.speculation": "true"},
            "custom_tags": {"team": "data"},
            "policy_id": "ABCDEF",
        }
        payload = build_cluster_edit_payload(cluster_info, "17.3.x-scala2.12")

        assert payload["spark_conf"] == {"spark.speculation": "true"}
        assert payload["custom_tags"] == {"team": "data"}
        assert payload["policy_id"] == "ABCDEF"


class TestBuildInstallPayload:
    """Tests for building the library install API payload."""

    def test_install_payload(self):
        """The install payload has the correct Maven and PyPI libraries."""
        payload = build_install_payload("cluster-1", "9.4.0-SNAPSHOT", "9.4.0.dev0")

        assert payload["cluster_id"] == "cluster-1"
        assert len(payload["libraries"]) == 2

        maven_lib = payload["libraries"][0]["maven"]
        assert (
            maven_lib["coordinates"]
            == "au.csiro.pathling:library-runtime:9.4.0-SNAPSHOT"
        )
        assert (
            maven_lib["repo"]
            == "https://central.sonatype.com/repository/maven-snapshots/"
        )

        pypi_lib = payload["libraries"][1]["pypi"]
        assert pypi_lib["package"] == "pathling==9.4.0.dev0"


class TestBuildNotebookRunPayload:
    """Tests for building the notebook run submission payload."""

    def test_notebook_run_payload(self):
        """The notebook run payload uses the v2.1 tasks array format."""
        payload = build_notebook_run_payload("cluster-1", "/Users/test/notebook", 1800)

        assert len(payload["tasks"]) == 1
        task = payload["tasks"][0]
        assert task["existing_cluster_id"] == "cluster-1"
        assert task["notebook_task"]["notebook_path"] == "/Users/test/notebook"
        assert task["task_key"] == "notebook_test"
        assert payload["timeout_seconds"] == 1800
        assert "pathling-release-test" in payload["run_name"]


class TestClusterNeedsReconfiguration:
    """Tests for checking whether cluster reconfiguration is needed."""

    def test_matching_config_returns_false(self):
        """A cluster with correct runtime and JNAME needs no reconfiguration."""
        cluster_info = {
            "spark_version": "17.3.x-scala2.13",
            "spark_env_vars": {"JNAME": "zulu21-ca-amd64"},
        }
        assert cluster_needs_reconfiguration(cluster_info, "17.3.x-scala2.13") is False

    def test_mismatched_runtime_returns_true(self):
        """A cluster with the wrong runtime needs reconfiguration."""
        cluster_info = {
            "spark_version": "15.4.x-scala2.12",
            "spark_env_vars": {"JNAME": "zulu21-ca-amd64"},
        }
        assert cluster_needs_reconfiguration(cluster_info, "17.3.x-scala2.13") is True

    def test_mismatched_jname_returns_true(self):
        """A cluster with the wrong JNAME needs reconfiguration."""
        cluster_info = {
            "spark_version": "17.3.x-scala2.13",
            "spark_env_vars": {"JNAME": "old-java"},
        }
        assert cluster_needs_reconfiguration(cluster_info, "17.3.x-scala2.13") is True

    def test_missing_jname_returns_true(self):
        """A cluster without JNAME set needs reconfiguration."""
        cluster_info = {
            "spark_version": "17.3.x-scala2.13",
            "spark_env_vars": {},
        }
        assert cluster_needs_reconfiguration(cluster_info, "17.3.x-scala2.13") is True

    def test_missing_env_vars_returns_true(self):
        """A cluster without spark_env_vars needs reconfiguration."""
        cluster_info = {
            "spark_version": "17.3.x-scala2.13",
        }
        assert cluster_needs_reconfiguration(cluster_info, "17.3.x-scala2.13") is True


class TestMainIntegration:
    """Integration tests for the main workflow using mocked subprocesses."""

    @patch("test_databricks_release.run_command")
    @patch("test_databricks_release.download_r_package")
    @patch("test_databricks_release.upload_r_package_to_dbfs")
    def test_successful_run(self, mock_upload, mock_download, mock_run_cmd):
        """A complete successful run exits with status 0."""
        # Set up return values for each CLI call.
        cluster_info = {
            "cluster_id": "test-123",
            "cluster_name": "test",
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 1,
            "state": "TERMINATED",
            "spark_env_vars": {},
        }

        def command_side_effect(args, **kwargs):
            cmd = " ".join(args)
            if "clusters get" in cmd and kwargs.get("capture_json"):
                return cluster_info
            if "clusters delete" in cmd:
                return MagicMock(returncode=0)
            if "clusters edit" in cmd:
                return MagicMock(returncode=0)
            if "libraries cluster-status" in cmd and kwargs.get("capture_json"):
                return {"library_statuses": []}
            if "libraries install" in cmd:
                return MagicMock(returncode=0)
            if "clusters start" in cmd:
                return MagicMock(returncode=0)
            if "jobs submit" in cmd and kwargs.get("capture_json"):
                return {"run_id": 42}
            if "jobs get-run" in cmd and kwargs.get("capture_json"):
                return {
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS",
                        "state_message": "",
                    },
                    "run_page_url": "https://example.com/run/42",
                }
            return MagicMock(returncode=0)

        mock_run_cmd.side_effect = command_side_effect
        mock_download.return_value = Path("/tmp/pathling_9.4.0.tar.gz")
        mock_upload.return_value = "dbfs:/FileStore/R/pathling_latest_tar.gz"

        exit_code = main(
            [
                "--maven-version",
                "9.4.0-SNAPSHOT",
                "--pypi-version",
                "9.4.0.dev0",
                "--github-run-id",
                "123",
                "--cluster-id",
                "test-123",
                "--notebooks",
                "/test/nb1",
            ]
        )

        assert exit_code == 0

    @patch("test_databricks_release.run_command")
    @patch("test_databricks_release.download_r_package")
    @patch("test_databricks_release.upload_r_package_to_dbfs")
    def test_notebook_failure_returns_nonzero(
        self, mock_upload, mock_download, mock_run_cmd
    ):
        """A failed notebook run causes exit with status 1."""
        cluster_info = {
            "cluster_id": "test-123",
            "cluster_name": "test",
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 1,
            "state": "TERMINATED",
            "spark_env_vars": {},
        }

        def command_side_effect(args, **kwargs):
            cmd = " ".join(args)
            if "clusters get" in cmd and kwargs.get("capture_json"):
                return cluster_info
            if "clusters delete" in cmd:
                return MagicMock(returncode=0)
            if "clusters edit" in cmd:
                return MagicMock(returncode=0)
            if "libraries cluster-status" in cmd and kwargs.get("capture_json"):
                return {"library_statuses": []}
            if "libraries install" in cmd:
                return MagicMock(returncode=0)
            if "clusters start" in cmd:
                return MagicMock(returncode=0)
            if "jobs submit" in cmd and kwargs.get("capture_json"):
                return {"run_id": 42}
            if "jobs get-run" in cmd and kwargs.get("capture_json"):
                return {
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                        "state_message": "Notebook execution error",
                    },
                    "run_page_url": "https://example.com/run/42",
                }
            return MagicMock(returncode=0)

        mock_run_cmd.side_effect = command_side_effect
        mock_download.return_value = Path("/tmp/pathling_9.4.0.tar.gz")
        mock_upload.return_value = "dbfs:/FileStore/R/pathling_latest_tar.gz"

        exit_code = main(
            [
                "--maven-version",
                "9.4.0-SNAPSHOT",
                "--pypi-version",
                "9.4.0.dev0",
                "--github-run-id",
                "123",
                "--cluster-id",
                "test-123",
                "--notebooks",
                "/test/nb1",
            ]
        )

        assert exit_code == 1

    @patch("test_databricks_release.run_command")
    @patch("test_databricks_release.download_r_package")
    @patch("test_databricks_release.upload_r_package_to_dbfs")
    def test_skips_reconfiguration_when_already_correct(
        self, mock_upload, mock_download, mock_run_cmd
    ):
        """Cluster reconfiguration is skipped when config already matches."""
        cluster_info = {
            "cluster_id": "test-123",
            "cluster_name": "test",
            "spark_version": "17.3.x-scala2.13",
            "node_type_id": "i3.xlarge",
            "num_workers": 1,
            "state": "RUNNING",
            "spark_env_vars": {"JNAME": "zulu21-ca-amd64"},
        }

        def command_side_effect(args, **kwargs):
            cmd = " ".join(args)
            if "clusters get" in cmd and kwargs.get("capture_json"):
                return cluster_info
            if "libraries cluster-status" in cmd and kwargs.get("capture_json"):
                return []
            if "libraries install" in cmd:
                return MagicMock(returncode=0)
            if "clusters restart" in cmd:
                return MagicMock(returncode=0)
            if "jobs submit" in cmd and kwargs.get("capture_json"):
                return {"run_id": 42}
            if "jobs get-run" in cmd and kwargs.get("capture_json"):
                return {
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS",
                        "state_message": "",
                    },
                    "run_page_url": "https://example.com/run/42",
                }
            return MagicMock(returncode=0)

        mock_run_cmd.side_effect = command_side_effect
        mock_download.return_value = Path("/tmp/pathling_9.4.0.tar.gz")
        mock_upload.return_value = "dbfs:/FileStore/R/pathling_latest_tar.gz"

        exit_code = main(
            [
                "--maven-version",
                "9.4.0-SNAPSHOT",
                "--pypi-version",
                "9.4.0.dev0",
                "--github-run-id",
                "123",
                "--cluster-id",
                "test-123",
                "--notebooks",
                "/test/nb1",
            ]
        )

        assert exit_code == 0

        # Verify no terminate or configure calls were made.
        calls = [" ".join(call.args[0]) for call in mock_run_cmd.call_args_list]
        assert not any("clusters delete" in c for c in calls)
        assert not any("clusters edit" in c for c in calls)
        # Verify restart was called instead of start.
        assert any("clusters restart" in c for c in calls)

    @patch("test_databricks_release.run_command")
    def test_command_failure_returns_nonzero(self, mock_run_cmd):
        """A CLI command failure causes exit with status 1."""
        mock_run_cmd.side_effect = RuntimeError("Command failed")

        exit_code = main(
            [
                "--maven-version",
                "9.4.0-SNAPSHOT",
                "--pypi-version",
                "9.4.0.dev0",
                "--github-run-id",
                "123",
                "--cluster-id",
                "test-123",
                "--notebooks",
                "/test/nb1",
            ]
        )

        assert exit_code == 1
