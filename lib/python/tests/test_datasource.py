#
# Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
# Organisation (CSIRO) ABN 41 687 119 230.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import os
from tempfile import TemporaryDirectory
from uuid import uuid4

from flask import Response
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, Row
from pytest import fixture, mark, raises

from pathling.datasource import DataSource


@fixture(scope="function", autouse=True)
def func_temp_dir(temp_dir):
    """
    Fixture to create a temporary directory for each test function.
    :param temp_dir:
    :return: existing temporary directory for each test function.
    """
    temp_ndjson_dir = TemporaryDirectory(dir=temp_dir, prefix="function")
    yield temp_ndjson_dir.name
    temp_ndjson_dir.cleanup()


@fixture(scope="module")
def ndjson_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "ndjson")


@fixture(scope="module", autouse=True)
def temp_ndjson_dir(temp_dir):
    temp_ndjson_dir = TemporaryDirectory(dir=temp_dir, prefix="ndjson")
    yield temp_ndjson_dir.name
    temp_ndjson_dir.cleanup()


@fixture(scope="module")
def bundles_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "bundles")


@fixture(scope="module")
def parquet_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "parquet")


@fixture(scope="module", autouse=True)
def temp_parquet_dir(temp_dir):
    temp_parquet_dir = TemporaryDirectory(dir=temp_dir, prefix="parquet")
    yield temp_parquet_dir.name
    temp_parquet_dir.cleanup()


@fixture(scope="module")
def delta_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "delta")


@fixture(scope="module", autouse=True)
def temp_delta_dir(temp_dir):
    temp_delta_dir = TemporaryDirectory(dir=temp_dir, prefix="delta")
    yield temp_delta_dir.name
    temp_delta_dir.cleanup()


@fixture(scope="function")
def bulk_server(mock_server, ndjson_test_data_dir):
    @mock_server.route("/fhir/$export", methods=["GET"])
    def export():
        resp = Response(status=202)
        resp.headers["content-location"] = mock_server.url("/pool")
        return resp

    @mock_server.route("/pool", methods=["GET"])
    def pool():
        return dict(
            transactionTime="1970-01-01T00:00:00.000Z",
            output=[
                dict(
                    type=resource, url=mock_server.url(f"/download/{resource}"), count=1
                )
                for resource in ["Patient", "Condition"]
            ],
        )

    @mock_server.route("/download/<resource>", methods=["GET"])
    def download(resource):
        with open(os.path.join(ndjson_test_data_dir, f"{resource}.ndjson"), "r") as f:
            return f.read()

    return mock_server


ResultRow = Row("count")


def test_datasource_read(ndjson_test_data_dir, pathling_ctx):
    patients = pathling_ctx.read.ndjson(ndjson_test_data_dir).read("Patient")
    assert patients.count() == 9


def test_datasource_ndjson(ndjson_test_data_dir, temp_ndjson_dir, pathling_ctx):
    pathling_ctx.read.ndjson(ndjson_test_data_dir).write.ndjson(temp_ndjson_dir)
    data_source = pathling_ctx.read.ndjson(temp_ndjson_dir)

    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_ndjson_mapper(ndjson_test_data_dir, temp_ndjson_dir, pathling_ctx):
    pathling_ctx.read.ndjson(ndjson_test_data_dir).write.ndjson(
        temp_ndjson_dir, file_name_mapper=lambda x: f"Custom{x}"
    )
    data_source = pathling_ctx.read.ndjson(
        temp_ndjson_dir,
        file_name_mapper=lambda x: {x.replace("^Custom", "")},
    )

    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_bundles(bundles_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.bundles(
        bundles_test_data_dir, ["Patient", "Condition"]
    )

    result = bundles_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(10),
    ]


def test_datasource_datasets(ndjson_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.datasets(
        resources={
            "Patient": pathling_ctx.encode(
                pathling_ctx.spark.read.text(
                    os.path.join(ndjson_test_data_dir, "Patient.ndjson")
                ),
                "Patient",
            ),
            "Condition": pathling_ctx.encode(
                pathling_ctx.spark.read.text(
                    os.path.join(ndjson_test_data_dir, "Condition.ndjson")
                ),
                "Condition",
            ),
        }
    )
    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_parquet(parquet_test_data_dir, temp_parquet_dir, pathling_ctx):
    pathling_ctx.read.parquet(parquet_test_data_dir).write.parquet(temp_parquet_dir)
    data_source = pathling_ctx.read.parquet(temp_parquet_dir)

    result = parquet_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def write_divergent_parquet(spark, warehouse: str) -> str:
    """
    Writes two batches of Patient rows whose schemas diverge in both directions into one Parquet
    directory, as two appends of one file each, and returns the directory's path.
    """
    table = os.path.join(warehouse, "Patient.parquet")
    spark.createDataFrame([Row(id="p1", gender="male")]).coalesce(1).write.mode(
        "overwrite"
    ).parquet(table)
    spark.createDataFrame([Row(id="p2", birthDate="1990-01-01")]).coalesce(
        1
    ).write.mode("append").parquet(table)
    return table


def test_datasource_parquet_merges_divergent_files(func_temp_dir, pathling_ctx):
    write_divergent_parquet(pathling_ctx.spark, func_temp_dir)

    patients = pathling_ctx.read.parquet(func_temp_dir).read("Patient")

    assert {"id", "gender", "birthDate"} <= set(patients.columns)
    assert patients.count() == 2


def test_datasource_parquet_with_supplied_schemas(func_temp_dir, pathling_ctx):
    table = write_divergent_parquet(pathling_ctx.spark, func_temp_dir)
    union = pathling_ctx.spark.read.option("mergeSchema", "true").parquet(table).schema

    patients = pathling_ctx.read.parquet(
        func_temp_dir, schemas={"Patient": union}
    ).read("Patient")

    assert patients.schema == union
    assert patients.count() == 2


def test_datasource_delta(delta_test_data_dir, temp_delta_dir, pathling_ctx):
    pathling_ctx.read.delta(delta_test_data_dir).write.delta(temp_delta_dir)
    data_source = pathling_ctx.read.delta(temp_delta_dir)

    result = delta_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_delta_merge(delta_test_data_dir, temp_delta_dir, pathling_ctx):
    pathling_ctx.read.delta(delta_test_data_dir).write.delta(
        temp_delta_dir, save_mode="merge"
    )
    data_source = pathling_ctx.read.delta(temp_delta_dir)

    result = delta_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


PREVIOUS_LAYOUT_REFUSAL = (
    "The Patient data is stored in the layout written by earlier releases of Pathling"
)


def write_previous_layout(pathling_ctx, kind: str, warehouse: str):
    """
    Writes Patients carrying the root versioned id that the layout of earlier releases stores
    beside every resource id, through the given kind of storage, and returns a function that
    opens a data source over them, taking the layout detection setting.
    """
    spark = pathling_ctx.spark
    patients = spark.createDataFrame(
        [Row(id="p1", id_versioned="Patient/p1/_history/1", gender="female")]
    )
    table = os.path.join(warehouse, "Patient.parquet")
    if kind == "parquet":
        patients.write.parquet(table)
        return lambda detection: pathling_ctx.read.parquet(
            warehouse, layout_detection=detection
        )
    if kind == "delta":
        patients.write.format("delta").save(table)
        return lambda detection: pathling_ctx.read.delta(
            warehouse, layout_detection=detection
        )
    database = "previous_" + uuid4().hex
    spark.sql(f"CREATE DATABASE {database}")
    patients.write.format("delta").saveAsTable(f"{database}.Patient")
    return lambda detection: pathling_ctx.read.tables(
        schema=database, layout_detection=detection
    )


@mark.parametrize("kind", ["parquet", "delta", "tables"])
def test_previous_layout_is_refused(kind, func_temp_dir, pathling_ctx):
    open_source = write_previous_layout(pathling_ctx, kind, func_temp_dir)

    with raises(Py4JJavaError) as error:
        open_source(True).read("Patient")

    assert "UnsupportedLayoutException" in str(error.value)
    assert PREVIOUS_LAYOUT_REFUSAL in str(error.value)
    assert "Re-import the data from its source" in str(error.value)


@mark.parametrize("kind", ["parquet", "delta", "tables"])
def test_layout_detection_opt_out_permits_the_read(kind, func_temp_dir, pathling_ctx):
    open_source = write_previous_layout(pathling_ctx, kind, func_temp_dir)

    patients = open_source(False).read("Patient")

    assert "id_versioned" in patients.columns
    assert patients.count() == 1


def test_layout_detection_opt_out_is_scoped_to_one_source(func_temp_dir, pathling_ctx):
    open_source = write_previous_layout(pathling_ctx, "parquet", func_temp_dir)
    opted_out = open_source(False)

    assert opted_out.read("Patient").count() == 1
    with raises(Py4JJavaError):
        pathling_ctx.read.parquet(func_temp_dir).read("Patient")


@mark.parametrize("layout_detection", [True, False])
def test_conforming_data_reads_alike_with_and_without_layout_detection(
    layout_detection, parquet_test_data_dir, delta_test_data_dir, pathling_ctx
):
    parquet = pathling_ctx.read.parquet(
        parquet_test_data_dir, layout_detection=layout_detection
    )
    delta = pathling_ctx.read.delta(
        delta_test_data_dir, layout_detection=layout_detection
    )

    assert parquet_query(parquet).collect() == [ResultRow(71)]
    assert delta_query(delta).collect() == [ResultRow(71)]


def test_datasource_tables(ndjson_test_data_dir, pathling_ctx):
    pathling_ctx.read.ndjson(ndjson_test_data_dir).write.tables()

    data_source = pathling_ctx.read.tables()
    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_tables_schema(ndjson_test_data_dir, pathling_ctx):
    pathling_ctx.read.ndjson(ndjson_test_data_dir).write.tables(schema="test")

    data_source = pathling_ctx.read.tables(schema="test")
    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def write_patients(directory: str, patients: list) -> str:
    """
    Writes the given Patient resources to an NDJSON file in a new directory, and returns the
    directory's path.
    """
    os.makedirs(directory)
    with open(os.path.join(directory, "Patient.ndjson"), "w") as file:
        file.writelines(
            json.dumps({"resourceType": "Patient", **patient}) + "\n"
            for patient in patients
        )
    return directory


def new_database(spark) -> str:
    """Creates a database of its own for one test, and returns its name."""
    database = "tables_" + uuid4().hex
    spark.sql(f"CREATE DATABASE {database}")
    return database


def table_provider(spark, table: str) -> str:
    """Returns the format the catalog records for a table, such as "delta" or "parquet"."""
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    return next(row.data_type for row in rows if row.col_name == "Provider").lower()


def test_datasource_tables_delta_format_takes_wider_append(func_temp_dir, pathling_ctx):
    spark = pathling_ctx.spark
    database = new_database(spark)
    narrow = write_patients(
        os.path.join(func_temp_dir, "narrow"), [{"id": "p1", "gender": "male"}]
    )
    wide = write_patients(
        os.path.join(func_temp_dir, "wide"),
        [{"id": "p2", "gender": "female", "birthDate": "1990-01-01"}],
    )

    pathling_ctx.read.ndjson(narrow).write.tables(schema=database, format="delta")
    # The first batch's fitted schema lacks the element the second carries.
    assert "birthDate" not in spark.table(f"{database}.Patient").columns
    pathling_ctx.read.ndjson(wide).write.tables(
        schema=database, save_mode="append", format="delta"
    )

    patients = spark.table(f"{database}.Patient")
    assert table_provider(spark, f"{database}.Patient") == "delta"
    assert "birthDate" in patients.columns
    assert sorted(row.id for row in patients.collect()) == ["p1", "p2"]


def test_datasource_tables_delta_format_takes_second_merge(func_temp_dir, pathling_ctx):
    spark = pathling_ctx.spark
    database = new_database(spark)
    first = write_patients(
        os.path.join(func_temp_dir, "first"), [{"id": "p1", "gender": "male"}]
    )
    second = write_patients(
        os.path.join(func_temp_dir, "second"),
        [
            {"id": "p1", "gender": "female"},
            {"id": "p2", "gender": "other", "birthDate": "1990-01-01"},
        ],
    )

    pathling_ctx.read.ndjson(first).write.tables(
        schema=database, save_mode="merge", format="delta"
    )
    pathling_ctx.read.ndjson(second).write.tables(
        schema=database, save_mode="merge", format="delta"
    )

    patients = spark.table(f"{database}.Patient")
    assert table_provider(spark, f"{database}.Patient") == "delta"
    assert sorted((row.id, row.gender) for row in patients.collect()) == [
        ("p1", "female"),
        ("p2", "other"),
    ]


def test_datasource_tables_format_without_schema(func_temp_dir, pathling_ctx):
    spark = pathling_ctx.spark
    database = new_database(spark)
    patients = write_patients(
        os.path.join(func_temp_dir, "patients"), [{"id": "p1", "gender": "male"}]
    )
    previous = spark.catalog.currentDatabase()
    spark.catalog.setCurrentDatabase(database)
    try:
        pathling_ctx.read.ndjson(patients).write.tables(format="delta")
    finally:
        spark.catalog.setCurrentDatabase(previous)

    assert table_provider(spark, f"{database}.Patient") == "delta"


def test_datasource_tables_default_format(func_temp_dir, pathling_ctx):
    spark = pathling_ctx.spark
    database = new_database(spark)
    patients = write_patients(
        os.path.join(func_temp_dir, "patients"), [{"id": "p1", "gender": "male"}]
    )

    pathling_ctx.read.ndjson(patients).write.tables(schema=database)

    default_format = spark.conf.get("spark.sql.sources.default").lower()
    assert table_provider(spark, f"{database}.Patient") == default_format
    assert spark.table(f"{database}.Patient").count() == 1


def test_datasource_bulk_with_temp_dir(pathling_ctx, bulk_server):
    # !!! this directory cannot exist for the datasource to work
    with bulk_server.run():
        data_source = pathling_ctx.read.bulk(fhir_endpoint_url=bulk_server.url("/fhir"))
        result = ndjson_query(data_source)
        assert result.columns == list(ResultRow)
        assert result.collect() == [
            ResultRow(71),
        ]


def test_datasource_bulk_with_existing_dir(pathling_ctx, bulk_server, func_temp_dir):
    assert os.path.exists(func_temp_dir)
    with bulk_server.run():
        data_source = pathling_ctx.read.bulk(
            fhir_endpoint_url=bulk_server.url("/fhir"),
            output_dir=func_temp_dir,
            overwrite=True,  # default anyway, but explicit for clarity
        )
        result = ndjson_query(data_source)
        assert result.columns == list(ResultRow)
        assert result.collect() == [
            ResultRow(71),
        ]


def ndjson_query(data_source: DataSource) -> DataFrame:
    return (
        data_source.view(
            resource="Condition", select=[{"column": [{"path": "id", "name": "id"}]}]
        )
        .groupby()
        .count()
    )


def bundles_query(data_source: DataSource) -> DataFrame:
    return (
        data_source.view(
            resource="Patient", select=[{"column": [{"path": "id", "name": "id"}]}]
        )
        .groupby()
        .count()
    )


def parquet_query(data_source: DataSource) -> DataFrame:
    return ndjson_query(data_source)


def delta_query(data_source: DataSource) -> DataFrame:
    return ndjson_query(data_source)


def test_datasource_resource_types(ndjson_test_data_dir, pathling_ctx):
    """Test that resource_types() returns a list of strings."""
    data_source = pathling_ctx.read.ndjson(ndjson_test_data_dir)

    # Call the resource_types method.
    resource_types = data_source.resource_types()

    # Verify it returns a list.
    assert isinstance(resource_types, list)

    # Verify the list contains strings.
    assert all(isinstance(rt, str) for rt in resource_types)

    # Verify expected resource types are present.
    assert "Patient" in resource_types
    assert "Condition" in resource_types


# Tests for WriteDetails return value from write operations.


def test_ndjson_write_returns_details(
    ndjson_test_data_dir, func_temp_dir, pathling_ctx
):
    """Verify that ndjson write returns WriteDetails with file information."""
    result = pathling_ctx.read.ndjson(ndjson_test_data_dir).write.ndjson(func_temp_dir)

    # Verify result is a WriteDetails object with file_infos.
    assert result is not None
    assert hasattr(result, "file_infos")
    assert len(result.file_infos) > 0

    # Verify each file_info has the expected attributes.
    for file_info in result.file_infos:
        assert hasattr(file_info, "fhir_resource_type")
        assert hasattr(file_info, "absolute_url")
        assert file_info.fhir_resource_type is not None
        assert file_info.absolute_url is not None

    # Verify that expected resource types are present.
    resource_types = {fi.fhir_resource_type for fi in result.file_infos}
    assert "Patient" in resource_types
    assert "Condition" in resource_types


def test_parquet_write_returns_details(
    parquet_test_data_dir, func_temp_dir, pathling_ctx
):
    """Verify that parquet write returns WriteDetails with file information."""
    result = pathling_ctx.read.parquet(parquet_test_data_dir).write.parquet(
        func_temp_dir
    )

    # Verify result is a WriteDetails object with file_infos.
    assert result is not None
    assert hasattr(result, "file_infos")
    assert len(result.file_infos) > 0

    # Verify each file_info has the expected attributes.
    for file_info in result.file_infos:
        assert hasattr(file_info, "fhir_resource_type")
        assert hasattr(file_info, "absolute_url")


def test_delta_write_returns_details(delta_test_data_dir, func_temp_dir, pathling_ctx):
    """Verify that delta write returns WriteDetails with file information."""
    result = pathling_ctx.read.delta(delta_test_data_dir).write.delta(func_temp_dir)

    # Verify result is a WriteDetails object with file_infos.
    assert result is not None
    assert hasattr(result, "file_infos")
    assert len(result.file_infos) > 0

    # Verify each file_info has the expected attributes.
    for file_info in result.file_infos:
        assert hasattr(file_info, "fhir_resource_type")
        assert hasattr(file_info, "absolute_url")


def test_tables_write_returns_details(ndjson_test_data_dir, pathling_ctx):
    """Verify that tables write returns WriteDetails with file information."""
    # Re-use the existing "test" schema to avoid conflicts.
    result = pathling_ctx.read.ndjson(ndjson_test_data_dir).write.tables(schema="test")

    # Verify result is a WriteDetails object with file_infos.
    assert result is not None
    assert hasattr(result, "file_infos")
    assert len(result.file_infos) > 0

    # Verify each file_info has the expected attributes.
    for file_info in result.file_infos:
        assert hasattr(file_info, "fhir_resource_type")
        assert hasattr(file_info, "absolute_url")
