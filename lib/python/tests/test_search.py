#  Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import os
from tempfile import mkdtemp

import pytest
from pyspark.sql import Column, SparkSession

from pathling import PathlingContext
from pathling._version import __java_version__

PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)


@pytest.fixture(scope="module")
def spark_session(request):
    """
    Fixture for creating a Spark Session available for all tests in this
    testing session.
    """

    gateway_log = logging.getLogger("java_gateway")
    gateway_log.setLevel(logging.ERROR)

    # Get the shaded JAR for testing purposes.
    spark = (
        SparkSession.builder.appName("pathling-search-test")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            f"au.csiro.pathling:library-runtime:{__java_version__}",
        )
        .config("spark.sql.warehouse.dir", mkdtemp())
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    request.addfinalizer(lambda: spark.stop())

    return spark


@pytest.fixture(scope="module")
def json_bundles_dir():
    return os.path.join(
        PROJECT_DIR, "encoders/src/test/resources/data/bundles/R4/json/"
    )


@pytest.fixture(scope="module")
def pathling_context(spark_session):
    return PathlingContext.create(spark_session)


@pytest.fixture(scope="module")
def json_bundles_df(spark_session, json_bundles_dir):
    return spark_session.read.text(json_bundles_dir, wholetext=True)


@pytest.fixture(scope="module")
def patients_df(pathling_context, json_bundles_df):
    return pathling_context.encode_bundle(json_bundles_df, "Patient")


def test_search_to_column_single_parameter(pathling_context):
    """Test that search_to_column returns a Column for a single parameter."""
    filter_col = pathling_context.search_to_column("Patient", "gender=male")
    assert filter_col is not None
    assert isinstance(filter_col, Column)


def test_search_to_column_multiple_parameters(pathling_context):
    """Test that search_to_column returns a Column for multiple parameters."""
    filter_col = pathling_context.search_to_column("Patient", "gender=male&active=true")
    assert filter_col is not None
    assert isinstance(filter_col, Column)


def test_search_to_column_date_prefix(pathling_context):
    """Test that search_to_column handles date prefixes correctly."""
    filter_col = pathling_context.search_to_column("Patient", "birthdate=ge1990-01-01")
    assert filter_col is not None
    assert isinstance(filter_col, Column)


def test_search_to_column_combine_with_and(pathling_context):
    """Test combining filters using the & operator (AND logic)."""
    gender_filter = pathling_context.search_to_column("Patient", "gender=male")
    active_filter = pathling_context.search_to_column("Patient", "active=true")

    # Use Pythonic & operator for AND
    combined = gender_filter & active_filter
    assert combined is not None
    assert isinstance(combined, Column)


def test_search_to_column_combine_with_or(pathling_context):
    """Test combining filters using the | operator (OR logic)."""
    male_filter = pathling_context.search_to_column("Patient", "gender=male")
    female_filter = pathling_context.search_to_column("Patient", "gender=female")

    # Use Pythonic | operator for OR
    combined = male_filter | female_filter
    assert combined is not None
    assert isinstance(combined, Column)


def test_search_to_column_apply_to_dataframe(pathling_context, patients_df):
    """Test applying a search filter to a DataFrame."""
    initial_count = patients_df.count()

    # Create filter and apply it
    gender_filter = pathling_context.search_to_column("Patient", "gender=male")
    filtered_patients = patients_df.filter(gender_filter)

    # Should return fewer or equal patients than the original dataset
    assert filtered_patients.count() <= initial_count


def test_search_to_column_empty_query_matches_all(pathling_context, patients_df):
    """Test that an empty query matches all resources."""
    initial_count = patients_df.count()

    # Empty filter should match all resources
    empty_filter = pathling_context.search_to_column("Patient", "")
    filtered_patients = patients_df.filter(empty_filter)

    assert filtered_patients.count() == initial_count


def test_search_to_column_invalid_parameter_raises_exception(pathling_context):
    """Test that an invalid search parameter raises an exception."""
    with pytest.raises(Exception):
        # This should raise an UnknownSearchParameterException from Java
        pathling_context.search_to_column("Patient", "invalid-param=value")


def test_search_to_column_combined_filters_apply_to_dataframe(
    pathling_context, patients_df
):
    """Test applying combined search filters to a DataFrame."""
    initial_count = patients_df.count()

    # Create and combine filters
    filter1 = pathling_context.search_to_column("Patient", "gender=male")
    filter2 = pathling_context.search_to_column("Patient", "gender=female")

    # OR should match male OR female
    combined = filter1 | filter2
    filtered_patients = patients_df.filter(combined)

    # Should have some results but not more than initial
    assert filtered_patients.count() <= initial_count
