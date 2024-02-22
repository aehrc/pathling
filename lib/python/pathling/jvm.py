#  Copyright 2023 Commonwealth Scientific and Industrial Research
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
from datetime import datetime, timedelta, timezone

from py4j.java_gateway import JVMView, JavaObject
from pyspark import SparkContext


# noinspection PyProtectedMember
def get_active_spark_context() -> SparkContext:
    """
    Get the active SparkContext.
    """
    if SparkContext._active_spark_context is None:
        raise ValueError("No active SparkContext")
    return SparkContext._active_spark_context


# noinspection PyProtectedMember
def get_active_java_spark_context() -> JavaObject:
    """
    Get the active Java SparkContext.
    """
    return get_active_spark_context()._jsc


# noinspection PyProtectedMember
def jvm() -> JVMView:
    """
    Get the active JVM.
    """
    return get_active_spark_context()._jvm


def jvm_pathling() -> JavaObject:
    """
    Gets the Pathling package `au.csiro.pathling` in the active JVM.
    """
    return jvm().au.csiro.pathling


_MILLI_FACTOR = 1000.0


def datetime_to_instant(dt: datetime) -> JavaObject:
    """
    Convert a Python datetime to a Java Instant with millisecond precision.
    :param dt: datetime to convert
    :return: the equivalent Java Instant
    """
    return jvm().java.time.Instant.ofEpochMilli(int(dt.timestamp() * _MILLI_FACTOR))


def instant_to_datetime(instant: JavaObject) -> datetime:
    """
    Convert a Java Instant to a Python datetime millisecond precision.
    :param instant: Instant to convert
    :return: the equivalent Python datetime
    """
    return datetime.fromtimestamp(
        instant.toEpochMilli() / _MILLI_FACTOR, tz=timezone.utc
    )


def timedelta_to_duration(td: timedelta) -> JavaObject:
    """
    Convert a Python timedelta to a Java Duration with millisecond precision.
    :param td: timedelta to convert
    :return: the equivalent Java Duration
    """
    return jvm().java.time.Duration.ofMillis(int(td.total_seconds() * _MILLI_FACTOR))
