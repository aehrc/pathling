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


import uuid

from py4j.java_gateway import JavaObject, JVMView
from pyspark import SparkContext
from pyspark.sql import SparkSession


class Dfs:
    """A class for interacting with the Hadoop Distributed File System (HDFS) in Spark."""

    def __init__(self, spark: SparkSession):
        """
        Initialize the Dfs class with a SparkSession.

        :param spark: SparkSession instance
        """
        if not spark:
            raise ValueError("SparkSession must be provided")
        sc: SparkContext = spark.sparkContext
        self._jvm: JVMView = sc._jvm
        self._hadoop_conf: JavaObject = sc._jsc.hadoopConfiguration()
        self._fs = self._jvm.org.apache.hadoop.fs.FileSystem.get(self._hadoop_conf)

    def get_temp_dir_path(self, prefix: str = "tmp-app", qualified=True) -> str:
        """
        Returns a unique path for a temporary directory in Spark's filesystem.
    
        The path is constructed by appending a UUID to the base temporary directory, 
        ensuring uniqueness for each call.
        The directory itself is not created, only the path is returned.
        
        :param prefix: String to insert between the base directory and the UUID (default: "tmp-app").
        :param qualified: If True, returns a fully qualified Hadoop path; if False, returns a raw path string.
        :return: String representing the unique temporary directory path.
        """
        base_tmp_dir = self._hadoop_conf.get("hadoop.tmp.dir")
        if not base_tmp_dir:
            raise ValueError("`hadoop.tmp.dir` must be set in Hadoop configuration.")
        uuid_suffix = str(uuid.uuid4())
        base_tmp_path = self._jvm.org.apache.hadoop.fs.Path(base_tmp_dir)
        tmp_path = self._jvm.org.apache.hadoop.fs.Path(base_tmp_path, f"{prefix}-{uuid_suffix}")
        return self._fs.makeQualified(tmp_path).toString() if qualified else tmp_path.toString()

    def exists(self, path: str) -> bool:
        """
        Check if a given path exists in the filesystem.

        :param path: Path to check for existence.
        :return: True if the path exists, False otherwise.
        """
        hadoop_path = self._jvm.org.apache.hadoop.fs.Path(path)
        return self._fs.exists(hadoop_path)

    def delete(self, path: str, recursive: bool = False) -> bool:
        """
        Delete a file or directory at the specified path.

        :param path: Path to the file or directory to delete.
        :param recursive: If True, delete directories and their contents recursively.
        :return: True if deletion was successful, False otherwise.
        """
        hadoop_path = self._jvm.org.apache.hadoop.fs.Path(path)
        return self._fs.delete(hadoop_path, recursive)

    def mkdirs(self, path: str) -> bool:
        """
        Create a directory at the specified path.

        :param path: Path to the directory to create.
        :return: True if the directory was created successfully, False otherwise.
        """
        hadoop_path = self._jvm.org.apache.hadoop.fs.Path(path)
        return self._fs.mkdirs(hadoop_path)
