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
#
import re
from pathling.spark import Dfs


def test_dfs_temp_dir(pathling_ctx):
    dfs = Dfs(pathling_ctx.spark)
    temp_path = dfs.get_temp_dir_path(prefix="test", qualified=True)
    # In local setup the path should be something like: 
    # file:/tmp/hadoop-username/test-8e4756c1-46e4-44a5-b36d-d6afff1b168a

    # Validate the format of the temp path using regex
    regex_pattern = r'^file:/tmp/hadoop-[^/]+/test-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    assert re.match(regex_pattern, temp_path), f"Temp path {temp_path} does not match expected format"


def test_dfs_operations(pathling_ctx):
    dfs = Dfs(pathling_ctx.spark)
    temp_path = dfs.get_temp_dir_path(prefix="test", qualified=True)
    # Check if the temporary directory exists (it should not exist yet)
    assert not dfs.exists(temp_path), f"Temporary path {temp_path} should not exist before creation"
    assert dfs.mkdirs(temp_path), f"Temporary path {temp_path} can be created"
    assert dfs.exists(temp_path), f"Temporary path {temp_path} should exist after creation"
    dfs.delete(temp_path, recursive=True)
    assert not dfs.exists(temp_path), f"Temporary path {temp_path} should not exist after deletion"
