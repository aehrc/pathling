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

import glob
import os

import pkg_resources


def find_jar(verbose: bool = False) -> str:
    """
    Gets the path to the Pathling library JAR, bundled with the python distribution.
    """
    # try to find the distribution jars first
    verbose and print("Package name is: %s" % __name__)
    verbose and print("Filename name is: %s" % __file__)

    jars_dir = pkg_resources.resource_filename(__name__, "jars")

    verbose and print("Packaged jars dir: %s" % jars_dir)
    if not os.path.isdir(jars_dir):
        verbose and print(
            "Package resource dir '%s' does not exist. Maybe a development install?"
            % jars_dir
        )
        # then it can be a development install
        project_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), os.pardir)
        )
        verbose and print("Project dir is: %s" % project_dir)
        jars_dir = os.path.join(project_dir, "target", "dependency")
    jar_file = glob.glob(os.path.join(jars_dir, "library-api-*-all.jar"))
    verbose and print("Found jar file(s): %s" % jar_file)
    if not jar_file:
        raise RuntimeError("Pathling jar not present at: %s" % jars_dir)
    return jar_file[0]
