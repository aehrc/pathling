import glob
import os

import pkg_resources


def find_jar(verbose: bool = False) -> str:
    """Gets the path to the pathling encoders jar bundled with the
    python distribution
    """
    # try to find the distribution jars first
    verbose and print("Package name is: %s" % __name__)
    verbose and print("Filename name is: %s" % __file__)

    jars_dir = pkg_resources.resource_filename(__name__, "jars")

    verbose and print("Packaged jars dir: %s" % jars_dir)
    if not os.path.isdir(jars_dir):
        verbose and print(
                "Package resource dir '%s' does not exist. Maybe a development install?" % jars_dir)
        # then it can be a development install
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        verbose and print("Project dir is: %s" % project_dir)
        jars_dir = os.path.join(project_dir, "target", "dependency")
    jar_file = glob.glob(os.path.join(jars_dir, "encoders-*-all.jar"))
    verbose and print("Found jar file(s): %s" % jar_file)
    if not jar_file:
        raise RuntimeError("Pathling jar not present at: %s" % jars_dir)
    return jar_file[0]
