#!/usr/bin/env python3

import xml.etree.ElementTree as ET

# Define namedtuple for test case result using the new type hint syntax
from collections import namedtuple
import itertools
import re
from typing import NamedTuple, List, Any, Optional

from results import TestResult, to_results


def main():
    # Load and parse the Surefire XML report
    tree = ET.parse(
        '../target/surefire-reports/TEST-au.csiro.pathling.fhirpath.yaml.YamlReferenceImplTest.xml')
    test_results = list(to_results(tree))
    sorted_results = sorted(test_results, key=lambda x: x.tag)
    # group test_results by error_message using itrtools.groupby
    test_by_tag = dict(
        (key, list(group)) for key, group in itertools.groupby(sorted_results, key=lambda x: x.tag))

    for key, group in test_by_tag.items():
        print(key)
        test_by_error = dict(
            (key, list(group)) for key, group in itertools.groupby(group, key=lambda x: x.error))
        for error, tests in test_by_error.items():
            print("\t" + error)
            for test in tests:
                print(f"\t\t{test.expr} [{test.desc}]")


if __name__ == '__main__':
    main()
