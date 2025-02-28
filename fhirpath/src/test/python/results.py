import xml.etree.ElementTree as ET

# Define namedtuple for test case result using the new type hint syntax
from collections import namedtuple
import itertools
import re
from typing import NamedTuple, List, Any, Optional


class TestResult(NamedTuple):
    name: str
    error: Optional[str]
    tag: str
    rule: str
    desc: Optional[str] = None
    expr: Optional[str] = None


def convert_message(message):
    # r"org\.apache\.spark\.SparkException: Job aborted due to stage failure: .* (192.168.1.101 executor driver):"

    msg1 = re.sub(r"Error parsing FHIRPath expression \(line: \d+, position: \d+\)",
                  "Error parsing FHIRPath expression (line: ??, position: ??)", message)

    return re.sub(
        r"Job aborted due to stage failure: .* executor driver\):",
        "Job aborted due to stage failure: ?? (executor driver):",
        msg1)


EX_TAG = re.compile(r"Exclusion: (.+)#(\w+)")
EX_DESC = re.compile(r"a\.c\.p\.test\.yaml\.YamlSpecTestBase - Description: (.+)")
EX_EXPR = re.compile(r"a\.c\.p\.test\.yaml\.YamlSpecTestBase - Expression: (.+)")


def to_results(tree: ET.ElementTree) -> List[TestResult]:
    root = tree.getroot()

    # Extract and print test case information
    for testcase in root.findall('testcase'):
        name = testcase.get('name')
        # Check for failures or errors
        failure = testcase.find('failure')
        if failure is not None:
            sysout = testcase.find("system-out")
            tags_match = EX_TAG.search(sysout.text)
            tag = tags_match.group(2) if tags_match else None
            rule = tags_match.group(1) if tags_match else None
            desc = EX_DESC.search(sysout.text).group(1) if EX_DESC.search(sysout.text) else None
            expr = EX_EXPR.search(sysout.text).group(1) if EX_EXPR.search(sysout.text) else None
            yield TestResult(name, failure.get("message"), tag, rule, desc, expr)
        # print(f'  Failure: {failure.get("message")}')

        error = testcase.find('error')
        if error is not None:
            sysout = testcase.find("system-out")
            tags_match = EX_TAG.search(sysout.text)
            tag = tags_match.group(2) if tags_match else None
            rule = tags_match.group(1) if tags_match else None
            desc = EX_DESC.search(sysout.text).group(1) if EX_DESC.search(sysout.text) else None
            expr = EX_EXPR.search(sysout.text).group(1) if EX_EXPR.search(sysout.text) else None
            yield TestResult(name, error.get("type") + ": " + convert_message(error.get("message")),
                             tag, rule, desc, expr)
