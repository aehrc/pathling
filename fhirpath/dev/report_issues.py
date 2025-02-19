#!/usr/bin/env python3

import xml.etree.ElementTree as ET

# Define namedtuple for test case result using the new type hint syntax
from collections import namedtuple
import itertools
import re
import yaml
import jinja2
import hashlib
from markupsafe import Markup, escape
from results import TestResult, to_results

SUMMARY_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>Validation Report</title>
        <style>
        body {
            font-family: Verdana, sans-serif;
            font-size: small;
        }
        table {
            border-collapse: collapse;
            width: 100%;
        }
    
        table#files {
            width: fit-content;
        }
        
        th, td {
            border: 1px solid black;
            padding: 8px;
            text-align: left;
        }

        th {
            background-color: #f2f2f2;
        }

        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        mark {
            background-color: red;
            color: black;
        }
        .desc {
            color: grey;
        }
    </style>
</head>
<body>
    <h1>Issues</h1>
    <h2>Summary - issues by type</h2>
    {% for group in issues %}
    <h3>{{group[0]}}</h3>
    <table id="files">
        <tr>
            <th>ID</th>
            <th>Title</th>
            <th>Category</th>
            <th>Comment</th>
            <th>#Warning</th>
            <th>#Info</th>
        </tr>
        {% for issue in group[1] %}
            <tr>
                <td>{{issue.id}}</td>
                <td>{{issue.title}}</td>
                <td>{{issue.category}}</td>
                <td>{{issue.comment}}</td>
                <td><a href="#{{issue.tag}}">{{issue.tag}}<a></td>
                <td>&nbsp;</td>
            </tr>
        {% endfor %}       
    </table>
    {% endfor %}
    <h2>Details - issues by category</h2>
    {% for group in issues %}
        {% for issue in group[1] %}
            <h4 id="{{issue.tag}}">{{issue.id}}: {{issue.title}} - {{issue.category}}</h4>
            <h5>Comment</h5>
            <p>{{issue.comment}}</p>
            <h5>Definition</h5>
            <code>
                <pre>{{issue.definition}}</pre>
            </code>
            <h5>Details</h5>
            {% set result = results.get(issue.tag) %}
            {% if result %}
                <code>
                {% for error, items in result|groupby("error") %}
                  <p>{{ error }}</p>
                    <ul>{% for r in items %}
                      <li>{{r.expr}} <span class="desc">[{{r.desc}}]</span></li>
                    {% endfor %}</ul>
                {% endfor %}
                </code>
            {% else %}
                <p>No test result</p>
            {% endif %}
        {% endfor %}     
    {% endfor %}       
</body>
</html>"""

from typing import NamedTuple, List, Any, Optional


class IssueInfo(NamedTuple):
    id: str
    title: str
    category: str
    type: str
    comment: Optional[str] = None
    definition: Optional[str] = None

    @property
    def tag(self) -> str:
        hash_object = hashlib.md5()
        hash_object.update((self.category + self.title).encode('utf-8'))
        return hash_object.hexdigest()


def main():
    # load yaml from "../src/test/resources/fhirpath-js/config.yaml"
    with open("../src/test/resources/fhirpath-js/config.yaml", 'r') as stream:
        config = yaml.full_load(stream)

    def to_definition(ex):
        return dict((k, v) for k, v in ex.items() if k in {"function", "any", "expression"})

    def to_issue_info(config) -> IssueInfo:
        exclude_set = config.get("excludeSet")
        for entry in exclude_set:
            exclude = entry.get("exclude")
            for ex in exclude:
                yield IssueInfo(
                    ex.get("id", "???"),
                    ex.get("title"),
                    entry.get("title"),
                    ex.get("type", "undefined"),
                    ex.get("comment"),
                    yaml.dump(to_definition(ex))
                )

    issues = list(to_issue_info(config))
    issues_by_type = [(key, list(group)) for key, group in
                      itertools.groupby(sorted(issues, key=lambda x: x.type), key=lambda x: x.type)]
    file_report = "../target/validation-report.html"

    # Load and parse the Surefire XML report
    tree = ET.parse(
        '../target/surefire-reports/TEST-au.csiro.pathling.fhirpath.yaml.YamlReferenceImplTest.xml')
    test_results = list(to_results(tree))
    sorted_results = sorted(test_results, key=lambda x: x.tag)
    # group test_results by error_message using itrtools.groupby
    test_by_tag = dict(
        (key, list(group)) for key, group in itertools.groupby(sorted_results, key=lambda x: x.tag))

    # Load and parse the Surefire XML report    
    template = jinja2.Template(SUMMARY_TEMPLATE, autoescape=True, trim_blocks=True,
                               lstrip_blocks=True)
    with open(file_report, 'w') as f:
        f.write(template.render(
            dict(issues=issues_by_type, results=test_by_tag)))


if __name__ == '__main__':
    main()
