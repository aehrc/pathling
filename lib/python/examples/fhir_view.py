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

import os
from pathling import PathlingContext

HERE = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(HERE, "data")
NDJSON_DIR = os.path.join(DATA_DIR, "resources")

pc = PathlingContext.create()
datasource = pc.read.ndjson(NDJSON_DIR)

view_ds = datasource.view(
    resource='Patient',
    select=[
        {
            'column': [
                {'path': 'id', 'name': 'id'},
                {'path': 'gender', 'name': 'gender'},
                {'path': "telecom.where(system='phone').value ", 'name': 'phone_numbers',
                 'collection': True},
            ]
        },
        {
            'forEach': 'name',
            'column': [
                {'path': 'use', 'name': 'name_use'},
                {'path': 'family', 'name': 'family_name'},
            ],
            'select': [
                {
                    'forEachOrNull': 'given',
                    'column': [
                        {'path': '$this', 'name': 'given_name'},
                    ],
                }
            ]
        },
    ],
    where=[
        {'path': "gender = 'male'"},
    ]
)

view_ds.show()
