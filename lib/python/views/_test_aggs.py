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


from pathling import PathlingContext
from pathling.sqlpath import *
from pathling.sqlpath import _
from pathling.sqlview import *

pc = PathlingContext.create()
ds = pc.read.parquet('/Users/szu004/dev/pathling-performance/data/synth_100/parquet/')


condition = ds.read('Condition')

cnd = to_struct(condition, 'Condition')

cnd.select(
    Condition.id().alias('id'),
).show(5)

result  = cnd.groupBy('Condition.subject.reference') \
    .agg(
        (Condition.id(agg=True)).alias('id'),
        ((Condition.id.exists().anyTrue() & Condition.id_versioned.exists().anyTrue())(agg=True)).alias('hasIds'),
    )

result.show(5)
print_exec_plan(result)
