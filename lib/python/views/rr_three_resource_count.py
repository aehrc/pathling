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
from pathling.sqlpath import _
from pathling.sqlview import *

pc = PathlingContext.create()
ds = pc.read.parquet('/Users/szu004/dev/pathling-performance/data/synth_100/parquet/')

#
# from: Patient
# id.alias('id')
# reverseResolve('Condition', 'subject.reference').count().alias('cnd_count')
# reverseResolve('Observation', 'subject.reference').count().alias('obs_count')
# reverseResolve('MedicationRequest', 'subject.reference').count().alias('mr_count')

view = View('Patient', [
    Path(_.id.alias('id')),
    From(_.Condition,
         Path(_.res_count.alias('cnd_count')),
         ),
    From(_.Observation,
         Path(_.res_count.alias('obs_count')),
         ),
    From(_.MedicationRequest,
         Path(_.res_count.alias('mr_count')),
         ),
], joins=[
    ReverseView('Condition', 'subject.reference',
                [
                    Path(_.alias('res_count')),
                ], [count]
                ),
    ReverseView('Observation', 'subject.reference',
                [
                    Path(_.alias('res_count')),
                ], [count]
                ),
    ReverseView('MedicationRequest', 'subject.reference',
                [
                    Path(_.alias('res_count')),
                ], [count]
                )
])
result = view(ds)
result.show(5)
print_exec_plan(result)
