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
from pathling.sqlview import _flatten_df

pc = PathlingContext.create()
ds = pc.read.parquet('/Users/szu004/dev/pathling-performance/data/synth_100/parquet/')


condition = ds.read('Condition')
patient = ds.read('Patient')
cnd_agg = condition.groupBy(col('subject.reference').alias('Condition_key'))\
    .agg(struct(*[collect_list(c).alias(c) for c in condition.columns]).alias('Condition'))
ptn = to_struct(patient.join(cnd_agg, patient.id_versioned == cnd_agg.Condition_key, 'leftouter'), 'Patient')
                                                                                                                                                
#
# forEach: reverseResolve(Patient.Condition).where(id_versioned.exits() 
#  id, 
#  code.text
#

result = ptn.select('Patient',
                    filter(sequence(lit(1), size('Patient.condition.id')),
                           lambda i:element_at('Patient.condition.id_versioned', i).isNotNull()).alias('wh1'),
                    ).select(
    'Patient.id',
    transform('wh1', lambda i: struct(
        element_at('Patient.Condition.id', i).alias('id'),
        element_at('Patient.Condition.code', i).getField('text').alias('text'),
    )).alias('@coding')
)

result.show()
print_exec_plan(result)

flat_result = _flatten_df(result)
flat_result.show()
print_exec_plan(flat_result)
