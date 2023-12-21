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
from pyspark.sql.functions import *

pc = PathlingContext.create()
ds = pc.read.parquet('/Users/szu004/dev/pathling-performance/data/synth_100/parquet/')


condition = ds.read('Condition')
patient = ds.read('Patient')

joined = patient.alias('Patient').join(condition.alias('Condition'), patient.id_versioned == condition.subject.reference, 'leftouter') 

result = joined.groupBy('Patient.id') \
    .agg(*[first("Patient." + c).alias(c) for c in patient.columns if c != 'id'], 
         collect_list(struct(
             'Condition.id',
             'Condition.code',
         )).alias('Condition')) \
    .select('id', 'Condition.id')
    
result.show()
print_exec_plan(result)
