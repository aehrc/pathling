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
from pathling.sqlpath import _, _unnest
from pathling.sqlview import *

pc = PathlingContext.create()
ds = pc.read.parquet('/Users/szu004/dev/pathling-performance/data/synth_100/parquet/')

cnd_agg_view = ReverseView('Condition', 'subject.reference',
                           [
           Path(_).alias('res_count'),
           Path(_.code.coding.count()).alias('sizeOfCodings'),
           Path(_.code.coding.display).alias('codingNames'),
           ForEachName('_forEach', _.code.coding,
                       Path(_.system).alias('system'),
                       Path(_.code).alias('code'),
                       ),
       ],
                           )
view = View('Patient', [
    Path(_.id).alias('id'),
    Path(_.identifier.count()).alias('sizeOfId'),
    ForEach(_.name,
            Path(_.family).alias('familyName'),
            ForEach(_.given,
                    Path(_).alias('givenName')
                    ),
            ),
    From(_.Condition,
         Path(_.res_count).alias('cnd_count'),
         Path(_.sizeOfCodings).alias('cnd_sizeOfCodings'),
         Path(_.codingNames).alias('cnd_codingNames'),         
         ),
    ForEach(_.Condition._forEach,
            Path(_.system).alias('cnd_system'),
            Path(_.code).alias('cnd_code'),
        ),
    ], joins = [
        cnd_agg_view
])

result = view(ds)
result.show(5)
