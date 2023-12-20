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


from pathling import PathlingContext, StorageType
from pathling.sqlpath import *
from pathling.sqlpath import _
from pathling.sqlview import *

pc = PathlingContext.create(
    cache_storage_type = StorageType.DISK,
    cache_storage_path = "/Users/szu004/dev/pathling/tmp/_term_cache",
)
ds = pc.read.parquet('/Users/szu004/dev/pathling-performance/data/synth_100/parquet/')

#"reverseResolve(MedicationRequest.subject).medicationCodeableConcept.subsumedBy(http://www.nlm.nih.gov/research/umls/rxnorm|314076).anyTrue()",
#"reverseResolve(MedicationRequest.subject).medicationCodeableConcept.subsumedBy(http://www.nlm.nih.gov/research/umls/rxnorm|106892).anyTrue()",
#"reverseResolve(Observation.subject).where(code.subsumedBy(http://loinc.org|29463-7)).exists(valueQuantity >= 2.9 'kg' and valueQuantity <= 147 'kg')",
#"reverseResolve(Observation.subject).where(code.subsumedBy(http://loinc.org|8302-2)).exists(valueQuantity >= 47 'cm' and valueQuantity <= 193 'cm')",
#"reverseResolve(Condition.subject).code.subsumedBy(http://snomed.info/sct|160903007).anyTrue()",        
#"reverseResolve(Condition.subject).code.subsumedBy(http://fhir.de/CodeSystem/bfarm/icd-10-gm|E10).allFalse()",
#"reverseResolve(Condition.subject).code.subsumedBy(http://fhir.de/CodeSystem/bfarm/icd-10-gm|N17).allFalse()"          


view = View('Patient', [
    Path(_.id).alias('id'),
    From(_.MedicationRequest,
         Path(_.sub1 & _.sub2).alias('mr_cnd'),
         ),
    From(_.Condition,
         Path(_.sub1).alias('cnd_sub1'),
         Path(_.sub2).alias('cnd_sub2'),
         Path(_.sub3).alias('cnd_sub3'),
         ),

], joins = [
    ReverseView('MedicationRequest', 'subject.reference',
                [
                    Path(_.medicationCodeableConcept._getField('coding')
                         .subsumedBy(coding('314076', 'http://www.nlm.nih.gov/research/umls/rxnorm'))
                         .anyTrue()).alias('sub1'),
                    Path(_.medicationCodeableConcept._getField('coding')
                         .subsumedBy(coding('106892', 'http://www.nlm.nih.gov/research/umls/rxnorm'))
                         .anyTrue()).alias('sub2')
                ], [max, max]
                ),
    ReverseView('Condition', 'subject.reference',
                [
                    Path(_.code._getField('coding')
                         .subsumedBy(coding('160903007', 'http://snomed.info/sct'))
                         .anyTrue()).alias('sub1'),
                    Path(_.code._getField('coding')
                         .subsumedBy(coding('E10', 'http://fhir.de/CodeSystem/bfarm/icd-10-gm'))
                         .allFalse()).alias('sub2'),
                    Path(_.code._getField('coding')
                         .subsumedBy(coding('N17', 'http://fhir.de/CodeSystem/bfarm/icd-10-gm'))
                         .allFalse()).alias('sub3'),
                ], [max, min, min]
                )
])

result = view(ds)
result.show(5)

agg_result = result \
    .filter(col('mr_cnd') & col('cnd_sub1') & col('cnd_sub2') & col('cnd_sub3')) \
    .groupBy() \
    .agg(count('*'))

agg_result.show()
print_exec_plan(agg_result)
