/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.fhirpath.evaluation.CrossResourceStrategy;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Factory for creating DatasetEvaluator instances from HAPI FHIR resources. This implementation
 * handles the conversion of HAPI FHIR resource objects into a format suitable for FHIRPath
 * expression evaluation using flat schema. The resource is serialised to FHIR JSON and read in the
 * Parquet on FHIR layout (T036).
 */
@Value
@AllArgsConstructor(staticName = "of")
public class HapiResolverFactory implements Function<RuntimeContext, DatasetEvaluator> {

  @Nonnull IBaseResource resource;

  @Override
  @Nonnull
  public DatasetEvaluator apply(final RuntimeContext rt) {
    final ResourceType resourceType = ResourceType.fromCode(resource.fhirType());

    final Dataset<Row> resourceDS =
        PofDatasets.fromResources(
            rt.getSpark(),
            rt.getFhirEncoders().getContext(),
            resource.fhirType(),
            List.of(resource));

    // Build DatasetEvaluator using the builder
    // Use EMPTY strategy for cross-resource references to return empty collections
    // rather than throwing exceptions (matching the behavior expected by tests)
    return DatasetEvaluatorBuilder.create(resourceType, rt.getFhirContext())
        .withDataset(resourceDS)
        .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
        .build();
  }
}
