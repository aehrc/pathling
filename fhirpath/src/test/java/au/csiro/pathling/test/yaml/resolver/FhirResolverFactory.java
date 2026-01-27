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
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Factory for creating DatasetEvaluator instances from FHIR JSON resources. This implementation
 * handles the parsing and conversion of FHIR resources into a format suitable for FHIRPath
 * expression evaluation using flat schema.
 */
@Value(staticConstructor = "of")
public class FhirResolverFactory implements Function<RuntimeContext, DatasetEvaluator> {

  @Nonnull String resourceJson;

  @Override
  @Nonnull
  public DatasetEvaluator apply(final RuntimeContext rt) {
    final IParser jsonParser = rt.getFhirEncoders().getContext().newJsonParser();
    final IBaseResource resource = jsonParser.parseResource(resourceJson);
    final ResourceType resourceType = ResourceType.fromCode(resource.fhirType());

    // Create flat dataset using FHIR encoders
    final Dataset<Row> resourceDS =
        rt.getSpark()
            .createDataset(List.of(resource), rt.getFhirEncoders().of(resource.fhirType()))
            .toDF();

    // Build DatasetEvaluator using the builder
    // Use EMPTY strategy for cross-resource references to return empty collections
    // rather than throwing exceptions (matching the behavior expected by YAML tests)
    return DatasetEvaluatorBuilder.create(resourceType, rt.getFhirContext())
        .withDataset(resourceDS)
        .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
        .build();
  }
}
