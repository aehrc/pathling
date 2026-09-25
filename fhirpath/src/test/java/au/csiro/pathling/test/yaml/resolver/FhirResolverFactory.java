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
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import ca.uhn.fhir.parser.IParser;
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
 * Factory for creating DatasetEvaluator instances from FHIR JSON resources. This implementation
 * handles the parsing and conversion of FHIR resources into a format suitable for FHIRPath
 * expression evaluation using flat schema, in the layout the {@link TestLayout} dimension selects
 * (T034). On the new layout the JSON is read through the new-layout transform as it stands, instead
 * of being parsed and encoded with the FHIR encoders.
 */
@Value
@AllArgsConstructor(staticName = "of")
public class FhirResolverFactory implements Function<RuntimeContext, DatasetEvaluator> {

  @Nonnull String resourceJson;

  @Nonnull TestLayout layout;

  /**
   * Returns a factory over a resource, in the active test layout.
   *
   * @param resourceJson the resource to evaluate against, as FHIR JSON
   * @return the factory
   */
  @Nonnull
  public static FhirResolverFactory of(@Nonnull final String resourceJson) {
    return of(resourceJson, TestLayout.active());
  }

  @Override
  @Nonnull
  public DatasetEvaluator apply(final RuntimeContext rt) {
    final IParser jsonParser = rt.getFhirEncoders().getContext().newJsonParser();
    final IBaseResource resource = jsonParser.parseResource(resourceJson);
    final ResourceType resourceType = ResourceType.fromCode(resource.fhirType());

    // Create flat dataset using FHIR encoders, or read the JSON through the new-layout transform.
    final Dataset<Row> resourceDS =
        layout.isPof()
            ? LayoutDatasets.fromJson(
                rt.getSpark(),
                rt.getFhirEncoders(),
                layout,
                resource.fhirType(),
                List.of(resourceJson))
            : LayoutDatasets.fromResources(
                rt.getSpark(),
                rt.getFhirEncoders(),
                layout,
                resource.fhirType(),
                List.of(resource));

    // Build DatasetEvaluator using the builder
    // Use EMPTY strategy for cross-resource references to return empty collections
    // rather than throwing exceptions (matching the behavior expected by YAML tests)
    return DatasetEvaluatorBuilder.create(resourceType, rt.getFhirContext())
        .withDataset(resourceDS)
        .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
        .build();
  }
}
