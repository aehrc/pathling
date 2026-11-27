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

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.fhirpath.evaluation.CrossResourceStrategy;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.schema.SchemaConfiguration;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Factory for creating DatasetEvaluator instances from FHIR JSON resources. This implementation
 * handles the parsing and conversion of FHIR resources into a format suitable for FHIRPath
 * expression evaluation. The JSON is read in the Parquet on FHIR layout with the schema of its
 * suite, merged by name with its own (FR-021b).
 */
@Value
@AllArgsConstructor(staticName = "of")
public class FhirResolverFactory implements Function<RuntimeContext, DatasetEvaluator> {

  /**
   * The configuration the resource is read with, which stores the ids and extensions of primitives,
   * as the inferred read of the suite schemas does.
   */
  @Nonnull
  private static final SchemaConfiguration CONFIGURATION =
      SchemaConfiguration.builder().storePrimitiveMetadata(true).build();

  @Nonnull String resourceJson;

  /** The schemas of the suite the case belongs to, which the case's resource is loaded with. */
  @Nonnull SuiteSchemas suiteSchemas;

  @Override
  @Nonnull
  public DatasetEvaluator apply(final RuntimeContext rt) {
    final IParser jsonParser = rt.getFhirEncoders().getContext().newJsonParser();
    final IBaseResource resource = jsonParser.parseResource(resourceJson);
    final ResourceType resourceType = ResourceType.fromCode(resource.fhirType());

    // The resource is read with the schema of its suite, so that it carries every element the
    // suite's resources of its type carry, as a table of them would (FR-021b).
    final Dataset<Row> resourceDS =
        FhirReader.of(
                rt.getSpark(),
                FhirDefinitionContext.of(rt.getFhirEncoders().getContext()),
                CONFIGURATION)
            .json()
            .read(
                resource.fhirType(),
                rt.getSpark().createDataset(List.of(resourceJson), Encoders.STRING()),
                suiteSchemas.definedSchemaFor(rt, resource.fhirType(), resourceJson));

    // Build DatasetEvaluator using the builder
    // Use EMPTY strategy for cross-resource references to return empty collections
    // rather than throwing exceptions (matching the behavior expected by YAML tests)
    return DatasetEvaluatorBuilder.create(resourceType, rt.getFhirContext())
        .withDataset(resourceDS)
        .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
        .build();
  }
}
