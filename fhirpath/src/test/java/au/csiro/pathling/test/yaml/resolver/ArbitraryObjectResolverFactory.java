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

import au.csiro.pathling.definition.defaults.DefaultDefinitionContext;
import au.csiro.pathling.definition.defaults.DefaultResourceDefinition;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DefinitionResourceResolver;
import au.csiro.pathling.fhirpath.evaluation.ResourceResolver;
import au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.test.yaml.YamlSupport;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Factory for creating DatasetEvaluator instances from arbitrary object representations. This class
 * handles the conversion of YAML-defined test data into a format that can be used for FHIRPath
 * expression evaluation. The subject is read as a defined read of {@link FhirReader}, with the
 * schema derived from the subject's definition alone, so that it is stored in the Parquet on FHIR
 * layout as a FHIR resource of the same definition would be (FR-021b).
 */
@Slf4j
@Value
@AllArgsConstructor(staticName = "of")
public class ArbitraryObjectResolverFactory implements Function<RuntimeContext, DatasetEvaluator> {

  /**
   * The configuration the subject is read with, which is that of the subjects that are FHIR
   * resources, so that both paths store primitive metadata alike (R23).
   */
  @Nonnull
  private static final SchemaConfiguration CONFIGURATION =
      SchemaConfiguration.builder().storePrimitiveMetadata(true).build();

  @Nonnull Map<Object, Object> subjectOM;

  @Override
  @Nonnull
  public DatasetEvaluator apply(final RuntimeContext rt) {
    final String subjectResourceCode =
        Optional.ofNullable(subjectOM.get("resourceType")).map(String.class::cast).orElse("Test");

    final DefaultResourceDefinition subjectDefinition =
        (DefaultResourceDefinition) YamlSupport.yamlToDefinition(subjectResourceCode, subjectOM);
    final DefaultDefinitionContext definitions = DefaultDefinitionContext.of(subjectDefinition);
    final StructType subjectSchema = YamlSupport.definitionToStruct(subjectDefinition);

    final String subjectJson = YamlSupport.omToJson(subjectOM);
    log.trace("Subject JSON: \n{}", subjectJson);
    log.trace("Yaml definition: {}", subjectDefinition);
    log.trace("Subject schema: {}", subjectSchema.treeString());
    final Dataset<Row> inputDS =
        FhirReader.of(rt.getSpark(), definitions, CONFIGURATION)
            .json()
            .read(
                subjectResourceCode,
                rt.getSpark().createDataset(List.of(subjectJson), Encoders.STRING()),
                subjectSchema);

    final ResourceResolver resolver =
        DefinitionResourceResolver.of(subjectResourceCode, definitions);
    final SingleResourceEvaluator evaluator =
        SingleResourceEvaluator.of(resolver, StaticFunctionRegistry.getInstance(), Map.of());

    return new DatasetEvaluator(evaluator, inputDS);
  }

  @Override
  @Nonnull
  public String toString() {
    return YamlSupport.YAML.dump(subjectOM);
  }
}
