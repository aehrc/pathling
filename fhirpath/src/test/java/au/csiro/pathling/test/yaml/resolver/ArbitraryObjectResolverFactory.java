/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.fhirpath.definition.defaults.DefaultDefinitionContext;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultResourceDefinition;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DefinitionResourceResolver;
import au.csiro.pathling.fhirpath.evaluation.ResourceResolver;
import au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.test.yaml.YamlSupport;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Factory for creating DatasetEvaluator instances from arbitrary object representations. This class
 * handles the conversion of YAML-defined test data into a format that can be used for FHIRPath
 * expression evaluation using flat schema.
 */
@Slf4j
@Value(staticConstructor = "of")
public class ArbitraryObjectResolverFactory implements Function<RuntimeContext, DatasetEvaluator> {

  @Nonnull
  Map<Object, Object> subjectOM;

  @Override
  @Nonnull
  public DatasetEvaluator apply(final RuntimeContext rt) {
    final String subjectResourceCode = Optional.ofNullable(subjectOM.get("resourceType"))
        .map(String.class::cast)
        .orElse("Test");

    // Create definition from YAML
    final DefaultResourceDefinition subjectDefinition = (DefaultResourceDefinition) YamlSupport
        .yamlToDefinition(subjectResourceCode, subjectOM);
    final StructType subjectSchema = YamlSupport.definitionToStruct(subjectDefinition);

    // Create flat Dataset with YAML schema
    final String subjectOMJson = YamlSupport.omToJson(subjectOM);
    log.trace("subjectOMJson: \n{}", subjectOMJson);
    final Dataset<Row> inputDS = rt.getSpark().read().schema(subjectSchema)
        .json(rt.getSpark().createDataset(List.of(subjectOMJson), Encoders.STRING()));

    log.trace("Yaml definition: {}", subjectDefinition);
    log.trace("Subject schema: {}", subjectSchema.treeString());

    // Create DefinitionResourceResolver
    final ResourceResolver resolver = DefinitionResourceResolver.of(
        subjectResourceCode,
        DefaultDefinitionContext.of(subjectDefinition));

    // Create evaluator with resolver
    final SingleResourceEvaluator evaluator = SingleResourceEvaluator.of(
        resolver,
        StaticFunctionRegistry.getInstance(),
        Map.of());

    return new DatasetEvaluator(evaluator, inputDS);
  }

  @Override
  @Nonnull
  public String toString() {
    return YamlSupport.YAML.dump(subjectOM);
  }
}
