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

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultDefinitionContext;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultResourceDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultResourceTag;
import au.csiro.pathling.fhirpath.execution.DefaultResourceResolver;
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
 * Factory for creating resource resolvers from arbitrary object representations. This class handles
 * the conversion of YAML-defined test data into a format that can be used for FHIRPath expression
 * evaluation.
 */
@Slf4j
@Value(staticConstructor = "of")
public class ArbitraryObjectResolverFactory implements Function<RuntimeContext, ResourceResolver> {

  @Nonnull Map<Object, Object> subjectOM; // Changed back to Map<Object, Object>

  @Override
  @Nonnull
  public ResourceResolver apply(final RuntimeContext rt) {
    final String subjectResourceCode =
        Optional.ofNullable(subjectOM.get("resourceType")).map(String.class::cast).orElse("Test");

    final DefaultResourceDefinition subjectDefinition =
        (DefaultResourceDefinition) YamlSupport.yamlToDefinition(subjectResourceCode, subjectOM);
    final StructType subjectSchema = YamlSupport.definitionToStruct(subjectDefinition);

    final String subjectOMJson = YamlSupport.omToJson(subjectOM);
    log.trace("subjectOMJson: \n{}", subjectOMJson);
    final Dataset<Row> inputDS =
        rt.getSpark()
            .read()
            .schema(subjectSchema)
            .json(rt.getSpark().createDataset(List.of(subjectOMJson), Encoders.STRING()));

    log.trace("Yaml definition: {}", subjectDefinition);
    log.trace("Subject schema: {}", subjectSchema.treeString());

    return DefaultResourceResolver.of(
        DefaultResourceTag.of(subjectResourceCode),
        DefaultDefinitionContext.of(subjectDefinition),
        inputDS);
  }

  @Override
  @Nonnull
  public String toString() {
    return YamlSupport.YAML.dump(subjectOM);
  }
}
