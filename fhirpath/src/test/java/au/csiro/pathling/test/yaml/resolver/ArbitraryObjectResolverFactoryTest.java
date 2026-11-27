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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.definition.defaults.DefaultDefinitionContext;
import au.csiro.pathling.definition.defaults.DefaultResourceDefinition;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.yaml.YamlSupport;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests the load of a YAML case whose subject is not a FHIR resource (path 2 of R11, FR-021b): the
 * subject is read through the defined read of {@code FhirReader}, with the schema derived from the
 * subject's definition alone, rather than a schema fitted to the subject by hand.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class ArbitraryObjectResolverFactoryTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Nonnull
  static Stream<Arguments> subjects() {
    return Stream.of(
        arguments(
            "primitive elements",
            """
            s: text
            i: 3
            b: true
            d: 1.5
            """),
        arguments(
            "complex elements",
            """
            el:
              a: 2
              b: text
              inner:
                c: false
            """),
        arguments(
            "repeating elements",
            """
            coll:
              - a: 1
              - a: 2
                b: text
            icoll:
              - 1
              - 2
              - 3
            """),
        arguments(
            "missing elements",
            """
            emptycoll: []
            el:
              a: 1
            coll:
              - a: 1
              - b: text
            """),
        arguments(
            "a resource type the FHIR definitions do not hold",
            """
            resourceType: TestResource
            s: text
            coll:
              - a: 1
            """));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("subjects")
  void resolvedDatasetIsTheDefinedReadOfTheSubject(
      @Nonnull final String description, @Nonnull final String yaml) {
    final Map<Object, Object> subject = YamlSupport.YAML.load(yaml);
    final String code = (String) subject.getOrDefault("resourceType", "Test");
    final DefaultResourceDefinition definition =
        (DefaultResourceDefinition) YamlSupport.yamlToDefinition(code, subject);

    final Dataset<Row> resolved =
        ArbitraryObjectResolverFactory.of(subject)
            .apply(RuntimeContext.of(spark, fhirEncoders))
            .getDataset();

    // The schema comes from the definition alone, not from the values of the subject.
    final Dataset<Row> expected =
        FhirReader.of(spark, DefaultDefinitionContext.of(definition))
            .json()
            .read(
                code,
                spark.createDataset(List.of(YamlSupport.omToJson(subject)), Encoders.STRING()),
                YamlSupport.definitionToStruct(definition));

    assertThat(resolved.schema()).isEqualTo(expected.schema());
    assertThat(resolved.collectAsList()).isEqualTo(expected.collectAsList());
  }

  @Test
  void resolvedDatasetKeepsTheValuesOfTheSubject() {
    final Map<Object, Object> subject =
        YamlSupport.YAML.load(
            """
            s: text
            i: 3
            b: true
            el:
              a: 2
            coll:
              - a: 1
              - a: 2
            icoll:
              - 1
              - 2
              - 3
            emptycoll: []
            """);

    final Dataset<Row> resolved =
        ArbitraryObjectResolverFactory.of(subject)
            .apply(RuntimeContext.of(spark, fhirEncoders))
            .getDataset();

    assertThat(resolved.count()).isEqualTo(1);
    final Row row = resolved.selectExpr("s", "i", "b", "el.a", "coll.a", "icoll").first();
    assertThat(row.getString(0)).isEqualTo("text");
    assertThat(row.getInt(1)).isEqualTo(3);
    assertThat(row.getBoolean(2)).isTrue();
    assertThat(row.getInt(3)).isEqualTo(2);
    assertThat(row.<Integer>getList(4)).containsExactly(1, 2);
    assertThat(row.<Integer>getList(5)).containsExactly(1, 2, 3);
  }
}
