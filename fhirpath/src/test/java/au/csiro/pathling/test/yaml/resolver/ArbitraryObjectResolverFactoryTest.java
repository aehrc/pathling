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

import au.csiro.pathling.definition.ChildDefinition;
import au.csiro.pathling.definition.defaults.DefaultDefinitionContext;
import au.csiro.pathling.definition.defaults.DefaultResourceDefinition;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.schema.SchemaConfiguration;
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

  /** The configuration path 1 reads with, which stores the metadata of primitives (R23). */
  private static final SchemaConfiguration PATH_1_CONFIGURATION =
      SchemaConfiguration.builder().storePrimitiveMetadata(true).build();

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
            """),
        arguments(
            "a scalar null with a metadata sibling",
            """
            i: null
            _i:
              id: iid
            s: text
            """),
        arguments(
            "a Coding with only a code",
            """
            c: !fhir.Coding '|c1'
            """),
        arguments(
            "a Quantity",
            """
            q: !fhir.Quantity "11.5 'mg'"
            """),
        arguments(
            "a decimal with a trailing zero",
            """
            d: 1.50
            """),
        arguments(
            "a choice",
            """
            __CHOICE__: value
            valueString: text
            valueInteger: null
            """),
        arguments(
            "an element of a named FHIR type",
            """
            hn:
              __FHIR_TYPE__: HumanName
              family: Smith
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
        FhirReader.of(spark, DefaultDefinitionContext.of(definition), PATH_1_CONFIGURATION)
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
            i: null
            _i:
              id: iid
            b: true
            d: 1.50
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
            c: !fhir.Coding '|c1'
            q: !fhir.Quantity "11.5 'mg'"
            hn:
              __FHIR_TYPE__: HumanName
              family: Smith
            """);

    final Dataset<Row> resolved =
        ArbitraryObjectResolverFactory.of(subject)
            .apply(RuntimeContext.of(spark, fhirEncoders))
            .getDataset();

    assertThat(resolved.count()).isEqualTo(1);
    final Row row =
        resolved
            .selectExpr(
                "s",
                "b",
                "d",
                "el.a",
                "coll.a",
                "icoll",
                "c.code",
                "q.value",
                "q.code",
                "q.system",
                "hn.family")
            .first();
    assertThat(row.getString(0)).isEqualTo("text");
    assertThat(row.getBoolean(1)).isTrue();
    // A decimal keeps the text it was written with.
    assertThat(row.getString(2)).isEqualTo("1.50");
    assertThat(row.getInt(3)).isEqualTo(2);
    assertThat(row.<Integer>getList(4)).containsExactly(1, 2);
    assertThat(row.<Integer>getList(5)).containsExactly(1, 2, 3);
    assertThat(row.getString(6)).isEqualTo("c1");
    assertThat(row.getString(7)).isEqualTo("11.5");
    assertThat(row.getString(8)).isEqualTo("mg");
    assertThat(row.getString(9)).isEqualTo("http://unitsofmeasure.org");
    assertThat(row.getString(10)).isEqualTo("Smith");
    // An empty list and a null are empty, whether the schema has a column for them or not.
    assertThat(emptyOrAbsent(resolved, "emptycoll")).isTrue();
    assertThat(emptyOrAbsent(resolved, "i")).isTrue();
  }

  @Test
  void definitionOfASubjectHasNoResourceTypeElement() {
    final Map<Object, Object> subject =
        YamlSupport.YAML.load(
            """
            resourceType: TestResource
            s: text
            """);

    final DefaultResourceDefinition definition =
        (DefaultResourceDefinition) YamlSupport.yamlToDefinition("TestResource", subject);

    assertThat(definition.getChildren())
        .extracting(ChildDefinition::getName)
        .doesNotContain("resourceType")
        .contains("s");
  }

  /** Whether a column is missing from a dataset, null in its row, or an empty array. */
  private static boolean emptyOrAbsent(
      @Nonnull final Dataset<Row> dataset, @Nonnull final String column) {
    if (!List.of(dataset.columns()).contains(column)) {
      return true;
    }
    final Row row = dataset.select(column).first();
    return row.isNullAt(0) || row.getList(0).isEmpty();
  }
}
