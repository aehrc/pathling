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
package au.csiro.pathling.test.layout;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import au.csiro.pathling.test.yaml.FhirTypedLiteral;
import au.csiro.pathling.test.yaml.resolver.ArbitraryObjectResolverFactory;
import au.csiro.pathling.test.yaml.resolver.FhirResolverFactory;
import au.csiro.pathling.test.yaml.resolver.HapiResolverFactory;
import au.csiro.pathling.test.yaml.resolver.RuntimeContext;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationStatus;
import org.hl7.fhir.r4.model.Quantity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Asserts that the layout and schema mode the build requested are the ones the fixtures are really
 * built in, by looking at the stored data rather than at the configuration (T038).
 *
 * <p>This repository has test configuration that silently does nothing, so a check that compared
 * the parsed property with itself would pass even if the property never reached the test JVM, or if
 * a fixture factory ignored it. Instead, one resource is built through every fixture entry point
 * the dimension governs, and the stored schema is checked for what distinguishes the layouts: a
 * decimal is a fixed-precision number on the previous layout and text on the new one (FR-002), and
 * an element the resource does not carry is present on the previous layout, whose schema is dense,
 * and absent from the new one, whose schema is pruned. The synthetic subject the DSL's {@code
 * withSubject} and the YAML suites evaluate against is not a resource, so it is checked separately,
 * for its decimals only. Run with {@code -Dpathling.testLayout=previous} and without it, the same
 * test sees different stored data.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@Slf4j
class TestLayoutSwitchTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Test
  void everyFixtureEntryPointIsInTheRequestedLayout() {
    final TestLayout requested = TestLayout.parse(System.getProperty(TestLayout.PROPERTY));
    final TestLayout active = TestLayout.active();
    final TestSchemaMode schemaMode = TestSchemaMode.active();
    log.info("Active test layout: {}, schema mode: {}", active, schemaMode);
    assertThat(active).isSameAs(requested);
    assertThat(schemaMode).isSameAs(TestSchemaMode.PRUNED);

    final Map<String, StructType> schemas = schemasFromEveryEntryPoint();
    schemas.forEach((entryPoint, schema) -> assertInLayout(entryPoint, schema, active));
  }

  @Test
  void theTwoLayoutsStoreDifferentData() {
    final Observation observation = observation();
    final StructType previous =
        LayoutDatasets.fromResources(
                spark, fhirEncoders, TestLayout.PREVIOUS, "Observation", List.of(observation))
            .schema();
    final StructType pof =
        LayoutDatasets.fromResources(
                spark, fhirEncoders, TestLayout.POF, "Observation", List.of(observation))
            .schema();

    assertThat(previous).isNotEqualTo(pof);
    assertInLayout("previous", previous, TestLayout.PREVIOUS);
    assertInLayout("pof", pof, TestLayout.POF);
  }

  @Test
  void syntheticSubjectStoresDecimalsInTheRequestedLayout() {
    final Map<Object, Object> subject = new LinkedHashMap<>();
    subject.put("plain", 1.5);
    subject.put("typed", FhirTypedLiteral.toDecimal("1.0E-7"));
    subject.put("many", List.of(9.5, 10.0));
    final Dataset<Row> dataset =
        ArbitraryObjectResolverFactory.of(subject)
            .apply(RuntimeContext.of(spark, fhirEncoders))
            .getDataset();
    final StructType schema = dataset.schema();
    final Row row = dataset.first();

    if (TestLayout.active().isPof()) {
      // The new layout stores a decimal as text, so a typed literal keeps its text exactly and a
      // plain number is stored as the text of the double.
      assertThat(schema.apply("plain").dataType()).isEqualTo(DataTypes.StringType);
      assertThat(schema.apply("many").dataType())
          .isEqualTo(DataTypes.createArrayType(DataTypes.StringType, true));
      assertThat(row.<String>getAs("plain")).isEqualTo("1.5");
      assertThat(row.<String>getAs("typed")).isEqualTo("1.0E-7");
      assertThat(row.<String>getList(row.fieldIndex("many"))).containsExactly("9.5", "10.0");
    } else {
      assertThat(schema.apply("plain").dataType()).isInstanceOf(DecimalType.class);
      assertThat(schema.apply("typed").dataType()).isInstanceOf(DecimalType.class);
    }
  }

  @Test
  void syntheticSubjectStoresQuantitiesInTheRequestedLayout() {
    final Map<Object, Object> subject = new LinkedHashMap<>();
    subject.put("single", FhirTypedLiteral.toQuantity("1.50 'mg'"));
    subject.put("many", List.of(FhirTypedLiteral.toQuantity("0.0000001 'kg'")));
    final Dataset<Row> dataset =
        ArbitraryObjectResolverFactory.of(subject)
            .apply(RuntimeContext.of(spark, fhirEncoders))
            .getDataset();
    final StructType schema = dataset.schema();
    final Row row = dataset.first();

    if (TestLayout.active().isPof()) {
      // The new layout stores a quantity as its FHIR structure, with the value as text and no
      // canonical form, and the text of the literal's value is kept exactly.
      final StructType stored =
          new StructType()
              .add("value", DataTypes.StringType)
              .add("unit", DataTypes.StringType)
              .add("system", DataTypes.StringType)
              .add("code", DataTypes.StringType);
      assertThat(schema.apply("single").dataType()).isEqualTo(stored);
      assertThat(schema.apply("many").dataType())
          .isEqualTo(DataTypes.createArrayType(stored, true));
      final Row single = row.getStruct(row.fieldIndex("single"));
      assertThat(single.<String>getAs("value")).isEqualTo("1.50");
      assertThat(single.<String>getAs("code")).isEqualTo("mg");
      assertThat(single.<String>getAs("system")).isEqualTo("http://unitsofmeasure.org");
      assertThat(row.<Row>getList(row.fieldIndex("many")).getFirst().<String>getAs("value"))
          .isEqualTo("0.0000001");
    } else {
      assertThat(schema.apply("single").dataType()).isEqualTo(QuantityEncoding.dataType());
    }
  }

  @Test
  void syntheticSubjectStoresCodingsInTheRequestedLayout() {
    final Map<Object, Object> subject = new LinkedHashMap<>();
    subject.put("single", FhirTypedLiteral.toCoding("http://a|x"));
    subject.put(
        "many",
        List.of(
            FhirTypedLiteral.toCoding("http://a|x|1"),
            FhirTypedLiteral.toCoding("http://b|y||'Why'|true")));
    final Dataset<Row> dataset =
        ArbitraryObjectResolverFactory.of(subject)
            .apply(RuntimeContext.of(spark, fhirEncoders))
            .getDataset();
    final StructType schema = dataset.schema();
    final Row row = dataset.first();

    if (TestLayout.active().isPof()) {
      // The new layout stores a Coding with only the fields that the codings at its path
      // populate, in the order of the FHIR definition, and without the previous layout's field id.
      assertThat(schema.apply("single").dataType())
          .isEqualTo(
              new StructType()
                  .add("system", DataTypes.StringType)
                  .add("code", DataTypes.StringType));
      assertThat(schema.apply("many").dataType())
          .isEqualTo(
              DataTypes.createArrayType(
                  new StructType()
                      .add("system", DataTypes.StringType)
                      .add("version", DataTypes.StringType)
                      .add("code", DataTypes.StringType)
                      .add("display", DataTypes.StringType)
                      .add("userSelected", DataTypes.BooleanType),
                  true));
      final Row second = row.<Row>getList(row.fieldIndex("many")).get(1);
      assertThat(second.<String>getAs("display")).isEqualTo("Why");
      assertThat(second.<Boolean>getAs("userSelected")).isTrue();
    } else {
      assertThat(schema.apply("single").dataType()).isEqualTo(CodingSchema.codingStructType());
    }
  }

  @Nonnull
  private Map<String, StructType> schemasFromEveryEntryPoint() {
    final Observation observation = observation();
    final String json =
        fhirEncoders.getContext().newJsonParser().encodeResourceToString(observation);
    final RuntimeContext runtime = RuntimeContext.of(spark, fhirEncoders);
    final Function<Function<RuntimeContext, DatasetEvaluator>, StructType> evaluatorSchema =
        factory -> factory.apply(runtime).getDataset().schema();
    return Map.of(
        "ObjectDataSource",
        new ObjectDataSource(spark, fhirEncoders, List.<IBaseResource>of(observation))
            .read("Observation")
            .schema(),
        "HapiResolverFactory",
        evaluatorSchema.apply(HapiResolverFactory.of(observation)),
        "FhirResolverFactory",
        evaluatorSchema.apply(FhirResolverFactory.of(json)),
        "LayoutDatasets.fromJson (FhirViewTest)",
        LayoutDatasets.fromJson(
                spark, fhirEncoders, TestLayout.active(), "Observation", List.of(json))
            .schema());
  }

  private static void assertInLayout(
      @Nonnull final String entryPoint,
      @Nonnull final StructType schema,
      @Nonnull final TestLayout layout) {
    final DataType value = field(field(schema, "valueQuantity"), "value");
    final boolean carriesSubject = Arrays.asList(schema.fieldNames()).contains("subject");
    if (layout.isPof()) {
      assertThat(value).as(entryPoint + ": a decimal is text").isEqualTo(DataTypes.StringType);
      assertThat(carriesSubject).as(entryPoint + ": an absent element is pruned").isFalse();
    } else {
      assertThat(value).as(entryPoint + ": a decimal is a number").isInstanceOf(DecimalType.class);
      assertThat(carriesSubject).as(entryPoint + ": the schema is dense").isTrue();
    }
  }

  @Nonnull
  private static DataType field(@Nonnull final DataType struct, @Nonnull final String name) {
    return ((StructType) struct).apply(name).dataType();
  }

  @Nonnull
  private static Observation observation() {
    final Observation observation = new Observation();
    observation.setId("o1");
    observation.setStatus(ObservationStatus.FINAL);
    observation.setValue(new Quantity().setValue(new BigDecimal("1.50")).setUnit("mg"));
    return observation;
  }
}
