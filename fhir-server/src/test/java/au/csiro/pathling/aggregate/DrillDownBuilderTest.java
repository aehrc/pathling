/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.aggregate;

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.helpers.TestHelpers;
import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Base64BinaryType;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.MarkdownType;
import org.hl7.fhir.r4.model.OidType;
import org.hl7.fhir.r4.model.PositiveIntType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.TimeType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UnsignedIntType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UrlType;
import org.hl7.fhir.r4.model.UuidType;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
@NotImplemented
class DrillDownBuilderTest {

  @Autowired
  SparkSession spark;

  @Value
  static class TestParameters {

    @Nonnull
    Type label;

    @Nonnull
    String expectedResult;

    @Override
    public String toString() {
      return label.getClass().getSimpleName();
    }

  }

  static Stream<TestParameters> parameters() {
    return Stream.of(
        new TestParameters(
            new BooleanType(true),
            "someElement"),
        new TestParameters(
            new Coding(TestHelpers.SNOMED_URL, "373067005", "No"),
            "(someElement) = http://snomed.info/sct|373067005||No"),
        new TestParameters(
            new DateType("2020-01-01"),
            "(someElement) = @2020-01-01"),
        new TestParameters(
            new DateTimeType("2018-05-19T11:03:55.123Z"),
            "(someElement) = @2018-05-19T11:03:55.123Z"),
        new TestParameters(
            new InstantType("2018-05-19T11:03:55.123Z"),
            "(someElement) = @2018-05-19T11:03:55.123Z"),
        new TestParameters(
            new DecimalType("3.4"),
            "(someElement) = 3.4"),
        new TestParameters(
            new IntegerType("3"),
            "(someElement) = 3"),
        new TestParameters(
            new UnsignedIntType("3"),
            "(someElement) = 3"),
        new TestParameters(
            new PositiveIntType("3"),
            "(someElement) = 3"),
        new TestParameters(
            new StringType("foo"),
            "(someElement) = 'foo'"),
        new TestParameters(
            new UuidType("urn:uuid:953b3678-ba7d-48ed-8a55-37835c0f0ffd"),
            "(someElement) = 'urn:uuid:953b3678-ba7d-48ed-8a55-37835c0f0ffd'"),
        new TestParameters(
            new UriType("urn:test:foo"),
            "(someElement) = 'urn:test:foo'"),
        new TestParameters(
            new UrlType("https://pathling.csiro.au"),
            "(someElement) = 'https://pathling.csiro.au'"),
        new TestParameters(
            new CodeType("foo"),
            "(someElement) = 'foo'"),
        new TestParameters(
            new OidType("1.3.6.1.4.1.343"),
            "(someElement) = '1.3.6.1.4.1.343'"),
        new TestParameters(
            new IdType("foo"),
            "(someElement) = 'foo'"),
        new TestParameters(
            new MarkdownType("foo"),
            "(someElement) = 'foo'"),
        new TestParameters(
            new Base64BinaryType("foo"),
            "(someElement) = 'foo='"),
        new TestParameters(
            new CanonicalType("urn:test:foo"),
            "(someElement) = 'urn:test:foo'"),
        new TestParameters(
            new TimeType("12:30"),
            "(someElement) = @T12:30")
    );
  }

  // TODO: Implement
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void labelTypes(@Nonnull final TestParameters parameters) {
  //   final List<Optional<Type>> labels = List.of(Optional.of(parameters.getLabel()));
  //   final PrimitivePath grouping = new ElementPathBuilder(spark)
  //       .expression("someElement")
  //       .singular(true)
  //       .build();
  //   final List<Collection> groupings = List.of(grouping);
  //   final DrillDownBuilder builder = new DrillDownBuilder(labels, groupings,
  //       Collections.emptyList());
  //   final Optional<String> drillDown = builder.build();
  //   assertTrue(drillDown.isPresent());
  //   assertEquals(parameters.getExpectedResult(), drillDown.get());
  // }

}
