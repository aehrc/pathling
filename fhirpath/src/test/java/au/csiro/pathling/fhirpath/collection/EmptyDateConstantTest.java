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

package au.csiro.pathling.fhirpath.collection;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.test.SpringBootUnitTest;
import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.InstantType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/** A date, dateTime or instant constant without a value is empty, as any other constant is. */
@SpringBootUnitTest
class EmptyDateConstantTest {

  @Autowired SparkSession spark;

  static Stream<IBase> valuesWithoutAValue() {
    return Stream.of(new DateType(), new DateTimeType(), new InstantType());
  }

  @ParameterizedTest
  @MethodSource("valuesWithoutAValue")
  void aConstantWithoutAValueIsEmpty(@Nonnull final IBase value) {
    final Collection constant = Collection.fromValue(value);
    assertThat(spark.range(1).select(constant.getColumn().getValue()).first().isNullAt(0)).isTrue();
  }
}
