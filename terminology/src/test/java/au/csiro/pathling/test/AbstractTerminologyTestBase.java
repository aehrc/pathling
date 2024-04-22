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

package au.csiro.pathling.test;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.junit.jupiter.params.provider.Arguments;
import scala.collection.mutable.WrappedArray;

public abstract class AbstractTerminologyTestBase {

  public static final String SYSTEM_A = "uuid:systemA";
  public static final String CODE_A = "codeA";
  public static final String SYSTEM_B = "uuid:systemB";
  public static final String CODE_B = "codeB";
  public static final String SYSTEM_C = "uuid:systemC";
  public static final String CODE_C = "codeC";
  public static final String SYSTEM_D = "uuid:systemD";
  public static final String CODE_D = "codeD";
  public static final String SYSTEM_E = "uuid:systemE";
  public static final String CODE_E = "codeE";


  public static final Coding CODING_AA = new Coding(SYSTEM_A, CODE_A, "displayAA");
  public static final Coding CODING_AB = new Coding(SYSTEM_A, CODE_B, "displayAB");

  public static final String VERSION_1 = "version1";
  public static final String VERSION_2 = "version2";

  public static final Coding CODING_AA_VERSION1 = new Coding(SYSTEM_A, CODE_A,
      "displayAA").setVersion(
      VERSION_1);
  public static final Coding CODING_AB_VERSION1 = new Coding(SYSTEM_A, CODE_B,
      "displayAB").setVersion(
      VERSION_1);
  public static final Coding CODING_AB_VERSION2 = new Coding(SYSTEM_A, CODE_B,
      "displayAB").setVersion(
      VERSION_2);

  public static final Coding CODING_AA_DISPLAY1 = new Coding(SYSTEM_A, CODE_A,
      "displayAA1");

  public static final Coding CODING_BA = new Coding(SYSTEM_B, CODE_A, "displayBA");
  public static final Coding CODING_BB = new Coding(SYSTEM_B, CODE_B, "displayBB");
  public static final Coding CODING_BB_VERSION1 = new Coding(SYSTEM_B, CODE_B,
      "displayB").setVersion(
      VERSION_1);

  public static final Coding CODING_A = CODING_AA;
  public static final Coding CODING_B = CODING_BB;
  public static final Coding CODING_C = new Coding(SYSTEM_C, CODE_C, "displayCC");
  public static final Coding CODING_D = new Coding(SYSTEM_D, CODE_D, "displayDD");
  public static final Coding CODING_E = new Coding(SYSTEM_E, CODE_E, "displayEE");

  public static final Coding INVALID_CODING_0 = new Coding(null, null, "");
  public static final Coding INVALID_CODING_1 = new Coding("uiid:system", null, "");
  public static final Coding INVALID_CODING_2 = new Coding(null, "someCode", "");


  @Nonnull
  public static Row[] asArray(@Nonnull final Coding... codings) {
    return Stream.of(codings).map(CodingEncoding::encode).toArray(Row[]::new);
  }

  @Nonnull
  public static WrappedArray<Object> encodeMany(Coding... codings) {
    return WrappedArray.make(Stream.of(codings).map(CodingEncoding::encode).toArray(Row[]::new));
  }

  @Nonnull
  private static <T> Arguments primitiveArguments(
      final String fhirType, final DataType sqlType,
      final Function<T, ? extends Type> constructor,
      final List<T> propertyAValues, final List<T> propertyBValues) {
    return arguments(fhirType, sqlType,
        propertyAValues.toArray(),
        propertyBValues.toArray(),
        propertyAValues,
        propertyBValues);
  }

  @Nonnull
  public static Stream<Arguments> propertyParameters() {
    return Stream.of(
        primitiveArguments("string", DataTypes.StringType, StringType::new,
            List.of("string_a"),
            List.of("string_b.0", "string_b.1")
        ),
        primitiveArguments("code", DataTypes.StringType, CodeType::new,
            List.of("code_a"),
            List.of("code_b.0", "code_b.1")
        ),
        primitiveArguments("integer", DataTypes.IntegerType, IntegerType::new,
            List.of(111),
            List.of(222, 333)
        ),
        primitiveArguments("boolean", DataTypes.BooleanType, BooleanType::new,
            List.of(true),
            List.of(false, true)
        ),
        primitiveArguments("decimal", DecimalCustomCoder.decimalType(), DecimalType::new,
            List.of(new BigDecimal("1.11")),
            List.of(new BigDecimal("2.22"), new BigDecimal("3.33"))
        ),
        primitiveArguments("dateTime", DataTypes.StringType, DateTimeType::new,
            List.of("1999-01-01"),
            List.of("2222-02-02", "3333-03-03")
        ),
        arguments("Coding", CodingEncoding.DATA_TYPE,
            List.of(CODING_C),
            List.of(CODING_D, CODING_E),
            CodingEncoding.encodeList(List.of(CODING_C)),
            CodingEncoding.encodeList(List.of(CODING_D, CODING_E))
        )
    );
  }
}
