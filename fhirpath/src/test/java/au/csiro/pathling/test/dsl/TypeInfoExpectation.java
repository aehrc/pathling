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

package au.csiro.pathling.test.dsl;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import jakarta.annotation.Nonnull;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

/**
 * Represents an expected TypeInfo result for DSL test assertions. Provides a compact string
 * representation in the format {@code Namespace.Name(BaseType)} that can be used with {@link
 * FhirPathTestBuilder#testEquals(Object, String, String)} for direct comparison against TypeInfo
 * struct results from the FHIRPath {@code type()} function.
 *
 * @author Piotr Szul
 */
@Value
public class TypeInfoExpectation {

  private static final Pattern TYPE_INFO_PATTERN = Pattern.compile("^(\\w+)\\.(\\w+)\\((.+)\\)$");

  @Nonnull String namespace;
  @Nonnull String name;
  @Nonnull String baseType;

  /**
   * Parses a TypeInfo string representation into a {@link TypeInfoExpectation}.
   *
   * @param repr the string representation in the format {@code Namespace.Name(BaseType)}, e.g.
   *     {@code "System.Integer(System.Any)"} or {@code "FHIR.Patient(FHIR.Resource)"}
   * @return a new TypeInfoExpectation with the parsed fields
   * @throws IllegalArgumentException if the string does not match the expected format
   */
  @Nonnull
  public static TypeInfoExpectation toTypeInfo(@Nonnull final String repr) {
    final Matcher matcher = TYPE_INFO_PATTERN.matcher(repr);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "Invalid TypeInfo representation: '"
              + repr
              + "'. Expected format: Namespace.Name(BaseType)");
    }
    return new TypeInfoExpectation(matcher.group(1), matcher.group(2), matcher.group(3));
  }

  /**
   * Builds a Spark struct column representing this TypeInfo expectation with {@code namespace},
   * {@code name}, and {@code baseType} fields. The schema matches the TypeInfo struct produced by
   * the FHIRPath {@code type()} function.
   *
   * @return a Spark struct column
   */
  @Nonnull
  public Column toStructColumn() {
    return struct(
        lit(namespace).cast(DataTypes.StringType).as("namespace"),
        lit(name).cast(DataTypes.StringType).as("name"),
        lit(baseType).cast(DataTypes.StringType).as("baseType"));
  }
}
