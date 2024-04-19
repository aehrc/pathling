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

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.fhirpath.literal.StringLiteral.toLiteral;
import static au.csiro.pathling.utilities.Strings.unSingleQuote;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.element.StringPath;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Function;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.StringType;

/**
 * Represents a FHIRPath string literal.
 *
 * @author John Grimes
 */
@SuppressWarnings("rawtypes")
@Getter
public class StringLiteralPath extends LiteralPath<PrimitiveType> implements
    Materializable<PrimitiveType>, Comparable {

  protected StringLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final PrimitiveType literalValue) {
    super(dataset, idColumn, literalValue);
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @param context An input context that can be used to build a {@link Dataset} to represent the
   * literal
   * @return A new instance of {@link LiteralPath}
   */
  @Nonnull
  public static StringLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) {
    // Remove the surrounding single quotes and unescape the string according to the rules within
    // the FHIRPath specification.
    String value = unSingleQuote(fhirPath);
    value = unescapeFhirPathString(value);

    return new StringLiteralPath(context.getDataset(), context.getIdColumn(),
        new StringType(value));
  }

  @Nonnull
  @Override
  public String getExpression() {
    return toLiteral(getValue().getValueAsString());
  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    return lit(getValue().getValueAsString());
  }

  /**
   * This method implements the rules for dealing with strings in the FHIRPath specification.
   *
   * @param value the string to be unescaped
   * @return the unescaped result
   * @see <a href="https://hl7.org/fhirpath/index.html#string">String</a>
   */
  @Nonnull
  public static String unescapeFhirPathString(@Nonnull String value) {
    value = value.replaceAll("\\\\/", "/");
    value = value.replaceAll("\\\\f", "\u000C");
    value = value.replaceAll("\\\\n", "\n");
    value = value.replaceAll("\\\\r", "\r");
    value = value.replaceAll("\\\\t", "\u0009");
    value = value.replaceAll("\\\\`", "`");
    value = value.replaceAll("\\\\'", "'");
    return value.replaceAll("\\\\\\\\", "\\\\");
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return StringPath.getComparableTypes().contains(type);
  }

  @Nonnull
  @Override
  public Optional<PrimitiveType> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return StringPath.valueFromRow(row, columnNumber, FHIRDefinedType.STRING);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof StringPath;
  }

}
