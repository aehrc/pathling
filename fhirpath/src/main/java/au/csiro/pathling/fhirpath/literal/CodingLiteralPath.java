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

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.comparison.CodingSqlComparator;
import au.csiro.pathling.fhirpath.element.CodingPath;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Function;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;

/**
 * Represents a FHIRPath Coding literal.
 *
 * @author John Grimes
 */
@Getter
public class CodingLiteralPath extends LiteralPath<Coding> implements Materializable<Coding>,
    Comparable {

  protected CodingLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Coding literalValue) {
    super(dataset, idColumn, literalValue);
  }

  protected CodingLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Coding literalValue, @Nonnull final String expression) {
    super(dataset, idColumn, literalValue, expression);
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @param context An input context that can be used to build a {@link Dataset} to represent the
   * literal
   * @return A new instance of {@link LiteralPath}
   * @throws IllegalArgumentException if the literal is malformed
   */
  @Nonnull
  public static CodingLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) throws IllegalArgumentException {
    return new CodingLiteralPath(context.getDataset(), context.getIdColumn(),
        CodingLiteral.fromString(fhirPath), fhirPath);
  }

  @Nonnull
  @Override
  public String getExpression() {
    return expression.orElse(CodingLiteral.toLiteral(getValue()));

  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    final Coding value = getValue();
    return struct(
        lit(value.getId()).as("id"),
        lit(value.getSystem()).as("system"),
        lit(value.getVersion()).as("version"),
        lit(value.getCode()).as("code"),
        lit(value.getDisplay()).as("display"),
        lit(value.hasUserSelected()
            ? value.getUserSelected()
            : null).as("userSelected"),
        lit(null).as("_fid"));
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return CodingSqlComparator.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return CodingPath.getComparableTypes().contains(type);
  }

  @Nonnull
  @Override
  public Optional<Coding> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return CodingPath.valueFromRow(row, columnNumber);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof CodingPath;
  }

  @Nonnull
  @Override
  public Column getExtractableColumn() {
    return lit(CodingLiteral.toLiteral(getValue()));
  }

}
