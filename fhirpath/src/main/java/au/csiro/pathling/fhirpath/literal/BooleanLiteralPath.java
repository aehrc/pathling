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

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.BooleanType;

/**
 * Represents a FHIRPath boolean literal.
 *
 * @author John Grimes
 */
public class BooleanLiteralPath extends LiteralPath<BooleanType> implements
    Materializable<BooleanType>, Comparable {

  @SuppressWarnings("WeakerAccess")
  protected BooleanLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final BooleanType literalValue) {
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
  public static BooleanLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) {
    final boolean value = fhirPath.equals("true");
    return new BooleanLiteralPath(context.getDataset(), context.getIdColumn(),
        new BooleanType(value));
  }

  @Nonnull
  @Override
  public String getExpression() {
    return getValue().asStringValue();
  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    return lit(getValue().booleanValue());
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return BooleanPath.getComparableTypes().contains(type);
  }

  @Nonnull
  @Override
  public Optional<BooleanType> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return BooleanPath.valueFromRow(row, columnNumber);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof BooleanPath;
  }

}
