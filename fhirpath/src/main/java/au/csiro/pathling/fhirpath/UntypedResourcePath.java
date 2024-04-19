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

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.createColumns;

import au.csiro.pathling.QueryHelpers.DatasetWithColumnMap;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ReferencePath;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a path that is a collection of resources of more than one type.
 *
 * @author John Grimes
 */
public class UntypedResourcePath extends ReferencePath {

  public UntypedResourcePath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> currentResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, currentResource,
        thisColumn, fhirType);
  }

  /**
   * @param referencePath a {@link ReferencePath} to base the new UntypedResourcePath on
   * @param expression the FHIRPath representation of this path
   * @return a shiny new UntypedResourcePath
   */
  @Nonnull
  public static UntypedResourcePath build(@Nonnull final ReferencePath referencePath,
      @Nonnull final String expression) {
    return new UntypedResourcePath(expression, referencePath.getDataset(),
        referencePath.getIdColumn(), referencePath.getEidColumn(), referencePath.getValueColumn(),
        referencePath.isSingular(), referencePath.getCurrentResource(),
        referencePath.getThisColumn(), referencePath.getFhirType());
  }

  @Nonnull
  public Column getReferenceColumn() {
    return valueColumn.getField(Referrer.REFERENCE_FIELD_NAME);
  }

  @Nonnull
  public Column getResourceEquality(@Nonnull final ResourcePath resourcePath) {
    return Referrer.resourceEqualityFor(this, resourcePath);
  }

  @Nonnull
  public Column getResourceEquality(@Nonnull final Column targetId,
      @Nonnull final Column targetCode) {
    return Referrer.resourceEqualityFor(this, targetCode, targetId);
  }

  @Nonnull
  @Override
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return Optional.empty();
  }

  @Nonnull
  @Override
  public UntypedResourcePath copy(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Optional<Column> eidColumn, @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final Optional<Column> thisColumn) {

    final DatasetWithColumnMap datasetWithColumns = eidColumn.map(eidCol -> createColumns(dataset,
        eidCol, valueColumn)).orElseGet(() -> createColumns(dataset, valueColumn));

    return new UntypedResourcePath(expression, datasetWithColumns.getDataset(), idColumn,
        eidColumn.map(datasetWithColumns::getColumn), datasetWithColumns.getColumn(valueColumn),
        singular, currentResource, thisColumn, getFhirType());
  }

  @Override
  @Nonnull
  public NonLiteralPath combineWith(@Nonnull final FhirPath target,
      @Nonnull final Dataset<Row> dataset, @Nonnull final String expression,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    if (target instanceof UntypedResourcePath) {
      return copy(expression, dataset, idColumn, eidColumn, valueColumn, singular, thisColumn);
    }
    // Anything else is invalid.
    throw new InvalidUserInputError(
        "Paths cannot be merged into a collection together: " + getExpression() + ", " + target
            .getExpression());
  }

}
