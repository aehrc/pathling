/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.ID_COLUMN_SUFFIX;
import static au.csiro.pathling.QueryHelpers.VALUE_COLUMN_SUFFIX;
import static au.csiro.pathling.QueryHelpers.applySelection;
import static au.csiro.pathling.utilities.Strings.randomShortString;

import au.csiro.pathling.fhirpath.element.ElementDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Represents any FHIRPath expression which is not a literal.
 *
 * @author John Grimes
 */
@Getter
public abstract class NonLiteralPath implements FhirPath {

  @Nonnull
  protected final String expression;

  @Nonnull
  protected final Dataset<Row> dataset;

  @Nonnull
  protected final Optional<Column> idColumn;

  @Nonnull
  protected final Column valueColumn;

  protected final boolean singular;

  @Nonnull
  protected Optional<Column> originColumn = Optional.empty();

  @Nonnull
  protected Optional<ResourceDefinition> originType = Optional.empty();

  protected NonLiteralPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn, @Nonnull final Column valueColumn,
      final boolean singular) {
    this.expression = expression;
    this.singular = singular;

    final String hash = randomShortString();
    final String idColumnName = hash + ID_COLUMN_SUFFIX;
    final String valueColumnName = hash + VALUE_COLUMN_SUFFIX;

    Dataset<Row> hashedDataset = dataset;
    if (idColumn.isPresent()) {
      hashedDataset = dataset.withColumn(idColumnName, idColumn.get());
    }
    hashedDataset = hashedDataset.withColumn(valueColumnName, valueColumn);

    if (idColumn.isPresent()) {
      this.idColumn = Optional.of(hashedDataset.col(idColumnName));
    } else {
      this.idColumn = Optional.empty();
    }
    this.valueColumn = hashedDataset.col(valueColumnName);
    this.dataset = applySelection(hashedDataset, this.idColumn);
  }

  @Nonnull
  @Override
  public abstract Optional<ElementDefinition> getChildElement(@Nonnull final String name);

}
