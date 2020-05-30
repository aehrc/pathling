/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.element.ElementDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
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
  private final String expression;

  @Nonnull
  private final Dataset<Row> dataset;

  @Nonnull
  private final Column idColumn;

  @Nonnull
  private final Column valueColumn;

  private final boolean singular;

  @Nonnull
  @Setter(AccessLevel.PROTECTED)
  private Optional<Column> originColumn = Optional.empty();

  @Nonnull
  @Setter(AccessLevel.PROTECTED)
  private Optional<ResourceDefinition> originType = Optional.empty();

  protected NonLiteralPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn, final boolean singular) {
    this.expression = expression;
    this.dataset = dataset;
    this.idColumn = idColumn;
    this.valueColumn = valueColumn;
    this.singular = singular;
  }

  @Nonnull
  @Override
  public abstract Optional<ElementDefinition> getChildElement(@Nonnull final String name);

}
