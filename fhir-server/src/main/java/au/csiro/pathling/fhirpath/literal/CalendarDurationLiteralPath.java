/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import au.csiro.pathling.fhirpath.FhirPath;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Type;

public class CalendarDurationLiteralPath extends LiteralPath implements Comparable {

  protected CalendarDurationLiteralPath(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
  }

  @Nonnull
  public static CalendarDurationLiteralPath fromString(final String s, final FhirPath left) {
    return null;
  }

  @Nonnull
  @Override
  public String getExpression() {
    return null;
  }

  @Nullable
  @Override
  public Object getJavaValue() {
    return null;
  }

  @Override
  public int compareTo(@Nonnull final Object o) {
    return 0;
  }
}
