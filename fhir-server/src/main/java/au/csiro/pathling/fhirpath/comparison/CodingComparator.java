/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.comparison;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.Comparable.Comparator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.lit;

/**
 * Implementation of comparator for Coding type.
 */
public class CodingComparator implements Comparator {

  private static final List<String> EQUALITY_COLUMNS = Arrays
      .asList("system", "code", "version", "display", "userSelected");

  public static final CodingComparator INSTANCE = new CodingComparator();

  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return functions.when(left.isNull().or(right.isNull()), lit(null))
        .otherwise(
            EQUALITY_COLUMNS.stream()
                .map(f -> left.getField(f).eqNullSafe(right.getField(f))).reduce(Column::and).get()
        );
  }

  @Override
  public Column lessThan(final Column left, final Column right) {
    throw new InvalidUserInputError(
        "Coding type does not support comparison operator: " + "lessThan");

  }

  @Override
  public Column greaterThan(final Column left, final Column right) {
    throw new InvalidUserInputError(
        "Coding type does not support comparison operator: " + "greaterThan");
  }
}
