/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Type;

/**
 * Designates an expression that is capable of being extracted from a Spark dataset and materialized
 * back into a value featured within a result, e.g. for use in a grouping label or as part of an
 * extract.
 *
 * @author John Grimes
 */
public interface Materializable<T extends Type> {

  /**
   * Extracts a value from a {@link Row} within a {@link org.apache.spark.sql.Dataset}.
   * <p>
   * Implementations of this method should use {@link Row#isNullAt} to check for a null value.
   *
   * @param row the {@link Row} to get the value from
   * @param columnNumber the index of the column within the row
   * @return the value, which may be missing
   */
  @Nonnull
  Optional<T> getValueFromRow(@Nonnull Row row, int columnNumber);

  /**
   * Provides a {@link Column} that can be used to materialize a value in the extract operation,
   * which uses a CSV representation that is not compatible with struct values.
   *
   * @return a column which can be written out as part of an extract result
   */
  @Nonnull
  Column getExtractableColumn();

}
