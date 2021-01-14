/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Type;

/**
 * Designates an expression that is capable of being extracted from a Spark dataset and materialized
 * back into a FHIR value, e.g. for use in a grouping label.
 *
 * @author John Grimes
 */
public interface Materializable<T extends Type> {

  /**
   * Extracts a value from a {@link Row} within a {@link org.apache.spark.sql.Dataset}.
   * <p>
   * Implementations of this method should use {@link Row#isNullAt} to check for a null value.
   *
   * @param row The {@link Row} to get the value from
   * @param columnNumber The index of the column within the row
   * @return The value, which may be null
   */
  @Nonnull
  Optional<T> getValueFromRow(@Nonnull Row row, int columnNumber);

}
