/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql;

import java.io.Serializable;

/**
 * Decodes a value of a 'raw' spark SQL type to an object of type T. Note: this is technically very
 * similar to spark SQL {@code Expression}, except that the the expressions need to be resolved
 * before they can be used (which seems to be very hard to do outside of the Spark SQL plan
 * analyzer).
 *
 * @param <T> type of the object to produce.
 */
@FunctionalInterface
public interface ObjectDecoder<T> extends Serializable {

  /**
   * Decodes a value of a 'raw' spark SQL type to an object of type T
   *
   * @param rawValue a `raw` spark SQL type (e.g. InternalRow, UTF8String, GenericArrayData)
   * @return the corresponding Java/Scala object of type T
   */
  T decode(Object rawValue);
}

