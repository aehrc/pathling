/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql;

import java.io.Serializable;

/**
 * Decodes a value of a 'raw' spark SQL type to an object of type T. Note: this is technically very
 * similar to spark SQL {@code Expression}, except that the expressions need to be resolved before
 * they can be used (which seems to be very hard to do outside of the Spark SQL plan analyzer).
 *
 * @param <T> type of the object to produce.
 */
@FunctionalInterface
@Deprecated
public interface ObjectDecoder<T> extends Serializable {

  /**
   * Decodes a value of a 'raw' spark SQL type to an object of type T
   *
   * @param rawValue a `raw` spark SQL type (e.g. InternalRow, UTF8String, GenericArrayData)
   * @return the corresponding Java/Scala object of type T
   */
  T decode(Object rawValue);
}
