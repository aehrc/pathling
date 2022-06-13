/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.encoders;

import static org.junit.Assert.assertTrue;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

public interface SchemaAsserts {

  @SuppressWarnings("SameParameterValue")
  static void assertFieldNotPresent(final String fieldName,
      final DataType maybeStructType) {
    assertTrue("Must be struct type.", maybeStructType instanceof StructType);
    assertTrue("Field: '" + fieldName + "' not present in struct type.",
        ((StructType) maybeStructType).getFieldIndex(
            fieldName).isEmpty());
  }

  @SuppressWarnings("SameParameterValue")
  static void assertFieldPresent(final String fieldName,
      final DataType maybeStructType) {
    assertTrue("Must be struct type.", maybeStructType instanceof StructType);
    assertTrue("Field: '" + fieldName + "' not present in struct type.",
        ((StructType) maybeStructType).getFieldIndex(
            fieldName).isDefined());
  }
}
