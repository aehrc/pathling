/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

public interface SchemaAsserts {

  static void assertFieldNotPresent(final String fieldName, final DataType maybeStructType) {
    assertInstanceOf(StructType.class, maybeStructType, "Must be struct type.");
    assertTrue(
        ((StructType) maybeStructType).getFieldIndex(fieldName).isEmpty(),
        "Field: '" + fieldName + "' not present in struct type.");
  }

  static void assertFieldPresent(final String fieldName, final DataType maybeStructType) {
    assertInstanceOf(StructType.class, maybeStructType, "Must be struct type.");
    assertTrue(
        ((StructType) maybeStructType).getFieldIndex(fieldName).isDefined(),
        "Field: '" + fieldName + "' not present in struct type.");
  }
}
