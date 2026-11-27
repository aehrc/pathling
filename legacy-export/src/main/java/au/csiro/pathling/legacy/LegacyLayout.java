/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
package au.csiro.pathling.legacy;

import au.csiro.pathling.encoders.datatypes.IdCustomCoder;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Recognises a table in the legacy layout from its schema, and refuses any other.
 *
 * <p>The legacy encoder writes an {@code id_versioned} string beside the string {@code id} of every
 * resource, with extensions enabled or not, and nothing else writes it, so it is what identifies
 * the legacy layout. A {@code resourceType} column at the root identifies the Parquet on FHIR
 * layout, which the legacy encoder never writes.
 *
 * @author Piotr Szul
 */
final class LegacyLayout {

  @Nonnull private static final String ID = "id";

  @Nonnull private static final String VERSIONED_ID = new IdCustomCoder(ID).versionedName();

  @Nonnull private static final String RESOURCE_TYPE = "resourceType";

  private LegacyLayout() {}

  /**
   * Checks that a table is in the legacy layout.
   *
   * @param resourceType the resource type the table is said to hold, for the message
   * @param schema the schema of the table
   * @throws LegacyExportException if the table is not in the legacy layout, or mixes it with the
   *     Parquet on FHIR layout
   */
  static void check(@Nonnull final String resourceType, @Nonnull final StructType schema) {
    final boolean legacy = hasVersionedId(schema);
    final boolean parquetOnFhir = hasRootString(schema, RESOURCE_TYPE);
    if (legacy && parquetOnFhir) {
      throw new LegacyExportException(
          "The "
              + resourceType
              + " table mixes the legacy layout with the Parquet on FHIR layout: it has both an "
              + VERSIONED_ID
              + " and a "
              + RESOURCE_TYPE
              + " column, as appending new data to a legacy table produces. The legacy encoder "
              + "cannot decode its rows in the Parquet on FHIR layout. Export a copy of the table "
              + "that holds only its legacy rows, those with a non-null "
              + VERSIONED_ID
              + ".");
    }
    if (!legacy) {
      final StringBuilder message =
          new StringBuilder()
              .append("The ")
              .append(resourceType)
              .append(" table is not in the legacy layout written by the Pathling FHIR encoders: ")
              .append("it has no ")
              .append(VERSIONED_ID)
              .append(" column beside a string ")
              .append(ID)
              .append(", which the legacy encoder writes for every resource.");
      if (parquetOnFhir) {
        message
            .append(" It has a ")
            .append(RESOURCE_TYPE)
            .append(" column, so it is likely in the Parquet on FHIR layout already, which is ")
            .append("read directly and needs no export.");
      }
      throw new LegacyExportException(message.toString());
    }
  }

  private static boolean hasVersionedId(@Nonnull final StructType schema) {
    return hasRootString(schema, VERSIONED_ID) && hasRootString(schema, ID);
  }

  private static boolean hasRootString(
      @Nonnull final StructType schema, @Nonnull final String name) {
    return Arrays.stream(schema.fields())
        .filter(field -> field.name().equals(name))
        .map(StructField::dataType)
        .anyMatch(DataTypes.StringType::equals);
  }
}
