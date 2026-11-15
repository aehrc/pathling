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

package au.csiro.pathling.io;

import au.csiro.pathling.legacy.LegacyExport;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.LayoutDetection;
import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.library.io.source.StorageLayout;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Converts data in the layout written before Parquet on FHIR (the legacy layout) into the Parquet
 * on FHIR layout.
 *
 * <p>Each resource is decoded with the legacy encoder, serialised as FHIR JSON, and encoded again
 * by the library, which is the migration route the library provides (its ADR-0001). The conversion
 * carries over only what the legacy layout stored: decimals come back at the fixed scale it stored
 * them at, and content it never stored, such as contained resources, the ids and extensions of
 * primitives, and anything beyond its nesting, extension and open-type bounds, stays lost.
 *
 * <pre>{@code
 * Dataset<Row> converted = LegacyLayoutConversion.convertIfLegacy(context, "Patient", table);
 * }</pre>
 *
 * @author John Grimes
 */
public final class LegacyLayoutConversion {

  /** The losses of the conversion, as reported to an operator. */
  public static final String LOSSES =
      "decimals keep the fixed scale of six decimal places the earlier layout stored them at, and "
          + "content the earlier layout never stored (contained resources, ids and extensions of "
          + "primitive elements, and content beyond its nesting, extension and open type limits) "
          + "remains absent";

  private LegacyLayoutConversion() {}

  /**
   * Returns whether a schema is that of a table in the legacy layout. A table that mixes the legacy
   * layout with the Parquet on FHIR layout counts as legacy.
   *
   * @param schema the schema of a resource table
   * @return true if the table is in the legacy layout
   */
  public static boolean isLegacy(@Nonnull final StructType schema) {
    return LayoutDetector.detect(schema).getLayout() == StorageLayout.PREVIOUS;
  }

  /**
   * Converts a resource table to the Parquet on FHIR layout if it is in the legacy layout, and
   * otherwise returns it as it is, provided its layout is one the library reads.
   *
   * @param context the Pathling context
   * @param resourceType the type of the resources the table holds
   * @param data the table
   * @return the table in the Parquet on FHIR layout
   * @throws au.csiro.pathling.library.io.source.UnsupportedLayoutException if the table mixes the
   *     two layouts, which the legacy decoder cannot read, or is in another layout the library does
   *     not read
   */
  @Nonnull
  public static Dataset<Row> convertIfLegacy(
      @Nonnull final PathlingContext context,
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> data) {
    final LayoutDetection detection = LayoutDetector.detect(data.schema());
    if (detection.getLayout() != StorageLayout.PREVIOUS || detection.isMixed()) {
      // Refuse what neither the library nor the legacy decoder can read.
      LayoutDetector.check(resourceType, data.schema());
      return data;
    }
    final Dataset<String> documents =
        LegacyExport.of(context.getSpark()).toNdjson(resourceType, data);
    return context.encode(documents.toDF(), resourceType);
  }
}
