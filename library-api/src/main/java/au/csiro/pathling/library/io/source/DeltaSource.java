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

package au.csiro.pathling.library.io.source;

import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;

/**
 * A class for making FHIR data in Delta tables on the filesystem available for query. It is assumed
 * that the schema of the Delta tables aligns with that of the Pathling FHIR encoders. Delta tables
 * store data in Parquet format internally, so this source uses the parquet extension for file
 * detection.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class DeltaSource extends FileSource {

  /**
   * Constructs a DeltaSource with the specified PathlingContext and path.
   *
   * @param context the PathlingContext to use
   * @param path the path to the Delta table directory
   */
  DeltaSource(@Nonnull final PathlingContext context, @Nonnull final String path) {
    super(context, path,
        // Use the "resource name with qualifier" mapper by default, which takes the resource name
        // from the file name and is tolerant of an optional qualifier string.
        null,
        // Delta tables store data in parquet format internally, so we use the parquet extension.
        "parquet",
        context.getSpark().read().format("delta"),
        // Apply no transformations on the data - we assume it has already been processed using the 
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        resourceType -> true);
  }

  /**
   * Constructs a DeltaSource with the specified PathlingContext and map of resource types to Delta table paths.
   *
   * @param context the PathlingContext to use
   * @param files a map where keys are resource type names and values are collections of Delta table paths
   */
  DeltaSource(@Nonnull final PathlingContext context, @Nonnull final Map<String, Collection<String>> files) {
    super(context, files,
        // Delta tables store data in parquet format internally, so we use the parquet extension.
        "parquet",
        context.getSpark().read().format("delta"),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        resourceType -> true);
  }

}
