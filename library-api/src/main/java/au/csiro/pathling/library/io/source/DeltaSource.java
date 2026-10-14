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
  public DeltaSource(@Nonnull final PathlingContext context, @Nonnull final String path) {
    this(context, path, true);
  }

  /**
   * Constructs a DeltaSource with the specified PathlingContext and path, saying whether the layout
   * of the stored data is classified as it is read.
   *
   * <p>Layout detection is enabled by the other constructors. It classifies each table from the
   * schema the read already resolves, and refuses a table in a layout this release does not read,
   * such as the layout written by earlier releases, with an {@link UnsupportedLayoutException}.
   * Disabling it is an opt-out for conforming data the detector misclassifies, and applies to this
   * source alone (FR-040).
   *
   * @param context the PathlingContext to use
   * @param path the path to the Delta table directory
   * @param layoutDetection whether the layout of each table is classified as it is read
   */
  public DeltaSource(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      final boolean layoutDetection) {
    super(
        context,
        // Use the "resource name with qualifier" mapper by default, which takes the resource name
        // from the file name and is tolerant of an optional qualifier string.
        retrieveFilesFromPath(path, context, FileSource::resourceNameWithQualifierMapper),
        // Delta tables store data in parquet format internally, so we use the parquet extension.
        "parquet",
        context.getSpark().read().format("delta"),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        resourceType -> true,
        layoutDetection);
  }

  /**
   * Constructs a DeltaSource with the specified PathlingContext and map of resource types to Delta
   * table paths.
   *
   * @param context the PathlingContext to use
   * @param files a map where keys are resource type names and values are collections of Delta table
   *     paths
   */
  public DeltaSource(
      @Nonnull final PathlingContext context,
      @Nonnull final Map<String, Collection<String>> files) {
    this(context, files, true);
  }

  /**
   * Constructs a DeltaSource with the specified PathlingContext and map of resource types to Delta
   * table paths, saying whether the layout of each table is classified as it is read. See {@link
   * #DeltaSource(PathlingContext, String, boolean)} for what layout detection does.
   *
   * @param context the PathlingContext to use
   * @param files a map where keys are resource type names and values are collections of Delta table
   *     paths
   * @param layoutDetection whether the layout of each table is classified as it is read
   */
  public DeltaSource(
      @Nonnull final PathlingContext context,
      @Nonnull final Map<String, Collection<String>> files,
      final boolean layoutDetection) {
    super(
        context,
        files,
        // Delta tables store data in parquet format internally, so we use the parquet extension.
        "parquet",
        context.getSpark().read().format("delta"),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        resourceType -> true,
        layoutDetection);
  }
}
