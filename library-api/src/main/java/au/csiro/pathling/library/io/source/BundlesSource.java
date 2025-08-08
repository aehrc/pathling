/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A class for making data within a set of FHIR Bundles available for query.
 *
 * @author John Grimes
 */
public class BundlesSource extends FileSource {

  private static final Map<String, String> MIME_TYPE_TO_EXTENSION;

  static {
    MIME_TYPE_TO_EXTENSION = new HashMap<>();
    MIME_TYPE_TO_EXTENSION.put(PathlingContext.FHIR_JSON, "json");
    MIME_TYPE_TO_EXTENSION.put(PathlingContext.FHIR_XML, "xml");
  }

  /**
   * Constructs a BundlesSource with the specified PathlingContext, path, MIME type, and resource
   * types.
   *
   * @param context the PathlingContext to use
   * @param path the path to the files containing FHIR Bundles
   * @param mimeType the MIME type of the FHIR data (e.g., "application/fhir+json")
   * @param resourceTypes the set of FHIR resource types that are expected in the bundles
   */
  BundlesSource(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final String mimeType, @Nonnull final Set<String> resourceTypes) {
    super(context, path,
        // Map to the fixed set of resource types for all files.
        p -> new HashSet<>(resourceTypes),
        // Use the file extension that matches the FHIR MIME type.
        MIME_TYPE_TO_EXTENSION.get(mimeType),
        // Treat the whole file as a record, rather than individual lines.
        context.getSpark().read().option("wholetext", true).format("text"),
        // Extract the nominated resource types from each file and encode using the specified MIME
        // type.
        (sourceData, resourceType) -> context.encodeBundle(sourceData, resourceType, mimeType));
  }

}
