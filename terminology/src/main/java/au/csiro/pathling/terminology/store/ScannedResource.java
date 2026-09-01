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

package au.csiro.pathling.terminology.store;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * The cheap structural facts about a single FHIR resource, gathered by the streaming pre-scan
 * before any content is written. The pre-scan reads only the leading metadata fields of each
 * resource, so a scanned resource never holds content proportional to the source size.
 *
 * @author John Grimes
 */
public class ScannedResource {

  @Nullable private final String resourceType;
  @Nullable private final String url;
  @Nullable private final String version;
  @Nonnull private final String entryName;
  private final long byteSize;

  /**
   * Creates a scanned resource.
   *
   * @param resourceType the FHIR {@code resourceType}, or null if the source was not a FHIR
   *     resource
   * @param url the canonical URL, or null if absent
   * @param version the business version, or null if absent
   * @param entryName the file path or archive entry name, for routing and error messages
   * @param byteSize the byte size from the file status or archive entry header
   */
  public ScannedResource(
      @Nullable final String resourceType,
      @Nullable final String url,
      @Nullable final String version,
      @Nonnull final String entryName,
      final long byteSize) {
    this.resourceType = resourceType;
    this.url = url;
    this.version = version;
    this.entryName = entryName;
    this.byteSize = byteSize;
  }

  /**
   * Returns the FHIR resource type.
   *
   * @return the resource type, or null if the entry was not a FHIR resource
   */
  @Nullable
  public String getResourceType() {
    return resourceType;
  }

  /**
   * Returns the canonical URL.
   *
   * @return the URL, or null if absent
   */
  @Nullable
  public String getUrl() {
    return url;
  }

  /**
   * Returns the business version.
   *
   * @return the version, or null if absent
   */
  @Nullable
  public String getVersion() {
    return version;
  }

  /**
   * Returns the file path or archive entry name.
   *
   * @return the entry name
   */
  @Nonnull
  public String getEntryName() {
    return entryName;
  }

  /**
   * Returns the byte size of the source entry.
   *
   * @return the byte size
   */
  public long getByteSize() {
    return byteSize;
  }

  /**
   * Reports whether this resource is one the importer can load.
   *
   * @return true for CodeSystem, ValueSet, ConceptMap, and Bundle resources
   */
  public boolean isImportable() {
    return "CodeSystem".equals(resourceType)
        || "ValueSet".equals(resourceType)
        || "ConceptMap".equals(resourceType)
        || "Bundle".equals(resourceType);
  }

  /**
   * Reports whether this resource is a CodeSystem, which the importer routes to the streaming path.
   *
   * @return true for a CodeSystem resource
   */
  public boolean isCodeSystem() {
    return "CodeSystem".equals(resourceType);
  }
}
