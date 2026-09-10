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

package au.csiro.pathling.terminology.local.index;

import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import jakarta.annotation.Nonnull;

/**
 * The runtime indexes for one code system version, loaded lazily per executor JVM from the store.
 * The concept dictionary is loaded eagerly because it gates every other lookup; the hierarchy,
 * reference set, and relationship indexes load on first use so a workload that only needs some of
 * them does not pay for the others.
 *
 * @author John Grimes
 */
public final class CodeSystemIndexes {

  @Nonnull private final ConceptDictionary dictionary;

  // The secondary indexes load lazily on first use so a workload that only needs some of them does
  // not pay for the others. Memoising suppliers give thread-safe, load-once semantics.
  @Nonnull private final Supplier<HierarchyIndex> hierarchy;
  @Nonnull private final Supplier<RefsetIndex> refsets;
  @Nonnull private final Supplier<RelationshipIndex> relationships;
  @Nonnull private final Supplier<DescriptionIndex> descriptions;
  @Nonnull private final Supplier<PropertyIndex> properties;

  private CodeSystemIndexes(
      @Nonnull final TerminologyStoreReader reader,
      @Nonnull final String systemVersionId,
      @Nonnull final ConceptDictionary dictionary) {
    this.dictionary = dictionary;
    this.hierarchy = Suppliers.memoize(() -> HierarchyIndex.load(reader, systemVersionId));
    this.refsets = Suppliers.memoize(() -> RefsetIndex.load(reader, systemVersionId));
    this.relationships = Suppliers.memoize(() -> RelationshipIndex.load(reader, systemVersionId));
    this.descriptions = Suppliers.memoize(() -> DescriptionIndex.load(reader, systemVersionId));
    this.properties = Suppliers.memoize(() -> PropertyIndex.load(reader, systemVersionId));
  }

  /**
   * Loads the indexes for a code system version, building the concept dictionary eagerly.
   *
   * @param reader the store reader
   * @param systemVersionId the code system version to load
   * @return the index container
   */
  @Nonnull
  public static CodeSystemIndexes load(
      @Nonnull final TerminologyStoreReader reader, @Nonnull final String systemVersionId) {
    return new CodeSystemIndexes(
        reader, systemVersionId, ConceptDictionary.load(reader, systemVersionId));
  }

  /**
   * Returns the concept dictionary.
   *
   * @return the concept dictionary
   */
  @Nonnull
  public ConceptDictionary dictionary() {
    return dictionary;
  }

  /**
   * Returns the hierarchy index, loading it on first use.
   *
   * @return the hierarchy index
   */
  @Nonnull
  public HierarchyIndex hierarchy() {
    return hierarchy.get();
  }

  /**
   * Returns the reference set index, loading it on first use.
   *
   * @return the reference set index
   */
  @Nonnull
  public RefsetIndex refsets() {
    return refsets.get();
  }

  /**
   * Returns the relationship index, loading it on first use.
   *
   * @return the relationship index
   */
  @Nonnull
  public RelationshipIndex relationships() {
    return relationships.get();
  }

  /**
   * Returns the description index, loading it on first use.
   *
   * @return the description index
   */
  @Nonnull
  public DescriptionIndex descriptions() {
    return descriptions.get();
  }

  /**
   * Returns the scalar property index, loading it on first use.
   *
   * @return the scalar property index
   */
  @Nonnull
  public PropertyIndex properties() {
    return properties.get();
  }
}
