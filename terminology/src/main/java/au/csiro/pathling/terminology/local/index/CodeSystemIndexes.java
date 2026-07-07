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

  @Nonnull private final TerminologyStoreReader reader;
  @Nonnull private final String systemVersionId;
  @Nonnull private final ConceptDictionary dictionary;

  private volatile HierarchyIndex hierarchy;
  private volatile RefsetIndex refsets;
  private volatile RelationshipIndex relationships;
  private volatile DescriptionIndex descriptions;
  private volatile PropertyIndex properties;

  private CodeSystemIndexes(
      @Nonnull final TerminologyStoreReader reader,
      @Nonnull final String systemVersionId,
      @Nonnull final ConceptDictionary dictionary) {
    this.reader = reader;
    this.systemVersionId = systemVersionId;
    this.dictionary = dictionary;
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
    HierarchyIndex local = hierarchy;
    if (local == null) {
      synchronized (this) {
        local = hierarchy;
        if (local == null) {
          local = HierarchyIndex.load(reader, systemVersionId);
          hierarchy = local;
        }
      }
    }
    return local;
  }

  /**
   * Returns the reference set index, loading it on first use.
   *
   * @return the reference set index
   */
  @Nonnull
  public RefsetIndex refsets() {
    RefsetIndex local = refsets;
    if (local == null) {
      synchronized (this) {
        local = refsets;
        if (local == null) {
          local = RefsetIndex.load(reader, systemVersionId);
          refsets = local;
        }
      }
    }
    return local;
  }

  /**
   * Returns the relationship index, loading it on first use.
   *
   * @return the relationship index
   */
  @Nonnull
  public RelationshipIndex relationships() {
    RelationshipIndex local = relationships;
    if (local == null) {
      synchronized (this) {
        local = relationships;
        if (local == null) {
          local = RelationshipIndex.load(reader, systemVersionId);
          relationships = local;
        }
      }
    }
    return local;
  }

  /**
   * Returns the description index, loading it on first use.
   *
   * @return the description index
   */
  @Nonnull
  public DescriptionIndex descriptions() {
    DescriptionIndex local = descriptions;
    if (local == null) {
      synchronized (this) {
        local = descriptions;
        if (local == null) {
          local = DescriptionIndex.load(reader, systemVersionId);
          descriptions = local;
        }
      }
    }
    return local;
  }

  /**
   * Returns the scalar property index, loading it on first use.
   *
   * @return the scalar property index
   */
  @Nonnull
  public PropertyIndex properties() {
    PropertyIndex local = properties;
    if (local == null) {
      synchronized (this) {
        local = properties;
        if (local == null) {
          local = PropertyIndex.load(reader, systemVersionId);
          properties = local;
        }
      }
    }
    return local;
  }
}
