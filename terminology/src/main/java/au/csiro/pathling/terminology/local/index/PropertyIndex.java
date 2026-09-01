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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_PROPERTY_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VALUE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VALUE_TYPE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.PROPERTY;

import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The scalar property index for one code system version: the declared FHIR CodeSystem properties of
 * each concept (integer, boolean, code, string, decimal, and dateTime values). SNOMED CT stores
 * have no scalar property table, so this index loads as empty for them. Coding-valued properties
 * are held in the relationship index instead.
 *
 * @author John Grimes
 */
public final class PropertyIndex {

  @Nonnull private final Map<Integer, List<PropertyValue>> byConcept;
  @Nonnull private final Set<String> propertyCodes;

  private PropertyIndex(
      @Nonnull final Map<Integer, List<PropertyValue>> byConcept,
      @Nonnull final Set<String> propertyCodes) {
    this.byConcept = byConcept;
    this.propertyCodes = propertyCodes;
  }

  /**
   * Loads the scalar property index for a code system version, tolerating the absence of the
   * property table.
   *
   * @param reader the store reader
   * @param systemVersionId the code system version to load
   * @return the loaded index
   */
  @Nonnull
  public static PropertyIndex load(
      @Nonnull final TerminologyStoreReader reader, @Nonnull final String systemVersionId) {
    final Map<Integer, List<PropertyValue>> byConcept = new HashMap<>();
    final Set<String> propertyCodes = new HashSet<>();
    reader.readTableIfPresent(
        PROPERTY,
        row -> {
          if (!systemVersionId.equals(row.getString(COLUMN_SYSTEM_VERSION_ID))) {
            return;
          }
          final String code = row.getString(COLUMN_PROPERTY_CODE);
          if (code == null) {
            return;
          }
          propertyCodes.add(code);
          byConcept
              .computeIfAbsent(row.getInt(COLUMN_CONCEPT_DENSE_ID), k -> new ArrayList<>())
              .add(
                  new PropertyValue(
                      code, row.getString(COLUMN_VALUE_TYPE), row.getString(COLUMN_VALUE)));
        });
    return new PropertyIndex(byConcept, propertyCodes);
  }

  /**
   * Returns the scalar properties of a concept.
   *
   * @param dense the dense identifier of the concept
   * @return the property values, empty if the concept has none
   */
  @Nonnull
  public List<PropertyValue> propertiesOf(final int dense) {
    return byConcept.getOrDefault(dense, List.of());
  }

  /**
   * Returns the set of property codes declared by any concept in this version.
   *
   * @return the declared property codes
   */
  @Nonnull
  public Set<String> propertyCodes() {
    return propertyCodes;
  }
}
