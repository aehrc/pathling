/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology;

import lombok.Value;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public interface TerminologyService2 {

  @Value(staticConstructor = "of")
  class Translation {

    @Nonnull
    ConceptMapEquivalence equivalence;

    @Nonnull
    Coding concept;
  }


  boolean validate(@Nonnull String url, @Nonnull Coding coding);

  @Nonnull
  List<Translation> translate(@Nonnull Coding coding,
      @Nonnull String conceptMapUrl,
      boolean reverse,
      @Nullable String target);

  @Nonnull
  ConceptSubsumptionOutcome subsumes(@Nonnull Coding codingA, @Nonnull Coding codingB);


  /**
   * Common interface for properties and designations
   */
  interface PropertyOrDesignation {
    // marker interface
  }


  /**
   * The representation of the property of a concept.
   */
  @Value(staticConstructor = "of")
  class Property implements PropertyOrDesignation {

    @Nonnull
    String code;
    @Nonnull
    Type value;

    /**
     * Gets the string representation of the property value.
     *
     * @return the string representation of the property value
     */
    @Nonnull
    public String getValueAsString() {
      return value.primitiveValue();
    }

    @Override
    public int hashCode() {
      // not supported for now because it's not possible to satisfy the hashCode/equals contract
      // without some form of deepHash corresponding to equalsDeep()
      throw new UnsupportedOperationException("hashCode not implemented.");
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Property property = (Property) o;

      if (!code.equals(property.code)) {
        return false;
      }
      return value.equalsDeep(property.value);
    }

  }

  /**
   * Looks up the properties and designations of given coding in the terminology server.
   *
   * @param coding the coding to lookup.
   * @param property the code of the property to lookup. If not null only the properties with
   * matching names are returned.
   * @param displayLanguage the language to use for narrowing down designations.
   * @return the list of properties and/or designations.
   */
  @Nonnull
  List<PropertyOrDesignation> lookup(@Nonnull Coding coding, @Nullable String property,
      @Nullable String displayLanguage);

}
