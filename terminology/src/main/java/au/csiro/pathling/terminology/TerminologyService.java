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

package au.csiro.pathling.terminology;

import au.csiro.pathling.fhir.ParametersUtils.DesignationPart;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Value;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;

/**
 * Abstraction layer for the terminology related operations.
 *
 * @author Piotr Szul
 */
public interface TerminologyService {

  /**
   * Represent a single translation of a code.
   */
  @Value(staticConstructor = "of")
  class Translation implements Serializable {

    @Serial
    private static final long serialVersionUID = -7551505530196865478L;

    /**
     * The equivalence relationship for this translation.
     */
    @Nonnull
    ConceptMapEquivalence equivalence;

    /**
     * The translated concept.
     */
    @Nonnull
    Coding concept;

    @Override
    public boolean equals(final Object o) {
      // We override this method because Coding does not have a sane equals method.
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Translation that = (Translation) o;
      return equivalence == that.equivalence && ImmutableCoding.of(concept)
          .equals(ImmutableCoding.of(that.concept));
    }

    @Override
    public int hashCode() {
      // We override this method because Coding does not have a sane hashCode method.
      return Objects.hash(equivalence, ImmutableCoding.of(concept));
    }

  }

  /**
   * Validates that a coded value is in a value set. Abstracts the FHIR <a
   * href="https://www.hl7.org/fhir/R4/valueset-operation-validate-code.html">ValueSet/$validate-code</a>
   * operation.
   *
   * @param valueSetUrl the URL of the value set to validate against
   * @param coding the coding to test
   * @return true if the coding is a valid member of the value set
   */
  boolean validateCode(@Nonnull String valueSetUrl, @Nonnull Coding coding);

  /**
   * Translates a code from one value set to another, based on the existing concept map. Abstracts
   * the FHIR <a
   * href="https://www.hl7.org/fhir/R4/operation-conceptmap-translate.html">ConceptMap/$translate</a>
   * operation.
   *
   * @param coding the code to translate.
   * @param conceptMapUrl the url of the concept map to use for translation.
   * @param reverse if this is true, then the operation should return all the codes that might be
   * mapped to this code.
   * @param target identifies the value set in which a translation is sought. If null all known
   * translations are returned.
   * @return the list of translations.
   */
  @Nonnull
  List<Translation> translate(@Nonnull Coding coding,
      @Nonnull String conceptMapUrl,
      boolean reverse,
      @Nullable String target);

  /**
   * Tests the subsumption relationship between two codings given the semantics of subsumption in
   * the underlying code system. Abstracts the <a
   * href="https://www.hl7.org/fhir/R4/codesystem-operation-subsumes.html">CodeSystem/$subsumes</a>
   * operation.
   *
   * @param codingA the left code to be tested.
   * @param codingB the right code to be tested.
   * @return {@link ConceptSubsumptionOutcome} representing the relation between codingA (left code)
   * and codingB (right code).
   */
  @Nonnull
  ConceptSubsumptionOutcome subsumes(@Nonnull Coding codingA, @Nonnull Coding codingB);

  /**
   * Gets additional details about the concept, including designations and properties. Abstracts
   * the
   * <a href="https://www.hl7.org/fhir/R4/codesystem-operation-lookup.html">CodeSystem/$lookup</a>
   * operation.
   *
   * @param coding the coding to lookup.
   * @param propertyCode the code of the propertyCode to lookup. If not null only the properties
   * with matching codes are returned.
   * @param acceptLanguage the preferred language for display and other localised properties.
   * @return the list of properties and/or designations.
   */
  @Nonnull
  List<PropertyOrDesignation> lookup(@Nonnull Coding coding, @Nullable String propertyCode,
      @Nullable String acceptLanguage);

  /**
   * Gets additional details about the concept, including designations and properties. Abstracts
   * the
   * <a href="https://www.hl7.org/fhir/R4/codesystem-operation-lookup.html">CodeSystem/$lookup</a>
   * operation.
   *
   * @param coding the coding to lookup.
   * @param propertyCode the code of the propertyCode to lookup. If not null only the properties
   * with matching codes are returned.
   * @return the list of properties and/or designations.
   */
  @Nonnull
  default List<PropertyOrDesignation> lookup(@Nonnull final Coding coding,
      @Nullable final String propertyCode) {
    return lookup(coding, propertyCode, null);
  }

  /**
   * Common interface for properties and designations
   */
  interface PropertyOrDesignation extends Serializable {
    // marker interface
  }

  /**
   * The representation of the property of a concept.
   */
  @Value(staticConstructor = "of")
  class Property implements PropertyOrDesignation {

    @Serial
    private static final long serialVersionUID = 8827691056493768863L;

    /**
     * The property code.
     */
    @Nonnull
    String code;

    /**
     * The property value.
     */
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

      final Property property = (Property) o;

      if (!code.equals(property.code)) {
        return false;
      }
      return value.equalsDeep(property.value);
    }

  }

  /**
   * The representation of a designation of a concept.
   */
  @Value(staticConstructor = "of")
  class Designation implements PropertyOrDesignation {

    @Serial
    private static final long serialVersionUID = -809107979219801186L;

    /**
     * The code of the designation properties
     */
    public static final String PROPERTY_CODE = "designation";

    /**
     * The use code for this designation.
     */
    @Nullable
    Coding use;

    /**
     * The language code for this designation.
     */
    @Nullable
    String language;

    /**
     * The display value of this designation.
     */
    @Nonnull
    String value;

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

      final Designation that = (Designation) o;

      if (use != null
          ? !use.equalsDeep(that.use)
          : that.use != null) {
        return false;
      }
      if (!Objects.equals(language, that.language)) {
        return false;
      }
      return value.equals(that.value);
    }

    /**
     * Creates a Designation from a DesignationPart.
     *
     * @param part the designation part to convert
     * @return a new Designation instance
     */
    @Nonnull
    public static Designation ofPart(@Nonnull final DesignationPart part) {
      return of(part.getUse(),
          Optional.ofNullable(part.getLanguage()).map(StringType::getValue).orElse(null),
          part.getValue().getValue());
    }
  }

}
