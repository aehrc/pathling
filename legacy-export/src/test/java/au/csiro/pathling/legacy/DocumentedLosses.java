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
package au.csiro.pathling.legacy;

import static au.csiro.pathling.legacy.LegacyTables.FHIR;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.model.api.TemporalPrecisionEnum;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseHasModifierExtensions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.DomainResource;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.Property;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;

/**
 * Applies to a source resource the losses the legacy export documents, giving the resource the
 * export and a re-import are expected to produce from it (SC-012).
 *
 * <p>Each loss is the one the module documentation lists, applied for the configuration the library
 * wrote by default: three levels of nesting, extensions enabled and the standard open types.
 */
final class DocumentedLosses {

  /** The scale of the legacy layout's decimal column. */
  private static final int DECIMAL_SCALE = 6;

  /** The number of digits the legacy layout's decimal column holds before the point. */
  private static final int DECIMAL_INTEGER_DIGITS = 26;

  /** The nesting level the library encoded by default. */
  private static final int NESTING_LEVEL = 3;

  @Nonnull private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  private DocumentedLosses() {}

  /**
   * Returns the document a source document is expected to come back as.
   *
   * @param document a FHIR JSON document
   * @return the document with every documented loss applied
   */
  @Nonnull
  static String apply(@Nonnull final String document) {
    final IBaseResource resource = FHIR.newJsonParser().parseResource(document);
    if (resource instanceof final DomainResource domainResource) {
      // Contained resources are not encoded.
      domainResource.getContained().clear();
    }
    truncate((Base) resource, new HashMap<>());
    final List<IBase> elements =
        FHIR.newTerser().getAllPopulatedChildElementsOfType(resource, IBase.class);
    elements.forEach(DocumentedLosses::applyTo);
    return FHIR.newJsonParser().encodeResourceToString(resource);
  }

  /**
   * An element nested within more than three elements of its own type is not encoded. Extensions
   * are exempt, because the legacy layout stores them apart from the elements they extend.
   *
   * @param element the element whose children are truncated
   * @param ancestors how many elements of each type enclose the children
   */
  private static void truncate(
      @Nonnull final Base element, @Nonnull final Map<Class<?>, Integer> ancestors) {
    for (final Property property : element.children()) {
      for (final Base child : List.copyOf(property.getValues())) {
        if (child instanceof Extension) {
          continue;
        }
        final Class<?> type = child.getClass();
        if (ancestors.getOrDefault(type, 0) > NESTING_LEVEL) {
          element.removeChild(property.getName(), child);
        } else {
          ancestors.merge(type, 1, Integer::sum);
          truncate(child, ancestors);
          ancestors.merge(type, -1, Integer::sum);
        }
      }
    }
  }

  private static void applyTo(@Nonnull final IBase element) {
    if (element instanceof final IBaseHasModifierExtensions modifiable) {
      // Modifier extensions are not encoded.
      modifiable.getModifierExtension().clear();
    }
    if (element instanceof final PrimitiveType<?> primitive) {
      // The id and extensions of a primitive are not encoded.
      primitive.setId(null);
      primitive.getExtension().clear();
    }
    if (element instanceof final Extension extension
        && extension.hasValue()
        && !FhirEncoders.STANDARD_OPEN_TYPES.contains(extension.getValue().fhirType())) {
      // An extension value of a type outside the open types is not encoded.
      extension.setValue(null);
    }
    if (element instanceof final ValueSetExpansionContainsComponent contains) {
      // The contains nested within a contains is not encoded.
      contains.getContains().clear();
    }
    if (element instanceof final DecimalType decimal && decimal.getValue() != null) {
      decimal.setValue(storedDecimal(decimal.getValue()));
    }
    if (element instanceof final InstantType instant && instant.getValue() != null) {
      // An instant is stored as a timestamp, and decoded in UTC to the millisecond.
      instant.setTimeZone(UTC);
      instant.setPrecision(TemporalPrecisionEnum.MILLI);
    }
  }

  /**
   * A decimal is stored at a scale of six, rounding half up, keeping its original scale up to six,
   * and is not stored at all where it has more than twenty-six digits before the point.
   */
  @Nullable
  private static BigDecimal storedDecimal(@Nonnull final BigDecimal value) {
    if (value.precision() - value.scale() > DECIMAL_INTEGER_DIGITS) {
      return null;
    }
    return value.scale() > DECIMAL_SCALE
        ? value.setScale(DECIMAL_SCALE, RoundingMode.HALF_UP)
        : value;
  }
}
