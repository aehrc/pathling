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
package au.csiro.pathling.definition;

import jakarta.annotation.Nonnull;
import lombok.Value;

/**
 * The type of an element, identified by its FHIR type code.
 *
 * <p>A type is represented by its code rather than by a value of a version-specific enumeration, so
 * that a type code the enumeration of any particular version does not contain is still
 * representable.
 */
@Value(staticConstructor = "of")
public class FhirType {

  /** The boolean primitive type. */
  @Nonnull public static final FhirType BOOLEAN = new FhirType("boolean");

  /** The string primitive type. */
  @Nonnull public static final FhirType STRING = new FhirType("string");

  /** The integer primitive type. */
  @Nonnull public static final FhirType INTEGER = new FhirType("integer");

  /** The decimal primitive type. */
  @Nonnull public static final FhirType DECIMAL = new FhirType("decimal");

  /** The date primitive type. */
  @Nonnull public static final FhirType DATE = new FhirType("date");

  /** The dateTime primitive type. */
  @Nonnull public static final FhirType DATETIME = new FhirType("dateTime");

  /** The time primitive type. */
  @Nonnull public static final FhirType TIME = new FhirType("time");

  /** The instant primitive type. */
  @Nonnull public static final FhirType INSTANT = new FhirType("instant");

  /** The code primitive type. */
  @Nonnull public static final FhirType CODE = new FhirType("code");

  /** The uri primitive type. */
  @Nonnull public static final FhirType URI = new FhirType("uri");

  /** The url primitive type. */
  @Nonnull public static final FhirType URL = new FhirType("url");

  /** The canonical primitive type. */
  @Nonnull public static final FhirType CANONICAL = new FhirType("canonical");

  /** The oid primitive type. */
  @Nonnull public static final FhirType OID = new FhirType("oid");

  /** The id primitive type. */
  @Nonnull public static final FhirType ID = new FhirType("id");

  /** The uuid primitive type. */
  @Nonnull public static final FhirType UUID = new FhirType("uuid");

  /** The markdown primitive type. */
  @Nonnull public static final FhirType MARKDOWN = new FhirType("markdown");

  /** The base64Binary primitive type. */
  @Nonnull public static final FhirType BASE64BINARY = new FhirType("base64Binary");

  /** The unsignedInt primitive type. */
  @Nonnull public static final FhirType UNSIGNEDINT = new FhirType("unsignedInt");

  /** The positiveInt primitive type. */
  @Nonnull public static final FhirType POSITIVEINT = new FhirType("positiveInt");

  /** The Coding complex type. */
  @Nonnull public static final FhirType CODING = new FhirType("Coding");

  /** The CodeableConcept complex type. */
  @Nonnull public static final FhirType CODEABLECONCEPT = new FhirType("CodeableConcept");

  /** The Quantity complex type. */
  @Nonnull public static final FhirType QUANTITY = new FhirType("Quantity");

  /** The Reference complex type. */
  @Nonnull public static final FhirType REFERENCE = new FhirType("Reference");

  /** The Extension complex type. */
  @Nonnull public static final FhirType EXTENSION = new FhirType("Extension");

  /** The Identifier complex type. */
  @Nonnull public static final FhirType IDENTIFIER = new FhirType("Identifier");

  /** The Period complex type. */
  @Nonnull public static final FhirType PERIOD = new FhirType("Period");

  /** The HumanName complex type. */
  @Nonnull public static final FhirType HUMANNAME = new FhirType("HumanName");

  /** The Address complex type. */
  @Nonnull public static final FhirType ADDRESS = new FhirType("Address");

  /** The ContactPoint complex type. */
  @Nonnull public static final FhirType CONTACTPOINT = new FhirType("ContactPoint");

  /** The BackboneElement type, which every backbone element within a resource specialises. */
  @Nonnull public static final FhirType BACKBONEELEMENT = new FhirType("BackboneElement");

  /**
   * The absence of a type, for a value that carries no FHIR type at all. It mirrors the sentinel
   * that the FHIR type enumerations carry for the same purpose.
   */
  @Nonnull public static final FhirType NULL = new FhirType("null");

  @Nonnull String code;

  /**
   * Returns the code that identifies this type.
   *
   * @return the type code
   */
  @Nonnull
  public String toCode() {
    return code;
  }

  @Override
  @Nonnull
  public String toString() {
    return code;
  }
}
