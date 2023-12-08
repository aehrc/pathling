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

package au.csiro.pathling.fhirpath;

import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a FHIRPath type specifier, which is a namespace and a type name.
 */
@Value
@AllArgsConstructor
public class TypeSpecifier {

  public static final String FHIR_NAMESPACE = "FHIR";
  public static final String DEFAULT_NAMESPACE = FHIR_NAMESPACE;

  @Nonnull
  String namespace;

  @Nonnull
  String typeName;

  public TypeSpecifier(@Nonnull final String typeName) {
    this(DEFAULT_NAMESPACE, typeName);
  }

  /**
   * @return true if this type specifier is a type in FHIR namespace.
   */
  boolean isFhirType() {
    return namespace.equals(FHIR_NAMESPACE);
  }


  /**
   * Returns a copy of this type specifier with the new namespace.
   *
   * @param namespace the new namespace
   * @return the type specifier with different namespace
   */
  @Nonnull
  public TypeSpecifier withNamespace(@Nonnull final String namespace) {
    return new TypeSpecifier(namespace, typeName);
  }

  /**
   * Converts this type specifier to a FHIR type.
   *
   * @return the FHIR type
   * @throws IllegalStateException if this type specifier is not a FHIR type
   */
  @Nonnull
  public FHIRDefinedType toFhirType() {
    if (!isFhirType()) {
      throw new IllegalStateException("Not a FHIR type: " + this);
    }
    return FHIRDefinedType.fromCode(typeName);
  }
  
  @Nonnull
  public ResourceType toResourceType() {
    if (!isFhirType()) {
      throw new IllegalStateException("Not a FHIR type: " + this);
    }
    return ResourceType.fromCode(typeName);
  }

  /**
   * Creates a type specifier from a FHRI type.
   *
   * @return the type specifier
   */
  public static TypeSpecifier fromFhirType(@Nonnull final FHIRDefinedType type) {
    return new TypeSpecifier(FHIR_NAMESPACE, type.toCode());
  }

  /**
   * Parses a type specifier from a string.
   *
   * @return the parsed type specifier
   */
  public static TypeSpecifier fromString(@Nonnull final String typeSpecifier) {
    final String[] parts = typeSpecifier.split("\\.");
    if (parts.length == 1) {
      return new TypeSpecifier(DEFAULT_NAMESPACE, parts[0]);
    } else if (parts.length == 2) {
      return new TypeSpecifier(parts[0], parts[1]);
    } else {
      throw new IllegalArgumentException("Invalid type specifier: " + typeSpecifier);
    }
  }
}
