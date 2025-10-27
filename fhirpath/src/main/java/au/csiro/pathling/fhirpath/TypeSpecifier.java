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

package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.Value;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a FHIRPath type specifier, which is a namespace and a type name.
 *
 * @author John Grimes
 */
@Value
public class TypeSpecifier {

  /**
   * The namespace identifier for System types.
   */
  public static final String SYSTEM_NAMESPACE = "System";

  /**
   * The namespace identifier for FHIR types.
   */
  public static final String FHIR_NAMESPACE = "FHIR";
  private static final List<String> NAMESPACE_SEARCH_ORDER = List.of(FHIR_NAMESPACE,
      SYSTEM_NAMESPACE);
  private static final Map<String, Predicate<String>> NAMESPACE_VALIDATORS = Map.of(
      FHIR_NAMESPACE, TypeSpecifier::isValidFhirType,
      SYSTEM_NAMESPACE, TypeSpecifier::isValidSystemType
  );

  @Nonnull
  String namespace;

  @Nonnull
  String typeName;

  /**
   * Creates a new type specifier.
   *
   * @param namespace The namespace
   * @param typeName The type name
   * @throws IllegalArgumentException if the namespace or type name is invalid
   */
  public TypeSpecifier(@Nonnull final String namespace, @Nonnull final String typeName)
      throws IllegalArgumentException {
    this.namespace = validateNamespace(namespace);
    this.typeName = validateTypeName(typeName, namespace);
  }

  /**
   * Creates a new type specifier with the namespace inferred from the type name.
   *
   * @param typeName The type name
   * @throws IllegalArgumentException if the type name is invalid
   */
  public TypeSpecifier(@Nonnull final String typeName) throws IllegalArgumentException {
    this.namespace = searchForTypeName(typeName);
    this.typeName = typeName;
  }

  /**
   * @return true if this type specifier is a type in FHIR namespace.
   */
  public boolean isFhirType() {
    return namespace.equals(FHIR_NAMESPACE);
  }


  /**
   * @return true if this type specifier is a type in System namespace.
   */
  public boolean isSystemType() {
    return SYSTEM_NAMESPACE.equals(namespace);
  }


  /**
   * Converts this type specifier to a System FHIRPath type.
   *
   * @return the System FHIRPath type
   * @throws IllegalStateException if this type specifier is not a System type
   */
  @Nonnull
  public FhirPathType toSystemType() {
    if (!isSystemType()) {
      throw new IllegalStateException("Not a System type: " + this);
    }
    return FhirPathType.valueOf(typeName.toUpperCase());
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

  /**
   * @return The FHIR resource type represented by this type specifier if a valid FHIR resource
   * type.
   */
  @Nonnull
  public Optional<ResourceType> asResourceType() {
    try {
      if (isFhirType()) {
        return Optional.of(ResourceType.fromCode(typeName));
      }
    } catch (final FHIRException ignored) {
      // If the typeName is not a valid ResourceType, we ignore the exception and return empty.
    }
    return Optional.empty();
  }

  private static String validateNamespace(final String namespace) throws IllegalArgumentException {
    if (!NAMESPACE_VALIDATORS.containsKey(namespace)) {
      throw new IllegalArgumentException("Invalid namespace: " + namespace);
    }
    return namespace;
  }

  private static String validateTypeName(final String typeName, final String namespace)
      throws IllegalArgumentException {
    if (!NAMESPACE_VALIDATORS.get(namespace).test(typeName)) {
      throw new IllegalArgumentException("Invalid type name: " + typeName);
    }
    return typeName;
  }

  private static String searchForTypeName(final String typeName) {
    for (final String namespace : NAMESPACE_SEARCH_ORDER) {
      if (NAMESPACE_VALIDATORS.get(namespace).test(typeName)) {
        return namespace;
      }
    }
    throw new IllegalArgumentException("Invalid type name: " + typeName);
  }

  private static boolean isValidFhirType(final String typeName) {
    try {
      return FHIRDefinedType.fromCode(typeName) != null;
    } catch (final FHIRException e) {
      return false;
    }
  }

  private static boolean isValidSystemType(final String typeName) {
    return FhirPathType.isValidFhirPathType(typeName);
  }

  @Override
  public String toString() {
    return namespace + "." + typeName;
  }

}
