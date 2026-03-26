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

package au.csiro.pathling.fhirpath;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultCompositeDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultPrimitiveDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents type information returned by the FHIRPath {@code type()} reflection function. Each
 * instance describes a type with its namespace, name, and base type.
 *
 * @author Piotr Szul
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#reflection">FHIRPath Reflection</a>
 */
@Value
public class TypeInfo {

  /**
   * Definition for TypeInfo structures returned by the {@code type()} reflection function. This
   * defines three string children: {@code namespace}, {@code name}, and {@code baseType}.
   */
  @Nonnull
  public static final ElementDefinition DEFINITION =
      DefaultCompositeDefinition.of(
          "TypeInfo",
          List.of(
              DefaultPrimitiveDefinition.single("namespace", FHIRDefinedType.STRING),
              DefaultPrimitiveDefinition.single("name", FHIRDefinedType.STRING),
              DefaultPrimitiveDefinition.single("baseType", FHIRDefinedType.STRING)),
          1,
          FHIRDefinedType.BACKBONEELEMENT);

  private static final String FHIR_RESOURCE_BASE = TypeSpecifier.FHIR_NAMESPACE + ".Resource";
  private static final String FHIR_ELEMENT_BASE = TypeSpecifier.FHIR_NAMESPACE + ".Element";
  private static final String SYSTEM_ANY_BASE = TypeSpecifier.SYSTEM_NAMESPACE + ".Any";

  @Nonnull String namespace;
  @Nonnull String name;
  @Nonnull String baseType;

  /**
   * Creates a TypeInfo for a FHIR type.
   *
   * @param fhirType the FHIR defined type
   * @param isResource true if the type is a resource type
   * @return a TypeInfo with the FHIR namespace
   */
  @Nonnull
  public static TypeInfo forFhirType(
      @Nonnull final FHIRDefinedType fhirType, final boolean isResource) {
    return new TypeInfo(
        TypeSpecifier.FHIR_NAMESPACE,
        fhirType.toCode(),
        isResource ? FHIR_RESOURCE_BASE : FHIR_ELEMENT_BASE);
  }

  /**
   * Creates a TypeInfo for a System type.
   *
   * @param fhirPathType the FHIRPath type
   * @return a TypeInfo with the System namespace
   */
  @Nonnull
  public static TypeInfo forSystemType(@Nonnull final FhirPathType fhirPathType) {
    return new TypeInfo(
        TypeSpecifier.SYSTEM_NAMESPACE, fhirPathType.getTypeSpecifier(), SYSTEM_ANY_BASE);
  }

  /**
   * Creates a TypeInfo representing the type of a TypeInfo result itself ({@code System.Object}).
   *
   * @return a TypeInfo for System.Object
   */
  @Nonnull
  public static TypeInfo forTypeInfo() {
    return new TypeInfo(TypeSpecifier.SYSTEM_NAMESPACE, "Object", SYSTEM_ANY_BASE);
  }

  /**
   * Resolves the TypeInfo for a given collection based on its definition and type information.
   * Returns empty if the type cannot be determined (e.g., for choice or empty collections).
   *
   * @param input the input collection
   * @return the resolved TypeInfo, or empty if the type cannot be determined
   */
  @Nonnull
  public static Optional<TypeInfo> fromCollection(@Nonnull final Collection input) {
    if (input instanceof EmptyCollection) {
      return Optional.empty();
    }

    final Optional<? extends NodeDefinition> definition = input.getDefinition();

    // TypeInfo collections return System.Object.
    if (definition.filter(d -> d == DEFINITION).isPresent()) {
      return Optional.of(forTypeInfo());
    }

    // FHIR model definitions use the FHIR namespace.
    if (definition.filter(NodeDefinition::isFhirDefinition).isPresent()) {
      return input
          .getFhirType()
          .map(fhirType -> forFhirType(fhirType, input instanceof ResourceCollection));
    }

    // System types (literals and operation results).
    return input.getType().map(TypeInfo::forSystemType);
  }

  /**
   * Builds a Spark struct column representing this TypeInfo with {@code namespace}, {@code name},
   * and {@code baseType} fields.
   *
   * @return a Spark struct column
   */
  @Nonnull
  public Column toStructColumn() {
    return struct(
        lit(namespace).cast(DataTypes.StringType).as("namespace"),
        lit(name).cast(DataTypes.StringType).as("name"),
        lit(baseType).cast(DataTypes.StringType).as("baseType"));
  }
}
