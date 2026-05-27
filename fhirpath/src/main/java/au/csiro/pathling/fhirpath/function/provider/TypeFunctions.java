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

package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.TypeInfo;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.mixed.ChoiceElementCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;

/**
 * Contains functions for type checking and type operations.
 *
 * @author Piotr Szul
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#types">FHIRPath Specification - Types</a>
 */
public class TypeFunctions {

  private TypeFunctions() {}

  /**
   * Returns true collection if the input collection contains a single item of the given type or a
   * subclass thereof. Returns false collection if the input contains a single item that is not of
   * the specified type. Returns empty if the input collection is empty. Throws an error if the
   * input collection contains more than one item.
   *
   * <p>The type argument is an identifier that must resolve to the name of a type in a model. For
   * implementations with compile-time typing, this requires special-case handling when processing
   * the argument to treat it as a type specifier rather than an identifier expression.
   *
   * @param input The input collection
   * @param typeSpecifier The type specifier
   * @return A boolean collection containing the result of type matching, or empty
   * @see <a href="https://hl7.org/fhirpath/#istype--type-specifier">is</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection is(
      @Nonnull final Collection input, @Nonnull final TypeSpecifier typeSpecifier) {
    return input.isOfType(typeSpecifier);
  }

  /**
   * Returns the value of the input collection if it contains a single item of the given type or a
   * subclass thereof. Returns empty collection if the input contains a single item that is not of
   * the specified type. Returns empty if the input collection is empty. Throws an error if the
   * input collection contains more than one item.
   *
   * <p>The type argument is an identifier that must resolve to the name of a type in a model. For
   * implementations with compile-time typing, this requires special-case handling when processing
   * the argument to treat it as a type specifier rather than an identifier expression.
   *
   * @param input The input collection
   * @param typeSpecifier The type specifier
   * @return The input value if type matches, or empty collection otherwise
   * @see <a href="https://hl7.org/fhirpath/#astype--type-specifier">as</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection as(
      @Nonnull final Collection input, @Nonnull final TypeSpecifier typeSpecifier) {
    return input.asType(typeSpecifier);
  }

  /**
   * Returns the type information for each element in the input collection, using concrete subtypes
   * of TypeInfo. The result is a collection of TypeInfo structures with {@code namespace}, {@code
   * name}, and {@code baseType} fields.
   *
   * <p>The namespace and name are determined by the origin of the collection:
   *
   * <ul>
   *   <li>FHIR model elements return {@code FHIR} namespace with the FHIR type code.
   *   <li>System literals return {@code System} namespace with the FHIRPath type specifier.
   *   <li>TypeInfo results return {@code System.Object}.
   *   <li>Choice collections return per-row TypeInfo based on which field is non-null.
   *   <li>Empty collections return empty.
   * </ul>
   *
   * @param input The input collection
   * @return A collection of TypeInfo structures, or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#reflection">FHIRPath Reflection</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection type(@Nonnull final Collection input) {
    // Choice collections require per-row type resolution based on which field is non-null.
    if (input instanceof final ChoiceElementCollection choice) {
      final UnaryOperator<Column> mapper =
          TypeInfo.choiceTypeInfoMapper(choice.getChoiceDefinition().getAllChildTypes());
      final Column mappedValue = choice.getParent().getColumn().transform(mapper).getValue();
      return Collection.buildWithDefinition(
          new DefaultRepresentation(mappedValue), TypeInfo.DEFINITION);
    }

    return TypeInfo.fromCollection(input)
        .map(
            typeInfo -> {
              // Transform each element to a TypeInfo struct element-wise, handling both singular
              // and plural (array) representations. Use transform (not map) so that array elements
              // are processed individually. Wrap in DefaultRepresentation so struct field access
              // works correctly for navigation (e.g., .namespace, .name, .baseType).
              final Column mappedValue =
                  input.getColumn().transform(col -> typeInfo.toStructColumn()).getValue();
              return Collection.buildWithDefinition(
                  new DefaultRepresentation(mappedValue), TypeInfo.DEFINITION);
            })
        .orElse(EmptyCollection.getInstance());
  }
}
