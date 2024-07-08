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

package au.csiro.pathling.sql;

import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.toLiteralColumn;
import static au.csiro.pathling.utilities.Preconditions.wrapInUserInputError;
import static java.util.Objects.nonNull;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.sql.udf.DesignationUdf;
import au.csiro.pathling.sql.udf.DisplayUdf;
import au.csiro.pathling.sql.udf.MemberOfUdf;
import au.csiro.pathling.sql.udf.PropertyUdf;
import au.csiro.pathling.sql.udf.SubsumesUdf;
import au.csiro.pathling.sql.udf.TranslateUdf;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;

/**
 * Provides access to the FHIR terminology UDFs, including overloads where this makes it more
 * convenient to use them.
 */
public interface Terminology {

  /**
   * Takes a Coding or an array of Codings column as its input. Returns the column which, contains a
   * Boolean value, indicating whether any of the input Codings is the member of the specified FHIR
   * ValueSet.
   *
   * @param coding a Column containing the struct representation of a Coding or an array of such
   * structs.
   * @param valueSetUrl the identifier for a FHIR ValueSet.
   * @return the Column containing the result of the operation
   */
  @Nonnull
  static Column member_of(@Nonnull final Column coding, @Nonnull final String valueSetUrl) {
    return member_of(coding, lit(valueSetUrl));
  }

  /**
   * Takes a Coding or an array of Codings column as its input. Returns the column which, contains a
   * Boolean value, indicating whether any of the input Codings is the member of the specified FHIR
   * ValueSet.
   *
   * @param coding a Column containing the struct representation of a Coding or an array of such
   * structs.
   * @param valueSetUrl the column with the identifier for a FHIR ValueSet.
   * @return the Column containing the result of the operation
   */
  @Nonnull
  static Column member_of(@Nonnull final Column coding, @Nonnull final Column valueSetUrl) {
    return call_udf(MemberOfUdf.FUNCTION_NAME, coding, valueSetUrl);
  }

  // TODO: consider the order of target and equivalences
  // TODO: consider other forms of passing equivalences (i.e collection of enum types)
  // TODO: add overloaded methods for default arguments.

  /**
   * Takes a Coding or an array of Codings column as its input.  Returns the Column which contains
   * an array of Coding value with translation targets from the specified FHIR ConceptMap. There may
   * be more than one target concept for each input concept.
   *
   * @param coding a Column containing the struct representation of a Coding or an array of such
   * structs.
   * @param conceptMapUri an identifier for a FHIR ConceptMap.
   * @param reverse the direction to traverse the map - false results in "source to target"
   * mappings, while true results in "target to source".
   * @param equivalences a collection of translation equivalences (ConceptMapEquivalence) to include
   * it the result.
   * @param target identifies the value set in which a translation is sought.  If there's no target
   * specified, the server should return all known translations.
   * @return the Column containing the result of the operation (an array of Coding structs).
   */
  @Nonnull
  static Column translate(@Nonnull final Column coding, @Nonnull final String conceptMapUri,
      final boolean reverse, @Nullable final Collection<ConceptMapEquivalence> equivalences,
      @Nullable final String target) {
    return call_udf(TranslateUdf.FUNCTION_NAME, coding, lit(conceptMapUri),
        lit(reverse),
        nonNull(equivalences)
        ? array(
            equivalences.stream().distinct().map(ConceptMapEquivalence::toCode).map(functions::lit)
                .toArray(Column[]::new))
        : lit(null),
        lit(target));
  }

  /**
   * Translates a coding using a concept map.
   *
   * @param coding a Column containing the struct representation of a Coding, or an array of such
   * structs
   * @param conceptMapUri an identifier for a FHIR ConceptMap
   * @param reverse the direction to traverse the map - false results in "source to target"
   * mappings, while true results in "target to source"
   * @param equivalences a collection of translation equivalences (ConceptMapEquivalence) to include
   * in the result
   * @return the Column containing the result of the operation (an array of Coding structs)
   * @see Terminology#translate(Column, String, boolean, Collection, String)
   */
  @Nonnull
  static Column translate(@Nonnull final Column coding, @Nonnull final String conceptMapUri,
      final boolean reverse, @Nullable final Collection<ConceptMapEquivalence> equivalences) {
    return translate(coding, conceptMapUri, reverse, equivalences, null);
  }

  /**
   * Takes two Coding or array of Codings columns as its input. Returns the Column, which contains a
   * Boolean value, indicating whether the left Coding subsumes the right Coding.
   *
   * @param codingA a Column containing a struct representation of a Coding or an array of Codings.
   * @param codingB a Column containing a struct representation of a Coding or an array of *
   * Codings.
   * @return the Column containing the result of the operation (boolean)
   */
  @Nonnull
  static Column subsumes(@Nonnull final Column codingA, @Nonnull final Column codingB) {
    return call_udf(SubsumesUdf.FUNCTION_NAME, codingA, codingB, lit(false));
  }

  /**
   * Takes two Coding or array of Codings columns as its input. Returns the Column, which contains a
   * Boolean value, indicating whether the left Coding is subsumed by the right Coding.
   *
   * @param codingA a Column containing a struct representation of a Coding or an array of Codings.
   * @param codingB a Column containing a struct representation of a Coding or an array of Codings.
   * @return the Column containing the result of the operation (boolean)
   */
  @Nonnull
  static Column subsumed_by(@Nonnull final Column codingA, @Nonnull final Column codingB) {
    return call_udf(SubsumesUdf.FUNCTION_NAME, codingA, codingB, lit(true));
  }

  /**
   * Takes a Coding column as its input. Returns the Column, which contains the canonical display
   * name associated with the given code.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @return the Column containing the result of the operation (String)
   */
  @Nonnull
  static Column display(@Nonnull final Column coding) {
    return display(coding, null);
  }


  /**
   * Takes a Coding column as its input. Returns the Column, which contains the canonical display
   * name associated with the given code in preferred language when specified.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @param acceptLanguage the optional language preferences for the returned display name.
   * Overrides the value of {@link TerminologyConfiguration#getAcceptLanguage()}.
   * @return the Column containing the result of the operation (String)
   */
  @Nonnull
  static Column display(@Nonnull final Column coding, @Nullable final String acceptLanguage) {
    return call_udf(DisplayUdf.FUNCTION_NAME, coding, lit(acceptLanguage));
  }

  /**
   * Takes a Coding column as its input. Returns the Column, which contains the values of properties
   * for this coding with specified names and types. The type of the result column depends on the
   * types of the properties. Primitive FHIR types are mapped to their corresponding SQL primitives.
   * Complex types are mapped to their corresponding structs. The allowed property types are: code |
   * Coding | string | integer | boolean | dateTime | decimal.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @param propertyCode the code of the property to retrieve
   * @param propertyType the type of the property to retrieve
   * @param acceptLanguage the optional language preferences for the returned property values.
   * Overrides the value of {@link TerminologyConfiguration#getAcceptLanguage()}.
   * @return the Column containing the result of the operation (array of property values)
   */
  @Nonnull
  static Column property_of(@Nonnull final Column coding, @Nonnull final String propertyCode,
      @Nonnull final FHIRDefinedType propertyType, @Nullable final String acceptLanguage) {
    return call_udf(PropertyUdf.getNameForType(propertyType), coding, lit(propertyCode),
        lit(acceptLanguage));
  }

  /**
   * Retrieves properties of a concept.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @param propertyCode the code of the property to retrieve
   * @param propertyType the FHIR data type of the property
   * @param acceptLanguage the optional language preferences for the returned property values.
   * Overrides the value of {@link TerminologyConfiguration#getAcceptLanguage()}.
   * @return the Column containing the result of the operation (array of property values)
   * @see Terminology#property_of(Column, String, FHIRDefinedType)
   */
  @Nonnull
  static Column property_of(@Nonnull final Column coding, @Nonnull final String propertyCode,
      @Nullable final String propertyType, @Nullable final String acceptLanguage) {

    return property_of(coding, propertyCode,
        nonNull(propertyType)
        ? wrapInUserInputError(FHIRDefinedType::fromCode).apply(propertyType)
        : PropertyUdf.DEFAULT_PROPERTY_TYPE,
        acceptLanguage);
  }

  /**
   * Takes a Coding column as its input. Returns the Column, which contains the values of properties
   * for this coding with specified names and types. The type of the result column depends on the
   * types of the properties. Primitive FHIR types are mapped to their corresponding SQL primitives.
   * Complex types are mapped to their corresponding structs. The allowed property types are: code |
   * Coding | string | integer | boolean | dateTime | decimal.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @param propertyCode the code of the property to retrieve.
   * @param propertyType the type of the property to retrieve.
   * @return the Column containing the result of the operation (array of property values)
   */
  @Nonnull
  static Column property_of(@Nonnull final Column coding, @Nonnull final String propertyCode,
      @Nonnull final FHIRDefinedType propertyType) {
    return property_of(coding, propertyCode, propertyType, null);
  }

  /**
   * Retrieves properties of a concept.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @param propertyCode the code of the property to retrieve
   * @param propertyType the FHIR data type of the property
   * @return the Column containing the result of the operation (array of property values)
   * @see Terminology#property_of(Column, String, FHIRDefinedType)
   */
  @Nonnull
  static Column property_of(@Nonnull final Column coding, @Nonnull final String propertyCode,
      @Nullable final String propertyType) {
    return property_of(coding, propertyCode, propertyType, null);
  }

  /**
   * Retrieves properties of a concept.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @param propertyCode the code of the property to retrieve
   * @return the Column containing the result of the operation (array of property values)
   * @see Terminology#property_of(Column, String, FHIRDefinedType)
   */
  @Nonnull
  static Column property_of(@Nonnull final Column coding, @Nonnull final String propertyCode) {
    return property_of(coding, propertyCode, PropertyUdf.DEFAULT_PROPERTY_TYPE);
  }


  /**
   * Takes a Coding column as its input. Returns the Column, which contains the values of
   * designations (strings) for this coding for the specified use and language. If the language is
   * not provided (is null) then all designations with the specified type are returned regardless of
   * their language.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @param use a Column containing use the code with the use of the designations
   * @param language the language of the designations
   * @return the Column containing the result of the operation (array of strings with designation
   * values)
   */
  @Nonnull
  static Column designation(@Nonnull final Column coding, @Nonnull final Column use,
      @Nullable final String language) {
    return call_udf(DesignationUdf.FUNCTION_NAME, coding, use, lit(language));
  }

  /**
   * Retrieves designations of a concept.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @param use a Column containing a Coding describing the use of the requested designation
   * @param language the language of the requested designation
   * @return the Column containing the result of the operation (array of strings with designation
   * values)
   * @see Terminology#designation(Column, Column, String)
   */
  @Nonnull
  static Column designation(@Nonnull final Column coding, @Nullable final Coding use,
      @Nullable final String language) {
    return designation(coding, toLiteralColumn(use), language);
  }

  /**
   * Retrieves designations of a concept for all languages.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @param use a Column containing a Coding describing the use of the requested designation
   * @return the Column containing the result of the operation (array of strings with designation
   * values)
   * @see Terminology#designation(Column, Coding, String)
   */
  @Nonnull
  static Column designation(@Nonnull final Column coding, @Nullable final Coding use) {
    return designation(coding, use, null);
  }

  /**
   * Retrieves designations of a concept for all languages and uses.
   *
   * @param coding a Column containing a struct representation of a Coding
   * @return the Column containing the result of the operation (array of strings with designation
   * values)
   * @see Terminology#designation(Column, Coding, String)
   */
  @Nonnull
  static Column designation(@Nonnull final Column coding) {
    return designation(coding, null);
  }
}
