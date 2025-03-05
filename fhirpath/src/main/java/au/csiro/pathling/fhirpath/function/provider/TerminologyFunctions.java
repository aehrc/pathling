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

package au.csiro.pathling.fhirpath.function.provider;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.fhirpath.Concepts;
import au.csiro.pathling.fhirpath.Concepts.Set;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.sql.udf.PropertyUdf;
import au.csiro.pathling.utilities.Functions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Functions for querying terminology from FHIRPath expressions.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@SuppressWarnings("unused")
public abstract class TerminologyFunctions {

  /**
   * When invoked on a {@code Coding}, returns the preferred display term, according to the
   * terminology server.
   * <p>
   * The optional {@code language} parameter can be used to specify the preferred language for the
   * display name.
   *
   * @param input The input collection of Codings
   * @param language The preferred language for the display name
   * @return A collection of display names
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#display">Pathling
   * documentation - display</a>
   * @see TerminologyConfiguration#getAcceptLanguage()
   */
  @FhirPathFunction
  @Nonnull
  public static StringCollection display(@Nonnull final CodingCollection input,
      @Nullable final StringCollection language) {

    return StringCollection.build(input.getColumn()
        .transformWithUdf("display", Optional.ofNullable(language)
            .map(StringCollection::getColumn)
            .map(ColumnRepresentation::singular)
            .orElse(DefaultRepresentation.empty()))
        .removeNulls()
    );
  }

  /**
   * When invoked on a {@code Coding}, returns any matching property values, using the specified
   * {@code name} and {@code type} parameters.
   * <p>
   * The {@code type} parameter has these possible values:
   * <ul>
   *     <li>string (default)</li>
   *     <li>code</li>
   *     <li>Coding</li>
   *     <li>integer</li>
   *     <li>boolean</li>
   *     <li>DateTime</li>
   * </ul>
   * Both the {@code code} and the {@code type} of the property must be present within a lookup response in order for it to be returned by this function. If there are no matches, the function will return an empty collection.
   * <p>
   * The optional {@code language} parameter can be used to specify the preferred language for the returned property values. It overrides the default value set in the configuration.
   *
   * @param input The input collection of Codings
   * @param code The name of the property to retrieve
   * @param type The type of the property to retrieve
   * @param language The preferred language for the property values
   * @return A collection of property values
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#property">Pathling
   * documentation - property</a>
   * @see TerminologyConfiguration#getAcceptLanguage()
   */
  @FhirPathFunction
  @Nonnull
  public static Collection property(@Nonnull final CodingCollection input,
      @Nonnull final StringCollection code,
      @Nullable final StringCollection type,
      @Nullable final StringCollection language) {

    final FHIRDefinedType propertyType = FHIRDefinedType.fromCode(Optional.ofNullable(type)
        .map(StringCollection::toLiteralValue)
        .orElse("string"));

    checkUserInput(PropertyUdf.ALLOWED_FHIR_TYPES.contains(propertyType),
        String.format("Invalid property type: %s", propertyType));

    final ColumnRepresentation resultCtx = input.getColumn()
        .transformWithUdf(PropertyUdf.getNameForType(propertyType),
            code.getColumn().singular(),
            Optional.ofNullable(language)
                .map(StringCollection::getColumn)
                .map(ColumnRepresentation::singular)
                .orElse(DefaultRepresentation.empty())
        ).flatten().removeNulls();

    return Collection.build(resultCtx, propertyType,
        input.getDefinition().flatMap(Functions.maybeCast(ElementDefinition.class))
            .filter(__ -> propertyType == FHIRDefinedType.CODING));
  }

  /**
   * When invoked on a collection of {@code Coding} elements, returns a collection of designation
   * values from the lookup operation. This can be used to retrieve synonyms, language translations
   * and more from the underlying terminology.
   * <p>
   * If the {@code use} parameter is specified, designation values are filtered to only those with a
   * matching use. If the {@code language} parameter is specified, designation values are filtered
   * to only those with a matching language. If both are specified, designation values must match
   * both the specified use and language.
   *
   * @param input The input collection of Codings
   * @param use The optional use parameter
   * @param language The optional language parameter
   * @return A collection of designation values
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#designation">Pathling
   * documentation - designation</a>
   * @see <a href="https://www.hl7.org/fhir/codesystem.html#designations">FHIR specification -
   * Display, Definition and Designations</a>
   */
  @FhirPathFunction
  @Nonnull
  public static StringCollection designation(@Nonnull final CodingCollection input,
      @Nullable final CodingCollection use,
      @Nullable final StringCollection language) {

    return StringCollection.build(input.getColumn()
        .transformWithUdf("designation",
            Optional.ofNullable(use)
                .map(CodingCollection::getColumn)
                .map(ColumnRepresentation::singular)
                .orElse(DefaultRepresentation.empty()),
            Optional.ofNullable(language)
                .map(StringCollection::getColumn)
                .map(ColumnRepresentation::singular)
                .orElse(DefaultRepresentation.empty())
        )
        .flatten().removeNulls()
    );
  }

  /**
   * This function can be invoked on a collection of {@code Coding} or {@code CodeableConcept}
   * values, returning a collection of {@code Boolean} values based on whether each concept is a
   * member of the ValueSet with the specified url.
   * <p>
   * For a {@code CodeableConcept}, the function will return true if any of the codings are members
   * of the value set.
   *
   * @param input The input collection of Codings or CodeableConcepts
   * @param valueSetURL The URL of the ValueSet to check membership against
   * @return A collection of boolean values
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#memberof">Pathling
   * documentation - memberOf</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection memberOf(@Nonnull final Concepts input,
      @Nonnull final StringCollection valueSetURL) {
    return BooleanCollection.build(
        input.apply("member_of", valueSetURL.getColumn().singular())
    );
  }

  /**
   * This function takes a collection of {@code Coding} or {@code CodeableConcept} elements as
   * input, and another collection as the argument. The result is a collection with a
   * {@code Boolean} value for each source concept, each value being {@code true} if the concept
   * subsumes any of the concepts within the argument collection, and {@code false} otherwise.
   *
   * @param input The input collection of Codings or CodeableConcepts
   * @param codes The collection of Codings or CodeableConcepts to check against
   * @return A collection of boolean values
   * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">FHIR specification - Additional
   * functions</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection subsumes(@Nonnull final Concepts input,
      @Nonnull final Concepts codes) {

    return BooleanCollection.build(
        input.apply("subsumes", codes.flatten().getCodings(),
            DefaultRepresentation.literal(false))
    );
  }

  /**
   * This is the inverse of the {@link #subsumes} function, examining whether each input concept is
   * subsumed by any of the argument concepts.
   *
   * @param input The input collection of Codings or CodeableConcepts
   * @param codes The collection of Codings or CodeableConcepts to check against
   * @return A collection of boolean values
   * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">FHIR specification - Additional
   * functions</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection subsumedBy(@Nonnull final Concepts input,
      @Nonnull final Concepts codes) {
    return BooleanCollection.build(
        input.apply("subsumes", codes.flatten().getCodings(),
            DefaultRepresentation.literal(true))
    );
  }

  /**
   * When invoked on a {@code Coding}, returns any matching concepts using the ConceptMap specified
   * using {@code conceptMapUrl}.
   * <p>
   * The {@code reverse} parameter controls the direction to traverse the map - {@code false}
   * results in "source to target" mappings, while {@code true} results in "target to source".
   * <p>
   * The {@code equivalence} parameter is a comma-delimited set of values from the
   * {@code ConceptMapEquivalence} ValueSet, and is used to filter the mappings returned to only
   * those that have an equivalence value in this list.
   * <p>
   * The {@code target} parameter identifies the value set in which a translation is sought — a
   * scope for the translation.
   *
   * @param input The input collection of Codings
   * @param conceptMapUrl The URL of the ConceptMap to use
   * @param reverse The optional reverse parameter
   * @param equivalence The optional equivalence parameter
   * @param target The optional target parameter
   * @return A collection of Codings
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#translate">Pathling
   * documentation - translate</a>
   */
  @FhirPathFunction
  @Nonnull
  public static CodingCollection translate(@Nonnull final Concepts input,
      @Nonnull final StringCollection conceptMapUrl,
      @Nullable final BooleanCollection reverse, @Nullable final StringCollection equivalence,
      @Nullable final StringCollection target) {

    final Set codings = input.flatten();
    return (CodingCollection) codings.getCodingTemplate().copyWith(
        codings.getCodings().callUdf("translate_coding",
            conceptMapUrl.getColumn().singular(),
            Optional.ofNullable(reverse).map(BooleanCollection::getColumn)
                .map(ColumnRepresentation::singular)
                .orElse(DefaultRepresentation.literal(false)),
            Optional.ofNullable(equivalence).map(StringCollection::getColumn).map(
                    ColumnRepresentation::singular)
                .orElse(DefaultRepresentation.literal("equivalent"))
                .transform(c -> functions.split(c, ",")),
            Optional.ofNullable(target).map(StringCollection::getColumn)
                .map(ColumnRepresentation::singular)
                .orElse(DefaultRepresentation.empty())
        ));
  }

}
