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

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.column.EmptyRepresentation;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.validation.FhirPathFunction;
import au.csiro.pathling.sql.udf.PropertyUdf;
import au.csiro.pathling.utilities.Functions;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

@SuppressWarnings("unused")
public abstract class TerminologyFunctions {


  /**
   * This function returns the display name for given Coding
   *
   * @author Piotr Szul
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#display">display</a>
   */
  @FhirPathFunction
  public static StringCollection display(@Nonnull final CodingCollection input,
      @Nullable final StringCollection language) {

    return StringCollection.build(input.getColumn()
        .transformWithUdf("display", Optional.ofNullable(language)
            .map(StringCollection::getColumn)
            .map(ColumnRepresentation::singular)
            .orElse(EmptyRepresentation.getInstance()))
        .removeNulls()
    );
  }

  /**
   * This function returns the value of a property for a Coding.
   *
   * @author Piotr Szul
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#property">property</a>
   */
  @FhirPathFunction
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
                .orElse(EmptyRepresentation.getInstance())
        ).flatten().removeNulls();

    return Collection.build(resultCtx, propertyType,
        input.getDefinition().flatMap(Functions.maybeCast(ElementDefinition.class))
            .filter(__ -> propertyType == FHIRDefinedType.CODING));
  }

  /**
   * This function returns the designations of a Coding.
   *
   * @author Piotr Szul
   * @see <a
   * href="https://pathling.csiro.au/docs/fhirpath/functions.html#designation">designation</a>
   */
  @FhirPathFunction
  public static StringCollection designation(@Nonnull final CodingCollection input,
      @Nullable final CodingCollection use,
      @Nullable final StringCollection language) {

    return StringCollection.build(input.getColumn()
        .transformWithUdf("designation",
            Optional.ofNullable(use)
                .map(CodingCollection::getColumn)
                .map(ColumnRepresentation::singular)
                .orElse(EmptyRepresentation.getInstance()),
            Optional.ofNullable(language)
                .map(StringCollection::getColumn)
                .map(ColumnRepresentation::singular)
                .orElse(EmptyRepresentation.getInstance())
        )
        .flatten().removeNulls()
    );
  }

  /**
   * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of
   * boolean values, based upon whether each item is present within the ValueSet identified by the
   * supplied URL.
   *
   * @author John Grimes
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#memberof">memberOf</a>
   */
  @FhirPathFunction
  public static BooleanCollection memberOf(@Nonnull final CodingCollection input,
      @Nonnull final StringCollection valueSetURL) {
    return BooleanCollection.build(
        input.getColumn().callUdf("member_of", valueSetURL.getColumn().singular())
    );
  }


  /**
   * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of
   * boolean values whether based upon whether each item subsumes one or more Codings or
   * CodeableConcepts in the argument set.
   *
   * @author John Grimes
   * @author Piotr Szul
   * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">Additional functions</a>
   */
  @FhirPathFunction
  public static BooleanCollection subsumes(@Nonnull final CodingCollection input,
      @Nonnull final CodingCollection codes) {
    return input.map(ctx ->
            ctx.callUdf("subsumes", codes.getColumn(),
                DefaultRepresentation.literal(false)),
        BooleanCollection::build);
  }

  /**
   * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of
   * boolean values whether based upon whether each item  is subsumedBy one or more Codings or
   * CodeableConcepts in the argument set.
   *
   * @author John Grimes
   * @author Piotr Szul
   * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">Additional functions</a>
   */
  @FhirPathFunction
  public static BooleanCollection subsumedBy(@Nonnull final CodingCollection input,
      @Nonnull final CodingCollection codes) {
    return input.map(ctx ->
            ctx.callUdf("subsumes", codes.getColumn(),
                DefaultRepresentation.literal(true)),
        BooleanCollection::build);
  }

  /**
   * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set Codings
   * translated using provided concept map URL.
   * <p>
   * Signature:
   * <pre>
   * collection&lt;Coding|CodeableConcept&gt; -&gt; translate(conceptMapUrl: string, reverse = false,
   * equivalence = 'equivalent') : collection&lt;Coding&gt;
   * </pre>
   * <p>
   * Uses: <a href="https://www.hl7.org/fhir/operation-conceptmap-translate.html">Translate
   * Operation</a>
   *
   * @author Piotr Szul
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#translate">translate</a>
   */
  @FhirPathFunction
  public static CodingCollection translate(@Nonnull final CodingCollection input,
      @Nonnull final StringCollection conceptMapUrl,
      @Nullable final BooleanCollection reverse, @Nullable final StringCollection equivalence,
      @Nullable final StringCollection target) {
    return (CodingCollection) input.copyWith(
        input.getColumn().callUdf("translate_coding",
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
                .orElse(EmptyRepresentation.getInstance())
        ));
  }

}
