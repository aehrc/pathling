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

import static au.csiro.pathling.fhirpath.Comparable.ComparisonOperation.EQUALS;
import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.MixedCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import au.csiro.pathling.utilities.Functions;
import au.csiro.pathling.utilities.Preconditions;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of standard FHIRPath functions.
 */
@SuppressWarnings("unused")
public class StandardFunctions {

  public static final String EXTENSION_ELEMENT_NAME = "extension";
  public static final String URL_ELEMENT_NAME = "url";
  public static final String JOIN_DEFAULT_SEPARATOR = "";

  public static final String REFERENCE_ELEMENT_NAME = "reference";

  /**
   * Describes a function which can scope down the previous invocation within a FHIRPath expression,
   * based upon an expression passed in as an argument. Supports the use of `$this` to reference the
   * element currently in scope.
   *
   * @param input the input collection
   * @param expression the expression to evaluate
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#where">where</a>
   */
  @Nonnull
  @FhirpathFunction
  public static Collection where(@Nonnull final Collection input,
      @Nonnull final CollectionExpression expression) {
    return input.filter(expression.requireBoolean().toColumnFunction(input));
  }
  //
  //
  // // Maybe these too can be implemented as colum functions????
  // @FhirpathFunction
  // public Collection iif(@Nonnull final Collection input,
  //     @Nonnull CollectionExpression expression, @Nonnull Collection thenValue,
  //     @Nonnull Collection otherwiseValue) {
  //   // if we do not need to modify the context then maybe enough to just pass the bound expressions
  //   // (but in fact it's lazy eval anyway and at some point we should check
  //   functions.when(requireBoolean(expression).apply(input).getSingleton(), thenValue.getColumn())
  //       .otherwise(otherwiseValue.getColumn());
  //   // we need to check that the result of the expression is boolean
  //   return Collection.nullCollection();
  // }

  /**
   * This function allows the selection of only the first element of a collection.
   *
   * @param input the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#first">first</a>
   */
  @FhirpathFunction
  public static Collection first(@Nonnull final Collection input) {
    return input.copyWith(input.getCtx().first());
  }

  /**
   * This function returns true if the input collection is empty.
   *
   * @param input the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#empty">empty</a>
   */
  @FhirpathFunction
  public static BooleanCollection empty(@Nonnull final Collection input) {
    return BooleanCollection.build(input.getCtx().empty());
  }

  /**
   * A function for aggregating data based on counting the number of rows within the result.
   *
   * @param input the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#count">count</a>
   */
  @FhirpathFunction
  public static IntegerCollection count(@Nonnull final Collection input) {
    return IntegerCollection.build(input.getCtx().count());
  }

  /**
   * A function that returns the extensions of the current element that match a given URL.
   *
   * @author Piotr Szul
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#extension">extension</a>
   */
  @FhirpathFunction
  public static Collection extension(@Nonnull final Collection input,
      @Nonnull final StringCollection url) {
    return input.traverse(EXTENSION_ELEMENT_NAME).map(extensionCollection ->
        where(extensionCollection, c -> c.traverse(URL_ELEMENT_NAME).map(
                urlCollection -> urlCollection.getComparison(EQUALS).apply(url))
            .map(BooleanCollection::build).orElse(BooleanCollection.falseCollection()))
    ).orElse(Collection.nullCollection());
  }

  /**
   * A function filters items in the input collection to only those that are of the given type.
   *
   * @param input the input collection
   * @param typeSpecifier the type specifier
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#oftype">ofType</a>
   */
  @FhirpathFunction
  public static Collection ofType(@Nonnull final MixedCollection input,
      @Nonnull final TypeSpecifier typeSpecifier) {
    // TODO: This should work on any collection type - not just mixed
    // if the type of the collection does not match the required type then it should return an empty collection.
    return input.resolveChoice(typeSpecifier.toFhirType().toCode())
        .orElse(Collection.nullCollection());
  }

  @FhirpathFunction
  public static StringCollection join(@Nonnull final StringCollection input,
      @Nullable final StringCollection separator) {
    return StringCollection.build(input.getCtx().join(
        nonNull(separator)
        ? separator.toLiteralValue()
        : JOIN_DEFAULT_SEPARATOR
    ));
  }

  /**
   * A function which is able to test whether the input collection is empty. It can also optionally
   * accept an argument which can filter the input collection prior to applying the test.
   *
   * @param input the input collection
   * @param criteria the criteria to apply to the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#exists">exists</a>
   */
  @FhirpathFunction
  public static BooleanCollection exists(@Nonnull final Collection input,
      @Nullable final CollectionExpression criteria) {
    return not(empty(nonNull(criteria)
                     ? where(input, criteria)
                     : input));

  }

  /**
   * Returns {@code true} if the input collection evaluates to {@code false}, and {@code false} if
   * it evaluates to {@code true}. Otherwise, the result is empty.
   *
   * @param input the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#not">not</a>
   */
  @FhirpathFunction
  public static BooleanCollection not(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getCtx().not());
  }

  @FhirpathFunction
  public static BooleanCollection allTrue(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getCtx().allTrue());
  }

  @FhirpathFunction
  public static BooleanCollection allFalse(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getCtx().allFalse());
  }

  @FhirpathFunction
  public static BooleanCollection anyTrue(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getCtx().anyTrue());
  }

  @FhirpathFunction
  public static BooleanCollection anyFalse(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getCtx().anyFalse());
  }

  public static boolean isTypeSpecifierFunction(@Nonnull final String functionName) {
    return "ofType".equals(functionName) || "getReferenceKey".equals(functionName);
  }

  // TODO: This should be a string collection with a StringCoerible argument
  @FhirpathFunction
  public static Collection toString(@Nonnull final Collection input) {
    Preconditions.checkUserInput(input instanceof StringCoercible,
        "toString() can only be applied to a StringCoercible path");
    return ((StringCoercible) input).asStringPath();
  }

  public StandardFunctions() {
  }

  // extended functions
  @FhirpathFunction
  public static ResourceCollection reverseResolve(@Nonnull final Collection input,
      @Nonnull final FhirPath<Collection> reference,
      @Nonnull final EvaluationContext evaluationContext) {
    // TODO: decompose the reference - this should be done in a better way
    // TODO: maybe it should be '
    final List<String> names = reference.asStream()
        .map(Functions.maybeCast(Paths.Traversal.class))
        .flatMap(Optional::stream)
        .map(Paths.Traversal::getPropertyName)
        .collect(Collectors.toUnmodifiableList());
    Preconditions.checkUserInput(names.size() == 2,
        "Reference must be in format: <ResourceType>.<referenceFieldName>");
    final ResourceType foreignResourceType = ResourceType.fromCode(names.get(0));
    final String foreignReferenceField = names.get(1);

    final Dataset<Row> dataset = evaluationContext.getDataSource().read(foreignResourceType);
    // TODO join and aggregate
    // not quite sure how to deal with extensions here - somehow nees to merge the maps as well
    // but in general
    // group by foreignReferenceField and collect_list(on everything else)

    final ResourceCollection foreignResource = ResourceCollection.build(
        evaluationContext.getFhirContext(),
        dataset,
        foreignResourceType);

    final Column referenceColumn = foreignResource.traverse(
            foreignReferenceField)
        .flatMap(c -> c.traverse("reference"))
        .map(Collection::getColumn)
        .orElseThrow();

    final List<Column> aggs = Stream.of(dataset.columns())
        .map(c -> functions.collect_list(c).alias(foreignResourceType.toCode() + "_" + c))
        .collect(Collectors.toUnmodifiableList());

    final Dataset<Row> foreignDataset = dataset.groupBy(referenceColumn.alias("ref_xxx"))
        .agg(aggs.get(0), aggs.subList(1, aggs.size()).toArray(new Column[0]));

    final Dataset<Row> joinedDataset = dataset.join(foreignDataset,
        dataset.col("id_versioned").equalTo(foreignDataset.col("ref_xxx")), "left_outer");

    return ResourceCollection.build(evaluationContext.getFhirContext(), joinedDataset,
        foreignResourceType);

  }

}
