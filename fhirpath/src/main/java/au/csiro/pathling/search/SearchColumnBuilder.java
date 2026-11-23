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

package au.csiro.pathling.search;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateTimeComparable;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.mixed.ChoiceElementCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.evaluation.CrossResourceStrategy;
import au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluator;
import au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.search.filter.NumericMatchingSupport;
import au.csiro.pathling.search.filter.QuantitySearchValue;
import au.csiro.pathling.search.filter.SearchFilter;
import au.csiro.pathling.search.filter.SearchPrefix;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Builds SparkSQL Column expressions from FHIR Search queries or FHIRPath expressions.
 *
 * <p>This builder creates Column expressions that can be applied to Pathling-encoded FHIR resource
 * datasets. Columns are created eagerly (SparkSQL Column expressions are built at creation time),
 * allowing them to be created without requiring a SparkSession or actual data.
 *
 * <p>The builder supports creating filter columns from:
 *
 * <ul>
 *   <li>FHIR Search queries ({@link #fromSearch}, {@link #fromQueryString})
 *   <li>FHIRPath expressions ({@link #fromExpression})
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * // Create builder with default registry
 * SearchColumnBuilder builder = SearchColumnBuilder.withDefaultRegistry(fhirContext);
 *
 * // Create filter column from FHIR search query string
 * Column genderFilter = builder.fromQueryString(ResourceType.PATIENT, "gender=male");
 *
 * // Create filter column from FHIRPath expression
 * Column activeFilter = builder.fromExpression(ResourceType.PATIENT, "active = true");
 *
 * // Combine filters using standard SparkSQL operations
 * Column combined = genderFilter.and(activeFilter);
 *
 * // Apply to dataset using standard SparkSQL
 * Dataset<Row> result = dataSource.read("Patient").filter(combined);
 * }</pre>
 */
@Value
public class SearchColumnBuilder {

  /** Resource path for the bundled R4 search parameters. */
  private static final String R4_REGISTRY_RESOURCE = "/fhir/R4/search-parameters.json";

  /**
   * Mappings from complex FHIR types to their string sub-fields.
   *
   * <p>When a string search parameter expression resolves to one of these complex types, the search
   * is expanded to match against each sub-field independently. A match on any sub-field satisfies
   * the search criterion.
   *
   * @see <a href="https://hl7.org/fhir/search.html#string">String Search</a>
   */
  private static final Map<FHIRDefinedType, List<String>> COMPLEX_TYPE_STRING_SUBFIELDS =
      Map.of(
          FHIRDefinedType.HUMANNAME, List.of("family", "given", "text", "prefix", "suffix"),
          FHIRDefinedType.ADDRESS,
              List.of("text", "line", "city", "district", "state", "postalCode", "country"));

  /** The FHIR context for resource definitions. */
  @Nonnull FhirContext fhirContext;

  /** The search parameter registry for looking up parameter definitions. */
  @Nonnull SearchParameterRegistry registry;

  /** The FHIRPath parser for parsing expressions. */
  @Nonnull Parser parser;

  /**
   * Creates a builder with the default bundled search parameter registry for FHIR R4.
   *
   * <p>This method requires an R4 FhirContext and uses the bundled R4 search parameters from the
   * HL7 FHIR specification.
   *
   * @param fhirContext the FHIR context (must be R4)
   * @return a new builder with the default R4 registry
   * @throws IllegalArgumentException if the FhirContext is not R4
   */
  @Nonnull
  public static SearchColumnBuilder withDefaultRegistry(@Nonnull final FhirContext fhirContext) {
    if (fhirContext.getVersion().getVersion() != FhirVersionEnum.R4) {
      throw new IllegalArgumentException(
          "Default registry requires FHIR R4 context, but got: "
              + fhirContext.getVersion().getVersion());
    }
    try (final InputStream is =
        SearchColumnBuilder.class.getResourceAsStream(R4_REGISTRY_RESOURCE)) {
      if (is == null) {
        throw new IllegalStateException(
            "Search parameters resource not found: " + R4_REGISTRY_RESOURCE);
      }
      return withRegistry(fhirContext, SearchParameterRegistry.fromInputStream(fhirContext, is));
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to load default search parameters", e);
    }
  }

  /**
   * Creates a builder with an explicit registry.
   *
   * @param fhirContext the FHIR context
   * @param registry the search parameter registry
   * @return a new builder
   */
  @Nonnull
  public static SearchColumnBuilder withRegistry(
      @Nonnull final FhirContext fhirContext, @Nonnull final SearchParameterRegistry registry) {
    return new SearchColumnBuilder(fhirContext, registry, new Parser());
  }

  /**
   * Creates a filter Column from a FHIR search query.
   *
   * <p>Multiple criteria in the search are combined with AND logic.
   *
   * @param resourceType the resource type to filter
   * @param search the FHIR search query
   * @return a SparkSQL Column representing the search criteria
   * @throws UnknownSearchParameterException if a search parameter is not found in the registry
   * @throws InvalidModifierException if an unsupported modifier is used
   * @throws InvalidSearchParameterException if the search parameter configuration is invalid, or
   *     the number in a number or quantity search value is not a number
   */
  @Nonnull
  public Column fromSearch(
      @Nonnull final ResourceType resourceType, @Nonnull final FhirSearch search) {

    // If there are no criteria, return a column that matches all resources
    if (search.getCriteria().isEmpty()) {
      return lit(true);
    }

    // Create the evaluator for FHIRPath evaluation
    final SingleResourceEvaluator evaluator = createEvaluator(resourceType);

    // Build the combined filter expression
    return search.getCriteria().stream()
        .map(criterion -> buildCriterionFilter(resourceType, criterion, evaluator))
        .reduce(Column::and)
        .orElse(lit(true));
  }

  /**
   * Creates a filter Column from a FHIR search query string.
   *
   * <p>The query string should be in standard URL query format without the leading '?'.
   *
   * @param resourceType the resource type to filter
   * @param queryString the query string (e.g., "gender=male&amp;birthdate=ge1990")
   * @return a SparkSQL Column representing the search criteria
   * @throws UnknownSearchParameterException if a search parameter is not found in the registry
   * @throws InvalidModifierException if an unsupported modifier is used
   * @throws InvalidSearchParameterException if the search parameter configuration is invalid, or
   *     the number in a number or quantity search value is not a number
   */
  @Nonnull
  public Column fromQueryString(
      @Nonnull final ResourceType resourceType, @Nonnull final String queryString) {
    return fromSearch(resourceType, FhirSearch.fromQueryString(queryString));
  }

  /**
   * Creates a filter Column from a FHIRPath boolean expression.
   *
   * <p>The expression should evaluate to a boolean value. Resources where the expression evaluates
   * to true will be included in the filtered result.
   *
   * @param resourceType the resource type to filter
   * @param fhirPathExpression the FHIRPath expression (must evaluate to boolean)
   * @return a SparkSQL Column representing the expression
   */
  @Nonnull
  public Column fromExpression(
      @Nonnull final ResourceType resourceType, @Nonnull final String fhirPathExpression) {

    // Parse the FHIRPath expression
    final FhirPath fhirPath = parser.parse(fhirPathExpression);

    // Create evaluator and evaluate
    final SingleResourceEvaluator evaluator = createEvaluator(resourceType);
    final Collection result = evaluator.evaluate(fhirPath);

    // Get the Column value - for boolean expressions this should be a scalar boolean. A value is
    // returned in the form the engine computes with, so that a quantity is a System quantity, in
    // its fixed shape, whatever the layout (FR-059), and is given as a caller is given it, so that
    // a date is its text.
    return result.toEngineForm().getOutputColumn().getValue();
  }

  /**
   * Creates a SingleResourceEvaluator in flat schema mode with EMPTY cross-resource strategy.
   *
   * <p>This evaluator generates Column references that work directly on flat Pathling-encoded
   * datasets without requiring schema wrapping/unwrapping. Column expressions are generated as
   * direct column access (e.g., {@code col("name")} instead of {@code
   * col("Patient").getField("name")}).
   *
   * <p>Cross-resource references (e.g., paths that reference foreign resources) are handled using
   * the EMPTY strategy, which returns empty collections with correct type information. This allows
   * search parameters with cross-resource paths to evaluate gracefully.
   *
   * @param resourceType the resource type
   * @return a new evaluator configured for flat schema mode
   */
  @Nonnull
  private SingleResourceEvaluator createEvaluator(@Nonnull final ResourceType resourceType) {
    return SingleResourceEvaluatorBuilder.create(resourceType, fhirContext)
        .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
        .build();
  }

  /**
   * Builds a filter expression for a single search criterion.
   *
   * <p>For polymorphic search parameters with multiple expressions, each expression is evaluated
   * separately and the filter results are combined with OR logic.
   *
   * <p>An expression of a FHIR type that the search parameter type does not allow is skipped, so
   * that it matches nothing, as long as at least one expression is of an allowed type. For example,
   * the R4 {@code value-quantity} parameter has the expressions {@code (Observation.value as
   * Quantity)} and {@code (Observation.value as SampledData)}, and a quantity search matches the
   * first only. If no expression is of an allowed type, the parameter cannot be searched and the
   * search fails.
   *
   * @param resourceType the resource type
   * @param criterion the search criterion
   * @param evaluator the FHIRPath evaluator
   * @return a SparkSQL Column expression for this criterion
   * @throws InvalidSearchParameterException if no expression of the parameter is of a FHIR type
   *     that its search parameter type allows
   */
  @Nonnull
  private Column buildCriterionFilter(
      @Nonnull final ResourceType resourceType,
      @Nonnull final SearchCriterion criterion,
      @Nonnull final SingleResourceEvaluator evaluator) {

    // Look up the parameter definition.
    final SearchParameterDefinition paramDef =
        registry
            .getParameter(resourceType.toCode(), criterion.getParameterCode())
            .orElseThrow(
                () ->
                    new UnknownSearchParameterException(
                        criterion.getParameterCode(), resourceType.toCode()));
    final SearchParameterType paramType = paramDef.type();

    // Evaluate each expression, keeping those of a type the parameter type allows. A choice element
    // is kept if any of its alternatives is of an allowed type.
    final List<Collection> results =
        paramDef.expressions().stream().map(expression -> evaluate(expression, evaluator)).toList();
    final List<Collection> allowedResults =
        results.stream()
            .filter(result -> fhirTypesOf(result).anyMatch(paramType::isAllowedFhirType))
            .toList();
    if (!results.isEmpty() && allowedResults.isEmpty()) {
      throw new InvalidSearchParameterException(
          String.format(
              "Search parameter '%s' on %s has no expression of a FHIR type allowed for search"
                  + " parameter type '%s'. Types found: %s. Allowed types: %s",
              paramDef.code(),
              resourceType.toCode(),
              paramType,
              typeCodes(results.stream().flatMap(SearchColumnBuilder::fhirTypesOf)),
              typeCodes(paramType.getAllowedFhirTypes().stream())));
    }

    // Build a filter for each remaining expression and combine them with OR. The values are checked
    // afterwards, so that an unsupported modifier is reported in preference to a value it would not
    // accept.
    final Column filter =
        allowedResults.stream()
            .map(result -> buildExpressionFilter(paramType, criterion, result, evaluator))
            .reduce(Column::or)
            .orElse(lit(false));
    checkNumbers(paramDef, criterion);
    return filter;
  }

  /**
   * Checks that the number in each value of a number or quantity search is a number. A matcher
   * parses its value only when the filter is resolved against a dataset, where the parameter is no
   * longer known, so a value that is not a number is rejected here, naming the parameter.
   *
   * @param paramDef the definition of the search parameter
   * @param criterion the search criterion
   * @throws InvalidSearchParameterException if the number in a value is not a number
   */
  private static void checkNumbers(
      @Nonnull final SearchParameterDefinition paramDef, @Nonnull final SearchCriterion criterion) {
    final Stream<String> numbers =
        switch (paramDef.type()) {
          case NUMBER -> criterion.getValues().stream().map(SearchPrefix::stripPrefix);
          case QUANTITY ->
              criterion.getValues().stream()
                  .map(value -> QuantitySearchValue.parse(value).getNumericValue());
          default -> Stream.empty();
        };
    try {
      numbers.forEach(NumericMatchingSupport::parseNumber);
    } catch (final InvalidSearchParameterException e) {
      throw new InvalidSearchParameterException(
          String.format(
              "Invalid value for search parameter '%s': %s", paramDef.code(), e.getMessage()));
    }
  }

  /**
   * Parses and evaluates a FHIRPath expression of a search parameter.
   *
   * @param expression the FHIRPath expression
   * @param evaluator the FHIRPath evaluator
   * @return the evaluated collection, which is a choice element or has a FHIR type
   * @throws InvalidSearchParameterException if the FHIR type of the result cannot be determined
   */
  @Nonnull
  private Collection evaluate(
      @Nonnull final String expression, @Nonnull final SingleResourceEvaluator evaluator) {
    final Collection result = evaluator.evaluate(parser.parse(expression));
    if (!(result instanceof ChoiceElementCollection) && result.getFhirType().isEmpty()) {
      throw new InvalidSearchParameterException(
          "Cannot determine FHIR type for expression: " + expression);
    }
    return result;
  }

  /**
   * Returns the FHIR types that a collection returned by {@link #evaluate} can hold: those of the
   * alternatives of a choice element, or otherwise its own type.
   *
   * @param result the evaluated collection
   * @return its FHIR types
   */
  @Nonnull
  private static Stream<FHIRDefinedType> fhirTypesOf(@Nonnull final Collection result) {
    if (result instanceof final ChoiceElementCollection choice) {
      return choice.getChoiceDefinition().getAllChildTypes().stream()
          .flatMap(alternative -> alternative.getFhirType().stream());
    }
    return Stream.of(fhirTypeOf(result));
  }

  /**
   * Returns the FHIR type of a collection returned by {@link #evaluate} that is not a choice
   * element.
   *
   * @param result the evaluated collection
   * @return its FHIR type
   */
  @Nonnull
  private static FHIRDefinedType fhirTypeOf(@Nonnull final Collection result) {
    return result.getFhirType().orElseThrow();
  }

  /**
   * Formats FHIR types as their codes, sorted and without repetition, for an error message.
   *
   * @param types the FHIR types
   * @return the formatted codes, for example {@code [Quantity, SampledData]}
   */
  @Nonnull
  private static String typeCodes(@Nonnull final Stream<FHIRDefinedType> types) {
    return types.map(FHIRDefinedType::toCode).distinct().sorted().toList().toString();
  }

  /**
   * Builds a filter expression for a single evaluated expression within a search criterion.
   *
   * <p>When the expression resolves to a complex type with known string sub-fields (HumanName,
   * Address), the expression is expanded into multiple sub-field expressions that are each
   * evaluated independently. The filter results are OR'd together so that a match on any sub-field
   * satisfies the criterion.
   *
   * @param paramType the search parameter type
   * @param criterion the search criterion (for modifier and values)
   * @param result the evaluated expression: a choice element with an alternative of a FHIR type
   *     that the parameter type allows, or otherwise of such a type
   * @param evaluator the FHIRPath evaluator
   * @return a SparkSQL Column expression for this expression
   */
  @Nonnull
  private Column buildExpressionFilter(
      @Nonnull final SearchParameterType paramType,
      @Nonnull final SearchCriterion criterion,
      @Nonnull final Collection result,
      @Nonnull final SingleResourceEvaluator evaluator) {

    // A choice element has no single type, so each of its alternatives is searched separately.
    if (result instanceof final ChoiceElementCollection choice) {
      return buildChoiceFilter(paramType, criterion, choice, evaluator);
    }

    // If the expression resolves to a complex type with known string sub-fields, expand into
    // sub-field expressions and OR the results together.
    final FHIRDefinedType fhirType = fhirTypeOf(result);
    final List<String> subFields = COMPLEX_TYPE_STRING_SUBFIELDS.get(fhirType);
    if (subFields != null) {
      return subFields.stream()
          .flatMap(
              subField -> {
                final Collection subResult = evaluator.evaluate(parser.parse(subField), result);
                // Skip sub-fields that do not exist for this resource type.
                return subResult
                    .getFhirType()
                    .map(
                        subFieldType ->
                            paramType
                                .createFilter(criterion.getModifier(), subFieldType)
                                .buildFilter(matchedColumn(subResult), criterion.getValues()))
                    .stream();
              })
          .reduce(Column::or)
          .orElse(lit(false));
    }

    // Standard path: build filter directly from the evaluated column.
    final SearchFilter filter = paramType.createFilter(criterion.getModifier(), fhirType);
    return filter.buildFilter(matchedColumn(result), criterion.getValues());
  }

  /**
   * Builds a filter expression for an expression that resolves to a choice element, such as {@code
   * Observation.effective}.
   *
   * <p>Each alternative of the choice that the search parameter type allows is selected with {@code
   * ofType} and searched as an expression of its own, and the filter results are OR'd together so
   * that a match on any alternative satisfies the criterion. With the {@code :not} modifier each
   * alternative's filter is already negated, so they are AND'd instead: a resource matches only if
   * no alternative holds a matching value. An alternative that the parameter type does not allow,
   * such as the {@code Timing} alternative of {@code Observation.effective} for a date parameter,
   * is skipped.
   *
   * @param paramType the search parameter type
   * @param criterion the search criterion (for modifier and values)
   * @param choice the evaluated choice element, with at least one alternative that the parameter
   *     type allows
   * @param evaluator the FHIRPath evaluator
   * @return a SparkSQL Column expression for this expression
   */
  @Nonnull
  private Column buildChoiceFilter(
      @Nonnull final SearchParameterType paramType,
      @Nonnull final SearchCriterion criterion,
      @Nonnull final ChoiceElementCollection choice,
      @Nonnull final SingleResourceEvaluator evaluator) {
    return fhirTypesOf(choice)
        .filter(paramType::isAllowedFhirType)
        .map(
            alternative ->
                choice.filterByType(
                    new TypeSpecifier(TypeSpecifier.FHIR_NAMESPACE, alternative.toCode())))
        .map(alternative -> buildExpressionFilter(paramType, criterion, alternative, evaluator))
        .reduce("not".equals(criterion.getModifier()) ? Column::and : Column::or)
        .orElseThrow();
  }

  /**
   * Returns the column a search filter matches against. A decimal is matched as its number, so a
   * stored decimal is converted to one, and a primitive held with its id and extensions is matched
   * as its System value. Any other value is matched as it is held: a stored quantity, whose matcher
   * reads its stored annotations, and a date, whose matcher reads only its text and bounds, so that
   * a filter on a stored bound is pushed down to the scan, which converting it would prevent.
   */
  @Nonnull
  private static ColumnRepresentation matchedColumn(@Nonnull final Collection result) {
    final boolean converted =
        result instanceof DecimalCollection
            || (result.holdsPrimitiveMetadata() && !(result instanceof DateTimeComparable));
    return converted ? result.toEngineForm().getColumn() : result.getColumn();
  }
}
