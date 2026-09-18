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
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.CrossResourceStrategy;
import au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluator;
import au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.search.filter.SearchFilter;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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
   * @throws InvalidSearchParameterException if the search parameter configuration is invalid
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
   * @throws InvalidSearchParameterException if the search parameter configuration is invalid
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

    // Get the Column value - for boolean expressions this should be a scalar boolean
    return result.getColumn().getValue();
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
   * @param resourceType the resource type
   * @param criterion the search criterion
   * @param evaluator the FHIRPath evaluator
   * @return a SparkSQL Column expression for this criterion
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

    // Build filter for each expression and combine with OR
    return paramDef.expressions().stream()
        .map(expression -> buildExpressionFilter(paramDef.type(), criterion, expression, evaluator))
        .reduce(Column::or)
        .orElse(lit(false));
  }

  /**
   * Builds a filter expression for a single FHIRPath expression within a search criterion.
   *
   * <p>When the expression resolves to a complex type with known string sub-fields (HumanName,
   * Address), the expression is expanded into multiple sub-field expressions that are each
   * evaluated independently. The filter results are OR'd together so that a match on any sub-field
   * satisfies the criterion.
   *
   * @param paramType the search parameter type
   * @param criterion the search criterion (for modifier and values)
   * @param expression the FHIRPath expression to evaluate
   * @param evaluator the FHIRPath evaluator
   * @return a SparkSQL Column expression for this expression
   */
  @Nonnull
  private Column buildExpressionFilter(
      @Nonnull final SearchParameterType paramType,
      @Nonnull final SearchCriterion criterion,
      @Nonnull final String expression,
      @Nonnull final SingleResourceEvaluator evaluator) {

    // Parse and evaluate the FHIRPath expression to determine its type.
    final FhirPath fhirPath = parser.parse(expression);
    final Collection result = evaluator.evaluate(fhirPath);

    // Get FHIR type from collection - fail if not available.
    final FHIRDefinedType fhirType =
        result
            .getFhirType()
            .orElseThrow(
                () ->
                    new InvalidSearchParameterException(
                        "Cannot determine FHIR type for expression: " + expression));

    // If the expression resolves to a complex type with known string sub-fields, expand into
    // sub-field expressions and OR the results together.
    final List<String> subFields = COMPLEX_TYPE_STRING_SUBFIELDS.get(fhirType);
    if (subFields != null) {
      return subFields.stream()
          .map(subField -> expression + "." + subField)
          .flatMap(
              subFieldExpr -> {
                final FhirPath subFhirPath = parser.parse(subFieldExpr);
                final Collection subResult = evaluator.evaluate(subFhirPath);
                // Skip sub-fields that do not exist for this resource type.
                if (subResult.getFhirType().isEmpty()) {
                  return Stream.empty();
                }
                final SearchFilter filter =
                    getFilterForType(
                        paramType, criterion.getModifier(), subResult.getFhirType().get());
                return Stream.of(filter.buildFilter(subResult.getColumn(), criterion.getValues()));
              })
          .reduce(Column::or)
          .orElse(lit(false));
    }

    // Standard path: build filter directly from the evaluated column.
    final SearchFilter filter = getFilterForType(paramType, criterion.getModifier(), fhirType);
    return filter.buildFilter(result.getColumn(), criterion.getValues());
  }

  /**
   * Gets the appropriate filter implementation for a search parameter type, modifier, and FHIR
   * type.
   *
   * @param type the search parameter type
   * @param modifier the search modifier (e.g., "not", "exact"), or null for no modifier
   * @param fhirType the FHIR type of the element being searched
   * @return the filter implementation
   * @throws InvalidModifierException if the modifier is not supported for the parameter type
   * @throws InvalidSearchParameterException if the FHIR type is not valid for the search parameter
   *     type
   */
  @Nonnull
  private SearchFilter getFilterForType(
      @Nonnull final SearchParameterType type,
      @Nullable final String modifier,
      @Nonnull final FHIRDefinedType fhirType) {

    // Validate FHIR type is allowed for this search parameter type
    if (!type.isAllowedFhirType(fhirType)) {
      throw new InvalidSearchParameterException(
          String.format(
              "FHIR type '%s' is not valid for search parameter type '%s'. " + "Allowed types: %s",
              fhirType.toCode(), type, type.getAllowedFhirTypes()));
    }

    // Delegate to enum constant's createFilter() implementation
    return type.createFilter(modifier, fhirType);
  }
}
