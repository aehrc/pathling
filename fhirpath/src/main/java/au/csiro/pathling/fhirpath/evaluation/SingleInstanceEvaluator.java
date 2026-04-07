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

package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.config.FhirpathConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.ListTraceCollector;
import au.csiro.pathling.fhirpath.ListTraceCollector.TraceEntry;
import au.csiro.pathling.fhirpath.TraceCollectorProxy;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult.TraceResult;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult.TypedValue;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.sql.SyntheticFieldUtils;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Evaluates FHIRPath expressions against a single encoded FHIR resource and returns materialised
 * typed results. This class owns the full evaluation lifecycle after encoding: expression parsing,
 * evaluation, context/variables handling, return type determination, result collection, value
 * materialisation, row sanitisation, and JSON conversion.
 *
 * @author John Grimes
 */
@UtilityClass
public class SingleInstanceEvaluator {

  /**
   * Evaluates a FHIRPath expression against a single encoded FHIR resource.
   *
   * @param resourceDf the encoded resource as a single-row Dataset
   * @param resourceType the FHIR resource type code (e.g. "Patient")
   * @param fhirContext the FHIR context
   * @param fhirPathExpression the FHIRPath expression to evaluate
   * @param contextExpression an optional context expression; if non-null, the main expression is
   *     composed with the context
   * @param variables optional named variables available via %variable syntax, or null
   * @return a {@link SingleInstanceEvaluationResult} containing typed result values and type
   *     metadata
   */
  @Nonnull
  public static SingleInstanceEvaluationResult evaluate(
      @Nonnull final Dataset<Row> resourceDf,
      @Nonnull final String resourceType,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final String fhirPathExpression,
      @Nullable final String contextExpression,
      @Nullable final Map<String, Object> variables) {
    return evaluate(
        resourceDf,
        resourceType,
        fhirContext,
        fhirPathExpression,
        contextExpression,
        variables,
        FhirpathConfiguration.DEFAULT);
  }

  /**
   * Evaluates a FHIRPath expression against a single encoded FHIR resource with custom
   * configuration.
   *
   * @param resourceDf the encoded resource as a single-row Dataset
   * @param resourceType the FHIR resource type code (e.g. "Patient")
   * @param fhirContext the FHIR context
   * @param fhirPathExpression the FHIRPath expression to evaluate
   * @param contextExpression an optional context expression; if non-null, the main expression is
   *     composed with the context
   * @param variables optional named variables available via %variable syntax, or null
   * @param configuration the FHIRPath evaluation configuration
   * @return a {@link SingleInstanceEvaluationResult} containing typed result values and type
   *     metadata
   */
  @Nonnull
  public static SingleInstanceEvaluationResult evaluate(
      @Nonnull final Dataset<Row> resourceDf,
      @Nonnull final String resourceType,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final String fhirPathExpression,
      @Nullable final String contextExpression,
      @Nullable final Map<String, Object> variables,
      @Nonnull final FhirpathConfiguration configuration) {

    // Convert incoming variables to Collection objects for the evaluator.
    final Map<String, Collection> variableCollections = convertVariables(variables);

    // Create a trace collector and serializable proxy to capture trace() output.
    final ListTraceCollector traceCollector = new ListTraceCollector();
    try (final TraceCollectorProxy proxy = TraceCollectorProxy.create(traceCollector)) {

      // Parse the FHIRPath expression.
      final Parser parser = new Parser();
      final FhirPath mainPath = parser.parse(fhirPathExpression);

      // Create a single resource evaluator for determining the return type.
      final SingleResourceEvaluator evaluator =
          SingleResourceEvaluatorBuilder.create(ResourceType.fromCode(resourceType), fhirContext)
              .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
              .withVariables(variableCollections)
              .withConfiguration(configuration)
              .withTraceCollector(proxy)
              .build();

      // Evaluate to determine the return type.
      final Collection resultCollection = evaluator.evaluate(mainPath);
      final String expectedReturnType = determineReturnType(resultCollection);

      if (contextExpression != null) {
        return evaluateWithContext(
            resourceDf,
            parser,
            mainPath,
            contextExpression,
            evaluator,
            expectedReturnType,
            traceCollector);
      }

      // Apply the result Column to the dataset and collect the results.
      final Column resultColumn = resultCollection.getColumn().getValue();
      final List<TypedValue> results = collectResults(resourceDf, resultColumn, expectedReturnType);
      final List<TraceResult> traces = buildTraceResults(traceCollector);
      return new SingleInstanceEvaluationResult(results, expectedReturnType, traces);
    }
  }

  /**
   * Converts incoming variable values into FHIRPath {@link Collection} objects suitable for the
   * evaluator.
   *
   * @param variables the variable map with primitive values (String, Integer, Boolean, BigDecimal),
   *     or null
   * @return a map of variable names to Collection objects
   */
  @Nonnull
  static Map<String, Collection> convertVariables(@Nullable final Map<String, Object> variables) {
    if (variables == null || variables.isEmpty()) {
      return Map.of();
    }

    final Map<String, Collection> result = new HashMap<>();
    for (final Map.Entry<String, Object> entry : variables.entrySet()) {
      final String name = entry.getKey();
      final Object value = entry.getValue();
      result.put(name, convertVariableValue(value));
    }
    return result;
  }

  /**
   * Converts a single variable value to a FHIRPath Collection.
   *
   * @param value the variable value
   * @return a Collection representing the value
   */
  @Nonnull
  private static Collection convertVariableValue(@Nonnull final Object value) {
    return switch (value) {
      case final String s -> StringCollection.fromValue(s);
      case final Integer i -> IntegerCollection.fromValue(i);
      case final Boolean b -> BooleanCollection.fromValue(b);
      case final BigDecimal bd -> DecimalCollection.fromLiteral(bd.toPlainString());
      // Handle doubles from py4j which may send decimals as doubles.
      case final Double d -> DecimalCollection.fromLiteral(BigDecimal.valueOf(d).toPlainString());
      // Fallback for other numeric types.
      case final Number n ->
          DecimalCollection.fromLiteral(new BigDecimal(n.toString()).toPlainString());
      default ->
          throw new IllegalArgumentException(
              "Unsupported variable type: " + value.getClass().getSimpleName());
    };
  }

  /**
   * Evaluates the main expression once per context item, returning flat results.
   *
   * @param resourceDf the encoded resource as a single-row DataFrame
   * @param parser the FHIRPath parser
   * @param mainPath the parsed main expression
   * @param contextExpression the context expression string
   * @param evaluator the single resource evaluator
   * @param expectedReturnType the inferred return type of the main expression
   * @param traceCollector the trace collector for capturing trace() output
   * @return a SingleInstanceEvaluationResult with results for each context item
   */
  @Nonnull
  private static SingleInstanceEvaluationResult evaluateWithContext(
      @Nonnull final Dataset<Row> resourceDf,
      @Nonnull final Parser parser,
      @Nonnull final FhirPath mainPath,
      @Nonnull final String contextExpression,
      @Nonnull final SingleResourceEvaluator evaluator,
      @Nonnull final String expectedReturnType,
      @Nonnull final ListTraceCollector traceCollector) {

    // Parse and evaluate the context expression.
    final FhirPath contextPath = parser.parse(contextExpression);
    final Collection contextCollection = evaluator.evaluate(contextPath);
    final Column contextColumn = contextCollection.getColumn().getValue();

    // Collect context items.
    final Dataset<Row> contextDf = resourceDf.select(contextColumn.alias("_ctx"));
    final List<Row> contextRows = contextDf.collectAsList();

    if (contextRows.isEmpty() || contextRows.getFirst().isNullAt(0)) {
      return new SingleInstanceEvaluationResult(new ArrayList<>(), expectedReturnType, List.of());
    }

    // The context value is an array; evaluate the main expression against the input context
    // for each item. In flat schema mode, the evaluator uses the context expression to scope
    // the main expression, so we compose the expressions.
    final FhirPath composedPath = contextPath.andThen(mainPath);
    final Collection composedResult = evaluator.evaluate(composedPath);
    final Column composedColumn = composedResult.getColumn().getValue();

    final List<TypedValue> results = collectResults(resourceDf, composedColumn, expectedReturnType);
    final List<TraceResult> traces = buildTraceResults(traceCollector);
    return new SingleInstanceEvaluationResult(results, expectedReturnType, traces);
  }

  /**
   * Determines the return type from a Collection's type information.
   *
   * @param collection the collection to inspect
   * @return the type name as a string
   */
  @Nonnull
  private static String determineReturnType(@Nonnull final Collection collection) {
    // Prefer the FHIR defined type code (e.g., "HumanName", "code", "string").
    return collection
        .getFhirType()
        .map(org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType::toCode)
        .orElseGet(
            () -> collection.getType().map(FhirPathType::getTypeSpecifier).orElse("unknown"));
  }

  /**
   * Collects materialised results from a Dataset by applying a result Column.
   *
   * @param resourceDf the single-row encoded resource Dataset
   * @param resultColumn the Column expression for the result
   * @param typeName the FHIR type name for the result values
   * @return a list of typed values
   */
  @Nonnull
  private static List<TypedValue> collectResults(
      @Nonnull final Dataset<Row> resourceDf,
      @Nonnull final Column resultColumn,
      @Nonnull final String typeName) {

    final Dataset<Row> resultDf = resourceDf.select(resultColumn.alias("_result"));
    final List<Row> rows = resultDf.collectAsList();

    if (rows.isEmpty()) {
      return new ArrayList<>();
    }

    final Row row = rows.getFirst();
    if (row.isNullAt(0)) {
      return new ArrayList<>();
    }

    final Object rawValue = row.get(0);
    return materialiseValues(rawValue, typeName);
  }

  /**
   * Materialises a raw Spark value into a list of TypedValue objects.
   *
   * <p>Array values are expanded into individual typed values. Complex struct values are sanitised
   * and serialised as JSON strings.
   *
   * @param rawValue the raw value from Spark
   * @param typeName the FHIR type name
   * @return a list of typed values
   */
  @Nonnull
  private static List<TypedValue> materialiseValues(
      @Nonnull final Object rawValue, @Nonnull final String typeName) {
    final List<TypedValue> results = new ArrayList<>();

    if (rawValue instanceof final scala.collection.Seq<?> seq) {
      // Array/list result - expand each element.
      for (int i = 0; i < seq.size(); i++) {
        final Object element = seq.apply(i);
        if (element != null) {
          results.add(new TypedValue(typeName, convertValue(element)));
        }
      }
    } else {
      // Singular result.
      results.add(new TypedValue(typeName, convertValue(rawValue)));
    }
    return results;
  }

  /**
   * Converts a raw Spark value to a Java value suitable for the result.
   *
   * <p>Struct types (complex FHIR types) are sanitised and converted to JSON strings. Primitive
   * types are returned as-is. Null values are returned as-is.
   *
   * @param value the raw value
   * @return the converted value
   */
  @Nullable
  private static Object convertValue(@Nullable final Object value) {
    if (value instanceof final Row row) {
      // Complex type: sanitise and convert to JSON string representation.
      return rowToJson(row);
    }
    return value;
  }

  /**
   * Converts a Spark Row to a JSON string representation, stripping synthetic fields first.
   *
   * @param row the row to convert
   * @return a JSON string with synthetic fields removed
   */
  @Nonnull
  static String rowToJson(@Nonnull final Row row) {
    final Row sanitised = sanitiseRow(row);
    return sanitised.json();
  }

  /**
   * Strips synthetic fields and null-valued fields from a Spark Row, recursively handling nested
   * struct types.
   *
   * @param row the row to sanitise
   * @return a new Row with synthetic and null-valued fields removed
   */
  @Nonnull
  static Row sanitiseRow(@Nonnull final Row row) {
    final StructType schema = row.schema();
    if (schema == null) {
      return row;
    }

    final List<StructField> filteredFields = new ArrayList<>();
    final List<Object> filteredValues = new ArrayList<>();

    for (final StructField field : schema.fields()) {
      if (!SyntheticFieldUtils.isSyntheticField(field.name())) {
        final Object value = row.get(row.fieldIndex(field.name()));
        // Skip fields with null values.
        if (value == null) {
          continue;
        }
        // Recursively sanitise nested struct values, updating the parent field's dataType
        // to match the sanitised schema. This is critical because Row.json() uses the parent's
        // dataType (not the nested row's own schema) to map field names positionally.
        if (value instanceof final Row nestedRow) {
          final Row sanitisedNested = sanitiseRow(nestedRow);
          filteredValues.add(sanitisedNested);
          filteredFields.add(
              new StructField(
                  field.name(), sanitisedNested.schema(), field.nullable(), field.metadata()));
        } else {
          filteredFields.add(field);
          filteredValues.add(value);
        }
      }
    }

    final StructType filteredSchema = new StructType(filteredFields.toArray(new StructField[0]));
    return new GenericRowWithSchema(filteredValues.toArray(), filteredSchema);
  }

  /**
   * Builds trace results from the collector, grouping entries by label and sanitizing Row values.
   *
   * @param collector the trace collector containing raw entries
   * @return a list of trace results grouped by label, in order of first appearance
   */
  @Nonnull
  static List<TraceResult> buildTraceResults(@Nonnull final ListTraceCollector collector) {
    final List<TraceEntry> entries = collector.getEntries();
    if (entries.isEmpty()) {
      return List.of();
    }

    // Group entries by label, preserving insertion order.
    final Map<String, List<TraceEntry>> grouped = new LinkedHashMap<>();
    for (final TraceEntry entry : entries) {
      grouped.computeIfAbsent(entry.label(), k -> new ArrayList<>()).add(entry);
    }

    // Build TraceResult for each group, expanding array values into individual entries.
    final List<TraceResult> results = new ArrayList<>();
    for (final Map.Entry<String, List<TraceEntry>> group : grouped.entrySet()) {
      final String label = group.getKey();
      final List<TypedValue> values = new ArrayList<>();
      for (final TraceEntry entry : group.getValue()) {
        expandTraceValue(entry.fhirType(), entry.value(), values);
      }
      results.add(new TraceResult(label, values));
    }
    return results;
  }

  /**
   * Expands a trace value into individual typed values. If the value is a Scala collection (which
   * occurs when the traced column is an array), each element is extracted and sanitized
   * individually. Otherwise the value is sanitized and added directly.
   *
   * @param fhirType the FHIR type code
   * @param value the raw trace value (may be a Scala Seq for array columns)
   * @param target the list to add expanded values to
   */
  private static void expandTraceValue(
      @Nonnull final String fhirType,
      @Nullable final Object value,
      @Nonnull final List<TypedValue> target) {
    if (value instanceof final scala.collection.Seq<?> seq) {
      for (int i = 0; i < seq.size(); i++) {
        final Object element = seq.apply(i);
        target.add(new TypedValue(fhirType, convertValue(element)));
      }
    } else {
      target.add(new TypedValue(fhirType, convertValue(value)));
    }
  }
}
