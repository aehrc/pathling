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

package au.csiro.pathling.views;

import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluators.SingleEvaluatorFactory;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.projection.ColumnSelection;
import au.csiro.pathling.projection.ExecutionContext;
import au.csiro.pathling.projection.GroupingSelection;
import au.csiro.pathling.projection.Projection;
import au.csiro.pathling.projection.ProjectionClause;
import au.csiro.pathling.projection.RequestedColumn;
import au.csiro.pathling.projection.UnionSelection;
import au.csiro.pathling.projection.UnnestingSelection;
import au.csiro.pathling.utilities.Lists;
import au.csiro.pathling.validation.ValidationUtils;
import au.csiro.pathling.views.ansi.AnsiSqlTypeParser;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Converts a {@link FhirView} to an executable Spark SQL query.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class FhirViewExecutor {

  private static final String HL7_FHIR_TYPE_URI_PREFIX = "http://hl7.org/fhir/StructureDefinition/";

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final Parser parser;

  /**
   * @param fhirContext The FHIR context to use for the execution context
   * @param sparkSession The Spark session to use for the execution context
   * @param dataset The data source to use for the execution context
   */
  public FhirViewExecutor(@Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final DataSource dataset) {
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataset;
    this.parser = new Parser();
  }

  /**
   * Builds a Spark SQL query for a given FHIR view.
   *
   * @param view the FHIR view to build the query for
   * @return a {@link Dataset} that represents an executable form of the query
   */
  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final FhirView view) {
    // Validate the view using JSR-380 validation
    ValidationUtils.ensureValid(view, "Valid SQL on FHIR view");

    final ExecutionContext executionContext = new ExecutionContext(sparkSession,
        SingleEvaluatorFactory.of(fhirContext, dataSource)
    );
    final Projection projection = buildProjection(view);
    return projection.execute(executionContext);
  }

  /**
   * Converts a {@link FhirView} to an {@link Projection}, which is an abstract representation of
   * the view that can we use for optimisation and execution.
   *
   * @param fhirView the FHIR view to convert
   * @return the converted view
   */
  @Nonnull
  private Projection buildProjection(@Nonnull final FhirView fhirView) {
    // Convert the select clause into a list of ProjectionClause objects.
    final List<ProjectionClause> selectionComponents = fhirView.getSelect().stream()
        .map(this::parseSelection)
        .toList();

    // Create a GroupingSelection object that represents all the select components.
    final ProjectionClause selection = new GroupingSelection(selectionComponents);

    // Convert the where clause into a {@link ProjectionClause} object, if there is one.
    final Optional<ProjectionClause> where = parseWhere(fhirView);

    // Pass the constants through so that they can be substituted into the FHIRPath expressions
    // during evaluation.

    // Create the Projection object that represents the view.
    return new Projection(ResourceType.fromCode(fhirView.getResource()), fhirView.getConstant(),
        selection,
        where);
  }

  /**
   * Parses a {@link SelectClause} into a {@link ProjectionClause} object.
   *
   * @param select the select clause to parse
   * @return the parsed selection
   */
  @Nonnull
  private ProjectionClause parseSelection(@Nonnull final SelectClause select) {
    // There are three types of select:
    // (1) A direct column selection
    // (2) A "for each" selection, which unnests a set of sub-select based on a parent path
    // (3) A "for each or null" selection, which is the same as (2) but creates a null row if
    //     the parent path evaluates to an empty collection

    if (isNull(select.getForEach()) && isNull(select.getForEachOrNull())) {
      // If this is a direct column selection, we use a FromSelection. This will produce the 
      // cartesian product of the collections that are produced by the FHIRPath expressions.
      return new GroupingSelection(parseSubSelection(select));
    } else if (nonNull(select.getForEach()) && nonNull(select.getForEachOrNull())) {
      throw new IllegalStateException(
          "Both forEach and forEachOrNull are set in the select clause");
    } else if (nonNull(select.getForEach())) {
      // If this is a "for each" selection, we use a ForEachSelectionX. This will produce a row for
      // each item in the collection produced by the parent path.
      return new UnnestingSelection(parser.parse(requireNonNull(select.getForEach())),
          parseSubSelection(select), false);
    } else { // this implies that forEachOrNull is non-null
      // If this is a "for each or null" selection, we use a ForEachSelectionX with a flag set to
      // true. This will produce a row for each item in the collection produced by the parent path,
      // or a single null row if the parent path evaluates to an empty collection.
      return new UnnestingSelection(parser.parse(requireNonNull(select.getForEachOrNull())),
          parseSubSelection(select), true);
    }
  }

  /**
   * Parses the nested selections inside a selection into a list of {@link ProjectionClause}
   * objects.
   *
   * @param selectClause the select clause to parse
   * @return the parsed sub-selection
   */
  @Nonnull
  private List<ProjectionClause> parseSubSelection(@Nonnull final SelectClause selectClause) {
    final boolean columnsPresent = !selectClause.getColumn().isEmpty();
    final boolean unionPresent = !selectClause.getUnionAll().isEmpty();

    final Stream<ProjectionClause> columnSelection;
    if (columnsPresent) {
      // If there are columns present, convert them into a list of {@link RequestedColumn} objects.
      final List<RequestedColumn> requestedColumns = selectClause.getColumn().stream()
          .map(this::buildRequestedColumn)
          .toList();
      columnSelection = Stream.of(new ColumnSelection(requestedColumns));
    } else {
      columnSelection = Stream.empty();
    }

    final Stream<ProjectionClause> unionAll;
    if (unionPresent) {
      // If there are unionAll clauses present, parse them into a list of {@link SelectClause} 
      // objects.
      final List<SelectClause> select = selectClause.getUnionAll();
      unionAll = Stream.of(new UnionSelection(select.stream()
          .map(this::parseSelection)
          .toList()));
    } else {
      unionAll = Stream.empty();
    }

    // Put the column selections, sub-selects and union selections together, flatten them and make
    // them into a list.
    return Stream.of(
            columnSelection,
            selectClause.getSelect().stream().map(this::parseSelection),
            unionAll
        )
        .flatMap(Function.identity())
        .toList();
  }

  /**
   * Parses a {@link Column} into a {@link RequestedColumn} object.
   *
   * @param column the column to parse
   * @return the parsed column
   */
  @Nonnull
  private RequestedColumn buildRequestedColumn(@Nonnull final Column column) {
    // Parse the FHIRPath expression using the parser.
    final FhirPath path = parser.parse(column.getPath());

    Optional<FHIRDefinedType> type = Optional.empty();
    if (column.getType() != null) {
      // Replace the HL7 FHIR type URI prefix with an empty string to get the FHIR type.
      final String fhirType = column.getType()
          .replaceFirst("^" + Pattern.quote(HL7_FHIR_TYPE_URI_PREFIX), "");

      // Attempt to retrieve the FHIR type.
      try {
        type = Optional.ofNullable(FHIRDefinedType.fromCode(fhirType));
      } catch (final FHIRException ignored) {
        // If the FHIR type is not valid, we ignore it and leave the type as empty.
      }
    }

    // Create a RequestedColumn object that represents the column.
    return new RequestedColumn(path, column.getName(), column.isCollection(), type,
        getSqlTypeHint(column));
  }

  /**
   * Gets the SQL type hint for a column, if it exists.
   *
   * @param column the column to get the SQL type hint for
   * @return an Optional containing the SQL type hint, or an empty Optional if there is no hint
   */
  @Nonnull
  private Optional<DataType> getSqlTypeHint(@Nonnull final Column column) {
    // Look for the ANSI type tag in the column's tags
    return column.getTagValue(ColumnTag.ANSI_TYPE_TAG)
        .map(this::convertAnsiTypeToSparkType);
  }

  /**
   * Converts an ANSI SQL type string to a Spark SQL DataType.
   *
   * @param ansiType the ANSI SQL type string
   * @return the corresponding Spark SQL DataType
   */
  @Nonnull
  private DataType convertAnsiTypeToSparkType(@Nonnull final String ansiType) {
    return AnsiSqlTypeParser.parseType(ansiType);
  }


  /**
   * Parses the where clause from a FHIR view into a {@link ProjectionClause} object.
   *
   * @param fhirView the FHIR view to parse the where clause from
   * @return the parsed where clause
   */
  @Nonnull
  private Optional<ProjectionClause> parseWhere(@Nonnull final FhirView fhirView) {
    final List<RequestedColumn> whereComponents = Optional.ofNullable(fhirView.getWhere())
        // Convert the Optional to a stream.
        .stream()
        // Convert the stream of lists to a stream of WhereClause objects.
        .flatMap(List::stream)
        // Get the FHIRPath expression from each WhereClause.
        .map(WhereClause::getExpression)
        // Parse the FHIRPath expression.
        .map(parser::parse)
        // Create a PrimitiveSelection object for each FHIRPath.
        .map(path -> new RequestedColumn(path, randomAlias(), false, Optional.empty(),
            Optional.empty()))
        .toList();

    // If there are no where components, return an empty Optional. 
    return Lists.optionalOf(whereComponents)
        // Otherwise, return a ColumnSelection object that represents all the where components.
        .map(ColumnSelection::new);
  }

}
