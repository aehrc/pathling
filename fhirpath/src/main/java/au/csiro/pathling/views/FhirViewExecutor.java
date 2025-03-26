package au.csiro.pathling.views;

import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluators.SingleEvaluatorFactory;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.utilities.Lists;
import au.csiro.pathling.view.ColumnSelection;
import au.csiro.pathling.view.ExecutionContext;
import au.csiro.pathling.view.GroupingSelection;
import au.csiro.pathling.view.Projection;
import au.csiro.pathling.view.ProjectionClause;
import au.csiro.pathling.view.RequestedColumn;
import au.csiro.pathling.view.UnionSelection;
import au.csiro.pathling.view.UnnestingSelection;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
    final List<ConstantDeclaration> constants = Optional.ofNullable(fhirView.getConstant())
        .orElse(Collections.emptyList());

    // Create the Projection object that represents the view.
    return new Projection(ResourceType.fromCode(fhirView.getResource()), constants, selection,
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

    if (select instanceof ColumnSelect) {
      // If this is a direct column selection, we use a FromSelection. This will produce the 
      // cartesian product of the collections that are produced by the FHIRPath expressions.
      return new GroupingSelection(parseSubSelection(select));

    } else if (select instanceof final ForEachSelect forEachSelect) {
      // If this is a "for each" selection, we use a ForEachSelectionX. This will produce a row for
      // each item in the collection produced by the parent path.
      return new UnnestingSelection(parser.parse(requireNonNull(forEachSelect.getPath())),
          parseSubSelection(select), false);

    } else if (select instanceof final ForEachOrNullSelect forEachOrNullSelect) {
      // If this is a "for each or null" selection, we use a ForEachSelectionX with a flag set to
      // true. This will produce a row for each item in the collection produced by the parent path,
      // or a single null row if the parent path evaluates to an empty collection.
      return new UnnestingSelection(parser.parse(requireNonNull(forEachOrNullSelect.getPath())),
          parseSubSelection(select), true);

    } else {
      // This will only ever happen if a new type of select clause is added to the FhirView
      // model that is not handled here.
      throw new IllegalStateException("Unknown select clause type: " + select.getClass());
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
      }
    }

    // Create a RequestedColumn object that represents the column.
    return new RequestedColumn(path, column.getName(), column.isCollection(), type);
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
        .map(path -> new RequestedColumn(path, randomAlias(), false, Optional.empty()))
        .toList();

    // If there are no where components, return an empty Optional. 
    return Lists.optionalOf(whereComponents)
        // Otherwise, return a ColumnSelection object that represents all the where components.
        .map(ColumnSelection::new);
  }

}
