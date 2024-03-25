package au.csiro.pathling.views;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.utilities.Lists;
import au.csiro.pathling.view.ColumnSelection;
import au.csiro.pathling.view.ExecutionContext;
import au.csiro.pathling.view.ExtractViewX;
import au.csiro.pathling.view.ForEachSelectionX;
import au.csiro.pathling.view.FromSelectionX;
import au.csiro.pathling.view.PrimitiveSelection;
import au.csiro.pathling.view.SelectionX;
import au.csiro.pathling.view.UnionAllSelection;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Converts a {@link FhirView} to an executable Spark SQL query.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class FhirViewExecutor {

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final Parser parser;

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
    final ExecutionContext executionContext = new ExecutionContext(sparkSession, fhirContext,
        dataSource);
    final ExtractViewX extractView = toExtractView(view);
    return extractView.evaluate(executionContext);
  }

  /**
   * Converts a {@link FhirView} to an {@link ExtractViewX}, which is an abstract representation of
   * the view that can we use for optimisation and execution.
   *
   * @param fhirView the FHIR view to convert
   * @return the converted view
   */
  @Nonnull
  private ExtractViewX toExtractView(@Nonnull final FhirView fhirView) {
    // Parse the selection and where clauses from the FHIR view into {@link SelectionX} objects.
    // Convert the select clause into a list of {@link SelectionX} objects.
    final List<SelectionX> selectionComponents = fhirView.getSelect().stream()
        .map(this::parseSelection)
        .collect(toUnmodifiableList());

    // Create a FromSelectionX object that represents the entire selection clause.
    final SelectionX selection = new FromSelectionX(selectionComponents);
    final Optional<SelectionX> where = parseWhere(fhirView);

    // We pass the constants through so that they can be substituted into the FHIRPath expressions
    // during evaluation.
    final List<ConstantDeclaration> constants = Optional.ofNullable(fhirView.getConstant())
        .orElse(Collections.emptyList());

    // Create the ExtractViewX object that represents the view.
    return new ExtractViewX(ResourceType.fromCode(fhirView.getResource()), constants, selection,
        where);
  }

  /**
   * Parses a {@link SelectClause} into a {@link SelectionX} object.
   *
   * @param select the select clause to parse
   * @return the parsed selection
   */
  @Nonnull
  private SelectionX parseSelection(@Nonnull final SelectClause select) {
    // There are three types of select:
    // (1) A direct column selection
    // (2) A "for each" selection, which unnests a set of sub-select based on a parent path
    // (3) A "for each or null" selection, which is the same as (2) but creates a null row if
    //     the parent path evaluates to an empty collection

    if (select instanceof ColumnSelect) {
      // If this is a direct column selection, we use a FromSelection. This will produce the 
      // cartesian product of the collections that are produced by the FHIRPath expressions.
      return new FromSelectionX(parseSubSelection(select));

    } else if (select instanceof ForEachSelect) {
      // If this is a "for each" selection, we use a ForEachSelectionX. This will produce a row for
      // each item in the collection produced by the parent path.
      final ForEachSelect forEachSelect = (ForEachSelect) select;
      return new ForEachSelectionX(parser.parse(requireNonNull(forEachSelect.getPath())),
          parseSubSelection(select), false);

    } else if (select instanceof ForEachOrNullSelect) {
      // If this is a "for each or null" selection, we use a ForEachSelectionX with a flag set to
      // true. This will produce a row for each item in the collection produced by the parent path,
      // or a single null row if the parent path evaluates to an empty collection.
      final ForEachOrNullSelect forEachOrNullSelect = (ForEachOrNullSelect) select;
      return new ForEachSelectionX(parser.parse(requireNonNull(forEachOrNullSelect.getPath())),
          parseSubSelection(select), true);

    } else {
      // This will only ever happen if a new type of select clause is added to the FhirView
      // model that is not handled here.
      throw new IllegalStateException("Unknown select clause type: " + select.getClass());
    }
  }

  /**
   * Parses the nested selections inside a selection into a list of {@link SelectionX} objects.
   *
   * @param selectClause the select clause to parse
   * @return the parsed sub-selection
   */
  @Nonnull
  private List<SelectionX> parseSubSelection(@Nonnull final SelectClause selectClause) {
    final boolean columnsPresent = !selectClause.getColumn().isEmpty();
    final boolean unionPresent = !selectClause.getUnionAll().isEmpty();

    final Stream<SelectionX> columnSelection;
    if (columnsPresent) {
      // If there are columns present, parse them into a list of {@link PrimitiveSelection} objects.
      final List<PrimitiveSelection> columnSelections = selectClause.getColumn().stream()
          .map(this::parseColumnSelection)
          .collect(toUnmodifiableList());
      columnSelection = Stream.of(new ColumnSelection(columnSelections));
    } else {
      columnSelection = Stream.empty();
    }

    final Stream<SelectionX> unionAll;
    if (unionPresent) {
      // If there are unionAll clauses present, parse them into a list of {@link SelectionX} 
      // objects.
      final List<SelectClause> select = selectClause.getUnionAll();
      unionAll = Stream.of(new UnionAllSelection(select.stream()
          .map(this::parseSelection)
          .collect(toUnmodifiableList())));
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
        .collect(toUnmodifiableList());
  }

  /**
   * Parses a {@link Column} into a {@link PrimitiveSelection} object.
   *
   * @param column the column to parse
   * @return the parsed column
   */
  @Nonnull
  private PrimitiveSelection parseColumnSelection(@Nonnull final Column column) {
    // Parse the FHIRPath expression using the parser.
    final FhirPath path = parser.parse(column.getPath());

    // Create a PrimitiveSelection object that represents the column.
    return new PrimitiveSelection(path, Optional.ofNullable(column.getName()),
        column.isCollection());
  }

  /**
   * Parses the where clause from a FHIR view into a {@link SelectionX} object.
   *
   * @param fhirView the FHIR view to parse the where clause from
   * @return the parsed where clause
   */
  @Nonnull
  private Optional<SelectionX> parseWhere(@Nonnull final FhirView fhirView) {
    final List<PrimitiveSelection> whereComponents = Optional.ofNullable(fhirView.getWhere())
        // Convert the Optional to a stream.
        .stream()
        // Convert the stream of lists to a stream of WhereClause objects.
        .flatMap(List::stream)
        // Get the FHIRPath expression from each WhereClause.
        .map(WhereClause::getExpression)
        // Parse the FHIRPath expression.
        .map(parser::parse)
        // Create a PrimitiveSelection object for each FHIRPath.
        .map(PrimitiveSelection::new)
        .collect(toUnmodifiableList());

    // If there are no where components, return an empty Optional. 
    return Lists.optionalOf(whereComponents)
        // Otherwise, return a ColumnSelection object that represents all the where components.
        .map(ColumnSelection::new);
  }

}
