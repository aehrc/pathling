package au.csiro.pathling.extract;

import static java.util.stream.Collectors.toList;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.This;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.utilities.Strings;
import au.csiro.pathling.view.ColumnSelection;
import au.csiro.pathling.view.ExecutionContext;
import au.csiro.pathling.view.GroupingSelection;
import au.csiro.pathling.view.Projection;
import au.csiro.pathling.view.ProjectionClause;
import au.csiro.pathling.view.RequestedColumn;
import au.csiro.pathling.view.UnnestingSelection;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Builds the overall query responsible for executing an extract request.
 *
 * @author John Grimes
 */
@Slf4j
@NotImplemented
public class ExtractQueryExecutor extends QueryExecutor {

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final Parser parser;

  public ExtractQueryExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    super(configuration, fhirContext, sparkSession, dataSource, terminologyServiceFactory);
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataSource;
    this.parser = new Parser();
  }

  /**
   * Builds up the query for an extract request.
   *
   * @param query an {@link ExtractRequest}
   * @return an uncollected {@link Dataset}
   */
  @SuppressWarnings("WeakerAccess")
  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final ExtractRequest query) {
    final ExecutionContext executionContext = new ExecutionContext(sparkSession, fhirContext,
        dataSource);
    final Projection projection = buildProjection(query);
    return projection.execute(executionContext);
  }

  @Nonnull
  private Projection buildProjection(@Nonnull final ExtractRequest query) {
    // Parse each column in the query into a RequestedColumn object, each containing a FhirPath and 
    // other information such as the requested column name and type assertions.
    final List<RequestedColumn> columns = query.getColumns().stream()
        .map(this::parseColumn)
        .collect(toList());

    // Build a ProjectionClause for each of the columns.
    final List<ProjectionClause> selectionComponents = columns.stream()
        .map((col) -> buildSelection(List.of(col)))
        .collect(toList());

    // Group the ProjectionClauses into a select.
    final ProjectionClause selection = new GroupingSelection(selectionComponents);

    // Return the final Projection object.
    // TODO: Implement filtering.
    return new Projection(query.getSubjectResource(), Collections.emptyList(), selection,
        Optional.empty());
  }

  @Nonnull
  private RequestedColumn parseColumn(@Nonnull final ExpressionWithLabel expression) {
    final FhirPath path = parser.parse(expression.getExpression());

    // If no label is provided, generate a random one.
    final String columnName = Optional.ofNullable(expression.getLabel())
        .orElse(Strings.randomAlias());

    return new RequestedColumn(path, columnName, true, Optional.empty());
  }

  @Nonnull
  private ProjectionClause buildSelection(@Nonnull final List<RequestedColumn> columns) {
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("Empty column list");
    }
    final RequestedColumn head = columns.get(0);

    // If there is only one column, we unnest it, terminating the recursion with a $this selection.
    if (columns.size() == 1) {
      final ColumnSelection thisSelection = new ColumnSelection(
          List.of(new RequestedColumn(new This(), head.getName(), true,
              Optional.empty())));
      return new UnnestingSelection(head.getPath(), List.of(thisSelection), true);
    }

    // If there is more than one column, we recurse on the tail and wrap it in an unnesting.
    final List<RequestedColumn> tail = columns.subList(1, columns.size());
    final ProjectionClause tailProjection = buildSelection(tail);
    return new UnnestingSelection(head.getPath(), List.of(tailProjection), true);
  }

  /**
   * Builds up the query for an extract request, with a constraint.
   *
   * @param query an {@link ExtractRequest}
   * @param constraint a {@link ProjectionConstraint}
   * @return an uncollected {@link Dataset}
   */
  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final ExtractRequest query,
      @Nonnull final ProjectionConstraint constraint) {
    return buildQuery(query);
  }

}
