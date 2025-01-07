package au.csiro.pathling.extract;

import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.view.ColumnSelection;
import au.csiro.pathling.view.ExecutionContext;
import au.csiro.pathling.view.GroupingSelection;
import au.csiro.pathling.view.Projection;
import au.csiro.pathling.view.ProjectionClause;
import au.csiro.pathling.view.RequestedColumn;
import au.csiro.pathling.view.UnnestingSelection;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Builds the overall query responsible for executing an extract request.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/libraries/fhirpath-query#extract">Pathling
 * documentation - extract</a>
 */
@Slf4j
public class ExtractQueryExecutor extends QueryExecutor {

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final Parser parser;

  /**
   * @param configuration A {@link QueryConfiguration} that controls the behaviour of the query
   * @param fhirContext A {@link FhirContext} for querying FHIR definitions
   * @param sparkSession A {@link SparkSession} for executing the query
   * @param dataSource A {@link DataSource} for reading data
   * @param terminologyServiceFactory An optional {@link TerminologyServiceFactory} for resolving
   * terminology queries
   */
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
    return buildQuery(query, ProjectionConstraint.UNCONSTRAINED);
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
    final ExecutionContext executionContext = new ExecutionContext(sparkSession, fhirContext,
        dataSource);

    // Build a Projection from the ExtractRequest.
    final Projection projection = buildProjection(query, constraint);

    // Execute the Projection to get the result dataset.
    Dataset<Row> result = projection.execute(executionContext);

    // Rename each column in the result to match the requested column names.
    final List<String> requestedColumnNames = query.getColumns().stream()
        .map(ExpressionWithLabel::getLabel)
        .toList();
    final List<String> resultColumnNames = Arrays.asList(result.columns());

    for (int i = 0; i < requestedColumnNames.size(); i++) {
      final String requestedName = requestedColumnNames.get(i);
      final String resultName = resultColumnNames.get(i);
      result = requestedName != null
               ? result.withColumnRenamed(resultName, requestedName)
               : result;
    }

    return result;
  }

  @Nonnull
  private Projection buildProjection(@Nonnull final ExtractRequest query,
      final ProjectionConstraint constraint) {
    // Parse each column in the query into a FhirPath object.
    final List<FhirPath> columns = query.getColumns().stream()
        .map(expression -> parser.parse(expression.getExpression()))
        .collect(toList());

    // Build the column selection.
    final ProjectionClause selection = buildSelectClause(columns);

    // Build the filters.
    final Optional<ProjectionClause> filters = buildFilterClause(query.getFilters());

    // Return the final Projection object.
    return new Projection(query.getSubjectResource(), Collections.emptyList(), selection, filters,
        constraint);
  }

  @Nonnull
  private ProjectionClause buildSelectClause(@Nonnull final List<FhirPath> paths) {
    if (paths.isEmpty()) {
      throw new IllegalArgumentException("Empty column list");
    }

    // If there is only one path, and it is a reference to $this, return a simple column selection.
    // This is the terminal state of the recursion.
    if (paths.size() == 1 && paths.get(0).isNull()) {
      final RequestedColumn requestedColumn = new RequestedColumn(paths.get(0),
          randomAlias(), false, Optional.empty());
      return new ColumnSelection(List.of(requestedColumn));
    }

    // Group the paths by their first element. We use a LinkedHashMap to preserve the order.
    final Map<FhirPath, List<FhirPath>> groupedPaths = paths.stream()
        .collect(groupingBy(FhirPath::first, LinkedHashMap::new, toList()));

    final List<ProjectionClause> selects = groupedPaths.entrySet().stream()
        .map(entry -> {
          // Take the suffix of each path and build a new ProjectionClause from it. The suffix
          // is all the components of the traversal except the first one.
          final List<ProjectionClause> tail = entry.getValue().stream()
              .map(FhirPath::suffix)
              .map(path -> buildSelectClause(List.of(path)))
              .collect(toList());
          // Create an UnnestingSelection with a base corresponding to the group, and the 
          // projections representing the suffixes as the components.
          return new UnnestingSelection(entry.getKey(), tail, true);
        })
        .collect(toList());

    // If there is more than one select, return a GroupingSelection.
    // Otherwise, return the single select by itself.
    return selects.size() > 1
           ? new GroupingSelection(selects)
           : selects.get(0);
  }

  @Nonnull
  private Optional<ProjectionClause> buildFilterClause(@Nonnull final List<String> filters) {
    // If there are no filters, return an empty result.
    if (filters.isEmpty()) {
      return Optional.empty();
    }

    // Parse each filter into a FhirPath object.
    final List<RequestedColumn> selections = filters.stream()
        .map(parser::parse)
        .map(expression ->
            new RequestedColumn(expression, randomAlias(), false, Optional.empty()))
        .collect(toList());

    // Return a ColumnSelection object containing all the filters.
    return Optional.of(new ColumnSelection(selections));
  }

}
