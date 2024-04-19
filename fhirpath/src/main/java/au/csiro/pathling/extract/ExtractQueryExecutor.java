package au.csiro.pathling.extract;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.query.ExpressionWithLabel.labelsAsStream;
import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class ExtractQueryExecutor extends QueryExecutor {

  public ExtractQueryExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    super(configuration, fhirContext, sparkSession, dataSource, terminologyServiceFactory);
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
    // Build a new expression parser, and parse all the column expressions within the query.
    final ResourcePath inputContext = ResourcePath
        .build(getFhirContext(), getDataSource(), query.getSubjectResource(),
            query.getSubjectResource().toCode(), true);
    // The context of evaluation is a single resource.
    final ParserContext parserContext = buildParserContext(inputContext,
        Collections.singletonList(inputContext.getIdColumn()));
    final List<FhirPathAndContext> columnParseResult =
        parseMaterializableExpressions(parserContext, query.getColumns(), "Column");
    final List<FhirPath> columnPaths = columnParseResult.stream()
        .map(FhirPathAndContext::getFhirPath)
        .collect(Collectors.toUnmodifiableList());

    // Join all the column expressions together.
    final FhirPathContextAndResult columnJoinResult = joinColumns(columnParseResult);
    final Dataset<Row> columnJoinResultDataset = columnJoinResult.getResult();
    final Dataset<Row> trimmedDataset = trimTrailingNulls(inputContext.getIdColumn(),
        columnPaths, columnJoinResultDataset);

    // Apply the filters.
    final List<String> filters = query.getFilters();
    final Dataset<Row> filteredDataset = filterDataset(inputContext, filters, trimmedDataset,
        Column::and);

    // Select the column values.
    final Column idColumn = inputContext.getIdColumn();
    final Column[] columnValues = labelColumns(
        columnPaths.stream().map(path -> ((Materializable<?>) path).getExtractableColumn()),
        labelsAsStream(query.getColumnsWithLabels())
    ).toArray(Column[]::new);
    final Dataset<Row> selectedDataset = filteredDataset.select(columnValues)
        .filter(idColumn.isNotNull());

    // If there is a limit, apply it.
    return query.getLimit().isPresent()
           ? selectedDataset.limit(query.getLimit().get())
           : selectedDataset;
  }

  @Nonnull
  private FhirPathContextAndResult joinColumns(
      @Nonnull final Collection<FhirPathAndContext> columnsAndContexts) {
    // Sort the columns in descending order of expression length.
    final List<FhirPathAndContext> sortedColumnsAndContexts = columnsAndContexts.stream()
        .sorted(Comparator.<FhirPathAndContext>comparingInt(p ->
            p.getFhirPath().getExpression().length()).reversed())
        .collect(Collectors.toList());

    FhirPathContextAndResult result = null;
    check(sortedColumnsAndContexts.size() > 0);
    for (final FhirPathAndContext current : sortedColumnsAndContexts) {
      if (result != null) {
        // Get the set of unique prefixes from the two parser contexts, and sort them in descending
        // order of prefix length.
        final Set<String> prefixes = new HashSet<>();
        final Map<String, Column> resultNodeIds = result.getContext().getNodeIdColumns();
        final Map<String, Column> currentNodeIds = current.getContext().getNodeIdColumns();
        prefixes.addAll(resultNodeIds.keySet());
        prefixes.addAll(currentNodeIds.keySet());
        final List<String> sortedCommonPrefixes = new ArrayList<>(prefixes).stream()
            .sorted(Comparator.comparingInt(String::length).reversed())
            .collect(Collectors.toList());
        final FhirPathContextAndResult finalResult = result;

        // Find the longest prefix that is common to the two expressions.
        final Optional<String> commonPrefix = sortedCommonPrefixes.stream()
            .filter(p -> finalResult.getFhirPath().getExpression().startsWith(p) &&
                current.getFhirPath().getExpression().startsWith(p))
            .findFirst();

        if (commonPrefix.isPresent() &&
            resultNodeIds.containsKey(commonPrefix.get()) &&
            currentNodeIds.containsKey(commonPrefix.get())) {
          // If there is a common prefix, we add the corresponding node identifier column to the
          // join condition.
          final Column previousNodeId = resultNodeIds
              .get(commonPrefix.get());
          final List<Column> previousJoinColumns = Arrays.asList(result.getFhirPath().getIdColumn(),
              previousNodeId);
          final Column currentNodeId = currentNodeIds
              .get(commonPrefix.get());
          final List<Column> currentJoinColumns = Arrays.asList(current.getFhirPath().getIdColumn(),
              currentNodeId);
          final Dataset<Row> dataset = join(result.getResult(), previousJoinColumns,
              current.getFhirPath().getDataset(),
              currentJoinColumns, JoinType.LEFT_OUTER);
          result = new FhirPathContextAndResult(current.getFhirPath(), current.getContext(),
              dataset);
        } else {
          // If there is no common prefix, we join using only the resource ID.
          final Dataset<Row> dataset = join(result.getResult(), result.getFhirPath().getIdColumn(),
              current.getFhirPath().getDataset(), current.getFhirPath().getIdColumn(),
              JoinType.LEFT_OUTER);
          result = new FhirPathContextAndResult(current.getFhirPath(), current.getContext(),
              dataset);
        }
      } else {
        result = new FhirPathContextAndResult(current.getFhirPath(), current.getContext(),
            current.getFhirPath().getDataset());
      }
    }
    return result;
  }

  @Nonnull
  private Dataset<Row> trimTrailingNulls(final @Nonnull Column idColumn,
      @Nonnull final List<FhirPath> expressions, @Nonnull final Dataset<Row> dataset) {
    checkArgument(!expressions.isEmpty(), "At least one expression is required");

    final Column[] nonSingularColumns = expressions.stream()
        .filter(fhirPath -> !fhirPath.isSingular())
        .map(FhirPath::getValueColumn)
        .toArray(Column[]::new);

    if (nonSingularColumns.length == 0) {
      return dataset;
    } else {
      final Column additionalCondition = Arrays.stream(nonSingularColumns)
          .map(Column::isNotNull)
          .reduce(Column::or)
          .get();
      final List<Column> filteringColumns = new ArrayList<>();
      filteringColumns.add(idColumn);
      final List<Column> singularColumns = expressions.stream()
          .filter(FhirPath::isSingular)
          .map(FhirPath::getValueColumn)
          .collect(Collectors.toList());
      filteringColumns.addAll(singularColumns);
      final Dataset<Row> filteringDataset = dataset
          .select(filteringColumns.toArray(new Column[0]))
          .distinct();
      return join(dataset, filteringColumns, filteringDataset, filteringColumns,
          additionalCondition, JoinType.RIGHT_OUTER);
    }
  }

  @Value
  private static class FhirPathContextAndResult {

    @Nonnull
    FhirPath fhirPath;

    @Nonnull
    ParserContext context;

    @Nonnull
    Dataset<Row> result;
  }
}
