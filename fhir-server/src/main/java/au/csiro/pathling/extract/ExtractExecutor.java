/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResultWriter;
import ca.uhn.fhir.context.FhirContext;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
@Profile("core")
@Getter
@Slf4j
public class ExtractExecutor extends QueryExecutor {

  @Nonnull
  private final ResultWriter resultWriter;

  /**
   * @param configuration A {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param resourceReader A {@link ResourceReader} for retrieving resources
   * @param terminologyClientFactory A {@link TerminologyServiceFactory} for resolving terminology
   * @param resultWriter A {@link ResultWriter} for writing results for later retrieval
   */
  public ExtractExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyClientFactory,
      @Nonnull final ResultWriter resultWriter) {
    super(configuration, fhirContext, sparkSession, resourceReader,
        terminologyClientFactory);
    this.resultWriter = resultWriter;
  }

  /**
   * Executes an extract request.
   *
   * @param query an {@link ExtractRequest}
   * @return an {@link ExtractResponse}
   */
  @Nonnull
  public ExtractResponse execute(@Nonnull final ExtractRequest query) {
    log.info("Executing request: {}", query);
    final Dataset<Row> result = buildQuery(query);

    // Write the result and get the URL.
    final String resultUrl = resultWriter.write(result, query.getRequestId());

    return new ExtractResponse(resultUrl);
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
    // Build a new expression parser, and parse all of the column expressions within the query.
    final ResourcePath inputContext = ResourcePath
        .build(getFhirContext(), getResourceReader(), query.getSubjectResource(),
            query.getSubjectResource().toCode(), true);
    final ParserContext parserContext = buildParserContext(inputContext);
    final List<FhirPathAndContext> columnParseResult =
        parseMaterializableExpressions(parserContext, query.getColumns(), "Column");
    final List<FhirPath> columns = columnParseResult.stream()
        .map(FhirPathAndContext::getFhirPath)
        .collect(Collectors.toList());

    // Parse the filter expressions.
    // final Parser filterParser = new Parser(parserContext);
    // final List<FhirPath> filters = parseFilters(filterParser, query.getFilters());

    // Join all filter and column expressions together.
    final FhirPathContextAndResult columnJoinResult = joinColumns(columnParseResult);

    // Apply filters.
    // final Dataset<Row> dataset = applyFilters(columnJoinResult.getResult(), filters);
    final Dataset<Row> dataset = columnJoinResult.getResult();

    // Select the column values.
    final Column[] columnValues = columns.stream()
        .map(path -> ((Materializable) path).getExtractableColumn())
        .toArray(Column[]::new);
    return dataset.select(columnValues);
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
        prefixes.addAll(result.getContext().getNodeIdColumns().keySet());
        prefixes.addAll(current.getContext().getNodeIdColumns().keySet());
        final List<String> sortedCommonPrefixes = new ArrayList<>(prefixes).stream()
            .sorted(Comparator.comparingInt(String::length).reversed())
            .collect(Collectors.toList());
        final FhirPathContextAndResult finalResult = result;

        // Find the longest prefix that is common to the two expressions.
        final Optional<String> commonPrefix = sortedCommonPrefixes.stream()
            .filter(p -> finalResult.getFhirPath().getExpression().startsWith(p) &&
                current.getFhirPath().getExpression().startsWith(p))
            .findFirst();

        if (commonPrefix.isPresent()) {
          // If there is a common prefix, we add the corresponding node identifier column to the
          // join condition.
          final Column previousNodeId = result.getContext().getNodeIdColumns()
              .get(commonPrefix.get());
          final List<Column> previousJoinColumns = Arrays.asList(result.getFhirPath().getIdColumn(),
              previousNodeId);
          final Column currentNodeId = current.getContext().getNodeIdColumns()
              .get(commonPrefix.get());
          final List<Column> currentJoinColumns = Arrays.asList(current.getFhirPath().getIdColumn(),
              currentNodeId);
          final Dataset<Row> dataset = join(result.getResult(), previousJoinColumns,
              current.getFhirPath().getDataset(), currentJoinColumns, JoinType.LEFT_OUTER);
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
