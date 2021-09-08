/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResultWriter;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
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
    final ParserContext groupingAndFilterContext = buildParserContext(inputContext);
    final Parser parser = new Parser(groupingAndFilterContext);
    final List<FhirPath> columns = parseMaterializableExpressions(parser,
        query.getColumns(), "Column");
    final List<FhirPath> filters = parseFilters(parser, query.getFilters());

    // Join all filter and column expressions together.
    final Column idColumn = inputContext.getIdColumn();
    Dataset<Row> columnsAndFilters = joinExpressionsAndFilters(inputContext, columns,
        filters,
        idColumn);

    // Apply filters.
    columnsAndFilters = applyFilters(columnsAndFilters, filters);

    // Select the column values.
    final Column[] columnValues = columns.stream()
        .map(path -> ((Materializable) path).getExtractableColumn())
        .toArray(Column[]::new);
    return columnsAndFilters.select(columnValues);
  }

}
