/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.ResultWriter;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
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
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
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

  @Nonnull
  private final ResultRegistry resultRegistry;

  /**
   * @param configuration a {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext a {@link FhirContext} for doing FHIR stuff
   * @param sparkSession a {@link SparkSession} for resolving Spark queries
   * @param database a {@link Database} for retrieving resources
   * @param terminologyClientFactory a {@link TerminologyServiceFactory} for resolving terminology
   * @param resultWriter a {@link ResultWriter} for writing results for later retrieval
   * @param resultRegistry a {@link ResultRegistry} for storing the mapping between request ID and
   * result URL
   */
  public ExtractExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final Database database,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyClientFactory,
      @Nonnull final ResultWriter resultWriter,
      @Nonnull final ResultRegistry resultRegistry) {
    super(configuration, fhirContext, sparkSession, database,
        terminologyClientFactory);
    this.resultWriter = resultWriter;
    this.resultRegistry = resultRegistry;
  }

  /**
   * Executes an extract request.
   *
   * @param query an {@link ExtractRequest}
   * @param serverBase the base URL of this server, used to construct result URLs
   * @return an {@link ExtractResponse}
   */
  @Nonnull
  public ExtractResponse execute(@Nonnull final ExtractRequest query,
      @Nonnull final String serverBase) {
    log.info("Executing request: {}", query);
    final String requestId = query.getRequestId();
    final Dataset<Row> result = buildQuery(query);

    // Write the result and get the URL.
    final String resultUrl = resultWriter.write(result, requestId);

    // Get the current user, if authenticated, and store alongside the result for later 
    // authorization.
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    final Optional<String> currentUserId = getCurrentUserId(authentication);

    // Store a mapping between the request ID and the result URL, for later retrieval via the result
    // operation.
    resultRegistry.put(requestId, new Result(resultUrl, currentUserId));

    return new ExtractResponse(serverBase + "/$result?id=" + requestId);
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
    // Build new input context
    final ResourcePath inputContext = ResourcePath
        .build(getFhirContext(), getDatabase(), query.getSubjectResource(),
            query.getSubjectResource().toCode(), true);

    // Parse columns and filters together
    final List<FhirPath> columns = new ArrayList<>(query.getColumns().size());
    final Dataset<Row> filteredDataset = parseFilteredValueColumns(inputContext, query.getColumns(),
        "Column", query.getFilters(), columns, null);

    // Select the column values.
    final Column idColumn = inputContext.getIdColumn();
    final Column[] columnValues = columns.stream()
        .map(path -> ((Materializable<?>) path).getExtractableColumn())
        .toArray(Column[]::new);
    final Dataset<Row> selectedDataset = filteredDataset.select(columnValues)
        .filter(idColumn.isNotNull());

    // If there is a limit, apply it.
    return query.getLimit().isPresent()
           ? selectedDataset.limit(query.getLimit().get())
           : selectedDataset;
  }

}
