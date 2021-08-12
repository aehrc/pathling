/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Contains functionality common to query executors.
 *
 * @author John Grimes
 */
@Getter
public abstract class QueryExecutor {

  @Nonnull
  private final Configuration configuration;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final ResourceReader resourceReader;

  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyClientFactory;

  protected QueryExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyClientFactory) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.resourceReader = resourceReader;
    this.terminologyClientFactory = terminologyClientFactory;
  }

  /**
   * Joins the datasets in a list together the provided set of shared columns.
   *
   * @param expressions a list of expressions to join
   * @param joinColumns the columns to join by; all columns must be present in all expressions.
   * @return the joined {@link Dataset}
   */
  @Nonnull
  protected static Dataset<Row> joinExpressionsByColumns(
      @Nonnull final List<FhirPath> expressions,
      @Nonnull final List<Column> joinColumns) {
    checkArgument(!expressions.isEmpty(), "expressions must not be empty");

    final Optional<Dataset<Row>> maybeJoinResult = expressions.stream()
        .map(FhirPath::getDataset)
        .reduce((l, r) -> join(l, joinColumns, r, joinColumns, JoinType.LEFT_OUTER));
    return maybeJoinResult.orElseThrow();
  }

  @Nonnull
  protected static Dataset<Row> applyFilters(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Collection<FhirPath> filters) {
    // Get the value column from each filter expression, and combine them with AND logic.
    return filters.stream()
        .map(FhirPath::getValueColumn)
        .reduce(Column::and)
        .flatMap(filter -> Optional.of(dataset.filter(filter)))
        .orElse(dataset);
  }

  protected ParserContext buildParserContext(@Nonnull final FhirPath inputContext) {
    return buildParserContext(inputContext, Optional.empty());
  }

  protected ParserContext buildParserContext(@Nonnull final FhirPath inputContext,
      @Nonnull final Optional<List<Column>> groupingColumns) {
    return new ParserContext(inputContext, fhirContext, sparkSession, resourceReader,
        terminologyClientFactory, groupingColumns);
  }

}
