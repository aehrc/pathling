/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;

/**
 * A specialisation of ParserContext that is used when the FHIRPath is being evaluated in the
 * context of a grouping of resources, for the purpose of aggregation.
 *
 * @author John Grimes
 */
@Getter
public class AggregationParserContext extends ParserContext {

  @Nonnull
  private List<Column> groupingColumns;

  /**
   * @param inputContext The input context from which the FHIRPath is to be evaluated
   * @param thisContext The item from an input collection currently under evaluation
   * @param fhirContext A {@link FhirContext} that can be used to do FHIR stuff
   * @param sparkSession A {@link SparkSession} that can be used to resolve Spark queries required
   * for this expression
   * @param resourceReader For retrieving data relating to resource references
   * @param terminologyClient The {@link TerminologyClient} that should be used to resolve
   * terminology queries
   * @param terminologyClientFactory A factory for {@link TerminologyClient} objects, used for
   * parallel processing
   * @param groupingColumns The set of {@link Column} objects that should be used for grouping, when
   * performing aggregations
   */
  public AggregationParserContext(@Nonnull final FhirPath inputContext,
      @Nonnull final Optional<FhirPath> thisContext, @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyClient> terminologyClient,
      @Nonnull final Optional<TerminologyClientFactory> terminologyClientFactory,
      @Nonnull final List<Column> groupingColumns) {
    super(inputContext, thisContext, fhirContext, sparkSession, resourceReader, terminologyClient,
        terminologyClientFactory);
    this.groupingColumns = groupingColumns;
  }

  @Nonnull
  @Override
  public Optional<Column[]> getGroupBy() {
    return Optional.of(groupingColumns.toArray(new Column[]{}));
  }

  @Override
  public void setGroupingColumns(@Nonnull final List<Column> columns) {
    groupingColumns = columns;
  }

}
