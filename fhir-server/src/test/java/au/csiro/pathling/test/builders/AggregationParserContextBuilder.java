/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.builders;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.AggregationParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.DefaultAnswer;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.SparkHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.mockito.Mockito;

/**
 * @author John Grimes
 */
@SuppressWarnings("unused")
public class AggregationParserContextBuilder {

  @Nonnull
  private FhirPath inputContext;

  @Nonnull
  private Optional<FhirPath> thisContext;

  @Nonnull
  private FhirContext fhirContext;

  @Nonnull
  private SparkSession sparkSession;

  @Nonnull
  private ResourceReader resourceReader;

  @Nonnull
  private Optional<TerminologyClient> terminologyClient;

  @Nonnull
  private Optional<TerminologyClientFactory> terminologyClientFactory;

  @Nonnull
  private List<Column> groupingColumns;

  public AggregationParserContextBuilder() {
    inputContext = mock(FhirPath.class);
    when(inputContext.getIdColumn()).thenReturn(Optional.of(mock(Column.class)));
    thisContext = Optional.empty();
    fhirContext = FhirHelpers.getFhirContext();
    sparkSession = SparkHelpers.getSparkSession();
    resourceReader = Mockito.mock(ResourceReader.class, new DefaultAnswer());
    terminologyClient = Optional.of(mock(TerminologyClient.class, new DefaultAnswer()));
    terminologyClientFactory = Optional
        .of(mock(TerminologyClientFactory.class, new DefaultAnswer()));
    groupingColumns = new ArrayList<>();
  }

  @Nonnull
  public AggregationParserContextBuilder inputContext(@Nonnull final FhirPath inputContext) {
    this.inputContext = inputContext;
    return this;
  }

  @Nonnull
  public AggregationParserContextBuilder inputExpression(@Nonnull final String inputExpression) {
    when(inputContext.getExpression()).thenReturn(inputExpression);
    return this;
  }

  @Nonnull
  public AggregationParserContextBuilder idColumn(@Nonnull final Column idColumn) {
    when(inputContext.getIdColumn()).thenReturn(Optional.of(idColumn));
    return this;
  }

  @Nonnull
  public AggregationParserContextBuilder thisContext(@Nonnull final FhirPath thisPath) {
    this.thisContext = Optional.of(thisPath);
    return this;
  }

  @Nonnull
  public AggregationParserContextBuilder fhirContext(@Nonnull final FhirContext fhirContext) {
    this.fhirContext = fhirContext;
    return this;
  }

  @Nonnull
  public AggregationParserContextBuilder sparkSession(@Nonnull final SparkSession sparkSession) {
    this.sparkSession = sparkSession;
    return this;
  }

  @Nonnull
  public AggregationParserContextBuilder resourceReader(
      @Nonnull final ResourceReader resourceReader) {
    this.resourceReader = resourceReader;
    return this;
  }

  @Nonnull
  public AggregationParserContextBuilder terminologyClient(
      @Nonnull final TerminologyClient terminologyClient) {
    this.terminologyClient = Optional.of(terminologyClient);
    return this;
  }

  @Nonnull
  public AggregationParserContextBuilder terminologyClientFactory(
      @Nonnull final TerminologyClientFactory terminologyClientFactory) {
    this.terminologyClientFactory = Optional.of(terminologyClientFactory);
    return this;
  }

  @Nonnull
  public AggregationParserContextBuilder groupingColumns(
      @Nonnull final List<Column> groupingColumns) {
    this.groupingColumns = groupingColumns;
    return this;
  }

  @Nonnull
  public AggregationParserContext build() {
    return new AggregationParserContext(inputContext, thisContext, fhirContext, sparkSession,
        resourceReader, terminologyClient, terminologyClientFactory, groupingColumns);
  }
}
