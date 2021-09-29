/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.builders;

import static org.apache.spark.sql.functions.lit;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.DefaultAnswer;
import ca.uhn.fhir.context.FhirContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.mockito.Mockito;

/**
 * @author John Grimes
 */
public class ParserContextBuilder {

  @Nonnull
  private FhirPath inputContext;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private ResourceReader resourceReader;

  @Nullable
  private TerminologyServiceFactory terminologyServiceFactory;

  @Nonnull
  private Optional<List<Column>> groupingColumns;

  @Nonnull
  private final Map<String, Column> nodeIdColumns;

  public ParserContextBuilder(@Nonnull final SparkSession spark,
      @Nonnull final FhirContext fhirContext) {
    this.fhirContext = fhirContext;
    this.spark = spark;
    inputContext = mock(FhirPath.class);
    when(inputContext.getIdColumn()).thenReturn(lit(null));
    when(inputContext.getDataset()).thenReturn(spark.emptyDataFrame());
    resourceReader = Mockito.mock(ResourceReader.class, new DefaultAnswer());
    groupingColumns = Optional.empty();
    nodeIdColumns = new HashMap<>();
  }

  @Nonnull
  public ParserContextBuilder inputContext(@Nonnull final FhirPath inputContext) {
    this.inputContext = inputContext;
    return this;
  }

  @Nonnull
  public ParserContextBuilder inputExpression(@Nonnull final String inputExpression) {
    when(inputContext.getExpression()).thenReturn(inputExpression);
    return this;
  }

  @Nonnull
  public ParserContextBuilder idColumn(@Nonnull final Column idColumn) {
    when(inputContext.getIdColumn()).thenReturn(idColumn);
    return this;
  }

  @Nonnull
  public ParserContextBuilder resourceReader(@Nonnull final ResourceReader resourceReader) {
    this.resourceReader = resourceReader;
    return this;
  }

  @Nonnull
  public ParserContextBuilder terminologyClientFactory(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
    return this;
  }

  @Nonnull
  public ParserContextBuilder groupingColumns(@Nonnull final List<Column> groupingColumns) {
    this.groupingColumns = Optional.of(groupingColumns);
    return this;
  }

  @Nonnull
  public ParserContext build() {
    return new ParserContext(inputContext, fhirContext, spark, resourceReader,
        Optional.ofNullable(terminologyServiceFactory), groupingColumns, nodeIdColumns);
  }

}
