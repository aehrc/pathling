/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import static org.mockito.Mockito.mock;

import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ThisPath;
import au.csiro.pathling.fhirpath.parser.AggregationParserContext;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.SparkHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
public class TestAggregationParserContext extends AggregationParserContext {

  public TestAggregationParserContext(@Nonnull final FhirPath inputContext,
      @Nonnull final Optional<ThisPath> thisContext, @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyClient> terminologyClient,
      @Nonnull final Optional<TerminologyClientFactory> terminologyClientFactory,
      @Nonnull final List<Column> groupingColumns) {
    super(inputContext, thisContext, fhirContext, sparkSession, resourceReader, terminologyClient,
        terminologyClientFactory, groupingColumns);
  }

  @Nonnull
  public static ParserContext build(@Nonnull final List<Column> groupingColumns) {
    final FhirPath inputContext = mock(FhirPath.class);
    final ThisPath thisContext = mock(ThisPath.class);
    final FhirContext fhirContext = FhirHelpers.getFhirContext();
    final SparkSession sparkSession = SparkHelpers.getSparkSession();
    final ResourceReader resourceReader = mock(ResourceReader.class, new DefaultAnswer());
    final TerminologyClient terminologyClient = mock(TerminologyClient.class, new DefaultAnswer());
    final TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class,
        new DefaultAnswer());

    return new AggregationParserContext(inputContext, Optional.of(thisContext), fhirContext,
        sparkSession, resourceReader, Optional.of(terminologyClient),
        Optional.of(terminologyClientFactory), groupingColumns);
  }

}
